package server

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"raftkv/kv"
)

// testCluster runs real nodes on localhost, talking over real HTTP.
type testCluster struct {
	t      *testing.T
	shards int
	dir    string
	addrs  map[int]string
	nodes  map[int]*Node
	cfgs   map[int]Config
	client *http.Client
	base   Config // security and routing settings every node starts with
}

func newTestCluster(t *testing.T, n, shards, replicas int) *testCluster {
	return newTestClusterWith(t, n, shards, replicas, Config{})
}

func newTestClusterWith(t *testing.T, n, shards, replicas int, base Config) *testCluster {
	c := &testCluster{
		t: t, shards: shards, dir: t.TempDir(),
		addrs: map[int]string{}, nodes: map[int]*Node{}, cfgs: map[int]Config{},
		client: &http.Client{Timeout: 5 * time.Second},
		base:   base,
	}
	listeners := map[int]net.Listener{}
	for id := 1; id <= n; id++ {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		listeners[id], c.addrs[id] = ln, ln.Addr().String()
	}
	for id := 1; id <= n; id++ {
		c.start(id, Config{Cluster: c.addrs, Replicas: replicas, Listener: listeners[id]})
	}
	t.Cleanup(func() {
		for _, nd := range c.nodes {
			nd.Close()
		}
	})
	c.waitForLeaders()
	return c
}

func (c *testCluster) start(id int, cfg Config) {
	cfg.ID, cfg.Shards, cfg.SnapshotEvery = id, c.shards, 50
	cfg.AuthToken, cfg.PublicReads = c.base.AuthToken, c.base.PublicReads
	cfg.ClusterSecret, cfg.Forward = c.base.ClusterSecret, c.base.Forward
	if cfg.Addr == "" {
		cfg.Addr = c.addrs[id]
	}
	cfg.DataDir = filepath.Join(c.dir, fmt.Sprintf("node%d", id))
	if cfg.Listener == nil {
		ln, err := net.Listen("tcp", cfg.Addr)
		if err != nil {
			c.t.Fatal(err)
		}
		cfg.Listener = ln
	}
	nd, err := Start(cfg)
	if err != nil {
		c.t.Fatal(err)
	}
	c.nodes[id] = nd
	cfg.Listener = nil
	c.cfgs[id] = cfg
}

func (c *testCluster) stop(id int) {
	c.nodes[id].Close()
	delete(c.nodes, id)
}

func (c *testCluster) anyAddr(skip int) string {
	ids := make([]int, 0, len(c.nodes))
	for id := range c.nodes {
		ids = append(ids, id)
	}
	sort.Ints(ids)
	return c.addrs[ids[skip%len(ids)]]
}

func (c *testCluster) waitForLeaders() {
	c.t.Helper()
	for deadline := time.Now().Add(10 * time.Second); time.Now().Before(deadline); time.Sleep(50 * time.Millisecond) {
		resp, err := c.client.Get("http://" + c.anyAddr(0) + "/shards")
		if err != nil {
			continue
		}
		var shards []ShardInfo
		json.NewDecoder(resp.Body).Decode(&shards)
		resp.Body.Close()
		ready := len(shards) == c.shards
		for _, s := range shards {
			ready = ready && s.LeaderAddr != ""
		}
		if ready {
			return
		}
	}
	c.t.Fatal("not every shard elected a leader")
}

// do sends a request through some node, following redirects and retrying
// across nodes until it gets a definite answer.
func (c *testCluster) do(method, key, value string) (int, string) {
	c.t.Helper()
	for attempt := 0; attempt < 100; attempt++ {
		url := "http://" + c.anyAddr(attempt) + "/kv/" + key
		req, _ := http.NewRequest(method, url, strings.NewReader(value))
		req.Header.Set(TokenHeader, c.base.AuthToken)
		resp, err := c.client.Do(req)
		if err != nil {
			time.Sleep(50 * time.Millisecond)
			continue
		}
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode == http.StatusServiceUnavailable {
			time.Sleep(50 * time.Millisecond)
			continue
		}
		return resp.StatusCode, string(body)
	}
	c.t.Fatalf("%s %s: no definite answer", method, key)
	return 0, ""
}

func (c *testCluster) put(key, value string) {
	c.t.Helper()
	if code, body := c.do(http.MethodPut, key, value); code != http.StatusNoContent {
		c.t.Fatalf("put %s: %d %s", key, code, body)
	}
}

func (c *testCluster) get(key string) string {
	c.t.Helper()
	code, body := c.do(http.MethodGet, key, "")
	if code != http.StatusOK {
		c.t.Fatalf("get %s: %d %s", key, code, body)
	}
	return body
}

func TestShardedEndToEnd(t *testing.T) {
	c := newTestCluster(t, 3, 4, 0)
	for i := 0; i < 200; i++ {
		c.put(fmt.Sprintf("key-%d", i), fmt.Sprintf("value-%d", i))
	}
	for i := 0; i < 200; i++ {
		if got := c.get(fmt.Sprintf("key-%d", i)); got != fmt.Sprintf("value-%d", i) {
			t.Fatalf("key-%d = %q", i, got)
		}
	}

	// Every replica of every shard holds exactly the keys that hash to it.
	time.Sleep(200 * time.Millisecond) // let followers apply the tail
	for id, nd := range c.nodes {
		total := 0
		for g := 0; g < c.shards; g++ {
			keys := nd.Store(g).Keys()
			for _, k := range keys {
				if kv.ShardFor(k, c.shards) != g {
					t.Fatalf("node %d shard %d holds %q, which belongs to shard %d", id, g, k, kv.ShardFor(k, c.shards))
				}
			}
			total += len(keys)
		}
		if total != 200 {
			t.Fatalf("node %d holds %d keys across shards, want 200", id, total)
		}
	}
}

func TestLeadersSpreadAcrossNodes(t *testing.T) {
	c := newTestCluster(t, 3, 6, 0)
	deadline := time.Now().Add(10 * time.Second)
	for {
		perNode := map[int]int{}
		for id, nd := range c.nodes {
			for g := 0; g < c.shards; g++ {
				if nd.Raft(g).Status().State == "leader" {
					perNode[id]++
				}
			}
		}
		if perNode[1] == 2 && perNode[2] == 2 && perNode[3] == 2 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("leaders per node after balancing: %v, want 2 each", perNode)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func TestShardedFailover(t *testing.T) {
	c := newTestCluster(t, 3, 3, 0)
	for i := 0; i < 30; i++ {
		c.put(fmt.Sprintf("k%d", i), "before")
	}
	// Kill the node leading shard 0; every shard must keep serving.
	victim := -1
	for id, nd := range c.nodes {
		if nd.Raft(0).Status().State == "leader" {
			victim = id
		}
	}
	c.stop(victim)
	c.waitForLeaders()
	for i := 0; i < 30; i++ {
		c.put(fmt.Sprintf("k%d", i), "after")
	}
	for i := 0; i < 30; i++ {
		if got := c.get(fmt.Sprintf("k%d", i)); got != "after" {
			t.Fatalf("k%d = %q after failover", i, got)
		}
	}

	// The node comes back from disk and catches up.
	c.start(victim, c.cfgs[victim])
	time.Sleep(time.Second)
	for g := 0; g < c.shards; g++ {
		if st := c.nodes[victim].Raft(g).Status(); st.LeaderID == -1 || len(st.Members) != 3 {
			t.Fatalf("restarted node, shard %d: %+v", g, st)
		}
	}
}

func TestMoveShardToNewNode(t *testing.T) {
	// 2 replicas per shard: shard 0 starts on nodes 1,2 and shard 1 on 2,3.
	c := newTestCluster(t, 3, 2, 2)
	var shard0Keys []string
	for i := 0; i < 60; i++ {
		k := fmt.Sprintf("m%d", i)
		c.put(k, "v-"+k)
		if kv.ShardFor(k, 2) == 0 {
			shard0Keys = append(shard0Keys, k)
		}
	}

	// A fourth node joins empty, then takes over node 1's replica of shard 0.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	c.addrs[4] = ln.Addr().String()
	c.start(4, Config{Listener: ln}) // no Cluster: join mode

	post := func(method, path string, body []byte) {
		t.Helper()
		for attempt := 0; attempt < 50; attempt++ {
			req, _ := http.NewRequest(method, "http://"+c.addrs[2]+path, bytes.NewReader(body))
			resp, err := c.client.Do(req)
			if err == nil {
				msg, _ := io.ReadAll(resp.Body)
				resp.Body.Close()
				if resp.StatusCode == http.StatusOK {
					return
				}
				if resp.StatusCode != http.StatusServiceUnavailable {
					t.Fatalf("%s %s: %d %s", method, path, resp.StatusCode, msg)
				}
			}
			time.Sleep(100 * time.Millisecond)
		}
		t.Fatalf("%s %s kept failing", method, path)
	}
	post(http.MethodPost, "/shards/0/members", []byte(fmt.Sprintf(`{"id":4,"addr":%q}`, c.addrs[4])))
	post(http.MethodDelete, "/shards/0/members/1", nil)

	for _, k := range shard0Keys {
		if got := c.get(k); got != "v-"+k {
			t.Fatalf("%s = %q after moving shard 0", k, got)
		}
	}
	time.Sleep(300 * time.Millisecond)
	have := map[string]bool{}
	for _, k := range c.nodes[4].Store(0).Keys() {
		have[k] = true
	}
	for _, k := range shard0Keys {
		if !have[k] {
			t.Fatalf("node 4 is missing %s after taking over shard 0", k)
		}
	}
	members := c.nodes[2].Raft(0).Status().Members
	if _, still := members[1]; still || members[4] == "" {
		t.Fatalf("shard 0 members after move: %v", members)
	}

	// Node 4 hosts only shard 0, but must still route requests for shard 1.
	for i := 0; i < 60; i++ {
		k := fmt.Sprintf("m%d", i)
		resp, err := c.client.Get("http://" + c.addrs[4] + "/kv/" + k)
		if err != nil {
			t.Fatal(err)
		}
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK || string(body) != "v-"+k {
			t.Fatalf("get %s via node 4: %d %q", k, resp.StatusCode, body)
		}
	}
}
