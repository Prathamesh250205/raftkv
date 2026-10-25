package kv

import (
	"errors"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/anishathalye/porcupine"

	"raftkv/raft"
)

// testCluster runs a replicated store over an in-memory network whose nodes
// can be partitioned, crashed and restarted.
type testCluster struct {
	t          *testing.T
	net        *raft.MemNetwork
	ids        []int
	mu         sync.Mutex
	stores     map[int]*Store
	rafts      map[int]*raft.Raft
	persisters map[int]*raft.MemPersister
}

func newTestCluster(t *testing.T, n int) *testCluster {
	c := &testCluster{
		t:          t,
		net:        raft.NewMemNetwork(),
		stores:     map[int]*Store{},
		rafts:      map[int]*raft.Raft{},
		persisters: map[int]*raft.MemPersister{},
	}
	for id := 0; id < n; id++ {
		c.ids = append(c.ids, id)
		c.persisters[id] = &raft.MemPersister{}
	}
	for _, id := range c.ids {
		c.start(id)
	}
	t.Cleanup(func() {
		for _, id := range c.ids {
			c.crash(id)
		}
	})
	return c
}

func (c *testCluster) start(id int) {
	peers := map[int]string{}
	for _, p := range c.ids {
		peers[p] = fmt.Sprint(p)
	}
	applyCh := make(chan raft.ApplyMsg, 64)
	r, err := raft.New(raft.Config{
		ID: id, Peers: peers, Transport: c.net.Transport(id),
		Persister: c.persisters[id], ApplyCh: applyCh,
	})
	if err != nil {
		c.t.Fatal(err)
	}
	s := NewStore(r, applyCh, 25) // snapshot often so compaction is exercised too
	c.mu.Lock()
	c.rafts[id], c.stores[id] = r, s
	c.mu.Unlock()
	c.net.Add(id, r)
}

func (c *testCluster) crash(id int) {
	c.net.Remove(id)
	c.mu.Lock()
	r := c.rafts[id]
	delete(c.rafts, id)
	delete(c.stores, id)
	c.mu.Unlock()
	if r != nil {
		r.Stop()
		c.mu.Lock()
		c.persisters[id] = c.persisters[id].Copy() // what survived on "disk"
		c.mu.Unlock()
	}
}

func (c *testCluster) store(id int) *Store {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.stores[id]
}

// client issues one operation at a time, retrying across nodes until it
// succeeds, the way a real client library would.
type client struct {
	c      *testCluster
	id     string
	seq    uint64
	leader int
}

func (cl *client) do(op Op, key, value string) Result {
	cmd := Command{Op: op, Key: key, Value: value}
	if op != OpGet {
		cl.seq++
		cmd.ClientID, cmd.Seq = cl.id, cl.seq // same (id, seq) on every retry
	}
	for {
		s := cl.c.store(cl.leader)
		if s == nil {
			cl.leader = (cl.leader + 1) % len(cl.c.ids)
			continue
		}
		var res Result
		var err error
		if op == OpGet {
			res, err = s.Get(key)
		} else {
			res, err = s.Do(cmd)
		}
		if err == nil {
			return res
		}
		if errors.Is(err, ErrNotLeader) {
			cl.leader = (cl.leader + 1) % len(cl.c.ids)
			time.Sleep(5 * time.Millisecond)
		}
	}
}

type opInput struct {
	Op    Op
	Key   string
	Value string
}

// kvModel is the sequential specification: a map of strings, partitioned by
// key so each key's history is checked independently.
var kvModel = porcupine.Model{
	Partition: func(history []porcupine.Operation) [][]porcupine.Operation {
		byKey := map[string][]porcupine.Operation{}
		for _, op := range history {
			k := op.Input.(opInput).Key
			byKey[k] = append(byKey[k], op)
		}
		var parts [][]porcupine.Operation
		for _, ops := range byKey {
			parts = append(parts, ops)
		}
		return parts
	},
	Init: func() interface{} { return "" },
	Step: func(state, input, output interface{}) (bool, interface{}) {
		in, cur := input.(opInput), state.(string)
		switch in.Op {
		case OpGet:
			return output.(string) == cur, cur
		case OpPut:
			return true, in.Value
		case OpAppend:
			return true, cur + in.Value
		}
		return false, cur
	},
	DescribeOperation: func(input, output interface{}) string {
		in := input.(opInput)
		if in.Op == OpGet {
			return fmt.Sprintf("get(%s) -> %q", in.Key, output)
		}
		return fmt.Sprintf("%s(%s, %q)", in.Op, in.Key, in.Value)
	},
}

func TestLinearizableUnderChaos(t *testing.T) {
	if testing.Short() {
		t.Skip("chaos test takes ~15s")
	}
	const (
		nodes    = 5
		clients  = 6
		duration = 8 * time.Second
	)
	c := newTestCluster(t, nodes)
	c.net.SetFaults(3*time.Millisecond, 0.02)

	start := time.Now()
	var (
		hmu     sync.Mutex
		history []porcupine.Operation
		stop    atomic.Bool
		wg      sync.WaitGroup
	)
	// Porcupine checks each key's history separately; enough keys keeps every
	// part small, since the cost grows exponentially with concurrent overlap.
	keys := []string{"a", "b", "c", "d", "e", "f", "g", "h"}
	for ci := 0; ci < clients; ci++ {
		wg.Add(1)
		go func(ci int) {
			defer wg.Done()
			cl := &client{c: c, id: fmt.Sprintf("client-%d", ci), leader: ci % nodes}
			for n := 0; !stop.Load(); n++ {
				in := opInput{Key: keys[rand.Intn(len(keys))], Value: fmt.Sprintf("<%d.%d>", ci, n)}
				switch rand.Intn(10) {
				case 0, 1, 2, 3:
					in.Op = OpGet
				case 4, 5:
					in.Op = OpPut
				default:
					in.Op = OpAppend
				}
				call := time.Since(start).Nanoseconds()
				res := cl.do(in.Op, in.Key, in.Value)
				ret := time.Since(start).Nanoseconds()
				hmu.Lock()
				history = append(history, porcupine.Operation{
					ClientId: ci, Input: in, Call: call, Output: res.Value, Return: ret,
				})
				hmu.Unlock()
			}
		}(ci)
	}

	// The nemesis: every few hundred ms, partition, heal, or crash and restart a node.
	actions := map[string]int{}
	down := map[int]bool{}
	for deadline := time.Now().Add(duration); time.Now().Before(deadline); {
		time.Sleep(time.Duration(200+rand.Intn(400)) * time.Millisecond)
		switch rand.Intn(4) {
		case 0: // random split into two sides
			perm := rand.Perm(nodes)
			cut := 1 + rand.Intn(nodes-1)
			c.net.Partition(perm[:cut], perm[cut:])
			actions["partition"]++
		case 1:
			c.net.Heal()
			actions["heal"]++
		case 2: // crash a node, keeping a majority alive
			if len(down) < (nodes-1)/2 {
				id := rand.Intn(nodes)
				if !down[id] {
					c.crash(id)
					down[id] = true
					actions["crash"]++
				}
			}
		case 3:
			for id := range down {
				c.start(id)
				delete(down, id)
				actions["restart"]++
				break
			}
		}
	}
	c.net.Heal()
	for id := range down {
		c.start(id)
	}
	stop.Store(true)
	wg.Wait()

	t.Logf("%d operations, nemesis actions: %v", len(history), actions)
	if len(history) < 100 {
		t.Fatalf("only %d operations completed; the cluster made too little progress", len(history))
	}

	res, info := porcupine.CheckOperationsVerbose(kvModel, history, 120*time.Second)
	switch res {
	case porcupine.Illegal:
		path := filepath.Join(os.TempDir(), "raftkv-linearizability.html")
		if err := porcupine.VisualizePath(kvModel, info, path); err == nil {
			t.Logf("visualization of the violation: %s", path)
		}
		t.Fatal("history is not linearizable")
	case porcupine.Unknown:
		t.Fatal("linearizability check timed out")
	}
}

func TestSessionsDeduplicateRetries(t *testing.T) {
	c := newTestCluster(t, 3)
	cl := &client{c: c, id: "dup"}
	cl.do(OpPut, "k", "x")

	// Re-submit the exact same append several times, as a client would after
	// timeouts: it must apply once.
	cl.seq++
	cmd := Command{Op: OpAppend, Key: "k", Value: "y", ClientID: cl.id, Seq: cl.seq}
	for i := 0; i < 5; i++ {
		for {
			if s := c.store(cl.leader); s != nil {
				if _, err := s.Do(cmd); err == nil {
					break
				}
			}
			cl.leader = (cl.leader + 1) % 3
		}
	}
	if got := cl.do(OpGet, "k", "").Value; got != "xy" {
		t.Fatalf("value after duplicate appends = %q, want %q", got, "xy")
	}
}

func TestReadsSeeCompletedWrites(t *testing.T) {
	c := newTestCluster(t, 3)
	cl := &client{c: c, id: "rw"}
	var want []string
	for i := 0; i < 50; i++ {
		v := fmt.Sprint(i)
		cl.do(OpPut, v, v)
		want = append(want, v)
	}
	sort.Strings(want)
	for _, k := range want {
		if got := cl.do(OpGet, k, "").Value; got != k {
			t.Fatalf("get(%s) = %q after it was written", k, got)
		}
	}
}
