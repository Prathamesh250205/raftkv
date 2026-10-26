// Command kvdemo runs a complete 3-node, 3-shard raftkv cluster in one process
// behind a live web dashboard, for public demos such as a Hugging Face Space.
// Visitors can read and write keys, and crash a node to watch Raft elect a new
// leader and the node recover from disk.
//
//	kvdemo                 # listens on $PORT, or :7860
//	kvdemo -addr :8080
//
// The cluster is a sandbox: data lives in a temporary directory, only one node
// may be down at a time, crashes are rate-limited and values are kept small.
package main

import (
	_ "embed"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"raftkv/server"
)

//go:embed index.html
var page []byte

const (
	nodeCount      = 3
	shardCount     = 3
	restartAfter   = 6 * time.Second
	crashCooldown  = 8 * time.Second
	maxKeyBytes    = 64
	maxValueBytes  = 256
	maxEventsShown = 30
)

func main() {
	addr := flag.String("addr", "", "address to serve the dashboard on (default :$PORT or :7860)")
	flag.Parse()
	if *addr == "" {
		*addr = ":" + envOr("PORT", "7860")
	}
	dir, err := os.MkdirTemp("", "kvdemo-")
	if err != nil {
		log.Fatal(err)
	}
	d, err := newDemo(dir)
	if err != nil {
		log.Fatal(err)
	}
	defer d.close()
	log.Printf("kvdemo: 3-node, 3-shard cluster up; dashboard on %s", *addr)
	log.Fatal(http.ListenAndServe(*addr, d.handler()))
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

type event struct {
	At   time.Time `json:"at"`
	Text string    `json:"text"`
}

// demo owns the three in-process nodes and the event log shown on the page.
type demo struct {
	dir   string
	addrs map[int]string

	mu        sync.Mutex
	nodes     map[int]*server.Node // nil while crashed
	events    []event
	lastCrash time.Time
	leaders   [shardCount]int       // last leader seen per shard, for election events
	lostAt    [shardCount]time.Time // when each shard last had no live leader
}

func newDemo(dir string) (*demo, error) {
	d := &demo{dir: dir, addrs: map[int]string{}, nodes: map[int]*server.Node{}}
	listeners := map[int]net.Listener{}
	for id := 1; id <= nodeCount; id++ {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			return nil, err
		}
		listeners[id], d.addrs[id] = ln, ln.Addr().String()
	}
	for id := 1; id <= nodeCount; id++ {
		if err := d.start(id, listeners[id]); err != nil {
			return nil, err
		}
	}
	d.logf("cluster started: %d nodes, %d shards, each shard replicated on every node", nodeCount, shardCount)
	go d.watchLeaders()
	return d, nil
}

func (d *demo) start(id int, ln net.Listener) error {
	if ln == nil {
		var err error
		if ln, err = net.Listen("tcp", d.addrs[id]); err != nil {
			return err
		}
	}
	n, err := server.Start(server.Config{
		ID:            id,
		Addr:          d.addrs[id],
		Cluster:       d.addrs,
		Shards:        shardCount,
		DataDir:       filepath.Join(d.dir, fmt.Sprintf("node%d", id)),
		SnapshotEvery: 200,
		Forward:       true,
		Listener:      ln,
	})
	if err != nil {
		return err
	}
	d.mu.Lock()
	d.nodes[id] = n
	d.mu.Unlock()
	return nil
}

func (d *demo) close() {
	d.mu.Lock()
	defer d.mu.Unlock()
	for _, n := range d.nodes {
		if n != nil {
			n.Close()
		}
	}
}

func (d *demo) logf(format string, args ...any) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.events = append(d.events, event{time.Now(), fmt.Sprintf(format, args...)})
	if len(d.events) > maxEventsShown {
		d.events = d.events[len(d.events)-maxEventsShown:]
	}
}

// crash stops a node abruptly and restarts it from its data directory a few
// seconds later. Only one node may be down at a time, so a majority (2 of 3)
// always survives and the cluster stays available.
func (d *demo) crash(id int) error {
	d.mu.Lock()
	n, ok := d.nodes[id]
	switch {
	case !ok && (id < 1 || id > nodeCount):
		d.mu.Unlock()
		return errors.New("no such node")
	case n == nil:
		d.mu.Unlock()
		return errors.New("that node is already down")
	case d.downCountLocked() > 0:
		d.mu.Unlock()
		return errors.New("one node is already down; crashing a second would lose the majority")
	case time.Since(d.lastCrash) < crashCooldown:
		d.mu.Unlock()
		return errors.New("give the cluster a few seconds before the next crash")
	}
	d.nodes[id] = nil
	d.lastCrash = time.Now()
	d.mu.Unlock()

	n.Close()
	d.logf("node %d crashed (process stopped without warning)", id)
	go func() {
		time.Sleep(restartAfter)
		if err := d.start(id, nil); err != nil {
			d.logf("node %d failed to restart: %v", id, err)
			return
		}
		d.logf("node %d restarted, reloaded its log from disk and is catching up", id)
	}()
	return nil
}

func (d *demo) downCountLocked() int {
	down := 0
	for _, n := range d.nodes {
		if n == nil {
			down++
		}
	}
	return down
}

// watchLeaders turns leadership changes into events. A shard that lost its
// leader (its leader's node crashed) is timed until a new one is elected; a
// change without a gap is the balancer handing leadership back.
func (d *demo) watchLeaders() {
	for range time.Tick(50 * time.Millisecond) {
		state := d.state()
		for g := 0; g < shardCount; g++ {
			leader, term := 0, 0
			up := map[int]bool{}
			for _, n := range state.Nodes {
				up[n.ID] = n.Up
				if n.Up && n.Shards[g].State == "leader" {
					leader, term = n.ID, n.Shards[g].Term
				}
			}
			d.mu.Lock()
			prev, lost := d.leaders[g], d.lostAt[g]
			// Only a leader whose node went down counts as lost; the brief gap
			// during a leadership handoff does not.
			if leader == 0 && prev != 0 && !up[prev] && lost.IsZero() {
				d.lostAt[g] = time.Now()
			}
			if leader != 0 && leader != prev {
				d.leaders[g], d.lostAt[g] = leader, time.Time{}
			}
			d.mu.Unlock()
			if leader == 0 || leader == prev {
				continue
			}
			switch {
			case prev == 0:
				d.logf("shard %d: node %d is leader (term %d)", g, leader, term)
			case !lost.IsZero():
				d.logf("shard %d lost its leader; node %d won the election in term %d after %d ms",
					g, leader, term, time.Since(lost).Milliseconds())
			default:
				d.logf("shard %d: leadership handed to node %d (term %d) to rebalance", g, leader, term)
			}
		}
	}
}

type shardView struct {
	Shard  int    `json:"shard"`
	State  string `json:"state"`
	Term   int    `json:"term"`
	Commit int    `json:"commit_index"`
}

type nodeView struct {
	ID     int         `json:"id"`
	Up     bool        `json:"up"`
	Shards []shardView `json:"shards"`
}

type stateView struct {
	Nodes  []nodeView `json:"nodes"`
	Events []event    `json:"events"`
}

func (d *demo) state() stateView {
	d.mu.Lock()
	nodes := map[int]*server.Node{}
	for id, n := range d.nodes {
		nodes[id] = n
	}
	events := append([]event(nil), d.events...)
	d.mu.Unlock()

	var out stateView
	for id := 1; id <= nodeCount; id++ {
		nv := nodeView{ID: id, Up: nodes[id] != nil}
		for g := 0; g < shardCount; g++ {
			sv := shardView{Shard: g, State: "down"}
			if nv.Up {
				st := nodes[id].Raft(g).Status()
				sv.State, sv.Term, sv.Commit = st.State, st.Term, st.CommitIndex
			}
			nv.Shards = append(nv.Shards, sv)
		}
		out.Nodes = append(out.Nodes, nv)
	}
	out.Events = events
	return out
}

func (d *demo) handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /{$}", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.Write(page)
	})
	mux.HandleFunc("GET /api/state", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(d.state())
	})
	mux.HandleFunc("POST /api/crash/{id}", func(w http.ResponseWriter, req *http.Request) {
		id, _ := strconv.Atoi(req.PathValue("id"))
		if err := d.crash(id); err != nil {
			http.Error(w, err.Error(), http.StatusConflict)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	})
	// Key-value requests and the shard map go to the cluster itself.
	mux.HandleFunc("GET /kv/{key}", d.proxy)
	mux.HandleFunc("PUT /kv/{key}", d.proxy)
	mux.HandleFunc("GET /shards", d.proxy)
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { w.Write([]byte("ok")) })
	return mux
}

// proxy passes a request to any live node; nodes run in forward mode, so it
// reaches the right shard leader from there.
func (d *demo) proxy(w http.ResponseWriter, req *http.Request) {
	if key := req.PathValue("key"); len(key) > maxKeyBytes || strings.ContainsAny(key, "/?#") {
		http.Error(w, fmt.Sprintf("keys are limited to %d characters in this demo", maxKeyBytes), http.StatusBadRequest)
		return
	}
	if req.Method == http.MethodPut {
		req.Body = http.MaxBytesReader(w, req.Body, maxValueBytes)
	}
	d.mu.Lock()
	target := ""
	for id := 1; id <= nodeCount; id++ {
		if d.nodes[id] != nil {
			target = d.addrs[id]
			break
		}
	}
	d.mu.Unlock()
	if target == "" {
		http.Error(w, "no node is up", http.StatusServiceUnavailable)
		return
	}
	u, _ := url.Parse("http://" + target)
	httputil.NewSingleHostReverseProxy(u).ServeHTTP(w, req)
}
