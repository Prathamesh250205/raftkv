//go:build js && wasm

// Command kvwasm runs the raftkv demo cluster inside a web page: the real raft
// and kv packages compiled to WebAssembly, with three nodes and three shards
// connected by raft.MemNetwork instead of TCP. The dashboard in
// cmd/kvdemo/index.html drives it through window.raftkv, so the demo can be
// hosted as static files (GitHub Pages) with no server at all.
//
//	GOOS=js GOARCH=wasm go build -o raftkv.wasm ./cmd/kvwasm
package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"syscall/js"
	"time"

	"raftkv/kv"
	"raftkv/raft"
)

const (
	nodeCount      = 3
	shardCount     = 3
	restartAfter   = 6 * time.Second
	crashCooldown  = 8 * time.Second
	maxEventsShown = 30
)

type event struct {
	At   time.Time `json:"at"`
	Text string    `json:"text"`
}

// cluster is nodeCount nodes, each running one Raft group per shard. Every
// shard has its own in-memory network, like each shard's RPC path in a node.
type cluster struct {
	nets       [shardCount]*raft.MemNetwork
	persisters [shardCount]map[int]*raft.MemPersister

	mu        sync.Mutex
	rafts     [shardCount]map[int]*raft.Raft // nil entry while the node is crashed
	stores    [shardCount]map[int]*kv.Store
	up        map[int]bool
	events    []event
	lastCrash time.Time
	leaders   [shardCount]int
	lostAt    [shardCount]time.Time
}

func newCluster() *cluster {
	c := &cluster{up: map[int]bool{}}
	for g := 0; g < shardCount; g++ {
		c.nets[g] = raft.NewMemNetwork()
		c.persisters[g] = map[int]*raft.MemPersister{}
		c.rafts[g] = map[int]*raft.Raft{}
		c.stores[g] = map[int]*kv.Store{}
		for id := 1; id <= nodeCount; id++ {
			c.persisters[g][id] = &raft.MemPersister{}
		}
	}
	for id := 1; id <= nodeCount; id++ {
		c.startNode(id)
	}
	c.logf("cluster started in your browser: %d nodes, %d shards, each shard replicated on every node", nodeCount, shardCount)
	go c.watchLeaders()
	return c
}

func (c *cluster) startNode(id int) {
	peers := map[int]string{}
	for p := 1; p <= nodeCount; p++ {
		peers[p] = fmt.Sprint(p)
	}
	for g := 0; g < shardCount; g++ {
		applyCh := make(chan raft.ApplyMsg, 64)
		r, err := raft.New(raft.Config{
			ID: id, Peers: peers, Transport: c.nets[g].Transport(id),
			Persister: c.persisters[g][id], ApplyCh: applyCh,
		})
		if err != nil {
			panic(err)
		}
		s := kv.NewStore(r, applyCh, 200)
		c.mu.Lock()
		c.rafts[g][id], c.stores[g][id] = r, s
		c.mu.Unlock()
		c.nets[g].Add(id, r)
	}
	c.mu.Lock()
	c.up[id] = true
	c.mu.Unlock()
}

// crash stops every Raft group on a node, as if its process died, and
// restarts it from its saved state a few seconds later. Only one node may be
// down at a time, so a majority of every shard survives.
func (c *cluster) crash(id int) error {
	c.mu.Lock()
	switch {
	case id < 1 || id > nodeCount:
		c.mu.Unlock()
		return errors.New("no such node")
	case !c.up[id]:
		c.mu.Unlock()
		return errors.New("that node is already down")
	case c.downCountLocked() > 0:
		c.mu.Unlock()
		return errors.New("one node is already down; crashing a second would lose the majority")
	case time.Since(c.lastCrash) < crashCooldown:
		c.mu.Unlock()
		return errors.New("give the cluster a few seconds before the next crash")
	}
	c.up[id] = false
	c.lastCrash = time.Now()
	var stopping []*raft.Raft
	for g := 0; g < shardCount; g++ {
		stopping = append(stopping, c.rafts[g][id])
		c.rafts[g][id], c.stores[g][id] = nil, nil
	}
	c.mu.Unlock()

	for g, r := range stopping {
		c.nets[g].Remove(id)
		r.Stop()
		c.persisters[g][id] = c.persisters[g][id].Copy() // what survived on "disk"
	}
	c.logf("node %d crashed (all its Raft groups stopped without warning)", id)
	go func() {
		time.Sleep(restartAfter)
		c.startNode(id)
		c.logf("node %d restarted, reloaded its saved state and is catching up", id)
	}()
	return nil
}

func (c *cluster) downCountLocked() int {
	down := 0
	for id := 1; id <= nodeCount; id++ {
		if !c.up[id] {
			down++
		}
	}
	return down
}

func (c *cluster) logf(format string, args ...any) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.events = append(c.events, event{time.Now(), fmt.Sprintf(format, args...)})
	if len(c.events) > maxEventsShown {
		c.events = c.events[len(c.events)-maxEventsShown:]
	}
}

// leaderStore returns the store of shard g's current leader, if there is one.
func (c *cluster) leaderStore(g int) *kv.Store {
	c.mu.Lock()
	defer c.mu.Unlock()
	for id := 1; id <= nodeCount; id++ {
		if r := c.rafts[g][id]; r != nil && r.Status().State == "leader" {
			return c.stores[g][id]
		}
	}
	return nil
}

// do runs op against the key's shard leader, retrying through elections for
// up to 3 seconds, the way the server's forward mode and a client would.
func (c *cluster) do(key string, op func(*kv.Store) (kv.Result, error)) (kv.Result, error) {
	g := kv.ShardFor(key, shardCount)
	deadline := time.Now().Add(3 * time.Second)
	for {
		if s := c.leaderStore(g); s != nil {
			res, err := op(s)
			if err == nil {
				return res, nil
			}
			if time.Now().After(deadline) {
				return kv.Result{}, err
			}
		} else if time.Now().After(deadline) {
			return kv.Result{}, errors.New("no leader for this shard right now; try again")
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// watchLeaders is the same event logic as cmd/kvdemo: a shard whose leader's
// node went down is timed until a new leader is elected; any other change is
// the balancer handing leadership over.
func (c *cluster) watchLeaders() {
	for range time.Tick(50 * time.Millisecond) {
		state := c.state()
		for g := 0; g < shardCount; g++ {
			leader, term := 0, 0
			up := map[int]bool{}
			for _, n := range state.Nodes {
				up[n.ID] = n.Up
				if n.Up && n.Shards[g].State == "leader" {
					leader, term = n.ID, n.Shards[g].Term
				}
			}
			c.mu.Lock()
			prev, lost := c.leaders[g], c.lostAt[g]
			if leader == 0 && prev != 0 && !up[prev] && lost.IsZero() {
				c.lostAt[g] = time.Now()
			}
			if leader != 0 && leader != prev {
				c.leaders[g], c.lostAt[g] = leader, time.Time{}
			}
			c.mu.Unlock()
			if leader == 0 || leader == prev {
				continue
			}
			switch {
			case prev == 0:
				c.logf("shard %d: node %d is leader (term %d)", g, leader, term)
			case !lost.IsZero():
				c.logf("shard %d lost its leader; node %d won the election in term %d after %d ms",
					g, leader, term, time.Since(lost).Milliseconds())
			default:
				c.logf("shard %d: leadership moved to node %d (term %d)", g, leader, term)
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

func (c *cluster) state() stateView {
	c.mu.Lock()
	defer c.mu.Unlock()
	var out stateView
	for id := 1; id <= nodeCount; id++ {
		nv := nodeView{ID: id, Up: c.up[id]}
		for g := 0; g < shardCount; g++ {
			sv := shardView{Shard: g, State: "down"}
			if r := c.rafts[g][id]; r != nil {
				st := r.Status()
				sv.State, sv.Term, sv.Commit = st.State, st.Term, st.CommitIndex
			}
			nv.Shards = append(nv.Shards, sv)
		}
		out.Nodes = append(out.Nodes, nv)
	}
	out.Events = append([]event(nil), c.events...)
	return out
}

// promise runs f off the JavaScript thread (Raft operations block on
// channels, which would deadlock the page's only thread) and resolves a
// JavaScript Promise with {status, body}, the same shape as an HTTP reply.
func promise(f func() (int, string)) js.Value {
	executor := js.FuncOf(func(_ js.Value, args []js.Value) any {
		resolve := args[0]
		go func() {
			status, body := f()
			resolve.Invoke(map[string]any{"status": status, "body": body})
		}()
		return nil
	})
	return js.Global().Get("Promise").New(executor)
}

func main() {
	c := newCluster()
	api := map[string]any{
		"state": js.FuncOf(func(js.Value, []js.Value) any {
			b, _ := json.Marshal(c.state())
			return string(b)
		}),
		"crash": js.FuncOf(func(_ js.Value, args []js.Value) any {
			if err := c.crash(args[0].Int()); err != nil {
				return err.Error()
			}
			return ""
		}),
		"put": js.FuncOf(func(_ js.Value, args []js.Value) any {
			key, value := args[0].String(), args[1].String()
			return promise(func() (int, string) {
				_, err := c.do(key, func(s *kv.Store) (kv.Result, error) {
					return s.Do(kv.Command{Op: kv.OpPut, Key: key, Value: value})
				})
				if err != nil {
					return 503, err.Error()
				}
				return 204, ""
			})
		}),
		"get": js.FuncOf(func(_ js.Value, args []js.Value) any {
			key := args[0].String()
			return promise(func() (int, string) {
				res, err := c.do(key, func(s *kv.Store) (kv.Result, error) { return s.Get(key) })
				switch {
				case err != nil:
					return 503, err.Error()
				case !res.Found:
					return 404, "key not found"
				}
				return 200, res.Value
			})
		}),
	}
	js.Global().Set("raftkv", js.ValueOf(api))
	js.Global().Call("dispatchEvent", js.Global().Get("Event").New("raftkv-ready"))
	select {} // keep the cluster running
}
