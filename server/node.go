// Package server runs one node of the sharded key-value store: a Raft group
// per shard, all sharing one HTTP port for client requests, administration,
// metrics and Raft RPCs.
package server

import (
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"path/filepath"
	"sort"
	"strconv"
	"sync"
	"time"

	"raftkv/kv"
	"raftkv/metrics"
	"raftkv/raft"
)

type Config struct {
	ID   int
	Addr string // host:port this node listens on and is known by
	// Cluster lists the founding nodes (ID -> address), including this one.
	// It is ignored for shards that already have state on disk. Leave it nil
	// for a node that will be added to an existing cluster.
	Cluster map[int]string
	// Shards is the number of Raft groups keys are hashed across. Every node
	// in a cluster must use the same value.
	Shards int
	// Replicas is how many founding nodes each shard starts on; 0 means all.
	// Shard g starts on Replicas consecutive nodes beginning at the g-th.
	Replicas int
	DataDir  string
	// SeparateFiles gives every shard its own state files instead of one
	// shared log per node. Slower with many shards; kept for comparison.
	SeparateFiles bool
	SnapshotEvery int
	LogReads      bool
	Logger        *log.Logger  // nil disables logging
	Listener      net.Listener // optional; otherwise Start listens on Addr
}

type shard struct {
	id       int
	raft     *raft.Raft
	store    *kv.Store
	founders map[int]string
}

type Node struct {
	cfg    Config
	shards []*shard
	wal    *raft.SharedLog // nil with SeparateFiles
	reg    *metrics.Registry
	srv    *http.Server
	conns  *connTracker
	done   chan struct{}
}

// balanceLeaders spreads shard leadership across nodes. Raft's random election
// timeouts give no balance guarantee (one node often wins every shard), which
// would leave one node doing all the work. Each shard has a preferred leader,
// its (g mod n)-th member in ID order; a leader that isn't preferred hands
// over once the preferred node has caught up.
func (n *Node) balanceLeaders() {
	t := time.NewTicker(time.Second)
	defer t.Stop()
	for {
		select {
		case <-n.done:
			return
		case <-t.C:
		}
		for _, sh := range n.shards {
			if sh.raft.Status().State != "leader" {
				continue
			}
			if p := preferredLeader(sh.raft.Members(), sh.id); p != -1 && p != n.cfg.ID {
				sh.raft.TransferLeadership(p) // not ready yet: try again next tick
			}
		}
	}
}

func preferredLeader(members map[int]string, g int) int {
	if len(members) == 0 {
		return -1
	}
	ids := make([]int, 0, len(members))
	for id := range members {
		ids = append(ids, id)
	}
	sort.Ints(ids)
	return ids[g%len(ids)]
}

// connTracker remembers every accepted connection. Raft RPCs hijack theirs
// (net/rpc over HTTP CONNECT), and http.Server.Close leaves hijacked
// connections open, so Close must shut them itself.
type connTracker struct {
	net.Listener
	mu    sync.Mutex
	conns map[net.Conn]struct{}
}

func (l *connTracker) Accept() (net.Conn, error) {
	c, err := l.Listener.Accept()
	if err == nil {
		l.mu.Lock()
		l.conns[c] = struct{}{}
		l.mu.Unlock()
	}
	return c, err
}

func (l *connTracker) closeAll() {
	l.mu.Lock()
	defer l.mu.Unlock()
	for c := range l.conns {
		c.Close()
	}
	l.conns = map[net.Conn]struct{}{}
}

// Start brings up every shard and begins serving. It returns once the node is
// listening; elections and catch-up continue in the background.
func Start(cfg Config) (*Node, error) {
	if cfg.Shards < 1 {
		cfg.Shards = 1
	}
	n := &Node{cfg: cfg, reg: metrics.NewRegistry()}
	if !cfg.SeparateFiles {
		var err error
		if n.wal, err = raft.OpenSharedLog(cfg.DataDir); err != nil {
			return nil, err
		}
	}
	mux := http.NewServeMux()
	for g := 0; g < cfg.Shards; g++ {
		sh, err := n.startShard(g, mux)
		if err != nil {
			n.stopShards()
			return nil, fmt.Errorf("shard %d: %w", g, err)
		}
		n.shards = append(n.shards, sh)
	}
	n.registerRoutes(mux)

	ln := cfg.Listener
	if ln == nil {
		var err error
		if ln, err = net.Listen("tcp", cfg.Addr); err != nil {
			n.stopShards()
			return nil, err
		}
	}
	n.conns = &connTracker{Listener: ln, conns: map[net.Conn]struct{}{}}
	n.done = make(chan struct{})
	go n.balanceLeaders()

	n.srv = &http.Server{
		Handler: mux,
		ConnState: func(c net.Conn, s http.ConnState) {
			if s == http.StateClosed { // hijacked connections never reach this state
				n.conns.mu.Lock()
				delete(n.conns.conns, c)
				n.conns.mu.Unlock()
			}
		},
	}
	go func() {
		if err := n.srv.Serve(n.conns); err != nil && !errors.Is(err, http.ErrServerClosed) && cfg.Logger != nil {
			cfg.Logger.Print(err)
		}
	}()
	return n, nil
}

func (n *Node) startShard(g int, mux *http.ServeMux) (*shard, error) {
	founders := placement(n.cfg.Cluster, g, n.cfg.Replicas)
	var peers map[int]string
	if _, ok := founders[n.cfg.ID]; ok {
		peers = founders
	}
	var persister raft.Persister
	if n.wal != nil {
		persister = n.wal.Persister(g)
	} else {
		fp, err := raft.NewFilePersister(filepath.Join(n.cfg.DataDir, fmt.Sprintf("shard-%d", g)))
		if err != nil {
			return nil, err
		}
		persister = fp
	}
	var logger *log.Logger
	if n.cfg.Logger != nil {
		logger = log.New(n.cfg.Logger.Writer(), fmt.Sprintf("[node %d shard %d] ", n.cfg.ID, g), n.cfg.Logger.Flags())
	}

	applyCh := make(chan raft.ApplyMsg, 256)
	r, err := raft.New(raft.Config{
		ID:        n.cfg.ID,
		Peers:     peers,
		Transport: raft.NewHTTPTransportPath(peers, rpcPath(g)),
		Persister: persister,
		ApplyCh:   applyCh,
		Logger:    logger,
	})
	if err != nil {
		return nil, err
	}
	store := kv.NewStore(r, applyCh, n.cfg.SnapshotEvery)
	store.LogReads = n.cfg.LogReads

	rpc, err := raft.NewRPCHandler(r)
	if err != nil {
		r.Stop()
		return nil, err
	}
	mux.Handle(rpcPath(g), rpc)
	r.RegisterMetrics(n.reg, metrics.Labels{"node": strconv.Itoa(n.cfg.ID), "shard": strconv.Itoa(g)})
	return &shard{id: g, raft: r, store: store, founders: founders}, nil
}

// Close stops serving and halts every Raft group. Durable state stays on disk.
func (n *Node) Close() {
	if n.srv != nil {
		close(n.done)
		n.srv.Close()
		n.conns.closeAll()
	}
	n.stopShards()
}

func (n *Node) stopShards() {
	for _, sh := range n.shards {
		sh.raft.Stop()
	}
	if n.wal != nil {
		n.wal.Close()
	}
}

// Store returns shard g's state machine, for tests and diagnostics.
func (n *Node) Store(g int) *kv.Store { return n.shards[g].store }

// Raft returns shard g's Raft instance, for tests and diagnostics.
func (n *Node) Raft(g int) *raft.Raft { return n.shards[g].raft }

func (n *Node) shardFor(key string) *shard { return n.shards[kv.ShardFor(key, len(n.shards))] }

func rpcPath(g int) string { return raft.RPCPath + "/" + strconv.Itoa(g) }

// placement picks the founding members of shard g: replicas consecutive
// nodes in ID order, starting at the g-th, so shards spread evenly.
func placement(cluster map[int]string, g, replicas int) map[int]string {
	ids := make([]int, 0, len(cluster))
	for id := range cluster {
		ids = append(ids, id)
	}
	sort.Ints(ids)
	if replicas <= 0 || replicas >= len(ids) {
		replicas = len(ids)
	}
	out := map[int]string{}
	for i := 0; i < replicas; i++ {
		id := ids[(g+i)%len(ids)]
		out[id] = cluster[id]
	}
	return out
}
