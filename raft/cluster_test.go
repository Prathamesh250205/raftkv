package raft

import (
	"bytes"
	"encoding/gob"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"
)

// cluster is an in-memory test harness: nodes talk through memTransport, and
// tests can partition, crash and restart nodes at will.
type cluster struct {
	t          *testing.T
	mu         sync.Mutex
	n          int
	nodes      []*Raft // nil while crashed
	persisters []*MemPersister
	connected  []bool
	applied    []map[int][]byte // per node: log index -> command
	lastIndex  []int
	snapEvery  int // if > 0, each node snapshots after every snapEvery entries
	founders   int // nodes 0..founders-1 bootstrap the cluster; the rest join later
}

var errDropped = errors.New("message dropped")

type memTransport struct {
	c    *cluster
	from int
}

func (m *memTransport) RequestVote(peer int, args *RequestVoteArgs, reply *RequestVoteReply) error {
	node := m.c.link(m.from, peer)
	if node == nil {
		return errDropped
	}
	if err := node.HandleRequestVote(args, reply); err != nil {
		return err
	}
	if m.c.link(m.from, peer) == nil { // partitioned while in flight: lose the reply
		return errDropped
	}
	return nil
}

func (m *memTransport) AppendEntries(peer int, args *AppendEntriesArgs, reply *AppendEntriesReply) error {
	node := m.c.link(m.from, peer)
	if node == nil {
		return errDropped
	}
	if err := node.HandleAppendEntries(args, reply); err != nil {
		return err
	}
	if m.c.link(m.from, peer) == nil {
		return errDropped
	}
	return nil
}

func (m *memTransport) InstallSnapshot(peer int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) error {
	node := m.c.link(m.from, peer)
	if node == nil {
		return errDropped
	}
	if err := node.HandleInstallSnapshot(args, reply); err != nil {
		return err
	}
	if m.c.link(m.from, peer) == nil {
		return errDropped
	}
	return nil
}

func newSnapshotCluster(t *testing.T, n, every int) *cluster {
	c := newCluster(t, n)
	c.mu.Lock()
	c.snapEvery = every
	c.mu.Unlock()
	return c
}

func newCluster(t *testing.T, n int) *cluster { return newGrowingCluster(t, n, n) }

// newGrowingCluster has room for n nodes but bootstraps only the first
// founders; the others start empty and wait to be added with AddMember.
func newGrowingCluster(t *testing.T, n, founders int) *cluster {
	c := &cluster{
		t:          t,
		n:          n,
		founders:   founders,
		nodes:      make([]*Raft, n),
		persisters: make([]*MemPersister, n),
		connected:  make([]bool, n),
		applied:    make([]map[int][]byte, n),
		lastIndex:  make([]int, n),
	}
	for i := 0; i < n; i++ {
		c.persisters[i] = &MemPersister{}
		c.connected[i] = true
		c.start(i)
	}
	t.Cleanup(c.shutdown)
	return c
}

func (c *cluster) link(from, to int) *Raft {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.connected[from] || !c.connected[to] {
		return nil
	}
	return c.nodes[to]
}

func (c *cluster) start(i int) {
	var peers map[int]string
	if i < c.founders {
		peers = map[int]string{}
		for j := 0; j < c.founders; j++ {
			peers[j] = fmt.Sprintf("node-%d", j)
		}
	}
	applyCh := make(chan ApplyMsg, 64)
	r, err := New(Config{ID: i, Peers: peers, Transport: &memTransport{c, i}, Persister: c.persisters[i], ApplyCh: applyCh})
	if err != nil {
		c.t.Fatal(err)
	}
	c.mu.Lock()
	c.nodes[i] = r
	c.applied[i] = map[int][]byte{}
	c.lastIndex[i] = 0
	c.mu.Unlock()

	go func() {
		for {
			select {
			case m := <-applyCh:
				c.record(i, m)
				if snap := c.maybeSnapshot(i, m); snap != nil {
					r.Snapshot(m.Index, snap)
				}
			case <-r.stopCh:
				return
			}
		}
	}()
}

// record checks the core safety property: no two nodes ever apply different
// commands at the same index, and each node applies indices in order.
func (c *cluster) record(i int, m ApplyMsg) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if m.SnapshotValid {
		// The test service's snapshot is simply every command it has applied.
		var applied map[int][]byte
		if err := gob.NewDecoder(bytes.NewReader(m.Snapshot)).Decode(&applied); err != nil {
			c.t.Errorf("node %d: decode snapshot: %v", i, err)
			return
		}
		for idx, cmd := range applied {
			c.checkLocked(i, idx, cmd)
		}
		c.applied[i] = applied
		c.lastIndex[i] = m.Index
		return
	}
	if m.Index != c.lastIndex[i]+1 {
		c.t.Errorf("node %d applied index %d out of order (last %d)", i, m.Index, c.lastIndex[i])
	}
	c.checkLocked(i, m.Index, m.Command)
	c.applied[i][m.Index] = m.Command
	c.lastIndex[i] = m.Index
}

func (c *cluster) checkLocked(i, index int, cmd []byte) {
	for j := 0; j < c.n; j++ {
		if v, ok := c.applied[j][index]; ok && !bytes.Equal(v, cmd) {
			c.t.Errorf("index %d: node %d applied %q but node %d applied %q", index, i, cmd, j, v)
		}
	}
}

// maybeSnapshot returns snapshot data when node i should compact after m.
func (c *cluster) maybeSnapshot(i int, m ApplyMsg) []byte {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.snapEvery == 0 || m.SnapshotValid || m.Index%c.snapEvery != 0 {
		return nil
	}
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(c.applied[i]); err != nil {
		c.t.Fatal(err)
	}
	return buf.Bytes()
}

func (c *cluster) crash(i int) {
	c.mu.Lock()
	r := c.nodes[i]
	c.nodes[i] = nil
	c.mu.Unlock()
	if r != nil {
		r.Stop()
	}
	c.mu.Lock()
	c.persisters[i] = c.persisters[i].Copy()
	c.mu.Unlock()
}

func (c *cluster) disconnect(i int) { c.mu.Lock(); c.connected[i] = false; c.mu.Unlock() }
func (c *cluster) connect(i int)    { c.mu.Lock(); c.connected[i] = true; c.mu.Unlock() }

func (c *cluster) shutdown() {
	for i := 0; i < c.n; i++ {
		c.crash(i)
	}
}

func (c *cluster) node(i int) *Raft {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.nodes[i]
}

func (c *cluster) isConnected(i int) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.connected[i] && c.nodes[i] != nil
}

// checkOneLeader waits for exactly one leader among connected nodes and returns it.
func (c *cluster) checkOneLeader() int {
	for iter := 0; iter < 30; iter++ {
		time.Sleep(100 * time.Millisecond)
		leaders := map[int][]int{}
		for i := 0; i < c.n; i++ {
			if !c.isConnected(i) {
				continue
			}
			if st := c.node(i).Status(); st.State == Leader.String() {
				leaders[st.Term] = append(leaders[st.Term], i)
			}
		}
		latest := -1
		for term, ids := range leaders {
			if len(ids) > 1 {
				c.t.Fatalf("term %d has %d leaders: %v", term, len(ids), ids)
			}
			latest = max(latest, term)
		}
		if latest >= 0 {
			return leaders[latest][0]
		}
	}
	c.t.Fatal("expected one leader, got none")
	return -1
}

func (c *cluster) checkNoLeader() {
	for i := 0; i < c.n; i++ {
		if c.isConnected(i) && c.node(i).Status().State == Leader.String() {
			c.t.Fatalf("node %d is leader, but expected no leader", i)
		}
	}
}

func (c *cluster) term(i int) int { return c.node(i).Status().Term }

// nCommitted returns how many nodes have applied index, and the command there.
func (c *cluster) nCommitted(index int) (int, []byte) {
	c.mu.Lock()
	defer c.mu.Unlock()
	count := 0
	var cmd []byte
	for i := 0; i < c.n; i++ {
		if v, ok := c.applied[i][index]; ok {
			count++
			cmd = v
		}
	}
	return count, cmd
}

// one submits cmd to whichever node is leader and waits until at least
// expected nodes have applied it. It retries across leader changes.
func (c *cluster) one(cmd []byte, expected int) int {
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		index := -1
		for i := 0; i < c.n; i++ {
			if !c.isConnected(i) {
				continue
			}
			if idx, _, ok := c.node(i).Start(cmd); ok {
				index = idx
				break
			}
		}
		if index == -1 {
			time.Sleep(50 * time.Millisecond)
			continue
		}
		wait := time.Now().Add(2 * time.Second)
		for time.Now().Before(wait) {
			if n, got := c.nCommitted(index); n >= expected && bytes.Equal(got, cmd) {
				return index
			}
			time.Sleep(20 * time.Millisecond)
		}
	}
	c.t.Fatalf("one(%q) failed to reach agreement on %d nodes", cmd, expected)
	return -1
}
