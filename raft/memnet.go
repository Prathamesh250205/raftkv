package raft

import (
	"errors"
	"math/rand"
	"sync"
	"time"
)

var errUnreachable = errors.New("raft: peer unreachable")

// MemNetwork connects Raft nodes in one process, for tests of code built on
// Raft. It can partition nodes, drop messages and add random delay.
type MemNetwork struct {
	mu       sync.Mutex
	nodes    map[int]*Raft
	group    map[int]int // nodes talk only within the same group
	maxDelay time.Duration
	dropRate float64
}

func NewMemNetwork() *MemNetwork {
	return &MemNetwork{nodes: map[int]*Raft{}, group: map[int]int{}}
}

// SetFaults makes every message wait up to maxDelay each way and fail with
// probability dropRate.
func (n *MemNetwork) SetFaults(maxDelay time.Duration, dropRate float64) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.maxDelay, n.dropRate = maxDelay, dropRate
}

// Add attaches r as node id, replacing any previous node with that id.
func (n *MemNetwork) Add(id int, r *Raft) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.nodes[id] = r
}

// Remove detaches node id, as if it had crashed.
func (n *MemNetwork) Remove(id int) {
	n.mu.Lock()
	defer n.mu.Unlock()
	delete(n.nodes, id)
}

// Partition splits the network: nodes can only reach others in the same list.
// Nodes not listed are each isolated on their own.
func (n *MemNetwork) Partition(groups ...[]int) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.group = map[int]int{}
	for id := range n.nodes {
		n.group[id] = -id - 1 // unique: isolated
	}
	for g, ids := range groups {
		for _, id := range ids {
			n.group[id] = g
		}
	}
}

// Heal reconnects every node.
func (n *MemNetwork) Heal() {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.group = map[int]int{}
}

// Transport returns the Transport that node from should use.
func (n *MemNetwork) Transport(from int) Transport { return &memNetTransport{n, from} }

func (n *MemNetwork) deliver(from, to int, call func(r *Raft) error) error {
	r, delay, ok := n.link(from, to)
	if !ok {
		return errUnreachable
	}
	time.Sleep(delay)
	if err := call(r); err != nil {
		return err
	}
	if _, delay, ok = n.link(from, to); !ok {
		return errUnreachable // the reply is lost
	}
	time.Sleep(delay)
	return nil
}

func (n *MemNetwork) link(from, to int) (*Raft, time.Duration, bool) {
	n.mu.Lock()
	defer n.mu.Unlock()
	r, ok := n.nodes[to]
	if !ok || n.nodes[from] == nil || n.group[from] != n.group[to] {
		return nil, 0, false
	}
	if n.dropRate > 0 && rand.Float64() < n.dropRate {
		return nil, 0, false
	}
	var delay time.Duration
	if n.maxDelay > 0 {
		delay = time.Duration(rand.Int63n(int64(n.maxDelay)))
	}
	return r, delay, true
}

type memNetTransport struct {
	net  *MemNetwork
	from int
}

func (t *memNetTransport) RequestVote(peer int, args *RequestVoteArgs, reply *RequestVoteReply) error {
	return t.net.deliver(t.from, peer, func(r *Raft) error { return r.HandleRequestVote(args, reply) })
}

func (t *memNetTransport) AppendEntries(peer int, args *AppendEntriesArgs, reply *AppendEntriesReply) error {
	return t.net.deliver(t.from, peer, func(r *Raft) error { return r.HandleAppendEntries(args, reply) })
}

func (t *memNetTransport) InstallSnapshot(peer int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) error {
	return t.net.deliver(t.from, peer, func(r *Raft) error { return r.HandleInstallSnapshot(args, reply) })
}

func (t *memNetTransport) TimeoutNow(peer int, args *TimeoutNowArgs, reply *TimeoutNowReply) error {
	return t.net.deliver(t.from, peer, func(r *Raft) error { return r.HandleTimeoutNow(args, reply) })
}
