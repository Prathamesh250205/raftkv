package raft

import (
	"bytes"
	"encoding/gob"
	"errors"
	"fmt"
)

// Cluster membership changes, one server at a time (Ongaro's thesis §4.1).
//
// A configuration is a map of member ID to address, stored in the log as an
// EntryConfig entry. Every node uses the latest configuration in its log as
// soon as it appends it, committed or not. Adding or removing a single server
// at a time guarantees that any majority of the old configuration overlaps any
// majority of the new one, so two leaders can never be elected for one term.

var (
	ErrConfigInProgress = errors.New("raft: another membership change has not committed yet")
	ErrAlreadyMember    = errors.New("raft: node is already a member")
	ErrNotMember        = errors.New("raft: node is not a member")
	ErrLastMember       = errors.New("raft: cannot remove the last member")
)

// PeerUpdater is implemented by transports that need to learn peer addresses
// as the configuration changes, such as HTTPTransport.
type PeerUpdater interface {
	UpdatePeers(addrs map[int]string)
}

// AddMember proposes adding node id, reachable at addr. It returns the log
// index of the change; the node is a full member from then on, and the change
// is durable once that index commits. The new node should be started with no
// bootstrap peers; the leader brings it up to date with a snapshot or entries.
func (r *Raft) AddMember(id int, addr string) (int, error) {
	return r.changeMembers(func(m map[int]string) error {
		if _, ok := m[id]; ok {
			return ErrAlreadyMember
		}
		m[id] = addr
		return nil
	})
}

// RemoveMember proposes removing node id. A leader may remove itself: it keeps
// leading until the change commits and then steps down.
func (r *Raft) RemoveMember(id int) (int, error) {
	return r.changeMembers(func(m map[int]string) error {
		if _, ok := m[id]; !ok {
			return ErrNotMember
		}
		if len(m) == 1 {
			return ErrLastMember
		}
		delete(m, id)
		return nil
	})
}

func (r *Raft) changeMembers(change func(map[int]string) error) (int, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.state != Leader || r.killed() {
		return 0, ErrNotLeader
	}
	// A leader must commit an entry of its own term before changing the
	// configuration, or an older leader's uncommitted change could be lost
	// while a new one is in flight (the bug noted in the thesis errata).
	if r.termAt(r.commitIndex) != r.currentTerm {
		return 0, ErrLeaderNotReady
	}
	if r.configIndex > r.commitIndex {
		return 0, ErrConfigInProgress
	}
	next := copyMembers(r.members)
	if err := change(next); err != nil {
		return 0, err
	}

	r.log = append(r.log, LogEntry{Term: r.currentTerm, Kind: EntryConfig, Command: encode(next)})
	r.setMembersLocked(next, r.lastIndex())
	r.logf("proposed membership %v at index %d", next, r.lastIndex())
	signal(r.flushCh)
	r.broadcastLocked()
	return r.lastIndex(), nil
}

// Members returns the configuration this node is currently using.
func (r *Raft) Members() map[int]string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return copyMembers(r.members)
}

// LeaderAddr returns the address of the node this node believes is leader, if known.
func (r *Raft) LeaderAddr() (string, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	addr, ok := r.members[r.leaderID]
	return addr, ok && r.leaderID != -1
}

// recomputeMembersLocked sets members from the last configuration entry in the
// log, or from the snapshot's configuration if there is none. It is called
// whenever the log or snapshot changes underneath the configuration.
func (r *Raft) recomputeMembersLocked() {
	for i := r.lastIndex(); i > r.snapIndex; i-- {
		if e := r.entry(i); e.Kind == EntryConfig {
			r.setMembersLocked(decodeMembers(e.Command), i)
			return
		}
	}
	r.setMembersLocked(copyMembers(r.baseMembers), r.snapIndex)
}

// membersAtLocked returns the configuration in effect at log index i.
func (r *Raft) membersAtLocked(i int) map[int]string {
	for ; i > r.snapIndex; i-- {
		if e := r.entry(i); e.Kind == EntryConfig {
			return decodeMembers(e.Command)
		}
	}
	return copyMembers(r.baseMembers)
}

// setMembersLocked switches to configuration m (set by the entry at index) and
// starts or stops replicators to match.
func (r *Raft) setMembersLocked(m map[int]string, index int) {
	r.members, r.configIndex = m, index

	for p := range m {
		if p == r.id {
			continue
		}
		if _, running := r.replCh[p]; !running {
			wake, stop := make(chan struct{}, 1), make(chan struct{})
			r.replCh[p], r.replStop[p] = wake, stop
			go r.replicator(p, wake, stop)
		}
		if _, ok := r.nextIndex[p]; !ok && r.state == Leader {
			r.nextIndex[p] = r.lastIndex() + 1 // backs off quickly to what the new peer has
			r.matchIndex[p] = 0
		}
	}
	for p, stop := range r.replStop {
		if _, member := m[p]; !member {
			close(stop)
			delete(r.replStop, p)
			delete(r.replCh, p)
			delete(r.nextIndex, p)
			delete(r.matchIndex, p)
		}
	}
	if pu, ok := r.transport.(PeerUpdater); ok {
		pu.UpdatePeers(copyMembers(m))
	}
}

func copyMembers(m map[int]string) map[int]string {
	out := make(map[int]string, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}

func decodeMembers(data []byte) map[int]string {
	var m map[int]string
	if err := gob.NewDecoder(bytes.NewReader(data)).Decode(&m); err != nil {
		panic(fmt.Sprintf("raft: decode membership entry: %v", err))
	}
	return m
}
