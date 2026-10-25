// Package raft implements the Raft consensus algorithm (Ongaro & Ousterhout, 2014):
// leader election, log replication, durable state and log compaction. Section
// numbers in comments refer to the extended Raft paper.
package raft

import (
	"bytes"
	"encoding/gob"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

const (
	HeartbeatInterval  = 50 * time.Millisecond
	ElectionTimeoutMin = 300 * time.Millisecond
	ElectionTimeoutMax = 600 * time.Millisecond

	// Caps a single AppendEntries so a far-behind follower catches up in batches.
	maxEntriesPerRPC = 512
)

var (
	ErrStopped        = errors.New("raft: node stopped")
	ErrNotLeader      = errors.New("raft: not the leader")
	ErrLeaderNotReady = errors.New("raft: leader has not committed an entry in its term yet")
	ErrReadTimeout    = errors.New("raft: could not confirm leadership in time")
)

type State int

const (
	Follower State = iota
	Candidate
	Leader
)

func (s State) String() string {
	switch s {
	case Follower:
		return "follower"
	case Candidate:
		return "candidate"
	case Leader:
		return "leader"
	}
	return "unknown"
}

type EntryKind uint8

const (
	EntryNormal EntryKind = iota // a service command; nil Command is a leader's no-op
	EntryConfig                  // a cluster membership change (see membership.go)
)

// LogEntry is one replicated command or membership change.
type LogEntry struct {
	Term    int
	Kind    EntryKind
	Command []byte
}

// ApplyMsg is delivered on ApplyCh, in log order, once an entry is committed.
// Membership changes are delivered with a nil Command, like no-ops.
// When SnapshotValid is set it carries a snapshot instead: the service must
// replace its whole state with Snapshot, which covers every entry up to Index.
type ApplyMsg struct {
	Index   int
	Term    int
	Command []byte

	SnapshotValid bool
	Snapshot      []byte
}

type Config struct {
	ID int
	// Peers bootstraps a brand-new cluster: every member's ID and address,
	// including this node. It is ignored once the node has persisted state.
	// Leave it empty for a node that will join an existing cluster through
	// AddMember; such a node waits until a leader contacts it.
	Peers     map[int]string
	Transport Transport
	Persister Persister
	ApplyCh   chan<- ApplyMsg
	Logger    *log.Logger // nil disables logging
}

type Status struct {
	ID            int    `json:"id"`
	State         string `json:"state"`
	Term          int    `json:"term"`
	LeaderID      int    `json:"leader_id"`
	CommitIndex   int    `json:"commit_index"`
	LastApplied   int    `json:"last_applied"`
	LastLogIndex  int    `json:"last_log_index"`
	SnapshotIndex int    `json:"snapshot_index"`
	// Members is the cluster configuration this node is using: ID -> address.
	Members map[int]string `json:"members"`
}

type Raft struct {
	mu        sync.Mutex
	applyCond *sync.Cond

	id        int
	transport Transport
	persister Persister
	applyCh   chan<- ApplyMsg
	logger    *log.Logger

	// Persistent state (§5, Figure 2). The log holds entries snapIndex..lastIndex();
	// log[0] stands in for the last entry covered by the snapshot (or for index 0
	// before any snapshot) and only its Term is meaningful.
	currentTerm int
	votedFor    int
	log         []LogEntry
	snapIndex   int
	snapshot    []byte // the service's state as of snapIndex

	// Cluster membership (thesis §4.1). members is the latest configuration in
	// the log, used as soon as it is appended; configIndex is that entry's index
	// (or snapIndex when it came from the snapshot). baseMembers is the
	// configuration as of snapIndex, persisted alongside the snapshot.
	members     map[int]string
	configIndex int
	baseMembers map[int]string

	// Volatile state.
	state           State
	leaderID        int
	commitIndex     int
	lastApplied     int
	pendingSnapshot bool // snapshot installed or restored, not yet handed to the service

	// Leader-only state, reinitialised after each election.
	nextIndex  map[int]int
	matchIndex map[int]int

	electionDeadline  time.Time
	lastBroadcast     time.Time
	lastLeaderContact time.Time

	// Group commit. The leader appends to its in-memory log and lets the flusher
	// fsync everything appended since the last flush in one write. stateVersion
	// orders snapshots so a slow flush can never overwrite newer state on disk.
	flushCh      chan struct{}
	persistMu    sync.Mutex // serialises Persister.Save; taken after mu, never before
	stateVersion uint64     // guarded by mu
	savedVersion uint64     // guarded by persistMu

	// One replicator goroutine per peer keeps a single AppendEntries in flight;
	// entries appended meanwhile ride along in the next batch. Replicators come
	// and go with membership changes; replStop ends one.
	replCh   map[int]chan struct{}
	replStop map[int]chan struct{}

	// ReadIndex (§8). Every AppendEntries carries the heartbeat round it was
	// sent in; a reply in our term proves the peer still saw us as leader then.
	hbRound     uint64
	peerAck     map[int]uint64 // latest round each peer has acknowledged
	readWaiters []readWaiter   // ordered by round

	metrics raftMetrics

	dead   atomic.Bool
	stopCh chan struct{}
}

func New(cfg Config) (*Raft, error) {
	if len(cfg.Peers) > 0 {
		if _, ok := cfg.Peers[cfg.ID]; !ok {
			return nil, fmt.Errorf("raft: node %d is not in its own bootstrap peers", cfg.ID)
		}
	}

	r := &Raft{
		id:         cfg.ID,
		transport:  cfg.Transport,
		persister:  cfg.Persister,
		applyCh:    cfg.ApplyCh,
		logger:     cfg.Logger,
		votedFor:   -1,
		log:        []LogEntry{{Term: 0}},
		leaderID:   -1,
		nextIndex:  map[int]int{},
		matchIndex: map[int]int{},
		flushCh:    make(chan struct{}, 1),
		replCh:     map[int]chan struct{}{},
		replStop:   map[int]chan struct{}{},
		peerAck:    map[int]uint64{},
		stopCh:     make(chan struct{}),
	}
	r.metrics.init()
	r.applyCond = sync.NewCond(&r.mu)
	if err := r.restore(); err != nil {
		return nil, err
	}
	if len(r.baseMembers) == 0 && len(cfg.Peers) > 0 {
		r.baseMembers = copyMembers(cfg.Peers) // fresh cluster (or state from before membership existed)
	}
	r.resetElectionTimerLocked()

	r.mu.Lock()
	r.recomputeMembersLocked() // also starts a replicator per peer
	r.mu.Unlock()
	go r.ticker()
	go r.applier()
	go r.flusher()
	return r, nil
}

// Start proposes cmd for replication. It returns immediately; the entry is not yet
// committed. The caller learns the outcome when (index, term) appears on ApplyCh.
func (r *Raft) Start(cmd []byte) (index, term int, isLeader bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.state != Leader || r.killed() {
		return 0, 0, false
	}
	r.log = append(r.log, LogEntry{Term: r.currentTerm, Command: cmd})
	r.metrics.proposals.Inc()
	// No fsync here: the flusher batches it, and only then counts the leader
	// toward a majority. Followers can receive the entry in parallel.
	signal(r.flushCh)
	r.broadcastLocked()
	return r.lastIndex(), r.currentTerm, true
}

// Snapshot tells Raft that the service has captured its state up to and
// including index in data, so log entries up to index can be discarded (§7).
// The service calls it after applying index.
func (r *Raft) Snapshot(index int, data []byte) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if index <= r.snapIndex || index > r.commitIndex || r.killed() {
		return
	}
	r.baseMembers = r.membersAtLocked(index) // must be read before the log is trimmed
	if r.configIndex < index {
		r.configIndex = index
	}
	r.log = append([]LogEntry{{Term: r.termAt(index)}}, r.log[index-r.snapIndex+1:]...)
	r.snapIndex = index
	r.snapshot = data
	r.persistSnapshotLocked()
	r.metrics.snapshotsTaken.Inc()
	r.logf("snapshot at index %d, %d entries left in log", index, len(r.log)-1)
}

type readWaiter struct {
	round uint64
	done  chan error
}

// ReadIndex returns a log index such that once the service has applied it,
// serving a read from local state is linearizable (§8). It confirms this node
// is still leader by collecting heartbeat acknowledgements from a majority,
// which costs one round trip and no disk write.
func (r *Raft) ReadIndex(timeout time.Duration) (int, error) {
	r.mu.Lock()
	if r.state != Leader || r.killed() {
		r.mu.Unlock()
		return 0, ErrNotLeader
	}
	// Until it commits an entry of its own term, a new leader doesn't know how
	// far the commit index really extends.
	if r.termAt(r.commitIndex) != r.currentTerm {
		r.mu.Unlock()
		return 0, ErrLeaderNotReady
	}
	readIndex := r.commitIndex
	if _, ok := r.members[r.id]; ok && len(r.members) == 1 {
		r.mu.Unlock()
		return readIndex, nil
	}
	r.hbRound++
	w := readWaiter{round: r.hbRound, done: make(chan error, 1)}
	r.readWaiters = append(r.readWaiters, w)
	r.broadcastLocked()
	r.mu.Unlock()

	select {
	case err := <-w.done:
		if err != nil {
			r.metrics.readIndexFailed.Inc()
		} else {
			r.metrics.readIndexOK.Inc()
		}
		return readIndex, err
	case <-time.After(timeout):
		r.metrics.readIndexFailed.Inc()
		return 0, ErrReadTimeout
	}
}

// ackLocked records that peer acknowledged our leadership in round, and
// releases every read that a majority has now confirmed.
func (r *Raft) ackLocked(peer int, round uint64) {
	if round <= r.peerAck[peer] {
		return
	}
	r.peerAck[peer] = round
	for len(r.readWaiters) > 0 {
		w := r.readWaiters[0]
		acks := 0
		for p := range r.members {
			if p == r.id || r.peerAck[p] >= w.round {
				acks++
			}
		}
		if acks <= len(r.members)/2 {
			return // later waiters need even newer rounds
		}
		w.done <- nil
		r.readWaiters = r.readWaiters[1:]
	}
}

func (r *Raft) failReadsLocked() {
	for _, w := range r.readWaiters {
		w.done <- ErrNotLeader
	}
	r.readWaiters = nil
}

func (r *Raft) Status() Status {
	r.mu.Lock()
	defer r.mu.Unlock()
	return Status{
		ID:            r.id,
		State:         r.state.String(),
		Term:          r.currentTerm,
		LeaderID:      r.leaderID,
		CommitIndex:   r.commitIndex,
		LastApplied:   r.lastApplied,
		LastLogIndex:  r.lastIndex(),
		SnapshotIndex: r.snapIndex,
		Members:       copyMembers(r.members),
	}
}

// Stop halts all background work. A stopped node never persists again, so its
// Persister can be handed to a fresh node to simulate a restart.
func (r *Raft) Stop() {
	if !r.dead.CompareAndSwap(false, true) {
		return
	}
	close(r.stopCh)
	r.mu.Lock()
	r.applyCond.Broadcast()
	r.mu.Unlock()
}

func (r *Raft) killed() bool { return r.dead.Load() }

// ---- RPC handlers ----

type RequestVoteArgs struct {
	Term         int
	CandidateID  int
	LastLogIndex int
	LastLogTerm  int
	// Transfer marks an election the leader asked for (TransferLeadership),
	// which receivers honour even while they are hearing from that leader.
	Transfer bool
}

type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term         int
	LeaderID     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
	// On failure, hints that let the leader skip a whole conflicting term at once
	// instead of decrementing nextIndex one entry per round trip (§5.3).
	ConflictIndex int
	ConflictTerm  int // -1 when the follower's log is simply too short
}

type InstallSnapshotArgs struct {
	Term              int
	LeaderID          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Members           map[int]string // configuration as of LastIncludedIndex
	Data              []byte
}

type InstallSnapshotReply struct {
	Term int
}

func (r *Raft) HandleRequestVote(args *RequestVoteArgs, reply *RequestVoteReply) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.killed() {
		return ErrStopped
	}
	// Disruptive servers (thesis §4.2.3): while we are hearing from a live
	// leader, ignore candidates entirely, so a node removed from the cluster
	// (which no longer gets heartbeats) can't depose the leader with ever
	// higher terms.
	if args.Term > r.currentTerm && !args.Transfer && r.hearingFromLeaderLocked() {
		reply.Term = r.currentTerm
		return nil
	}
	if args.Term > r.currentTerm {
		r.stepDownLocked(args.Term)
	}
	reply.Term = r.currentTerm
	if args.Term < r.currentTerm {
		return nil
	}

	// Election restriction (§5.4.1): only vote for candidates whose log is at least as up to date.
	upToDate := args.LastLogTerm > r.lastTerm() ||
		(args.LastLogTerm == r.lastTerm() && args.LastLogIndex >= r.lastIndex())
	if (r.votedFor == -1 || r.votedFor == args.CandidateID) && upToDate {
		r.votedFor = args.CandidateID
		r.persistLocked()
		r.resetElectionTimerLocked()
		reply.VoteGranted = true
	}
	return nil
}

func (r *Raft) HandleAppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.killed() {
		return ErrStopped
	}
	if !r.acceptLeaderLocked(args.Term, args.LeaderID) {
		reply.Term = r.currentTerm
		return nil
	}
	reply.Term = r.currentTerm

	prev, prevTerm, entries := args.PrevLogIndex, args.PrevLogTerm, args.Entries
	if prev < r.snapIndex {
		// Entries up to snapIndex are committed here already and therefore match
		// the leader's; skip that prefix.
		skip := min(r.snapIndex-prev, len(entries))
		entries = entries[skip:]
		prev, prevTerm = r.snapIndex, r.termAt(r.snapIndex)
	}

	// Consistency check (§5.3).
	if prev > r.lastIndex() {
		reply.ConflictIndex = r.lastIndex() + 1
		reply.ConflictTerm = -1
		return nil
	}
	if t := r.termAt(prev); t != prevTerm {
		i := prev
		for i > r.snapIndex+1 && r.termAt(i-1) == t {
			i--
		}
		reply.ConflictIndex = i
		reply.ConflictTerm = t
		return nil
	}

	// Append new entries, truncating only on a real conflict so that a delayed,
	// stale RPC can never erase entries this follower already has.
	changed := false
	for i, e := range entries {
		idx := prev + 1 + i
		if idx <= r.lastIndex() {
			if r.termAt(idx) == e.Term {
				continue
			}
			r.log = r.log[:idx-r.snapIndex]
		}
		r.log = append(r.log, entries[i:]...)
		changed = true
		break
	}
	if changed {
		r.recomputeMembersLocked() // entries may add or truncate a config change
		r.persistLocked()
	}

	if commit := min(args.LeaderCommit, args.PrevLogIndex+len(args.Entries)); commit > r.commitIndex {
		r.commitIndex = commit
		r.applyCond.Broadcast()
	}
	reply.Success = true
	return nil
}

func (r *Raft) HandleInstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.killed() {
		return ErrStopped
	}
	if !r.acceptLeaderLocked(args.Term, args.LeaderID) {
		reply.Term = r.currentTerm
		return nil
	}
	reply.Term = r.currentTerm

	if args.LastIncludedIndex <= r.commitIndex {
		return nil // we already have everything this snapshot covers
	}
	// Keep any log suffix that follows the snapshot and agrees with it (§7).
	lii, lit := args.LastIncludedIndex, args.LastIncludedTerm
	if lii <= r.lastIndex() && r.termAt(lii) == lit {
		r.log = append([]LogEntry{{Term: lit}}, r.log[lii-r.snapIndex+1:]...)
	} else {
		r.log = []LogEntry{{Term: lit}}
	}
	r.snapIndex = lii
	r.snapshot = args.Data
	r.baseMembers = copyMembers(args.Members)
	r.commitIndex = lii
	r.lastApplied = lii
	r.pendingSnapshot = true
	r.recomputeMembersLocked()
	r.persistSnapshotLocked()
	r.metrics.snapshotsInstalled.Inc()
	r.applyCond.Broadcast()
	r.logf("installed snapshot at index %d from leader %d", lii, args.LeaderID)
	return nil
}

// acceptLeaderLocked handles the term check shared by leader RPCs. It returns
// false if the sender is stale; otherwise this node follows the sender.
func (r *Raft) acceptLeaderLocked(term, leader int) bool {
	if term < r.currentTerm {
		return false
	}
	if term > r.currentTerm {
		r.stepDownLocked(term)
	} else if r.state == Candidate {
		r.state = Follower // someone else won this term
	}
	r.leaderID = leader
	r.lastLeaderContact = time.Now()
	r.resetElectionTimerLocked()
	return true
}

func (r *Raft) hearingFromLeaderLocked() bool {
	if r.state == Leader {
		return true
	}
	return r.leaderID != -1 && time.Since(r.lastLeaderContact) < ElectionTimeoutMin
}

// ---- Elections ----

func (r *Raft) ticker() {
	t := time.NewTicker(10 * time.Millisecond)
	defer t.Stop()
	for {
		select {
		case <-r.stopCh:
			return
		case <-t.C:
		}
		r.mu.Lock()
		now := time.Now()
		if r.state == Leader {
			if now.Sub(r.lastBroadcast) >= HeartbeatInterval {
				r.broadcastLocked()
			}
		} else if now.After(r.electionDeadline) {
			if _, member := r.members[r.id]; member {
				r.startElectionLocked(false)
			} else {
				r.resetElectionTimerLocked() // joining or removed: never campaign
			}
		}
		r.mu.Unlock()
	}
}

func (r *Raft) startElectionLocked(transfer bool) {
	r.state = Candidate
	r.currentTerm++
	r.votedFor = r.id
	r.leaderID = -1
	r.persistLocked()
	r.resetElectionTimerLocked()
	r.metrics.electionsStarted.Inc()
	r.logf("election timeout, starting election for term %d", r.currentTerm)

	term := r.currentTerm
	args := &RequestVoteArgs{
		Term:         term,
		CandidateID:  r.id,
		LastLogIndex: r.lastIndex(),
		LastLogTerm:  r.lastTerm(),
		Transfer:     transfer,
	}
	votes := 1
	if votes > len(r.members)/2 {
		r.becomeLeaderLocked()
		return
	}
	for p := range r.members {
		if p == r.id {
			continue
		}
		go func(p int) {
			var reply RequestVoteReply
			if err := r.transport.RequestVote(p, args, &reply); err != nil {
				return
			}
			r.mu.Lock()
			defer r.mu.Unlock()
			if reply.Term > r.currentTerm {
				r.stepDownLocked(reply.Term)
				return
			}
			if r.state != Candidate || r.currentTerm != term || !reply.VoteGranted {
				return
			}
			if _, member := r.members[p]; !member {
				return // the configuration changed under us
			}
			votes++
			if votes > len(r.members)/2 {
				r.becomeLeaderLocked()
			}
		}(p)
	}
}

func (r *Raft) becomeLeaderLocked() {
	r.state = Leader
	r.leaderID = r.id
	r.metrics.leadershipsWon.Inc()
	r.logf("became leader for term %d", r.currentTerm)

	// A no-op entry from the new term lets entries from earlier terms commit (§5.4.2, §8).
	r.log = append(r.log, LogEntry{Term: r.currentTerm})
	r.persistLocked()
	r.nextIndex, r.matchIndex = map[int]int{}, map[int]int{}
	for p := range r.members {
		r.nextIndex[p] = r.lastIndex()
		r.matchIndex[p] = 0
	}
	r.peerAck = map[int]uint64{}
	r.matchIndex[r.id] = r.lastIndex()
	r.advanceCommitLocked()
	r.broadcastLocked()
}

// stepDownLocked reverts to follower, adopting term if it is newer than ours.
func (r *Raft) stepDownLocked(term int) {
	if term > r.currentTerm {
		r.currentTerm = term
		r.votedFor = -1
		r.leaderID = -1
		r.persistLocked()
	}
	if r.state != Follower {
		r.logf("stepping down to follower in term %d", r.currentTerm)
	}
	r.state = Follower
	r.failReadsLocked()
}

func (r *Raft) resetElectionTimerLocked() {
	spread := int64(ElectionTimeoutMax - ElectionTimeoutMin)
	r.electionDeadline = time.Now().Add(ElectionTimeoutMin + time.Duration(rand.Int63n(spread)))
}

// ---- Replication ----

func (r *Raft) broadcastLocked() {
	r.lastBroadcast = time.Now()
	for _, ch := range r.replCh {
		signal(ch)
	}
}

// signal wakes a goroutine waiting on ch. Signals that arrive while it is busy
// coalesce into one, which is what turns a burst of appends into one batch.
func signal(ch chan struct{}) {
	select {
	case ch <- struct{}{}:
	default:
	}
}

func (r *Raft) replicator(peer int, wake, stop <-chan struct{}) {
	for {
		select {
		case <-r.stopCh:
			return
		case <-stop:
			return // peer left the cluster
		case <-wake:
		}
		r.replicateTo(peer)
	}
}

// replicateTo sends one AppendEntries to peer, carrying every entry it is missing
// (up to maxEntriesPerRPC), or an empty heartbeat if it is up to date. A peer
// that needs entries already compacted away gets the snapshot instead.
func (r *Raft) replicateTo(peer int) {
	r.mu.Lock()
	if r.state != Leader || r.killed() {
		r.mu.Unlock()
		return
	}
	if r.nextIndex[peer] <= r.snapIndex {
		r.sendSnapshot(peer) // unlocks
		return
	}
	term, round := r.currentTerm, r.hbRound
	prev := r.nextIndex[peer] - 1
	end := min(r.lastIndex(), prev+maxEntriesPerRPC)
	entries := make([]LogEntry, end-prev)
	copy(entries, r.log[prev+1-r.snapIndex:end+1-r.snapIndex])
	args := &AppendEntriesArgs{
		Term:         term,
		LeaderID:     r.id,
		PrevLogIndex: prev,
		PrevLogTerm:  r.termAt(prev),
		Entries:      entries,
		LeaderCommit: r.commitIndex,
	}
	r.mu.Unlock()
	if len(entries) > 0 {
		r.metrics.batchEntries.Observe(float64(len(entries)))
	}

	var reply AppendEntriesReply
	if err := r.transport.AppendEntries(peer, args, &reply); err != nil {
		return // unreachable; the next heartbeat retries
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	if reply.Term > r.currentTerm {
		r.stepDownLocked(reply.Term)
		return
	}
	if r.state != Leader || r.currentTerm != term {
		return
	}
	r.ackLocked(peer, round) // any reply in our term confirms leadership

	if reply.Success {
		r.recordMatchLocked(peer, prev+len(entries))
		return
	}

	next := reply.ConflictIndex
	if reply.ConflictTerm >= 0 {
		for i := r.lastIndex(); i > r.snapIndex && r.termAt(i) >= reply.ConflictTerm; i-- {
			if r.termAt(i) == reply.ConflictTerm {
				next = i + 1
				break
			}
		}
	}
	next = max(next, r.matchIndex[peer]+1, 1)
	r.nextIndex[peer] = min(next, r.lastIndex()+1)
	if ch, ok := r.replCh[peer]; ok {
		signal(ch)
	}
}

// sendSnapshot is called with mu held and releases it.
func (r *Raft) sendSnapshot(peer int) {
	term, round := r.currentTerm, r.hbRound
	args := &InstallSnapshotArgs{
		Term:              term,
		LeaderID:          r.id,
		LastIncludedIndex: r.snapIndex,
		LastIncludedTerm:  r.termAt(r.snapIndex),
		Members:           copyMembers(r.baseMembers),
		Data:              r.snapshot,
	}
	r.mu.Unlock()
	r.metrics.snapshotsSent.Inc()

	var reply InstallSnapshotReply
	if err := r.transport.InstallSnapshot(peer, args, &reply); err != nil {
		return
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	if reply.Term > r.currentTerm {
		r.stepDownLocked(reply.Term)
		return
	}
	if r.state == Leader && r.currentTerm == term {
		r.ackLocked(peer, round)
		r.recordMatchLocked(peer, args.LastIncludedIndex)
	}
}

func (r *Raft) recordMatchLocked(peer, match int) {
	if match > r.matchIndex[peer] {
		r.matchIndex[peer] = match
	}
	if match+1 > r.nextIndex[peer] {
		r.nextIndex[peer] = match + 1
	}
	r.advanceCommitLocked()
	if ch, ok := r.replCh[peer]; ok && r.nextIndex[peer] <= r.lastIndex() {
		signal(ch) // more batches to send
	}
}

// advanceCommitLocked commits the highest index replicated on a majority of the
// current configuration. Only entries from the current term are committed by
// counting replicas (§5.4.2).
func (r *Raft) advanceCommitLocked() {
	for n := r.lastIndex(); n > r.commitIndex && r.termAt(n) == r.currentTerm; n-- {
		count := 0
		for p := range r.members {
			if r.matchIndex[p] >= n {
				count++
			}
		}
		if count > len(r.members)/2 {
			r.commitIndex = n
			r.applyCond.Broadcast()
			break
		}
	}
	// A leader that removed itself keeps leading until that change commits,
	// then steps down (thesis §4.2.2).
	if _, member := r.members[r.id]; !member && r.state == Leader && r.commitIndex >= r.configIndex {
		r.logf("removed from the cluster; stepping down")
		r.state = Follower
		r.leaderID = -1
		r.failReadsLocked()
	}
}

func (r *Raft) applier() {
	for {
		r.mu.Lock()
		for !r.pendingSnapshot && r.lastApplied >= r.commitIndex && !r.killed() {
			r.applyCond.Wait()
		}
		if r.killed() {
			r.mu.Unlock()
			return
		}

		var msgs []ApplyMsg
		if r.pendingSnapshot {
			r.pendingSnapshot = false
			msgs = []ApplyMsg{{
				SnapshotValid: true,
				Snapshot:      r.snapshot,
				Index:         r.snapIndex,
				Term:          r.termAt(r.snapIndex),
			}}
		} else {
			// lastApplied >= snapIndex always holds, so these entries are all in the log.
			for i := r.lastApplied + 1; i <= r.commitIndex; i++ {
				e := r.entry(i)
				m := ApplyMsg{Index: i, Term: e.Term, Command: e.Command}
				if e.Kind == EntryConfig {
					m.Command = nil // the service sees membership changes as no-ops
				}
				msgs = append(msgs, m)
			}
			r.lastApplied = r.commitIndex
		}
		r.mu.Unlock()

		for _, m := range msgs {
			select {
			case r.applyCh <- m:
			case <-r.stopCh:
				return
			}
		}
	}
}

// ---- Persistence ----

type persistentState struct {
	CurrentTerm int
	VotedFor    int
	SnapIndex   int
	BaseMembers map[int]string // configuration as of SnapIndex
	Log         []LogEntry
}

// snapshotFile is what reaches disk for a snapshot. Its index and term let
// restore reconcile a crash between writing the snapshot and the state.
type snapshotFile struct {
	Index   int
	Term    int
	Members map[int]string
	Data    []byte
}

// persistLocked durably saves the current state before returning. It is used for
// term and vote changes and on followers, where the reply must not be sent
// until the state is on disk.
func (r *Raft) persistLocked() {
	if r.killed() {
		return
	}
	data, version := r.encodeLocked()
	r.save(data, nil, version)
}

// persistSnapshotLocked saves the state together with a new snapshot.
func (r *Raft) persistSnapshotLocked() {
	if r.killed() {
		return
	}
	data, version := r.encodeLocked()
	r.save(data, encode(snapshotFile{r.snapIndex, r.termAt(r.snapIndex), r.baseMembers, r.snapshot}), version)
}

// flusher is the group-commit loop: each pass saves everything the leader has
// appended so far in one write, while new appends queue up for the next pass.
func (r *Raft) flusher() {
	for {
		select {
		case <-r.stopCh:
			return
		case <-r.flushCh:
		}
		r.mu.Lock()
		if r.killed() {
			r.mu.Unlock()
			return
		}
		data, version := r.encodeLocked()
		term, durable := r.currentTerm, r.lastIndex()
		r.mu.Unlock()

		r.save(data, nil, version) // slow part, done without holding mu

		r.mu.Lock()
		// The log is never truncated during a leader's own term, so everything up
		// to durable is now on disk and the leader may count itself for it.
		if r.state == Leader && r.currentTerm == term && durable > r.matchIndex[r.id] {
			r.matchIndex[r.id] = durable
			r.advanceCommitLocked()
		}
		r.mu.Unlock()
	}
}

func (r *Raft) encodeLocked() ([]byte, uint64) {
	r.stateVersion++
	return encode(persistentState{r.currentTerm, r.votedFor, r.snapIndex, r.baseMembers, r.log}), r.stateVersion
}

func (r *Raft) save(state, snapshot []byte, version uint64) {
	r.persistMu.Lock()
	defer r.persistMu.Unlock()
	if version <= r.savedVersion {
		return // newer state already reached disk
	}
	// Continuing without durable state could break safety, so failing to save is fatal.
	start := time.Now()
	if err := r.persister.Save(state, snapshot); err != nil {
		if r.killed() {
			return // shutting down; storage may already be closed
		}
		panic(fmt.Sprintf("raft: persist state: %v", err))
	}
	r.metrics.fsyncSeconds.Observe(time.Since(start).Seconds())
	r.savedVersion = version
}

func (r *Raft) restore() error {
	data, snapData, err := r.persister.Load()
	if err != nil {
		return err
	}
	if len(data) > 0 {
		var st persistentState
		if err := gob.NewDecoder(bytes.NewReader(data)).Decode(&st); err != nil {
			return fmt.Errorf("raft: decode state: %w", err)
		}
		r.currentTerm, r.votedFor, r.snapIndex, r.log = st.CurrentTerm, st.VotedFor, st.SnapIndex, st.Log
		r.baseMembers = st.BaseMembers
	}
	if len(snapData) > 0 {
		var snap snapshotFile
		if err := gob.NewDecoder(bytes.NewReader(snapData)).Decode(&snap); err != nil {
			return fmt.Errorf("raft: decode snapshot: %w", err)
		}
		if snap.Index > r.snapIndex {
			// Crashed after writing the snapshot but before the state that trims the log.
			if snap.Index <= r.lastIndex() && r.termAt(snap.Index) == snap.Term {
				r.log = append([]LogEntry{{Term: snap.Term}}, r.log[snap.Index-r.snapIndex+1:]...)
			} else {
				r.log = []LogEntry{{Term: snap.Term}}
			}
			r.snapIndex = snap.Index
			r.baseMembers = snap.Members
		}
		r.snapshot = snap.Data
		r.commitIndex, r.lastApplied = r.snapIndex, r.snapIndex
		r.pendingSnapshot = true
	}
	if len(data) > 0 || len(snapData) > 0 {
		r.logf("restored term %d, snapshot at %d, log up to %d", r.currentTerm, r.snapIndex, r.lastIndex())
	}
	return nil
}

func encode(v any) []byte {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(v); err != nil {
		panic(fmt.Sprintf("raft: encode: %v", err))
	}
	return buf.Bytes()
}

// ---- Helpers ----

func (r *Raft) lastIndex() int       { return r.snapIndex + len(r.log) - 1 }
func (r *Raft) lastTerm() int        { return r.log[len(r.log)-1].Term }
func (r *Raft) termAt(i int) int     { return r.log[i-r.snapIndex].Term }
func (r *Raft) entry(i int) LogEntry { return r.log[i-r.snapIndex] }

func (r *Raft) logf(format string, args ...any) {
	if r.logger != nil {
		r.logger.Printf(format, args...)
	}
}
