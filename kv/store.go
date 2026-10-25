// Package kv is a linearizable key-value store replicated with Raft. Every
// operation, reads included, goes through the Raft log, so a client never sees
// stale data even right after a leader change.
package kv

import (
	"bytes"
	"crypto/rand"
	"encoding/gob"
	"encoding/hex"
	"encoding/json"
	"errors"
	"log"
	"sync"
	"time"

	"raftkv/raft"
)

type Op string

const (
	OpGet    Op = "get"
	OpPut    Op = "put"
	OpAppend Op = "append"
	OpDelete Op = "delete"
)

type Command struct {
	Op    Op     `json:"op"`
	Key   string `json:"key"`
	Value string `json:"value,omitempty"`
	// Optional client session. A retried write with the same (ClientID, Seq)
	// is applied at most once.
	ClientID string `json:"client_id,omitempty"`
	Seq      uint64 `json:"seq,omitempty"`
	// Identifies this proposal so its submitter can tell whether the entry that
	// committed at its log index is really its own.
	ReqID string `json:"req_id"`
}

type Result struct {
	Value string
	Found bool
}

var (
	ErrNotLeader      = errors.New("kv: not the leader")
	ErrLeadershipLost = errors.New("kv: leadership lost before the request committed")
	ErrTimeout        = errors.New("kv: request timed out")
)

type applied struct {
	reqID string
	res   Result
}

type session struct {
	Seq uint64
	Res Result
}

// snapshot is the store's entire replicated state, as handed to Raft for compaction.
type snapshot struct {
	Data     map[string]string
	Sessions map[string]session
}

type Store struct {
	raft          *raft.Raft
	timeout       time.Duration
	snapshotEvery int
	// LogReads sends reads through the Raft log instead of ReadIndex. Slower;
	// kept for benchmarking the difference.
	LogReads bool

	mu           sync.Mutex
	appliedCond  *sync.Cond // signalled whenever lastApplied advances
	data         map[string]string
	sessions     map[string]session
	waiters      map[int]chan applied // log index -> submitter waiting for it
	lastApplied  int
	lastSnapshot int
}

// NewStore starts applying committed entries from applyCh. Every snapshotEvery
// applied entries it hands Raft a snapshot so the log can be compacted; 0
// disables snapshots.
func NewStore(r *raft.Raft, applyCh <-chan raft.ApplyMsg, snapshotEvery int) *Store {
	s := &Store{
		raft:          r,
		timeout:       2 * time.Second,
		snapshotEvery: snapshotEvery,
		data:          map[string]string{},
		sessions:      map[string]session{},
		waiters:       map[int]chan applied{},
	}
	s.appliedCond = sync.NewCond(&s.mu)
	go func() {
		for msg := range applyCh {
			s.apply(msg)
		}
	}()
	return s
}

// Get performs a linearizable read. It confirms leadership with ReadIndex,
// waits until the local state has caught up to that index, and reads from
// memory, so no log entry or fsync is needed.
func (s *Store) Get(key string) (Result, error) {
	if s.LogReads {
		return s.Do(Command{Op: OpGet, Key: key})
	}
	idx, err := s.raft.ReadIndex(s.timeout)
	switch {
	case errors.Is(err, raft.ErrLeaderNotReady):
		return s.Do(Command{Op: OpGet, Key: key}) // fresh leader: the log path is still safe
	case errors.Is(err, raft.ErrNotLeader):
		return Result{}, ErrNotLeader
	case err != nil:
		return Result{}, ErrTimeout
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	deadline := time.Now().Add(s.timeout)
	timer := time.AfterFunc(s.timeout, func() {
		s.mu.Lock()
		s.appliedCond.Broadcast()
		s.mu.Unlock()
	})
	defer timer.Stop()
	for s.lastApplied < idx {
		if time.Now().After(deadline) {
			return Result{}, ErrTimeout
		}
		s.appliedCond.Wait()
	}
	v, ok := s.data[key]
	return Result{Value: v, Found: ok}, nil
}

// Do replicates cmd through Raft and returns its result once committed and applied.
func (s *Store) Do(cmd Command) (Result, error) {
	cmd.ReqID = newID()
	data, err := json.Marshal(cmd)
	if err != nil {
		return Result{}, err
	}

	// Hold s.mu across Start so the apply loop can't consume this index
	// before the waiter is registered.
	s.mu.Lock()
	idx, _, ok := s.raft.Start(data)
	if !ok {
		s.mu.Unlock()
		return Result{}, ErrNotLeader
	}
	if old, ok := s.waiters[idx]; ok {
		old <- applied{} // an earlier proposal at this index was overwritten by a new leader
	}
	ch := make(chan applied, 1)
	s.waiters[idx] = ch
	s.mu.Unlock()

	select {
	case a := <-ch:
		if a.reqID != cmd.ReqID {
			return Result{}, ErrLeadershipLost
		}
		return a.res, nil
	case <-time.After(s.timeout):
		s.mu.Lock()
		if s.waiters[idx] == ch {
			delete(s.waiters, idx)
		}
		s.mu.Unlock()
		return Result{}, ErrTimeout
	}
}

func (s *Store) apply(msg raft.ApplyMsg) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if msg.SnapshotValid {
		s.installSnapshotLocked(msg)
		return
	}
	if msg.Index <= s.lastApplied {
		return // already covered by a snapshot
	}
	s.lastApplied = msg.Index
	s.appliedCond.Broadcast()

	var cmd Command
	var res Result
	if len(msg.Command) > 0 { // empty means a leader no-op
		if err := json.Unmarshal(msg.Command, &cmd); err != nil {
			log.Printf("kv: skipping undecodable entry %d: %v", msg.Index, err)
		} else {
			res = s.executeLocked(cmd)
		}
	}
	if ch, ok := s.waiters[msg.Index]; ok {
		ch <- applied{reqID: cmd.ReqID, res: res}
		delete(s.waiters, msg.Index)
	}

	if s.snapshotEvery > 0 && msg.Index-s.lastSnapshot >= s.snapshotEvery {
		var buf bytes.Buffer
		if err := gob.NewEncoder(&buf).Encode(snapshot{s.data, s.sessions}); err != nil {
			log.Printf("kv: encode snapshot: %v", err)
			return
		}
		s.raft.Snapshot(msg.Index, buf.Bytes())
		s.lastSnapshot = msg.Index
	}
}

func (s *Store) installSnapshotLocked(msg raft.ApplyMsg) {
	if msg.Index <= s.lastApplied {
		return
	}
	var snap snapshot
	if err := gob.NewDecoder(bytes.NewReader(msg.Snapshot)).Decode(&snap); err != nil {
		log.Printf("kv: decode snapshot at %d: %v", msg.Index, err)
		return
	}
	s.data, s.sessions = snap.Data, snap.Sessions
	if s.data == nil {
		s.data = map[string]string{}
	}
	if s.sessions == nil {
		s.sessions = map[string]session{}
	}
	s.lastApplied, s.lastSnapshot = msg.Index, msg.Index
	s.appliedCond.Broadcast()

	// Proposals at indices the snapshot covers can't be matched to a result any
	// more. Their submitters get ErrLeadershipLost and retry; sessions keep the
	// retry from applying a write twice.
	for idx, ch := range s.waiters {
		if idx <= msg.Index {
			ch <- applied{}
			delete(s.waiters, idx)
		}
	}
}

func (s *Store) executeLocked(cmd Command) Result {
	dedup := cmd.ClientID != "" && cmd.Op != OpGet
	if dedup {
		if sess, ok := s.sessions[cmd.ClientID]; ok && cmd.Seq <= sess.Seq {
			return sess.Res
		}
	}

	var res Result
	switch cmd.Op {
	case OpGet:
		res.Value, res.Found = s.data[cmd.Key]
	case OpPut:
		s.data[cmd.Key] = cmd.Value
		res = Result{Value: cmd.Value, Found: true}
	case OpAppend:
		s.data[cmd.Key] += cmd.Value
		res = Result{Value: s.data[cmd.Key], Found: true}
	case OpDelete:
		_, res.Found = s.data[cmd.Key]
		delete(s.data, cmd.Key)
	}

	if dedup {
		s.sessions[cmd.ClientID] = session{Seq: cmd.Seq, Res: res}
	}
	return res
}

func newID() string {
	b := make([]byte, 12)
	rand.Read(b)
	return hex.EncodeToString(b)
}
