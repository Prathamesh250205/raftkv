// Package kv is a linearizable key-value store replicated with Raft. Every
// operation, reads included, goes through the Raft log, so a client never sees
// stale data even right after a leader change.
package kv

import (
	"crypto/rand"
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
	seq uint64
	res Result
}

type Store struct {
	raft    *raft.Raft
	timeout time.Duration

	mu       sync.Mutex
	data     map[string]string
	sessions map[string]session
	waiters  map[int]chan applied // log index -> submitter waiting for it
}

func NewStore(r *raft.Raft, applyCh <-chan raft.ApplyMsg) *Store {
	s := &Store{
		raft:     r,
		timeout:  2 * time.Second,
		data:     map[string]string{},
		sessions: map[string]session{},
		waiters:  map[int]chan applied{},
	}
	go func() {
		for msg := range applyCh {
			s.apply(msg)
		}
	}()
	return s
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
}

func (s *Store) executeLocked(cmd Command) Result {
	dedup := cmd.ClientID != "" && cmd.Op != OpGet
	if dedup {
		if sess, ok := s.sessions[cmd.ClientID]; ok && cmd.Seq <= sess.seq {
			return sess.res
		}
	}

	var res Result
	switch cmd.Op {
	case OpGet:
		res.Value, res.Found = s.data[cmd.Key]
	case OpPut:
		s.data[cmd.Key] = cmd.Value
		res = Result{Value: cmd.Value, Found: true}
	case OpDelete:
		_, res.Found = s.data[cmd.Key]
		delete(s.data, cmd.Key)
	}

	if dedup {
		s.sessions[cmd.ClientID] = session{seq: cmd.Seq, res: res}
	}
	return res
}

func newID() string {
	b := make([]byte, 12)
	rand.Read(b)
	return hex.EncodeToString(b)
}
