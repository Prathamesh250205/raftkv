package raft

import (
	"os"
	"path/filepath"
	"sync"
)

// Persister stores Raft's durable state (term, vote and log) and its latest
// snapshot. A nil snapshot in Save means "unchanged", so the common case of
// persisting new log entries doesn't rewrite the snapshot.
type Persister interface {
	Save(state, snapshot []byte) error
	Load() (state, snapshot []byte, err error)
}

// FilePersister writes each file atomically: write a temp file, fsync, rename.
// A crash mid-write leaves the previous version intact. The snapshot is written
// before the state, and Raft's restore copes with a crash between the two.
type FilePersister struct {
	statePath    string
	snapshotPath string
}

func NewFilePersister(dir string) (*FilePersister, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	return &FilePersister{
		statePath:    filepath.Join(dir, "raft-state.bin"),
		snapshotPath: filepath.Join(dir, "snapshot.bin"),
	}, nil
}

func (p *FilePersister) Save(state, snapshot []byte) error {
	if snapshot != nil {
		if err := writeAtomic(p.snapshotPath, snapshot); err != nil {
			return err
		}
	}
	return writeAtomic(p.statePath, state)
}

func (p *FilePersister) Load() (state, snapshot []byte, err error) {
	if state, err = readIfExists(p.statePath); err != nil {
		return nil, nil, err
	}
	if snapshot, err = readIfExists(p.snapshotPath); err != nil {
		return nil, nil, err
	}
	return state, snapshot, nil
}

func writeAtomic(path string, data []byte) error {
	tmp := path + ".tmp"
	f, err := os.Create(tmp)
	if err != nil {
		return err
	}
	if _, err := f.Write(data); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

func readIfExists(path string) ([]byte, error) {
	data, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return nil, nil
	}
	return data, err
}

// MemPersister keeps state in memory; used by tests to simulate crashes and restarts.
type MemPersister struct {
	mu       sync.Mutex
	state    []byte
	snapshot []byte
}

func (p *MemPersister) Save(state, snapshot []byte) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.state = append([]byte(nil), state...)
	if snapshot != nil {
		p.snapshot = append([]byte(nil), snapshot...)
	}
	return nil
}

func (p *MemPersister) Load() (state, snapshot []byte, err error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]byte(nil), p.state...), append([]byte(nil), p.snapshot...), nil
}

// Size reports the bytes held, so tests can check that compaction bounds it.
func (p *MemPersister) Size() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.state)
}

func (p *MemPersister) Copy() *MemPersister {
	state, snapshot, _ := p.Load()
	return &MemPersister{state: state, snapshot: snapshot}
}
