package raft

import (
	"os"
	"path/filepath"
	"sync"
)

// Persister stores Raft's durable state (term, vote and log).
type Persister interface {
	Save(data []byte) error
	Load() ([]byte, error)
}

// FilePersister writes state to disk atomically: write a temp file, fsync, rename.
// A crash mid-write leaves the previous state intact.
type FilePersister struct {
	path string
}

func NewFilePersister(dir string) (*FilePersister, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	return &FilePersister{path: filepath.Join(dir, "raft-state.bin")}, nil
}

func (p *FilePersister) Save(data []byte) error {
	tmp := p.path + ".tmp"
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
	return os.Rename(tmp, p.path)
}

func (p *FilePersister) Load() ([]byte, error) {
	data, err := os.ReadFile(p.path)
	if os.IsNotExist(err) {
		return nil, nil
	}
	return data, err
}

// MemPersister keeps state in memory; used by tests to simulate crashes and restarts.
type MemPersister struct {
	mu   sync.Mutex
	data []byte
}

func (p *MemPersister) Save(data []byte) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.data = append([]byte(nil), data...)
	return nil
}

func (p *MemPersister) Load() ([]byte, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]byte(nil), p.data...), nil
}

func (p *MemPersister) Copy() *MemPersister {
	data, _ := p.Load()
	return &MemPersister{data: data}
}
