package raft

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sync"
)

// SharedLog is one append-only file that persists many Raft groups at once.
//
// With a file per group, every group pays for its own fsync, and on one disk
// those fsyncs queue behind each other: six shards on one machine ran at a
// quarter of the throughput of one. A SharedLog collects saves from every
// group and makes them all durable with a single write and fsync (cross-group
// group commit), the way TiKV and CockroachDB share one log per node.
//
// Each record is a group's complete state or snapshot, so the latest record
// per (group, kind) is all that matters on restart. When the file grows to
// several times its live size it is compacted: the live records are copied
// to a new file, which atomically replaces the old one.
type SharedLog struct {
	path string

	mu      sync.Mutex
	cond    *sync.Cond
	f       *os.File
	size    int64
	live    map[recordKey][]byte // latest data per group and kind
	pending []*saveReq
	closed  bool
}

type recordKey struct {
	group int
	kind  byte
}

const (
	kindState    byte = 1
	kindSnapshot byte = 2

	recordHeader   = 13 // length(4) crc(4) group(4) kind(1)
	compactMinSize = 8 << 20
)

type saveReq struct {
	records []record
	done    chan error
}

type record struct {
	key  recordKey
	data []byte
}

// OpenSharedLog opens (or creates) the log at dir/raft.wal and reads back
// the latest state of every group. A torn record at the tail, left by a crash
// mid-write, is discarded.
func OpenSharedLog(dir string) (*SharedLog, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	l := &SharedLog{path: filepath.Join(dir, "raft.wal"), live: map[recordKey][]byte{}}
	l.cond = sync.NewCond(&l.mu)

	f, err := os.OpenFile(l.path, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, err
	}
	valid, err := l.replay(f)
	if err != nil {
		f.Close()
		return nil, err
	}
	if err := f.Truncate(valid); err != nil { // drop a torn tail
		f.Close()
		return nil, err
	}
	if _, err := f.Seek(valid, io.SeekStart); err != nil {
		f.Close()
		return nil, err
	}
	l.f, l.size = f, valid
	go l.syncer()
	return l, nil
}

func (l *SharedLog) replay(f *os.File) (int64, error) {
	r := bufio.NewReader(f)
	var offset int64
	hdr := make([]byte, recordHeader)
	for {
		if _, err := io.ReadFull(r, hdr); err != nil {
			return offset, nil // clean end, or a torn header
		}
		n := binary.LittleEndian.Uint32(hdr[0:])
		sum := binary.LittleEndian.Uint32(hdr[4:])
		key := recordKey{int(binary.LittleEndian.Uint32(hdr[8:])), hdr[12]}
		data := make([]byte, n)
		if _, err := io.ReadFull(r, data); err != nil || crc32.ChecksumIEEE(data) != sum {
			return offset, nil // torn or corrupt tail: everything before it is good
		}
		l.live[key] = data
		offset += recordHeader + int64(n)
	}
}

// Persister returns the Persister for Raft group id.
func (l *SharedLog) Persister(group int) Persister { return &sharedPersister{l, group} }

// Close stops the log. Saves still waiting fail.
func (l *SharedLog) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.closed = true
	l.cond.Broadcast()
	return l.f.Close()
}

func (l *SharedLog) save(records []record) error {
	req := &saveReq{records: records, done: make(chan error, 1)}
	l.mu.Lock()
	if l.closed {
		l.mu.Unlock()
		return errors.New("raft: shared log closed")
	}
	l.pending = append(l.pending, req)
	l.cond.Signal()
	l.mu.Unlock()
	return <-req.done
}

// syncer writes every pending save in one batch and fsyncs once for all of them.
func (l *SharedLog) syncer() {
	for {
		l.mu.Lock()
		for len(l.pending) == 0 && !l.closed {
			l.cond.Wait()
		}
		if l.closed {
			for _, req := range l.pending {
				req.done <- errors.New("raft: shared log closed")
			}
			l.mu.Unlock()
			return
		}
		batch := l.pending
		l.pending = nil
		l.mu.Unlock()

		err := l.writeBatch(batch)
		for _, req := range batch {
			req.done <- err
		}
	}
}

func (l *SharedLog) writeBatch(batch []*saveReq) error {
	var buf []byte
	for _, req := range batch {
		for _, rec := range req.records {
			buf = appendRecord(buf, rec)
		}
	}
	if _, err := l.f.Write(buf); err != nil {
		return err
	}
	if err := l.f.Sync(); err != nil {
		return err
	}

	l.mu.Lock()
	defer l.mu.Unlock()
	l.size += int64(len(buf))
	for _, req := range batch {
		for _, rec := range req.records {
			l.live[rec.key] = rec.data
		}
	}
	return l.maybeCompactLocked()
}

// maybeCompactLocked rewrites the file with only live records once it is
// mostly garbage. It runs on the syncer goroutine, so no write is in flight.
func (l *SharedLog) maybeCompactLocked() error {
	var liveSize int64
	for _, data := range l.live {
		liveSize += recordHeader + int64(len(data))
	}
	if l.size < compactMinSize || l.size < 4*liveSize {
		return nil
	}
	var buf []byte
	for key, data := range l.live {
		buf = appendRecord(buf, record{key, data})
	}
	tmp := l.path + ".tmp"
	if err := writeAtomic(tmp, buf); err != nil { // writes and fsyncs tmp
		return err
	}
	l.f.Close()
	if err := os.Rename(tmp, l.path); err != nil {
		return fmt.Errorf("raft: compact shared log: %w", err)
	}
	f, err := os.OpenFile(l.path, os.O_RDWR|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	l.f, l.size = f, int64(len(buf))
	return nil
}

func appendRecord(buf []byte, rec record) []byte {
	var hdr [recordHeader]byte
	binary.LittleEndian.PutUint32(hdr[0:], uint32(len(rec.data)))
	binary.LittleEndian.PutUint32(hdr[4:], crc32.ChecksumIEEE(rec.data))
	binary.LittleEndian.PutUint32(hdr[8:], uint32(rec.key.group))
	hdr[12] = rec.key.kind
	return append(append(buf, hdr[:]...), rec.data...)
}

type sharedPersister struct {
	log   *SharedLog
	group int
}

func (p *sharedPersister) Save(state, snapshot []byte) error {
	// The snapshot record comes first, matching FilePersister's write order.
	var recs []record
	if snapshot != nil {
		recs = append(recs, record{recordKey{p.group, kindSnapshot}, append([]byte(nil), snapshot...)})
	}
	recs = append(recs, record{recordKey{p.group, kindState}, append([]byte(nil), state...)})
	return p.log.save(recs)
}

func (p *sharedPersister) Load() (state, snapshot []byte, err error) {
	p.log.mu.Lock()
	defer p.log.mu.Unlock()
	return p.log.live[recordKey{p.group, kindState}], p.log.live[recordKey{p.group, kindSnapshot}], nil
}
