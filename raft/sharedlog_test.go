package raft

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

func TestSharedLogRoundTrip(t *testing.T) {
	dir := t.TempDir()
	l, err := OpenSharedLog(dir)
	if err != nil {
		t.Fatal(err)
	}
	a, b := l.Persister(0), l.Persister(1)
	a.Save([]byte("a-state-1"), []byte("a-snap"))
	a.Save([]byte("a-state-2"), nil) // nil keeps the previous snapshot
	b.Save([]byte("b-state"), nil)
	l.Close()

	l, err = OpenSharedLog(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	check := func(p Persister, wantState, wantSnap string) {
		t.Helper()
		state, snap, _ := p.Load()
		if string(state) != wantState || string(snap) != wantSnap {
			t.Fatalf("got (%q, %q), want (%q, %q)", state, snap, wantState, wantSnap)
		}
	}
	check(l.Persister(0), "a-state-2", "a-snap")
	check(l.Persister(1), "b-state", "")
	check(l.Persister(2), "", "")
}

func TestSharedLogDiscardsTornTail(t *testing.T) {
	dir := t.TempDir()
	l, _ := OpenSharedLog(dir)
	l.Persister(0).Save([]byte("good"), nil)
	l.Persister(0).Save([]byte("this record will be cut short"), nil)
	l.Close()

	// Simulate a crash mid-write: chop the last few bytes off.
	path := filepath.Join(dir, "raft.wal")
	info, _ := os.Stat(path)
	if err := os.Truncate(path, info.Size()-5); err != nil {
		t.Fatal(err)
	}

	l, err := OpenSharedLog(dir)
	if err != nil {
		t.Fatal(err)
	}
	state, _, _ := l.Persister(0).Load()
	if string(state) != "good" {
		t.Fatalf("after torn tail, state = %q, want %q", state, "good")
	}
	// The log keeps working after the torn tail is dropped.
	l.Persister(0).Save([]byte("after"), nil)
	l.Close()
	l, _ = OpenSharedLog(dir)
	defer l.Close()
	if state, _, _ := l.Persister(0).Load(); string(state) != "after" {
		t.Fatalf("state = %q, want %q", state, "after")
	}
}

func TestSharedLogCompacts(t *testing.T) {
	dir := t.TempDir()
	l, _ := OpenSharedLog(dir)
	big := bytes.Repeat([]byte("x"), 64<<10)
	for i := 0; i < 400; i++ { // ~25 MB written, ~128 KB live
		l.Persister(i % 2).Save(append([]byte(fmt.Sprint(i)), big...), nil)
	}
	l.Close()

	info, _ := os.Stat(filepath.Join(dir, "raft.wal"))
	if info.Size() > compactMinSize+(4<<20) {
		t.Fatalf("log is %d bytes; compaction should keep it near its live size", info.Size())
	}
	l, _ = OpenSharedLog(dir)
	defer l.Close()
	for g, want := range map[int]string{0: "398", 1: "399"} {
		state, _, _ := l.Persister(g).Load()
		if !bytes.HasPrefix(state, []byte(want)) {
			t.Fatalf("group %d state starts %q, want %q", g, state[:3], want)
		}
	}
}

func TestSharedLogBatchesConcurrentSaves(t *testing.T) {
	l, _ := OpenSharedLog(t.TempDir())
	defer l.Close()
	var wg sync.WaitGroup
	for g := 0; g < 16; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < 20; i++ {
				if err := l.Persister(g).Save([]byte(fmt.Sprintf("%d-%d", g, i)), nil); err != nil {
					t.Error(err)
				}
			}
		}(g)
	}
	wg.Wait()
	for g := 0; g < 16; g++ {
		if state, _, _ := l.Persister(g).Load(); string(state) != fmt.Sprintf("%d-19", g) {
			t.Fatalf("group %d: %q", g, state)
		}
	}
}
