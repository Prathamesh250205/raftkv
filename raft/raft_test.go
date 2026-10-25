package raft

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

func cmd(s string, i int) []byte { return []byte(fmt.Sprintf("%s-%d", s, i)) }

func TestInitialElection(t *testing.T) {
	c := newCluster(t, 3)
	leader := c.checkOneLeader()
	term := c.term(leader)

	// With a healthy network, heartbeats must suppress further elections.
	time.Sleep(2 * ElectionTimeoutMax)
	if got := c.term(leader); got != term {
		t.Fatalf("term changed from %d to %d without any failures", term, got)
	}
	if again := c.checkOneLeader(); again != leader {
		t.Fatalf("leader changed from %d to %d without any failures", leader, again)
	}
}

func TestReElection(t *testing.T) {
	c := newCluster(t, 3)
	leader1 := c.checkOneLeader()

	c.disconnect(leader1)
	leader2 := c.checkOneLeader()
	if leader2 == leader1 {
		t.Fatal("disconnected leader is still the only leader")
	}

	// The old leader rejoins and must step down rather than cause a split brain.
	c.connect(leader1)
	leader3 := c.checkOneLeader()

	// Without a majority, nobody can be elected.
	other := (leader3 + 1) % 3
	c.disconnect(leader3)
	c.disconnect(other)
	time.Sleep(2 * ElectionTimeoutMax)
	c.checkNoLeader()

	// Restoring a majority restores a leader.
	c.connect(other)
	c.checkOneLeader()
}

func TestBasicAgreement(t *testing.T) {
	c := newCluster(t, 3)
	prev := 0
	for i := 0; i < 10; i++ {
		idx := c.one(cmd("basic", i), 3)
		if idx <= prev {
			t.Fatalf("index went backwards: %d after %d", idx, prev)
		}
		prev = idx
	}
}

func TestSingleNodeCommits(t *testing.T) {
	// With no followers, commits depend entirely on the leader's own flush.
	c := newCluster(t, 1)
	for i := 0; i < 5; i++ {
		c.one(cmd("solo", i), 1)
	}
}

func TestConcurrentStarts(t *testing.T) {
	// A burst of proposals is group-committed in batches; every one must
	// still commit exactly once, on every node.
	c := newCluster(t, 3)
	leader := c.checkOneLeader()

	const n = 100
	var wg sync.WaitGroup
	indices := make(chan int, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			if idx, _, ok := c.node(leader).Start(cmd("burst", i)); ok {
				indices <- idx
			}
		}(i)
	}
	wg.Wait()
	close(indices)
	if len(indices) != n {
		t.Skipf("leadership changed mid-burst (%d of %d accepted)", len(indices), n)
	}

	seen := map[int]bool{}
	deadline := time.Now().Add(5 * time.Second)
	for idx := range indices {
		if seen[idx] {
			t.Fatalf("two proposals got index %d", idx)
		}
		seen[idx] = true
		for {
			if count, _ := c.nCommitted(idx); count == 3 {
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("index %d not committed on all 3 nodes", idx)
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
}

func TestAgreementWithFollowerDown(t *testing.T) {
	c := newCluster(t, 3)
	c.one(cmd("before", 0), 3)

	follower := (c.checkOneLeader() + 1) % 3
	c.disconnect(follower)
	for i := 0; i < 5; i++ {
		c.one(cmd("partitioned", i), 2) // a majority of 2/3 still commits
	}

	// The follower must catch up on everything it missed.
	c.connect(follower)
	c.one(cmd("after", 0), 3)
}

func TestNoCommitWithoutMajority(t *testing.T) {
	c := newCluster(t, 5)
	c.one(cmd("before", 0), 5)

	leader := c.checkOneLeader()
	for i := 1; i <= 3; i++ {
		c.disconnect((leader + i) % 5)
	}
	idx, _, ok := c.node(leader).Start(cmd("minority", 0))
	if !ok {
		t.Fatal("leader rejected Start")
	}
	time.Sleep(2 * ElectionTimeoutMax)
	if n, _ := c.nCommitted(idx); n > 0 {
		t.Fatalf("%d nodes committed index %d with only 2 of 5 connected", n, idx)
	}

	for i := 1; i <= 3; i++ {
		c.connect((leader + i) % 5)
	}
	c.one(cmd("after", 0), 5)
}

func TestDivergentLeaderRejoins(t *testing.T) {
	c := newCluster(t, 3)
	c.one(cmd("base", 0), 3)

	// An isolated leader accepts entries that can never commit...
	old := c.checkOneLeader()
	c.disconnect(old)
	for i := 0; i < 3; i++ {
		c.node(old).Start(cmd("orphan", i))
	}

	// ...while the majority elects a new leader and moves on.
	for i := 0; i < 3; i++ {
		c.one(cmd("majority", i), 2)
	}

	// On rejoining, the old leader's uncommitted entries must be overwritten.
	// cluster.record fails the test if any index ever diverges.
	c.connect(old)
	c.one(cmd("final", 0), 3)
}

func TestPersistAcrossRestart(t *testing.T) {
	c := newCluster(t, 3)
	first := c.one(cmd("durable", 0), 3)
	c.one(cmd("durable", 1), 3)

	for i := 0; i < 3; i++ {
		c.crash(i)
	}
	for i := 0; i < 3; i++ {
		c.start(i)
	}

	c.one(cmd("after-restart", 0), 3)
	// Committed entries survive the restart and are re-applied on every node.
	if n, got := c.nCommitted(first); n != 3 || string(got) != "durable-0" {
		t.Fatalf("index %d after restart: %d nodes, command %q", first, n, got)
	}
}

func TestLeaderCrashAndRestart(t *testing.T) {
	c := newCluster(t, 3)
	c.one(cmd("a", 0), 3)

	leader := c.checkOneLeader()
	c.crash(leader)
	c.one(cmd("b", 0), 2)

	c.start(leader)
	c.one(cmd("c", 0), 3)
}
