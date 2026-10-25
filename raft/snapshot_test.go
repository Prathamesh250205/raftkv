package raft

import "testing"

func TestSnapshotBoundsLog(t *testing.T) {
	c := newSnapshotCluster(t, 3, 10)
	for i := 0; i < 100; i++ {
		c.one(cmd("compact", i), 3)
	}
	for i := 0; i < 3; i++ {
		st := c.node(i).Status()
		if st.SnapshotIndex == 0 {
			t.Fatalf("node %d never took a snapshot", i)
		}
		if kept := st.LastLogIndex - st.SnapshotIndex; kept > 20 {
			t.Fatalf("node %d still holds %d log entries after compaction", i, kept)
		}
	}
}

func TestSnapshotInstallOnLaggingFollower(t *testing.T) {
	c := newSnapshotCluster(t, 3, 10)
	c.one(cmd("before", 0), 3)

	// While the follower is away, the others compact past everything it has,
	// so it can only catch up through InstallSnapshot.
	lagger := (c.checkOneLeader() + 1) % 3
	c.disconnect(lagger)
	for i := 0; i < 60; i++ {
		c.one(cmd("while-away", i), 2)
	}
	c.connect(lagger)
	last := c.one(cmd("after", 0), 3)

	if st := c.node(lagger).Status(); st.SnapshotIndex == 0 || st.LastApplied < last {
		t.Fatalf("lagging follower did not catch up via snapshot: %+v", st)
	}
	if n, _ := c.nCommitted(5); n != 3 {
		t.Fatalf("entry 5 is known to %d nodes, want 3 (snapshot contents lost?)", n)
	}
}

func TestSnapshotSurvivesRestart(t *testing.T) {
	c := newSnapshotCluster(t, 3, 10)
	for i := 0; i < 35; i++ {
		c.one(cmd("pre", i), 3)
	}
	for i := 0; i < 3; i++ {
		c.crash(i)
	}
	for i := 0; i < 3; i++ {
		c.start(i)
	}
	c.one(cmd("post", 0), 3)
	if n, got := c.nCommitted(3); n != 3 || got == nil {
		t.Fatalf("index 3 after restart: %d nodes, command %q", n, got)
	}
}

func TestSnapshotWithRepeatedPartitions(t *testing.T) {
	c := newSnapshotCluster(t, 5, 7)
	for round := 0; round < 6; round++ {
		victim := (c.checkOneLeader() + 1 + round) % 5
		c.disconnect(victim)
		for i := 0; i < 15; i++ {
			c.one(cmd("round", round*100+i), 4)
		}
		c.connect(victim)
		if round%2 == 1 {
			c.crash(victim)
			c.start(victim)
		}
	}
	c.one(cmd("final", 0), 5)
}
