package raft

import (
	"errors"
	"testing"
	"time"
)

func TestReadIndexOnHealthyLeader(t *testing.T) {
	c := newCluster(t, 3)
	idx := c.one(cmd("x", 0), 3)
	leader := c.checkOneLeader()

	got, err := c.node(leader).ReadIndex(time.Second)
	if err != nil {
		t.Fatalf("ReadIndex on healthy leader: %v", err)
	}
	if got < idx {
		t.Fatalf("ReadIndex = %d, but %d is already committed", got, idx)
	}

	follower := (leader + 1) % 3
	if _, err := c.node(follower).ReadIndex(time.Second); !errors.Is(err, ErrNotLeader) {
		t.Fatalf("ReadIndex on follower: got %v, want ErrNotLeader", err)
	}
}

func TestReadIndexRefusedByDeposedLeader(t *testing.T) {
	// A leader cut off from the majority still believes it leads. It must not
	// confirm a read, or a client could see data older than a later write.
	c := newCluster(t, 5)
	c.one(cmd("before", 0), 5)

	old := c.checkOneLeader()
	c.disconnect(old)
	c.one(cmd("newer-write", 0), 4)

	if c.node(old).Status().State != Leader.String() {
		t.Skip("old leader already noticed it was deposed")
	}
	if _, err := c.node(old).ReadIndex(500 * time.Millisecond); err == nil {
		t.Fatal("isolated leader confirmed a read; it could return stale data")
	}
}

func TestReadIndexAfterNewLeaderCommits(t *testing.T) {
	c := newCluster(t, 3)
	c.one(cmd("a", 0), 3)
	c.disconnect(c.checkOneLeader())

	leader := c.checkOneLeader()
	idx := c.one(cmd("b", 0), 2)
	got, err := c.node(leader).ReadIndex(time.Second)
	if err != nil || got < idx {
		t.Fatalf("ReadIndex = %d, %v; want >= %d", got, err, idx)
	}
}
