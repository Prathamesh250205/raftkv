package raft

import (
	"errors"
	"testing"
	"time"
)

func TestTransferLeadership(t *testing.T) {
	c := newCluster(t, 3)
	c.one(cmd("before", 0), 3)
	old := c.checkOneLeader()
	target := (old + 1) % 3

	deadline := time.Now().Add(5 * time.Second)
	for {
		err := c.node(old).TransferLeadership(target)
		if err == nil {
			break
		}
		if !errors.Is(err, ErrTransferNotReady) || time.Now().After(deadline) {
			t.Fatalf("TransferLeadership: %v", err)
		}
		time.Sleep(20 * time.Millisecond)
	}
	for c.checkOneLeader() != target {
		if time.Now().After(deadline) {
			t.Fatalf("leader is %d, want transfer target %d", c.checkOneLeader(), target)
		}
	}
	c.one(cmd("after", 0), 3)
}

func TestTransferToLaggingNodeWaits(t *testing.T) {
	c := newCluster(t, 3)
	c.one(cmd("before", 0), 3)
	leader := c.checkOneLeader()
	lagger := (leader + 1) % 3
	c.disconnect(lagger)
	c.one(cmd("missed", 0), 2)
	if err := c.node(leader).TransferLeadership(lagger); !errors.Is(err, ErrTransferNotReady) {
		t.Fatalf("transfer to a lagging node: got %v, want ErrTransferNotReady", err)
	}
	if c.checkOneLeader() != leader {
		t.Fatal("leadership moved to a node that was behind")
	}
}
