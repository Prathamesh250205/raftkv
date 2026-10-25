package raft

import (
	"errors"
	"fmt"
	"testing"
	"time"
)

// change applies a membership change through whichever node is leader,
// retrying across elections, and waits for it to commit there.
func (c *cluster) change(f func(r *Raft) (int, error)) {
	c.t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		leader := c.checkOneLeader()
		r := c.node(leader)
		idx, err := f(r)
		if errors.Is(err, ErrNotLeader) || errors.Is(err, ErrLeaderNotReady) || errors.Is(err, ErrConfigInProgress) {
			time.Sleep(50 * time.Millisecond)
			continue
		}
		if err != nil {
			c.t.Fatalf("membership change: %v", err)
		}
		for time.Now().Before(deadline) {
			if st := r.Status(); st.CommitIndex >= idx {
				return
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
	c.t.Fatal("membership change did not commit")
}

func addr(id int) string { return fmt.Sprintf("node-%d", id) }

func TestAddMember(t *testing.T) {
	c := newGrowingCluster(t, 4, 3)
	c.one(cmd("before", 0), 3)

	c.change(func(r *Raft) (int, error) { return r.AddMember(3, addr(3)) })
	c.one(cmd("after-add", 0), 4) // the new node receives and applies it

	if got := len(c.node(3).Status().Members); got != 4 {
		t.Fatalf("new node sees %d members, want 4", got)
	}
	// With 4 members a majority is 3, so losing one node is still fine.
	c.crash((c.checkOneLeader() + 1) % 4)
	c.one(cmd("one-down", 0), 3)
}

func TestRemoveFollowerDoesNotDisrupt(t *testing.T) {
	c := newCluster(t, 5)
	c.one(cmd("before", 0), 5)
	leader := c.checkOneLeader()
	gone := []int{(leader + 1) % 5, (leader + 2) % 5}
	for _, id := range gone {
		c.change(func(r *Raft) (int, error) { return r.RemoveMember(id) })
	}
	c.one(cmd("three-left", 0), 3)

	// Removed nodes may never learn they were removed. They will time out and
	// campaign with higher terms; the remaining members must ignore them.
	leader = c.checkOneLeader()
	term := c.term(leader)
	time.Sleep(4 * ElectionTimeoutMax)
	if now := c.checkOneLeader(); now != leader || c.term(now) != term {
		t.Fatalf("removed nodes disrupted the cluster: leader %d term %d -> leader %d term %d",
			leader, term, now, c.term(now))
	}
	c.one(cmd("still-fine", 0), 3)
}

func TestLeaderRemovesItself(t *testing.T) {
	c := newCluster(t, 3)
	c.one(cmd("before", 0), 3)
	old := c.checkOneLeader()
	c.change(func(r *Raft) (int, error) { return r.RemoveMember(old) })

	deadline := time.Now().Add(5 * time.Second)
	for {
		if l := c.checkOneLeader(); l != old {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("removed leader is still leading")
		}
	}
	c.one(cmd("after", 0), 2)
	if c.node(old).Status().State == Leader.String() {
		t.Fatal("removed node became leader again")
	}
}

func TestOneMembershipChangeAtATime(t *testing.T) {
	c := newGrowingCluster(t, 5, 3)
	c.one(cmd("before", 0), 3)
	leader := c.checkOneLeader()
	for i := 0; i < 3; i++ {
		if i != leader {
			c.disconnect(i) // the next change can't commit
		}
	}
	if _, err := c.node(leader).AddMember(3, addr(3)); err != nil {
		t.Fatalf("first change: %v", err)
	}
	if _, err := c.node(leader).AddMember(4, addr(4)); !errors.Is(err, ErrConfigInProgress) {
		t.Fatalf("second concurrent change: got %v, want ErrConfigInProgress", err)
	}
}

func TestRollingReplacementOfEveryNode(t *testing.T) {
	// Replace founders 0,1,2 with 3,4,5, one change at a time, while
	// snapshots are on so newcomers learn the configuration from them.
	c := newGrowingCluster(t, 6, 3)
	c.mu.Lock()
	c.snapEvery = 10
	c.mu.Unlock()
	for i := 0; i < 15; i++ {
		c.one(cmd("warmup", i), 3)
	}
	for step := 0; step < 3; step++ {
		newID, oldID := step+3, step
		c.change(func(r *Raft) (int, error) { return r.AddMember(newID, addr(newID)) })
		for i := 0; i < 12; i++ {
			c.one(cmd(fmt.Sprintf("step%d-add", step), i), 3)
		}
		c.change(func(r *Raft) (int, error) { return r.RemoveMember(oldID) })
		c.one(cmd(fmt.Sprintf("step%d-remove", step), 0), 3)
	}
	for i := 0; i < 3; i++ {
		c.crash(i) // founders are gone for good
	}
	last := c.one(cmd("final", 0), 3)
	for id := 3; id < 6; id++ {
		st := c.node(id).Status()
		if len(st.Members) != 3 || st.Members[0] != "" || st.LastApplied < last {
			t.Fatalf("node %d: members %v, applied %d (want %d)", id, st.Members, st.LastApplied, last)
		}
	}
}

func TestMembershipSurvivesRestart(t *testing.T) {
	c := newGrowingCluster(t, 4, 3)
	c.one(cmd("before", 0), 3)
	c.change(func(r *Raft) (int, error) { return r.AddMember(3, addr(3)) })
	c.one(cmd("added", 0), 4)

	for i := 0; i < 4; i++ {
		c.crash(i)
	}
	for i := 0; i < 4; i++ {
		c.start(i) // founders restart with their old bootstrap list; disk must win
	}
	c.one(cmd("after-restart", 0), 4)
	if got := len(c.node(0).Status().Members); got != 4 {
		t.Fatalf("after restart node 0 sees %d members, want 4", got)
	}
}
