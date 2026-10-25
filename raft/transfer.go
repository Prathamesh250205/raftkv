package raft

import "errors"

// Leadership transfer (thesis §3.10): the leader waits until the target's log
// matches its own, then tells it to start an election at once. The target
// wins quickly because its log is as up to date as anyone's, and its vote
// requests carry Transfer so followers don't ignore them as disruptive.

var ErrTransferNotReady = errors.New("raft: transfer target is still catching up")

type TimeoutNowArgs struct {
	Term     int
	LeaderID int
}

type TimeoutNowReply struct {
	Term int
}

// TransferLeadership asks target to take over as leader. It returns
// ErrTransferNotReady if target's log is behind; replication is nudged so a
// retry shortly after usually succeeds.
func (r *Raft) TransferLeadership(target int) error {
	r.mu.Lock()
	if r.state != Leader || r.killed() {
		r.mu.Unlock()
		return ErrNotLeader
	}
	if target == r.id {
		r.mu.Unlock()
		return nil
	}
	if _, ok := r.members[target]; !ok {
		r.mu.Unlock()
		return ErrNotMember
	}
	if r.matchIndex[target] < r.lastIndex() {
		if ch, ok := r.replCh[target]; ok {
			signal(ch)
		}
		r.mu.Unlock()
		return ErrTransferNotReady
	}
	args := &TimeoutNowArgs{Term: r.currentTerm, LeaderID: r.id}
	r.mu.Unlock()

	var reply TimeoutNowReply
	if err := r.transport.TimeoutNow(target, args, &reply); err != nil {
		return err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if reply.Term > r.currentTerm {
		r.stepDownLocked(reply.Term)
	}
	r.logf("handed leadership to node %d", target)
	return nil
}

func (r *Raft) HandleTimeoutNow(args *TimeoutNowArgs, reply *TimeoutNowReply) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.killed() {
		return ErrStopped
	}
	reply.Term = r.currentTerm
	if args.Term != r.currentTerm || r.state == Leader {
		return nil // stale request
	}
	if _, member := r.members[r.id]; !member {
		return nil
	}
	r.startElectionLocked(true)
	return nil
}
