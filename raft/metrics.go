package raft

import "raftkv/metrics"

// raftMetrics are the counters Raft updates as it runs.
type raftMetrics struct {
	electionsStarted   metrics.Counter
	leadershipsWon     metrics.Counter
	proposals          metrics.Counter
	snapshotsTaken     metrics.Counter
	snapshotsInstalled metrics.Counter
	snapshotsSent      metrics.Counter
	readIndexOK        metrics.Counter
	readIndexFailed    metrics.Counter
	fsyncSeconds       *metrics.Histogram
	batchEntries       *metrics.Histogram // entries per non-empty AppendEntries
}

func (m *raftMetrics) init() {
	m.fsyncSeconds = metrics.NewHistogram(metrics.ExponentialBuckets(0.0005, 2, 14)...)
	m.batchEntries = metrics.NewHistogram(1, 2, 4, 8, 16, 32, 64, 128, 256, 512)
}

// RegisterMetrics exposes this node's Raft state and counters on reg.
func (r *Raft) RegisterMetrics(reg *metrics.Registry, labels metrics.Labels) {
	status := func(f func(Status) float64) func() float64 {
		return func() float64 { return f(r.Status()) }
	}
	reg.Gauge("raft_term", "Current Raft term.", labels, status(func(s Status) float64 { return float64(s.Term) }))
	reg.Gauge("raft_is_leader", "1 if this node is the leader.", labels, status(func(s Status) float64 {
		if s.State == Leader.String() {
			return 1
		}
		return 0
	}))
	reg.Gauge("raft_commit_index", "Highest committed log index.", labels, status(func(s Status) float64 { return float64(s.CommitIndex) }))
	reg.Gauge("raft_last_applied", "Highest log index handed to the service.", labels, status(func(s Status) float64 { return float64(s.LastApplied) }))
	reg.Gauge("raft_last_log_index", "Index of the last log entry.", labels, status(func(s Status) float64 { return float64(s.LastLogIndex) }))
	reg.Gauge("raft_snapshot_index", "Last index covered by the snapshot.", labels, status(func(s Status) float64 { return float64(s.SnapshotIndex) }))
	reg.Gauge("raft_log_entries", "Entries held in the log after compaction.", labels, status(func(s Status) float64 {
		return float64(s.LastLogIndex - s.SnapshotIndex)
	}))

	m := &r.metrics
	reg.Counter("raft_elections_started_total", "Elections this node started.", labels, &m.electionsStarted)
	reg.Counter("raft_leaderships_won_total", "Elections this node won.", labels, &m.leadershipsWon)
	reg.Counter("raft_proposals_total", "Entries proposed to this node as leader.", labels, &m.proposals)
	reg.Counter("raft_snapshots_taken_total", "Snapshots taken locally.", labels, &m.snapshotsTaken)
	reg.Counter("raft_snapshots_installed_total", "Snapshots received from a leader.", labels, &m.snapshotsInstalled)
	reg.Counter("raft_snapshots_sent_total", "Snapshots sent to lagging followers.", labels, &m.snapshotsSent)
	reg.Counter("raft_read_index_total", "ReadIndex requests by outcome.", labels.With("result", "ok"), &m.readIndexOK)
	reg.Counter("raft_read_index_total", "ReadIndex requests by outcome.", labels.With("result", "failed"), &m.readIndexFailed)
	reg.Histogram("raft_fsync_seconds", "Time to durably save Raft state.", labels, m.fsyncSeconds)
	reg.Histogram("raft_append_batch_entries", "Entries per AppendEntries RPC that carried entries.", labels, m.batchEntries)
}
