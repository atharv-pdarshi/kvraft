// Package raft implements the Raft consensus algorithm (Ongaro & Ousterhout,
// 2014). It is deliberately free of anything about keys and values: the log
// carries opaque command bytes, so the algorithm can be tested on its own.
//
// The implementation follows Figure 2 of the paper box for box. Indices are
// 1-based; index 0 is a dummy entry (term 0) that is always present so that
// prevLogIndex == 0 has something to compare against.
package raft

// Role is a node's current role in the cluster.
type Role int

const (
	Follower Role = iota
	Candidate
	Leader
)

func (r Role) String() string {
	switch r {
	case Follower:
		return "Follower"
	case Candidate:
		return "Candidate"
	case Leader:
		return "Leader"
	default:
		return "Unknown"
	}
}

// LogEntry is one entry in the replicated log. Command is opaque to Raft.
type LogEntry struct {
	Term    uint64
	Command []byte
}

// ApplyMsg is handed to the state machine for each committed entry, in order,
// over Node.ApplyCh.
type ApplyMsg struct {
	Index   uint64
	Term    uint64
	Command []byte
}

// RequestVoteArgs / Reply — Figure 2, RequestVote RPC.
type RequestVoteArgs struct {
	Term         uint64
	CandidateID  string
	LastLogIndex uint64
	LastLogTerm  uint64
}

type RequestVoteReply struct {
	Term        uint64
	VoteGranted bool
}

// AppendEntriesArgs / Reply — Figure 2, AppendEntries RPC. ConflictIndex and
// ConflictTerm carry the fast-backtracking hints; ConflictTerm == -1 means the
// follower's log was simply too short.
type AppendEntriesArgs struct {
	Term         uint64
	LeaderID     string
	PrevLogIndex uint64
	PrevLogTerm  uint64
	Entries      []LogEntry
	LeaderCommit uint64
}

type AppendEntriesReply struct {
	Term          uint64
	Success       bool
	ConflictIndex uint64
	ConflictTerm  int64
}
