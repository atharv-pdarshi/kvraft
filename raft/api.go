package raft

// Start proposes a new command. On the leader it appends the command to the
// log and returns the index it will occupy, the current term and true. On a
// follower it returns isLeader == false and the caller should retry elsewhere.
//
// Stage 2 is a skeleton with no replication, so it always reports "not leader";
// Stage 4 implements the real append.
func (n *Node) Start(command []byte) (index uint64, term uint64, isLeader bool) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.role != Leader {
		return 0, n.currentTerm, false
	}
	return 0, n.currentTerm, false
}

// Status is a snapshot of a node's state for diagnostics (the Status RPC).
type Status struct {
	ID          string
	Term        uint64
	Role        string
	Leader      string
	CommitIndex uint64
	LastApplied uint64
}

// Status returns a consistent snapshot of this node's state.
func (n *Node) Status() Status {
	n.mu.Lock()
	defer n.mu.Unlock()
	return Status{
		ID:          n.id,
		Term:        n.currentTerm,
		Role:        n.role.String(),
		Leader:      n.leaderID,
		CommitIndex: n.commitIndex,
		LastApplied: n.lastApplied,
	}
}
