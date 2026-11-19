package raft

// Start proposes a new command. On the leader it appends the command to the
// log, persists it, kicks off replication and returns the index it will occupy,
// the current term and true. On a follower it returns isLeader == false and the
// caller should retry elsewhere.
func (n *Node) Start(command []byte) (index uint64, term uint64, isLeader bool) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.role != Leader {
		return 0, n.currentTerm, false
	}
	entry := LogEntry{Term: n.currentTerm, Command: append([]byte(nil), command...)}
	n.log = append(n.log, entry)
	if err := n.storage.AppendLog([]LogEntry{entry}); err != nil {
		n.logger.Printf("WARN append log: %v", err)
	}
	index = n.lastLogIndex()
	term = n.currentTerm
	// Replicate immediately rather than waiting for the next heartbeat.
	go n.broadcastAppendEntries()
	return index, term, true
}

// LogView returns a copy of this node's log (including the dummy index 0). It
// exists so cluster tests can assert that every replica holds identical logs.
func (n *Node) LogView() []LogEntry {
	n.mu.Lock()
	defer n.mu.Unlock()
	return append([]LogEntry(nil), n.log...)
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
