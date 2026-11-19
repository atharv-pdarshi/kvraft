package raft

// This file holds the receiver side of the two RPCs. Stage 3 implements the
// full RequestVote rules and enough of AppendEntries to accept heartbeats and
// keep followers' timers reset. Stage 4 fills in AppendEntries log matching.

// RequestVote is the receiver side of the RequestVote RPC (Figure 2).
func (n *Node) RequestVote(args *RequestVoteArgs) *RequestVoteReply {
	n.mu.Lock()
	defer n.mu.Unlock()

	reply := &RequestVoteReply{}

	// 1. Reply false if term < currentTerm.
	if args.Term < n.currentTerm {
		reply.Term = n.currentTerm
		return reply
	}
	// If the candidate's term is newer, adopt it and step down (clears our vote).
	if args.Term > n.currentTerm {
		n.becomeFollower(args.Term)
	}
	reply.Term = n.currentTerm

	// 2. Grant iff we haven't voted for someone else this term AND the
	//    candidate's log is at least as up to date as ours.
	canVote := n.votedFor == "" || n.votedFor == args.CandidateID
	if canVote && n.candidateUpToDate(args.LastLogTerm, args.LastLogIndex) {
		n.votedFor = args.CandidateID
		n.persistState()
		n.resetElectionTimer() // only reset when we actually grant
		reply.VoteGranted = true
		n.logger.Printf("term %d: granted vote to %s", n.currentTerm, args.CandidateID)
	}
	return reply
}

// candidateUpToDate reports whether a candidate's last log entry is at least as
// up to date as ours: a higher last term wins, or an equal last term with an
// index at least as large. Caller holds n.mu.
func (n *Node) candidateUpToDate(lastLogTerm, lastLogIndex uint64) bool {
	myTerm := n.lastLogTerm()
	myIndex := n.lastLogIndex()
	if lastLogTerm != myTerm {
		return lastLogTerm > myTerm
	}
	return lastLogIndex >= myIndex
}

// AppendEntries is the receiver side of the AppendEntries RPC (Figure 2),
// including the fast-backtracking conflict hints.
func (n *Node) AppendEntries(args *AppendEntriesArgs) *AppendEntriesReply {
	n.mu.Lock()
	defer n.mu.Unlock()

	reply := &AppendEntriesReply{ConflictTerm: -1}

	// 1. Reply false if term < currentTerm.
	if args.Term < n.currentTerm {
		reply.Term = n.currentTerm
		return reply
	}
	// A valid leader for our term (or a newer one): adopt term and follow.
	if args.Term > n.currentTerm {
		n.becomeFollower(args.Term)
	}
	n.role = Follower
	n.leaderID = args.LeaderID
	n.resetElectionTimer()
	reply.Term = n.currentTerm

	// 2. Reply false if our log is too short to hold prevLogIndex.
	if args.PrevLogIndex > n.lastLogIndex() {
		reply.ConflictIndex = n.lastLogIndex() + 1
		reply.ConflictTerm = -1
		return reply
	}
	// 3. Reply false if the terms at prevLogIndex disagree, and report the
	//    first index of our conflicting term so the leader can skip it whole.
	if args.PrevLogIndex >= 1 && n.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		ct := n.log[args.PrevLogIndex].Term
		reply.ConflictTerm = int64(ct)
		ci := args.PrevLogIndex
		for ci > 1 && n.log[ci-1].Term == ct {
			ci--
		}
		reply.ConflictIndex = ci
		return reply
	}

	// 4. prevLog matches. Splice in the new entries, deleting an existing
	//    suffix only where it actually conflicts — never when it already
	//    matches, so a delayed old AppendEntries can't erase newer entries.
	for i := range args.Entries {
		idx := args.PrevLogIndex + 1 + uint64(i)
		if idx <= n.lastLogIndex() {
			if n.log[idx].Term == args.Entries[i].Term {
				continue // already present and matching
			}
			// Conflict: drop this entry and everything after it.
			n.log = n.log[:idx]
			if err := n.storage.TruncateSuffix(idx); err != nil {
				n.logger.Printf("WARN truncate: %v", err)
			}
		}
		rest := append([]LogEntry(nil), args.Entries[i:]...)
		n.log = append(n.log, rest...)
		if err := n.storage.AppendLog(rest); err != nil {
			n.logger.Printf("WARN append log: %v", err)
		}
		break
	}

	// 5. Advance the commit index toward the leader's.
	if args.LeaderCommit > n.commitIndex {
		last := args.PrevLogIndex + uint64(len(args.Entries))
		n.commitIndex = min64(args.LeaderCommit, last)
		n.applyCond.Signal()
	}

	reply.Success = true
	return reply
}

func min64(a, b uint64) uint64 {
	if a < b {
		return a
	}
	return b
}
