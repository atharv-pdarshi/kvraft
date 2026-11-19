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

// AppendEntries is the receiver side of the AppendEntries RPC. Stage 3 handles
// the term rules and timer reset; the log-matching body arrives in Stage 4.
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
	reply.Success = true
	return reply
}
