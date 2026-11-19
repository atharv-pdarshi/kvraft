package raft

// This file holds the receiver side of the two RPCs. Stage 2 implements only
// the universal rule that is true in every box of Figure 2 — "a higher term
// means step down" — and otherwise rejects. Stages 3 and 4 fill in the vote
// granting and log handling.

// RequestVote is the receiver side of the RequestVote RPC.
func (n *Node) RequestVote(args *RequestVoteArgs) *RequestVoteReply {
	n.mu.Lock()
	defer n.mu.Unlock()

	reply := &RequestVoteReply{}

	// 1. Reply false if term < currentTerm.
	if args.Term < n.currentTerm {
		reply.Term = n.currentTerm
		reply.VoteGranted = false
		return reply
	}
	// If we see a newer term, adopt it and step down.
	if args.Term > n.currentTerm {
		n.becomeFollower(args.Term)
	}
	reply.Term = n.currentTerm
	reply.VoteGranted = false // Stage 3 decides whether to grant.
	return reply
}

// AppendEntries is the receiver side of the AppendEntries RPC.
func (n *Node) AppendEntries(args *AppendEntriesArgs) *AppendEntriesReply {
	n.mu.Lock()
	defer n.mu.Unlock()

	reply := &AppendEntriesReply{ConflictTerm: -1}

	// Reply false if term < currentTerm.
	if args.Term < n.currentTerm {
		reply.Term = n.currentTerm
		reply.Success = false
		return reply
	}
	if args.Term > n.currentTerm {
		n.becomeFollower(args.Term)
	}
	reply.Term = n.currentTerm
	reply.Success = false // Stage 4 implements log matching.
	return reply
}
