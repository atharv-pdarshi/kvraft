package raft

import "context"

// Transport is how a node reaches its peers. The in-memory Network (package
// transport) and the gRPC transport (Stage 8) both implement it, so the exact
// same Raft code runs under test and in production.
//
// The "from" node is implicit in the args (CandidateID / LeaderID), which is
// all the in-memory network needs to enforce partitions.
type Transport interface {
	SendRequestVote(ctx context.Context, to string, args *RequestVoteArgs) (*RequestVoteReply, error)
	SendAppendEntries(ctx context.Context, to string, args *AppendEntriesArgs) (*AppendEntriesReply, error)
}

// RPCHandler is the server side of the two RPCs. A Node implements it, and the
// transport dispatches incoming calls to it.
type RPCHandler interface {
	RequestVote(args *RequestVoteArgs) *RequestVoteReply
	AppendEntries(args *AppendEntriesArgs) *AppendEntriesReply
}
