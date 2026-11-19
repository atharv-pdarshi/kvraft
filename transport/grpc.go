package transport

import (
	"context"
	"fmt"
	"sync"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	raftpb "github.com/atharv-pdarshi/kvraft/proto/raftpb"
	"github.com/atharv-pdarshi/kvraft/raft"
)

// GRPC is the production transport: it carries the same raft.Transport calls
// over gRPC. It keeps one lazily-dialed connection per peer and reuses it. The
// server half (RaftService) dispatches incoming RPCs to a node's handlers.
type GRPC struct {
	self  string
	peers map[string]string // id -> host:port

	mu      sync.Mutex
	clients map[string]raftpb.RaftClient
	conns   map[string]*grpc.ClientConn
}

// NewGRPC builds a transport for node self given the full id->address map.
func NewGRPC(self string, peers map[string]string) *GRPC {
	return &GRPC{
		self:    self,
		peers:   peers,
		clients: make(map[string]raftpb.RaftClient),
		conns:   make(map[string]*grpc.ClientConn),
	}
}

// client returns a (lazily dialed) RaftClient for a peer.
func (g *GRPC) client(to string) (raftpb.RaftClient, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if c, ok := g.clients[to]; ok {
		return c, nil
	}
	addr, ok := g.peers[to]
	if !ok {
		return nil, fmt.Errorf("transport: unknown peer %q", to)
	}
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	c := raftpb.NewRaftClient(conn)
	g.conns[to] = conn
	g.clients[to] = c
	return c, nil
}

// Close shuts every peer connection.
func (g *GRPC) Close() {
	g.mu.Lock()
	defer g.mu.Unlock()
	for _, conn := range g.conns {
		conn.Close()
	}
}

// SendRequestVote implements raft.Transport.
func (g *GRPC) SendRequestVote(ctx context.Context, to string, args *raft.RequestVoteArgs) (*raft.RequestVoteReply, error) {
	c, err := g.client(to)
	if err != nil {
		return nil, err
	}
	resp, err := c.RequestVote(ctx, &raftpb.RequestVoteArgs{
		Term:         args.Term,
		CandidateId:  args.CandidateID,
		LastLogIndex: args.LastLogIndex,
		LastLogTerm:  args.LastLogTerm,
	})
	if err != nil {
		return nil, err
	}
	return &raft.RequestVoteReply{Term: resp.Term, VoteGranted: resp.VoteGranted}, nil
}

// SendAppendEntries implements raft.Transport.
func (g *GRPC) SendAppendEntries(ctx context.Context, to string, args *raft.AppendEntriesArgs) (*raft.AppendEntriesReply, error) {
	c, err := g.client(to)
	if err != nil {
		return nil, err
	}
	resp, err := c.AppendEntries(ctx, &raftpb.AppendEntriesArgs{
		Term:         args.Term,
		LeaderId:     args.LeaderID,
		PrevLogIndex: args.PrevLogIndex,
		PrevLogTerm:  args.PrevLogTerm,
		Entries:      toPbEntries(args.Entries),
		LeaderCommit: args.LeaderCommit,
	})
	if err != nil {
		return nil, err
	}
	return &raft.AppendEntriesReply{
		Term:          resp.Term,
		Success:       resp.Success,
		ConflictIndex: resp.ConflictIndex,
		ConflictTerm:  resp.ConflictTerm,
	}, nil
}

// RaftService is the gRPC server side: it adapts raftpb requests to a node's
// RPCHandler and back.
type RaftService struct {
	raftpb.UnimplementedRaftServer
	h raft.RPCHandler
}

// RegisterRaftServer registers a node's handler on a gRPC server.
func RegisterRaftServer(s *grpc.Server, h raft.RPCHandler) {
	raftpb.RegisterRaftServer(s, &RaftService{h: h})
}

func (r *RaftService) RequestVote(_ context.Context, in *raftpb.RequestVoteArgs) (*raftpb.RequestVoteReply, error) {
	reply := r.h.RequestVote(&raft.RequestVoteArgs{
		Term:         in.Term,
		CandidateID:  in.CandidateId,
		LastLogIndex: in.LastLogIndex,
		LastLogTerm:  in.LastLogTerm,
	})
	return &raftpb.RequestVoteReply{Term: reply.Term, VoteGranted: reply.VoteGranted}, nil
}

func (r *RaftService) AppendEntries(_ context.Context, in *raftpb.AppendEntriesArgs) (*raftpb.AppendEntriesReply, error) {
	reply := r.h.AppendEntries(&raft.AppendEntriesArgs{
		Term:         in.Term,
		LeaderID:     in.LeaderId,
		PrevLogIndex: in.PrevLogIndex,
		PrevLogTerm:  in.PrevLogTerm,
		Entries:      fromPbEntries(in.Entries),
		LeaderCommit: in.LeaderCommit,
	})
	return &raftpb.AppendEntriesReply{
		Term:          reply.Term,
		Success:       reply.Success,
		ConflictIndex: reply.ConflictIndex,
		ConflictTerm:  reply.ConflictTerm,
	}, nil
}

func toPbEntries(in []raft.LogEntry) []*raftpb.LogEntry {
	if len(in) == 0 {
		return nil
	}
	out := make([]*raftpb.LogEntry, len(in))
	for i, e := range in {
		out[i] = &raftpb.LogEntry{Term: e.Term, Command: e.Command}
	}
	return out
}

func fromPbEntries(in []*raftpb.LogEntry) []raft.LogEntry {
	if len(in) == 0 {
		return nil
	}
	out := make([]raft.LogEntry, len(in))
	for i, e := range in {
		out[i] = raft.LogEntry{Term: e.Term, Command: e.Command}
	}
	return out
}
