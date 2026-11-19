package kv

import (
	"context"
	"sync"
	"time"

	"github.com/atharv-pdarshi/kvraft/raft"
	kvpb "github.com/atharv-pdarshi/kvraft/proto/kvpb"
)

// applyTimeout bounds how long a request waits for its log index to be applied
// before it tells the client to retry.
const applyTimeout = 2 * time.Second

// Server is the Raft-backed key-value server. A client's OK means the write was
// committed by a majority and applied, so it survives failures. It implements
// kvpb.KVServer, so the same type is registered on gRPC (Stage 8) and called
// directly in tests.
type Server struct {
	kvpb.UnimplementedKVServer

	id    string
	rf    *raft.Node
	store *Store

	mu      sync.Mutex
	waiters map[uint64]chan applied // log index -> waiter
	lastSeq map[string]uint64       // client id -> highest applied write seq (dedup)

	stopCh chan struct{}
	wg     sync.WaitGroup
}

// applied is what the applier hands back to a waiting request: the command that
// actually landed at the index, and the result of running it.
type applied struct {
	cmd    *kvpb.Command
	result ApplyResult
}

// NewServer wires a Store to a Raft node and starts consuming its ApplyCh.
func NewServer(id string, rf *raft.Node) *Server {
	s := &Server{
		id:      id,
		rf:      rf,
		store:   NewStore(),
		waiters: make(map[uint64]chan applied),
		lastSeq: make(map[string]uint64),
		stopCh:  make(chan struct{}),
	}
	s.wg.Add(1)
	go s.applyLoop()
	return s
}

// Stop halts the applier.
func (s *Server) Stop() {
	close(s.stopCh)
	s.wg.Wait()
}

// Raft exposes the underlying node (used to register the Raft gRPC service).
func (s *Server) Raft() *raft.Node { return s.rf }

// Snapshot returns a copy of this server's state machine (for test invariants).
func (s *Server) Snapshot() map[string]string { return s.store.Snapshot() }

// --- client RPCs ---

func (s *Server) Put(ctx context.Context, req *kvpb.PutRequest) (*kvpb.PutResponse, error) {
	cmd := &kvpb.Command{Op: kvpb.Command_PUT, Key: req.Key, Value: req.Value, ClientId: req.ClientId, Seq: req.Seq}
	status, _, hint := s.propose(ctx, cmd)
	return &kvpb.PutResponse{Status: status, LeaderHint: hint}, nil
}

func (s *Server) Delete(ctx context.Context, req *kvpb.DeleteRequest) (*kvpb.DeleteResponse, error) {
	cmd := &kvpb.Command{Op: kvpb.Command_DELETE, Key: req.Key, ClientId: req.ClientId, Seq: req.Seq}
	status, _, hint := s.propose(ctx, cmd)
	return &kvpb.DeleteResponse{Status: status, LeaderHint: hint}, nil
}

func (s *Server) Get(ctx context.Context, req *kvpb.GetRequest) (*kvpb.GetResponse, error) {
	// Reads go through the log too, so they are linearizable: the value
	// returned reflects every write ordered before this Get.
	cmd := &kvpb.Command{Op: kvpb.Command_GET, Key: req.Key, ClientId: req.ClientId, Seq: req.Seq}
	status, res, hint := s.propose(ctx, cmd)
	return &kvpb.GetResponse{Status: status, Value: res.Value, Found: res.Found, LeaderHint: hint}, nil
}

func (s *Server) GetStatus(_ context.Context, _ *kvpb.StatusRequest) (*kvpb.StatusResponse, error) {
	st := s.rf.Status()
	return &kvpb.StatusResponse{
		Id:          st.ID,
		Term:        st.Term,
		Role:        st.Role,
		Leader:      st.Leader,
		CommitIndex: st.CommitIndex,
		LastApplied: st.LastApplied,
	}, nil
}

// propose submits a command to Raft and waits for it to be applied. It returns
// the status to report, the apply result (for reads) and a leader hint.
func (s *Server) propose(ctx context.Context, cmd *kvpb.Command) (kvpb.Status, ApplyResult, string) {
	data, err := EncodeCommand(cmd)
	if err != nil {
		return kvpb.Status_ERROR, ApplyResult{}, ""
	}
	index, _, isLeader := s.rf.Start(data)
	if !isLeader {
		return kvpb.Status_NOT_LEADER, ApplyResult{}, s.rf.Status().Leader
	}

	ch := make(chan applied, 1)
	s.mu.Lock()
	s.waiters[index] = ch
	s.mu.Unlock()
	defer func() {
		s.mu.Lock()
		delete(s.waiters, index)
		s.mu.Unlock()
	}()

	select {
	case out := <-ch:
		// If a different command landed at our index (leadership changed and the
		// entry was overwritten), tell the client to retry.
		if !sameRequest(out.cmd, cmd) {
			return kvpb.Status_WRONG_LEADER, ApplyResult{}, s.rf.Status().Leader
		}
		return kvpb.Status_OK, out.result, ""
	case <-time.After(applyTimeout):
		return kvpb.Status_TIMEOUT, ApplyResult{}, s.rf.Status().Leader
	case <-ctx.Done():
		return kvpb.Status_TIMEOUT, ApplyResult{}, s.rf.Status().Leader
	case <-s.stopCh:
		return kvpb.Status_ERROR, ApplyResult{}, ""
	}
}

// sameRequest reports whether the applied command is the one this request
// proposed, identified by client id + seq (unique per client operation).
func sameRequest(got, want *kvpb.Command) bool {
	if got == nil {
		return false
	}
	if want.ClientId != "" {
		return got.ClientId == want.ClientId && got.Seq == want.Seq
	}
	// No client id (e.g. internal): fall back to full field comparison.
	return got.Op == want.Op && got.Key == want.Key && got.Value == want.Value
}

// applyLoop consumes committed entries in order, applying each to the store
// (deduplicating retried writes) and waking any request waiting on that index.
func (s *Server) applyLoop() {
	defer s.wg.Done()
	ch := s.rf.ApplyCh()
	for {
		select {
		case <-s.stopCh:
			return
		case msg, ok := <-ch:
			if !ok {
				return
			}
			s.handleApply(msg)
		}
	}
}

func (s *Server) handleApply(msg raft.ApplyMsg) {
	cmd, err := DecodeCommand(msg.Command)
	if err != nil {
		return
	}

	var result ApplyResult
	switch cmd.Op {
	case kvpb.Command_PUT, kvpb.Command_DELETE:
		// Deduplicate: apply a write only if its seq is newer than the last we
		// applied for this client. A retried Put is then applied exactly once.
		if cmd.ClientId != "" && cmd.Seq != 0 && cmd.Seq <= s.lastSeq[cmd.ClientId] {
			// duplicate — skip the mutation but still report success
		} else {
			result = s.store.Apply(cmd)
			if cmd.ClientId != "" && cmd.Seq != 0 {
				s.lastSeq[cmd.ClientId] = cmd.Seq
			}
		}
	case kvpb.Command_GET:
		result = s.store.Apply(cmd)
	default:
		// no-op entry (empty command from a new leader): nothing to apply
	}

	s.mu.Lock()
	if w, ok := s.waiters[msg.Index]; ok {
		w <- applied{cmd: cmd, result: result}
	}
	s.mu.Unlock()
}
