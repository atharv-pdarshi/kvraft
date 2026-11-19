package kv

import (
	"context"

	kvpb "github.com/atharv-pdarshi/kvraft/proto/kvpb"
)

// DirectServer is the Stage 1 server: a single-node KV store reachable over
// gRPC with no replication. It implements kvpb.KVServer by calling the Store
// directly. Later stages put a Raft log between the RPC and the Store; this one
// stays useful for single-node runs and smoke tests.
type DirectServer struct {
	kvpb.UnimplementedKVServer
	store *Store
	id    string
}

// NewDirectServer wraps a fresh Store.
func NewDirectServer(id string) *DirectServer {
	return &DirectServer{store: NewStore(), id: id}
}

func (s *DirectServer) Put(_ context.Context, req *kvpb.PutRequest) (*kvpb.PutResponse, error) {
	s.store.Apply(&kvpb.Command{Op: kvpb.Command_PUT, Key: req.Key, Value: req.Value})
	return &kvpb.PutResponse{Status: kvpb.Status_OK}, nil
}

func (s *DirectServer) Get(_ context.Context, req *kvpb.GetRequest) (*kvpb.GetResponse, error) {
	v, ok := s.store.Get(req.Key)
	return &kvpb.GetResponse{Status: kvpb.Status_OK, Value: v, Found: ok}, nil
}

func (s *DirectServer) Delete(_ context.Context, req *kvpb.DeleteRequest) (*kvpb.DeleteResponse, error) {
	s.store.Apply(&kvpb.Command{Op: kvpb.Command_DELETE, Key: req.Key})
	return &kvpb.DeleteResponse{Status: kvpb.Status_OK}, nil
}

func (s *DirectServer) GetStatus(_ context.Context, _ *kvpb.StatusRequest) (*kvpb.StatusResponse, error) {
	return &kvpb.StatusResponse{Id: s.id, Role: "Standalone"}, nil
}
