// Package kv holds the key-value state machine and the gRPC server that
// drives it through Raft. The Store itself knows nothing about Raft; it just
// applies Commands in the order it is handed them.
package kv

import (
	"sync"

	"google.golang.org/protobuf/proto"

	kvpb "github.com/atharv-pdarshi/kvraft/proto/kvpb"
)

// Store is a replicated map. Apply must be called with committed commands in
// log order so every replica reaches the same state.
type Store struct {
	mu   sync.RWMutex
	data map[string]string
}

// NewStore returns an empty Store.
func NewStore() *Store {
	return &Store{data: make(map[string]string)}
}

// ApplyResult is what a single command produced. For reads it carries the
// value; for writes Value/Found are zero.
type ApplyResult struct {
	Value string
	Found bool
}

// Apply runs one command against the state machine and returns its result.
// Writes mutate the map; reads only observe it.
func (s *Store) Apply(cmd *kvpb.Command) ApplyResult {
	s.mu.Lock()
	defer s.mu.Unlock()

	switch cmd.Op {
	case kvpb.Command_PUT:
		s.data[cmd.Key] = cmd.Value
		return ApplyResult{}
	case kvpb.Command_DELETE:
		delete(s.data, cmd.Key)
		return ApplyResult{}
	case kvpb.Command_GET:
		v, ok := s.data[cmd.Key]
		return ApplyResult{Value: v, Found: ok}
	default:
		return ApplyResult{}
	}
}

// Get reads a key directly. Used for local reads and tests; linearizable reads
// still go through Apply via the log.
func (s *Store) Get(key string) (string, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	v, ok := s.data[key]
	return v, ok
}

// Len reports how many keys are stored.
func (s *Store) Len() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.data)
}

// Snapshot returns a copy of the whole map, used by tests to check that every
// replica's state machine converged.
func (s *Store) Snapshot() map[string]string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make(map[string]string, len(s.data))
	for k, v := range s.data {
		out[k] = v
	}
	return out
}

// EncodeCommand serializes a Command into the bytes carried by the Raft log.
func EncodeCommand(cmd *kvpb.Command) ([]byte, error) {
	return proto.Marshal(cmd)
}

// DecodeCommand parses the bytes stored in a Raft log entry back into a Command.
func DecodeCommand(b []byte) (*kvpb.Command, error) {
	cmd := &kvpb.Command{}
	if err := proto.Unmarshal(b, cmd); err != nil {
		return nil, err
	}
	return cmd, nil
}
