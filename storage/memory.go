// Package storage provides durable backends for a Raft node: an in-memory one
// used by the test harness (it survives Crash/Restart because the harness keeps
// the object alive) and a file-backed one for real nodes (Stage 6).
package storage

import (
	"sync"

	"github.com/atharv-pdarshi/kvraft/raft"
)

// Memory is an in-memory implementation of raft.Storage. It does not touch
// disk, but it keeps state across a Node being stopped and a new Node being
// built from the same Memory object — which is exactly how the harness models
// a crash and restart.
type Memory struct {
	mu       sync.Mutex
	term     uint64
	votedFor string
	log      []raft.LogEntry // real entries at indices 1..N (log[0] here == index 1)
}

// NewMemory returns an empty in-memory storage.
func NewMemory() *Memory {
	return &Memory{}
}

func (m *Memory) SaveState(term uint64, votedFor string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.term = term
	m.votedFor = votedFor
	return nil
}

func (m *Memory) AppendLog(entries []raft.LogEntry) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.log = append(m.log, entries...)
	return nil
}

// TruncateSuffix removes entries with index >= from (from is 1-based).
func (m *Memory) TruncateSuffix(from uint64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if from < 1 {
		from = 1
	}
	keep := int(from - 1) // number of real entries to keep
	if keep < len(m.log) {
		m.log = append([]raft.LogEntry(nil), m.log[:keep]...)
	}
	return nil
}

func (m *Memory) Load() (uint64, string, []raft.LogEntry, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := append([]raft.LogEntry(nil), m.log...)
	return m.term, m.votedFor, out, nil
}
