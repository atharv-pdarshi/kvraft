package raft

import (
	"context"
	"testing"
	"time"
)

// nopTransport is a transport whose peers never answer — enough to let a single
// node's RPC handlers be driven directly in a unit test.
type nopTransport struct{}

func (nopTransport) SendRequestVote(context.Context, string, *RequestVoteArgs) (*RequestVoteReply, error) {
	return nil, context.DeadlineExceeded
}
func (nopTransport) SendAppendEntries(context.Context, string, *AppendEntriesArgs) (*AppendEntriesReply, error) {
	return nil, context.DeadlineExceeded
}

// memStore is a tiny in-package in-memory Storage so the raft package can test
// persistence without importing the storage package (which would be a cycle).
type memStore struct {
	term    uint64
	vote    string
	entries []LogEntry
}

func (m *memStore) SaveState(term uint64, vote string) error { m.term, m.vote = term, vote; return nil }
func (m *memStore) AppendLog(e []LogEntry) error             { m.entries = append(m.entries, e...); return nil }
func (m *memStore) TruncateSuffix(from uint64) error {
	if from < 1 {
		from = 1
	}
	if int(from-1) < len(m.entries) {
		m.entries = m.entries[:from-1]
	}
	return nil
}
func (m *memStore) Load() (uint64, string, []LogEntry, error) {
	return m.term, m.vote, append([]LogEntry(nil), m.entries...), nil
}

// TestPersistVote: a node grants a vote, "crashes" and restarts from the same
// storage in the same term, and must refuse a different candidate — because its
// vote was persisted.
func TestPersistVote(t *testing.T) {
	store := &memStore{}
	cfg := Config{
		ID:        "node1",
		Peers:     []string{"node2", "node3"},
		Transport: nopTransport{},
		Storage:   store,
		// Long timeout so the node under test doesn't start its own election.
		ElectionTimeoutMin: 5 * time.Second,
		ElectionTimeoutMax: 6 * time.Second,
	}
	n1, err := NewNode(cfg)
	if err != nil {
		t.Fatal(err)
	}

	// Candidate A wins node1's vote in term 5.
	reply := n1.RequestVote(&RequestVoteArgs{Term: 5, CandidateID: "node2", LastLogIndex: 0, LastLogTerm: 0})
	if !reply.VoteGranted {
		t.Fatal("expected node1 to grant its vote to node2")
	}
	n1.Stop()

	if store.term != 5 || store.vote != "node2" {
		t.Fatalf("persisted (%d,%q), want (5,node2)", store.term, store.vote)
	}

	// Restart from the same storage; node1 must still remember it voted for
	// node2 in term 5 and refuse node3.
	n2, err := NewNode(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer n2.Stop()
	reply = n2.RequestVote(&RequestVoteArgs{Term: 5, CandidateID: "node3", LastLogIndex: 0, LastLogTerm: 0})
	if reply.VoteGranted {
		t.Fatal("node1 voted twice in term 5 after restart")
	}
}
