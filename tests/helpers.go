package tests

import (
	"testing"
	"time"

	"github.com/atharv-pdarshi/kvraft/raft"
)

// CheckOneLeader verifies that at most one leader exists per term and returns
// the id of a leader in the newest term it observed. It retries for a few
// election cycles because an election may be in progress when first called.
func (c *Cluster) CheckOneLeader() string {
	c.t.Helper()
	for attempt := 0; attempt < 12; attempt++ {
		time.Sleep(c.electionMax + 50*time.Millisecond)

		leadersByTerm := map[uint64][]string{}
		c.mu.Lock()
		nodes := map[string]*raft.Node{}
		for id, n := range c.nodes {
			nodes[id] = n
		}
		c.mu.Unlock()

		for id, n := range nodes {
			// Ignore nodes an observer cannot reach: a leader isolated in a
			// minority still believes it leads its old term.
			if !c.net.IsConnected(id) {
				continue
			}
			if term, isLeader := n.State(); isLeader {
				leadersByTerm[term] = append(leadersByTerm[term], id)
			}
		}

		lastTerm := uint64(0)
		var lastLeader string
		for term, leaders := range leadersByTerm {
			if len(leaders) > 1 {
				c.t.Fatalf("ELECTION SAFETY violated: term %d has leaders %v", term, leaders)
			}
			if term >= lastTerm {
				lastTerm = term
				lastLeader = leaders[0]
			}
		}
		if lastLeader != "" {
			return lastLeader
		}
	}
	c.t.Fatalf("expected one leader, got none")
	return ""
}

// CheckNoLeader fails if any node currently thinks it is leader. Used to prove
// that a minority cannot elect anyone.
func (c *Cluster) CheckNoLeader() {
	c.t.Helper()
	time.Sleep(2 * c.electionMax)
	c.mu.Lock()
	defer c.mu.Unlock()
	for id, n := range c.nodes {
		if _, isLeader := n.State(); isLeader {
			c.t.Fatalf("expected no leader, but %s claims leadership", id)
		}
	}
}

// Leader returns the current leader's id and node (newest term), if any.
func (c *Cluster) Leader() (string, *raft.Node, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	var bestID string
	var best *raft.Node
	var bestTerm uint64
	found := false
	for id, n := range c.nodes {
		if !c.net.IsConnected(id) {
			continue
		}
		if term, isLeader := n.State(); isLeader && (!found || term >= bestTerm) {
			found, bestID, best, bestTerm = true, id, n, term
		}
	}
	return bestID, best, found
}

// WaitLeader blocks until a leader emerges (or fails after a timeout) and
// returns its id and node.
func (c *Cluster) WaitLeader() (string, *raft.Node) {
	c.t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if id, n, ok := c.Leader(); ok {
			return id, n
		}
		time.Sleep(20 * time.Millisecond)
	}
	c.t.Fatalf("no leader elected within timeout")
	return "", nil
}

// Submit proposes a command through the current leader. Returns the assigned
// index, the term, and whether a leader accepted it.
func (c *Cluster) Submit(cmd []byte) (uint64, uint64, bool) {
	_, leader, ok := c.Leader()
	if !ok {
		return 0, 0, false
	}
	return leader.Start(cmd)
}

// nCommitted reports how many nodes have applied index, and the agreed command.
func (c *Cluster) nCommitted(index uint64) (int, []byte) {
	c.mu.Lock()
	defer c.mu.Unlock()
	count := 0
	var cmd []byte
	for _, perNode := range c.applied {
		if v, ok := perNode[index]; ok {
			count++
			cmd = v
		}
	}
	return count, cmd
}

// WaitCommitted waits until at least count nodes have applied index, then
// returns the command. Fails the test on timeout.
func (c *Cluster) WaitCommitted(index uint64, count int) []byte {
	c.t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if n, cmd := c.nCommitted(index); n >= count {
			return cmd
		}
		time.Sleep(20 * time.Millisecond)
	}
	n, _ := c.nCommitted(index)
	c.t.Fatalf("index %d committed on only %d/%d nodes within timeout", index, n, count)
	return nil
}

// SubmitAndWait submits a command and waits for it to commit on count nodes,
// retrying through leader changes. Returns the index it committed at.
func (c *Cluster) SubmitAndWait(t *testing.T, cmd []byte, count int) uint64 {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		index, _, ok := c.Submit(cmd)
		if !ok {
			time.Sleep(20 * time.Millisecond)
			continue
		}
		// Give this index a chance to commit under the current leader.
		inner := time.Now().Add(1 * time.Second)
		for time.Now().Before(inner) {
			if n, got := c.nCommitted(index); n >= count {
				if string(got) == string(cmd) {
					return index
				}
				break // a different command took this index; resubmit
			}
			time.Sleep(20 * time.Millisecond)
		}
	}
	t.Fatalf("command never committed on %d nodes", count)
	return 0
}
