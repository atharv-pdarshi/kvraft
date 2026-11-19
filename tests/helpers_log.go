package tests

import (
	"time"

	"github.com/atharv-pdarshi/kvraft/raft"
)

// nodeOf returns the live node at cluster index i (may be nil if crashed).
func (c *Cluster) nodeOf(i int) *raft.Node {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.nodes[c.ids[i]]
}

// nReplicated reports how many live nodes hold an entry at index, and the most
// common command found there.
func (c *Cluster) nReplicated(index uint64) (int, []byte) {
	c.mu.Lock()
	nodes := make([]*raft.Node, 0, len(c.nodes))
	for _, n := range c.nodes {
		nodes = append(nodes, n)
	}
	c.mu.Unlock()

	counts := map[string]int{}
	for _, n := range nodes {
		lv := n.LogView()
		if uint64(len(lv)) > index {
			counts[string(lv[index].Command)]++
		}
	}
	best, bestCmd := 0, ""
	for cmd, cnt := range counts {
		if cnt > best {
			best, bestCmd = cnt, cmd
		}
	}
	return best, []byte(bestCmd)
}

// WaitReplicated blocks until at least count nodes hold the same command at
// index, and returns that command.
func (c *Cluster) WaitReplicated(index uint64, count int) []byte {
	c.t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if n, cmd := c.nReplicated(index); n >= count {
			return cmd
		}
		time.Sleep(20 * time.Millisecond)
	}
	n, _ := c.nReplicated(index)
	c.t.Fatalf("index %d replicated on only %d/%d nodes within timeout", index, n, count)
	return nil
}

// logsEqual reports whether two logs are identical.
func logsEqual(a, b []raft.LogEntry) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i].Term != b[i].Term || string(a[i].Command) != string(b[i].Command) {
			return false
		}
	}
	return true
}

// WaitLogsConverge blocks until count live nodes share one identical log, and
// returns that log. Proves conflicting followers were reconciled to the leader.
func (c *Cluster) WaitLogsConverge(count int) []raft.LogEntry {
	c.t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		c.mu.Lock()
		var views [][]raft.LogEntry
		for _, n := range c.nodes {
			views = append(views, n.LogView())
		}
		c.mu.Unlock()

		// Group identical logs; return a log shared by >= count nodes.
		for i := range views {
			same := 0
			for j := range views {
				if logsEqual(views[i], views[j]) {
					same++
				}
			}
			if same >= count {
				return views[i]
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	c.t.Fatalf("logs did not converge on %d nodes within timeout", count)
	return nil
}

// logContainsCommand reports whether any entry in log carries cmd.
func logContainsCommand(log []raft.LogEntry, cmd []byte) bool {
	for _, e := range log {
		if string(e.Command) == string(cmd) {
			return true
		}
	}
	return false
}
