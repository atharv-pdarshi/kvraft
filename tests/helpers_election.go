package tests

import (
	"time"

	"github.com/atharv-pdarshi/kvraft/raft"
)

// anyTerm returns the highest currentTerm seen among live nodes.
func (c *Cluster) anyTerm() uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	var max uint64
	for _, n := range c.nodes {
		if term, _ := n.State(); term > max {
			max = term
		}
	}
	return max
}

// indexOf returns the cluster index of a node id.
func (c *Cluster) indexOf(id string) int {
	for i, v := range c.ids {
		if v == id {
			return i
		}
	}
	return -1
}

// disconnectByID / connectByID cut a node's links by id (not a crash).
func (c *Cluster) disconnectByID(id string) { c.net.Disconnect(id) }
func (c *Cluster) connectByID(id string)    { c.net.Connect(id) }

// CheckNoLeaderAmong fails if any of the given nodes claims leadership.
func (c *Cluster) CheckNoLeaderAmong(idx []int) {
	c.t.Helper()
	time.Sleep(2 * c.electionMax)
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, i := range idx {
		id := c.ids[i]
		if n, ok := c.nodes[id]; ok {
			if _, isLeader := n.State(); isLeader {
				c.t.Fatalf("expected no leader among %v, but %s leads", idx, id)
			}
		}
	}
}

// leaderAmong waits for and returns the leader node among the given indices.
func (c *Cluster) leaderAmong(idx []int) *raft.Node {
	c.t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		for _, i := range idx {
			if n := c.nodeOf(i); n != nil {
				if _, isLeader := n.State(); isLeader {
					return n
				}
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	c.t.Fatalf("no leader among %v within timeout", idx)
	return nil
}

// assertAtMostOneLeaderPerTerm checks election safety across live nodes.
func (c *Cluster) assertAtMostOneLeaderPerTerm() {
	c.t.Helper()
	c.mu.Lock()
	defer c.mu.Unlock()
	leaders := map[uint64][]string{}
	for id, n := range c.nodes {
		if term, isLeader := n.State(); isLeader {
			leaders[term] = append(leaders[term], id)
		}
	}
	for term, ls := range leaders {
		if len(ls) > 1 {
			c.t.Fatalf("ELECTION SAFETY violated: term %d has leaders %v", term, ls)
		}
	}
}
