package tests

import (
	"fmt"
	"testing"
	"time"
)

// TestCommitWithTwoDown: with 2 of 5 followers disconnected, the remaining 3
// are still a majority, so new commands commit and get applied.
func TestCommitWithTwoDown(t *testing.T) {
	c := NewCluster(t, 5)
	leaderID, leader := c.WaitLeader()
	li := c.indexOf(leaderID)

	// Disconnect two followers (not the leader).
	down := 0
	for i := 0; i < 5 && down < 2; i++ {
		if i != li {
			c.Disconnect(i)
			down++
		}
	}

	cmd := []byte("still-commits")
	index, _, ok := leader.Start(cmd)
	if !ok {
		t.Fatal("leader rejected command")
	}
	got := c.WaitCommitted(index, 3) // applied on the 3-node majority
	if string(got) != string(cmd) {
		t.Fatalf("committed %q, want %q", got, cmd)
	}
}

// TestNoCommitWithThreeDown: with 3 of 5 disconnected there is no majority, so
// the leader's new entries never commit — until the nodes come back.
func TestNoCommitWithThreeDown(t *testing.T) {
	c := NewCluster(t, 5)
	leaderID, leader := c.WaitLeader()
	li := c.indexOf(leaderID)

	var downIdx []int
	for i := 0; i < 5 && len(downIdx) < 3; i++ {
		if i != li {
			downIdx = append(downIdx, i)
			c.Disconnect(i)
		}
	}

	cmd := []byte("stuck")
	index, _, ok := leader.Start(cmd)
	if !ok {
		t.Fatal("leader rejected command")
	}

	// Give it time; it must NOT commit on a minority.
	time.Sleep(600 * time.Millisecond)
	if n, _ := c.nCommitted(index); n > 0 {
		t.Fatalf("entry committed on %d nodes without a majority", n)
	}

	// Reconnect the three; now it can commit.
	for _, i := range downIdx {
		c.Connect(i)
	}
	c.WaitCommitted(index, 3)
}

// TestApplyOrder: every node delivers indexes 1,2,3,... with no gaps or
// repeats. The harness checker enforces this on every ApplyCh; here we drive a
// stream of commands through it.
func TestApplyOrder(t *testing.T) {
	c := NewCluster(t, 5)
	_, leader := c.WaitLeader()

	var lastIndex uint64
	for i := 0; i < 30; i++ {
		idx, _, ok := leader.Start([]byte(fmt.Sprintf("a%d", i)))
		if !ok {
			// Leadership may have moved; re-find and continue.
			_, leader = c.WaitLeader()
			continue
		}
		lastIndex = idx
	}
	c.WaitCommitted(lastIndex, 5)
	// If any node had applied out of order or skipped an index, the background
	// checker in cluster.go would already have failed the test.
}
