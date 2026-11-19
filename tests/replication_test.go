package tests

import (
	"fmt"
	"testing"
	"time"
)

// TestBasicAgree: submit 10 commands; all 5 nodes store them in the same order.
func TestBasicAgree(t *testing.T) {
	c := NewCluster(t, 5)
	_, leader := c.WaitLeader()

	for i := 1; i <= 10; i++ {
		cmd := []byte(fmt.Sprintf("cmd-%d", i))
		index, _, ok := leader.Start(cmd)
		if !ok {
			t.Fatalf("leader rejected command %d", i)
		}
		if index != uint64(i) {
			t.Fatalf("command %d got index %d, want %d", i, index, i)
		}
		got := c.WaitReplicated(index, 5)
		if string(got) != string(cmd) {
			t.Fatalf("index %d replicated %q, want %q", index, got, cmd)
		}
	}
}

// TestFollowerCatchUp: disconnect a follower, submit 50 commands, reconnect; it
// must receive all 50.
func TestFollowerCatchUp(t *testing.T) {
	c := NewCluster(t, 5)
	leaderID, leader := c.WaitLeader()

	// Pick a follower to isolate.
	follower := (c.indexOf(leaderID) + 1) % 5
	c.Disconnect(follower)

	var lastIndex uint64
	for i := 1; i <= 50; i++ {
		cmd := []byte(fmt.Sprintf("v%d", i))
		idx, _, ok := leader.Start(cmd)
		if !ok {
			t.Fatalf("leader lost leadership at %d", i)
		}
		lastIndex = idx
	}
	// The 4 connected nodes get everything.
	c.WaitReplicated(lastIndex, 4)

	// Reconnect the straggler; it must catch up to all 50.
	c.Connect(follower)
	c.WaitReplicated(lastIndex, 5)
}

// TestConflictingLogs: an old leader in a minority partition collects
// uncommitted entries; after healing they are replaced by the new leader's.
func TestConflictingLogs(t *testing.T) {
	c := NewCluster(t, 5)
	l1id, l1 := c.WaitLeader()
	li := c.indexOf(l1id)

	// Partition L1 with one follower (minority of 2) against the other three.
	partner := (li + 1) % 5
	minority := []int{li, partner}
	var majority []int
	for i := 0; i < 5; i++ {
		if i != li && i != partner {
			majority = append(majority, i)
		}
	}
	c.Partition(minority, majority)

	// L1 keeps accepting writes it can never commit.
	staleCmd := []byte("STALE")
	for i := 0; i < 5; i++ {
		l1.Start(staleCmd)
	}
	time.Sleep(100 * time.Millisecond)

	// The majority elects a new leader and makes real progress.
	var l2 = c.leaderAmong(majority)
	goodCmd := []byte("GOOD")
	var lastGood uint64
	for i := 0; i < 5; i++ {
		idx, _, ok := l2.Start(goodCmd)
		if ok {
			lastGood = idx
		}
	}
	c.WaitReplicated(lastGood, 3)

	// Heal: L1 must step down and its uncommitted STALE entries must be
	// replaced by the majority's log.
	c.Heal()
	converged := c.WaitLogsConverge(5)
	if logContainsCommand(converged, staleCmd) {
		t.Fatalf("converged log still contains stale uncommitted entries")
	}
	if !logContainsCommand(converged, goodCmd) {
		t.Fatalf("converged log is missing the committed GOOD entries")
	}
}

// TestBacktrackingSpeed: a follower a thousand entries behind, and holding
// conflicting entries of a stale term, catches up within a few seconds — which
// only works if nextIndex backs up a whole term at a time.
func TestBacktrackingSpeed(t *testing.T) {
	c := NewCluster(t, 5)
	l1id, l1 := c.WaitLeader()
	li := c.indexOf(l1id)

	// Give L1 (and a partner) a handful of conflicting entries in a minority.
	partner := (li + 1) % 5
	minority := []int{li, partner}
	var majority []int
	for i := 0; i < 5; i++ {
		if i != li && i != partner {
			majority = append(majority, i)
		}
	}
	c.Partition(minority, majority)
	for i := 0; i < 8; i++ {
		l1.Start([]byte("conflict"))
	}
	time.Sleep(100 * time.Millisecond)

	// The majority races a thousand entries ahead.
	l2 := c.leaderAmong(majority)
	var lastIdx uint64
	for i := 0; i < 1000; i++ {
		idx, _, ok := l2.Start([]byte(fmt.Sprintf("x%d", i)))
		if ok {
			lastIdx = idx
		}
	}
	c.WaitReplicated(lastIdx, 3)

	// Heal and time how long it takes the two stragglers to reconcile 1000
	// entries and drop their conflicting ones.
	start := time.Now()
	c.Heal()
	c.WaitLogsConverge(5)
	if elapsed := time.Since(start); elapsed > 5*time.Second {
		t.Fatalf("catch-up took %v, too slow — fast backtracking not working", elapsed)
	}
}
