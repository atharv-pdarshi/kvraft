package tests

import (
	"fmt"
	"math/rand"
	"testing"
	"time"
)

// TestRestartFollower: crash a follower, commit 50 entries, restart it; it must
// reload its log and catch up to everything.
func TestRestartFollower(t *testing.T) {
	c := NewCluster(t, 5)
	leaderID, leader := c.WaitLeader()

	follower := (c.indexOf(leaderID) + 1) % 5
	c.Crash(follower)

	var last uint64
	for i := 0; i < 50; i++ {
		if idx, _, ok := leader.Start([]byte(fmt.Sprintf("e%d", i))); ok {
			last = idx
		}
	}
	c.WaitCommitted(last, 3)

	c.Restart(follower)
	c.WaitCommitted(last, 5)
}

// TestRestartLeader: crash the leader right after it accepts entries; no
// committed entry may be lost, and the cluster keeps going.
func TestRestartLeader(t *testing.T) {
	c := NewCluster(t, 5)
	leaderID, leader := c.WaitLeader()

	var committed uint64
	for i := 0; i < 20; i++ {
		if idx, _, ok := leader.Start([]byte(fmt.Sprintf("pre%d", i))); ok {
			committed = idx
		}
	}
	c.WaitCommitted(committed, 3)

	// Crash and restart the leader.
	li := c.indexOf(leaderID)
	c.Crash(li)
	c.Restart(li)

	// A new leader takes over; the previously committed entries survive and new
	// writes still commit on all 5.
	c.WaitLeader()
	final := c.SubmitAndWait(t, []byte("post-restart"), 5)
	if got := c.WaitCommitted(committed, 5); string(got) == "" && committed > 0 {
		t.Fatalf("committed entry %d lost after leader restart", committed)
	}
	_ = final
}

// TestRestartAll: commit 100 entries, crash and restart all 5; every entry must
// be applied again on every node.
func TestRestartAll(t *testing.T) {
	c := NewCluster(t, 5)
	_, leader := c.WaitLeader()

	var last uint64
	for i := 0; i < 100; i++ {
		if idx, _, ok := leader.Start([]byte(fmt.Sprintf("d%d", i))); ok {
			last = idx
		} else {
			_, leader = c.WaitLeader()
		}
	}
	c.WaitCommitted(last, 5)

	for i := 0; i < 5; i++ {
		c.Crash(i)
	}
	for i := 0; i < 5; i++ {
		c.Restart(i)
	}

	// After a full restart a leader re-emerges, its no-op commits the carried
	// log, and all 100 entries are re-applied everywhere.
	c.WaitLeader()
	c.WaitCommitted(last, 5)
}

// TestTornWriteCluster: on a file-backed cluster, crash a follower, chop the
// tail off its on-disk log, and restart it. It must still start, drop the torn
// record, and catch back up.
func TestTornWriteCluster(t *testing.T) {
	c := NewFileCluster(t, 5)
	leaderID, leader := c.WaitLeader()

	var last uint64
	for i := 0; i < 20; i++ {
		if idx, _, ok := leader.Start([]byte(fmt.Sprintf("w%d", i))); ok {
			last = idx
		}
	}
	c.WaitCommitted(last, 5)

	victim := (c.indexOf(leaderID) + 2) % 5
	c.Crash(victim)
	c.truncateLogTail(victim, 3) // simulate a half-written final record
	c.Restart(victim)

	// It rejoins and catches up to the full committed log.
	c.WaitCommitted(last, 5)
}

// TestFigure8Crash reruns the Figure 8 churn, but with real crashes and
// restarts rather than disconnects. State-machine safety must still hold.
func TestFigure8Crash(t *testing.T) {
	c := NewCluster(t, 5)
	rng := rand.New(rand.NewSource(7))
	c.WaitLeader()

	up := []bool{true, true, true, true, true}
	nup := 5
	for iter := 0; iter < 120; iter++ {
		if _, leader, ok := c.Leader(); ok {
			leader.Start([]byte(fmt.Sprintf("c%d", iter)))
		}
		if rng.Intn(1000) < 150 {
			time.Sleep(time.Duration(rng.Int63n(int64(c.electionMax))))
		} else {
			time.Sleep(time.Duration(rng.Intn(13)) * time.Millisecond)
		}
		if rng.Intn(1000) < 450 {
			i := rng.Intn(5)
			if up[i] {
				c.Crash(i)
				up[i] = false
				nup--
			}
		}
		for nup < 3 {
			i := rng.Intn(5)
			if !up[i] {
				c.Restart(i)
				up[i] = true
				nup++
			}
		}
	}
	for i := 0; i < 5; i++ {
		if !up[i] {
			c.Restart(i)
		}
	}
	c.WaitLeader()
	c.SubmitAndWait(t, []byte("figure8-crash-final"), 5)
}
