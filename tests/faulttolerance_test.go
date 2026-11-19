package tests

import (
	"testing"
	"time"
)

// runScenario boots a 5-node KV cluster with clients writing throughout, runs
// the given fault injection, then heals, and finally checks every invariant:
// election safety and log matching (continuously / at the end) and durability
// (every acknowledged write is still readable).
func runScenario(t *testing.T, fault func(c *KVCluster)) {
	c := NewKVCluster(t, 5)
	c.WaitLeader()

	stop := make(chan struct{})
	mon := c.electionMonitor(stop)

	w := c.startWorkload(3)
	time.Sleep(200 * time.Millisecond) // let some writes land first

	fault(c)

	acked := w.stopAndWait()
	close(stop)
	mon.Wait()

	// Heal everything and let the cluster settle.
	c.Heal()
	for i := 0; i < 5; i++ {
		c.Connect(i)
	}
	c.WaitLeader()

	c.checkLogMatching()
	c.verifyAcked(acked)
}

// TestLeaderCrash: crash the leader; a new one must take over and no
// acknowledged write may be lost.
func TestLeaderCrash(t *testing.T) {
	runScenario(t, func(c *KVCluster) {
		leader := c.WaitLeader()
		c.Crash(leader)
		time.Sleep(1 * time.Second)
		c.WaitLeader()
		time.Sleep(500 * time.Millisecond)
		c.Restart(leader)
	})
}

// TestRepeatedLeaderCrash: crash each new leader several times; elections keep
// succeeding and all acknowledged writes survive.
func TestRepeatedLeaderCrash(t *testing.T) {
	runScenario(t, func(c *KVCluster) {
		for i := 0; i < 5; i++ {
			leader := c.WaitLeader()
			c.Crash(leader)
			time.Sleep(700 * time.Millisecond)
			c.WaitLeader()
			c.Restart(leader)
			time.Sleep(300 * time.Millisecond)
		}
	})
}

// TestTwoFollowersDown: crash two followers; writes keep committing on 3 of 5.
func TestTwoFollowersDown(t *testing.T) {
	runScenario(t, func(c *KVCluster) {
		leader := c.WaitLeader()
		down := 0
		for i := 0; i < 5 && down < 2; i++ {
			if i != leader {
				c.Crash(i)
				down++
			}
		}
		time.Sleep(1500 * time.Millisecond)
	})
}

// TestLeaderAndFollowerDown: crash the leader and one follower; the remaining 3
// elect a leader and keep committing.
func TestLeaderAndFollowerDown(t *testing.T) {
	runScenario(t, func(c *KVCluster) {
		leader := c.WaitLeader()
		c.Crash(leader)
		follower := (leader + 1) % 5
		c.Crash(follower)
		time.Sleep(1500 * time.Millisecond)
		c.WaitLeader()
	})
}

// TestThreeDown: crash 3 nodes; no commits and no wrong data; restarting one
// restores a majority and writes resume.
func TestThreeDown(t *testing.T) {
	runScenario(t, func(c *KVCluster) {
		leader := c.WaitLeader()
		var downIdx []int
		downIdx = append(downIdx, leader)
		for i := 0; i < 5 && len(downIdx) < 3; i++ {
			if i != leader {
				downIdx = append(downIdx, i)
			}
		}
		for _, i := range downIdx {
			c.Crash(i)
		}
		time.Sleep(1 * time.Second) // minority: no progress, but no corruption
		// Restart one to restore a majority.
		c.Restart(downIdx[0])
		time.Sleep(1 * time.Second)
		c.WaitLeader()
	})
}

// TestPartitionLeaderInMinority: split the leader into a 2-node minority; the 3
// elect a new leader and commit; the old leader's writes never commit, and
// after healing its entries are replaced and it steps down.
func TestPartitionLeaderInMinority(t *testing.T) {
	runScenario(t, func(c *KVCluster) {
		leader := c.WaitLeader()
		partner := (leader + 1) % 5
		minority := []int{leader, partner}
		var majority []int
		for i := 0; i < 5; i++ {
			if i != leader && i != partner {
				majority = append(majority, i)
			}
		}
		c.Partition(minority, majority)
		time.Sleep(2 * time.Second)
		c.Heal()
		time.Sleep(1 * time.Second)
	})
}

// TestPartitionLeaderInMajority: split the leader into a 3-node majority; it
// keeps committing, and the 2-node side catches up after healing.
func TestPartitionLeaderInMajority(t *testing.T) {
	runScenario(t, func(c *KVCluster) {
		leader := c.WaitLeader()
		major := []int{leader}
		var minor []int
		for i := 0; i < 5 && len(major) < 3; i++ {
			if i != leader {
				major = append(major, i)
			}
		}
		for i := 0; i < 5; i++ {
			inMajor := false
			for _, m := range major {
				if m == i {
					inMajor = true
				}
			}
			if !inMajor {
				minor = append(minor, i)
			}
		}
		c.Partition(major, minor)
		time.Sleep(2 * time.Second)
		c.Heal()
		time.Sleep(1 * time.Second)
	})
}

// TestFlakyNetwork: drop 20% of messages and delay the rest; progress must
// continue and all invariants hold.
func TestFlakyNetwork(t *testing.T) {
	runScenario(t, func(c *KVCluster) {
		c.SetDropRate(0.2)
		c.SetDelay(5*time.Millisecond, 25*time.Millisecond)
		time.Sleep(4 * time.Second)
		c.SetDropRate(0)
		c.SetDelay(0, 0)
	})
}
