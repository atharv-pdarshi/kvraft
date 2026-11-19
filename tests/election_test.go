package tests

import (
	"testing"
	"time"
)

// TestInitialElection: 5 nodes elect exactly one leader, and with no failures
// the term stays stable.
func TestInitialElection(t *testing.T) {
	c := NewCluster(t, 5)

	leader := c.CheckOneLeader()
	if leader == "" {
		t.Fatal("no leader elected")
	}

	// Term should be stable while everyone is healthy.
	term1 := c.anyTerm()
	time.Sleep(2 * c.electionMax)
	term2 := c.anyTerm()
	if term1 != term2 {
		t.Fatalf("term changed with no failures: %d -> %d", term1, term2)
	}
	// Still exactly one leader.
	c.CheckOneLeader()
}

// TestReElection: kill the leader, a new one takes over; when the old leader
// returns it steps down.
func TestReElection(t *testing.T) {
	c := NewCluster(t, 5)

	leader1 := c.CheckOneLeader()
	c.disconnectByID(leader1)

	leader2 := c.CheckOneLeader()
	if leader2 == leader1 {
		t.Fatalf("new leader should differ from disconnected %s", leader1)
	}

	// Old leader comes back and must not disrupt the current term.
	c.connectByID(leader1)
	leader3 := c.CheckOneLeader()
	if leader3 == "" {
		t.Fatal("no leader after reconnect")
	}
}

// TestNoQuorumNoLeader: disconnect 3 of 5 (including the current leader), so the
// two survivors are followers that can never reach a majority. They must never
// elect a new leader. Reconnecting one of them restores a quorum of 3.
func TestNoQuorumNoLeader(t *testing.T) {
	c := NewCluster(t, 5)
	leader := c.CheckOneLeader()
	li := c.indexOf(leader)

	// Disconnect the leader plus two others — three nodes total.
	disc := []int{li}
	for i := 0; i < 5 && len(disc) < 3; i++ {
		if i != li {
			disc = append(disc, i)
		}
	}
	remaining := []int{}
	for i := 0; i < 5; i++ {
		if i != disc[0] && i != disc[1] && i != disc[2] {
			remaining = append(remaining, i)
		}
	}
	for _, i := range disc {
		c.Disconnect(i)
	}

	// The two survivors were followers and cannot form a majority.
	c.CheckNoLeaderAmong(remaining)

	// Reconnect one of the disconnected followers: now 3 are connected and a
	// leader must appear.
	c.Connect(disc[1])
	c.CheckOneLeader()
}

// TestManyElections: repeatedly disconnect and reconnect up to 2 nodes; never
// two leaders in one term. The harness checker also guards state-machine safety.
func TestManyElections(t *testing.T) {
	c := NewCluster(t, 5)
	c.CheckOneLeader()

	deadline := time.Now().Add(8 * time.Second)
	round := 0
	for time.Now().Before(deadline) {
		// Drop up to two random nodes, then restore them.
		a := round % 5
		b := (round + 2) % 5
		c.Disconnect(a)
		c.Disconnect(b)
		time.Sleep(c.electionMax)
		// There must still be at most one leader per term among the connected.
		c.assertAtMostOneLeaderPerTerm()
		c.Connect(a)
		c.Connect(b)
		time.Sleep(c.electionMax)
		round++
	}
	c.CheckOneLeader()
}
