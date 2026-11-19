package tests

import (
	"testing"
	"time"
)

// TestClusterStartStop is the Stage 2 "Done when": five idle nodes start and
// stop cleanly (prove it under -race), and a node can be disconnected and
// reconnected without anything panicking.
func TestClusterStartStop(t *testing.T) {
	c := NewCluster(t, 5)
	time.Sleep(100 * time.Millisecond)

	// No algorithm yet, so no node should claim leadership.
	for _, n := range c.alive() {
		if _, isLeader := n.State(); isLeader {
			t.Fatalf("skeleton node unexpectedly became leader")
		}
	}

	c.Disconnect(2)
	time.Sleep(50 * time.Millisecond)
	c.Connect(2)
	time.Sleep(50 * time.Millisecond)
	// c.Stop runs via t.Cleanup.
}

// TestCrashRestartSkeleton exercises the harness crash/restart path on the
// idle skeleton.
func TestCrashRestartSkeleton(t *testing.T) {
	c := NewCluster(t, 5)
	time.Sleep(50 * time.Millisecond)
	c.Crash(0)
	time.Sleep(50 * time.Millisecond)
	c.Restart(0)
	time.Sleep(50 * time.Millisecond)
}
