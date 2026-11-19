package tests

import (
	"math/rand"
	"testing"
	"time"
)

// chaosDuration is how long TestChaos injects faults. The plan calls for 60s;
// the default here is shorter so `go test` stays quick, and -count raises
// confidence. Raise it for a soak run.
const chaosDuration = 10 * time.Second

// TestChaos: random crashes, restarts and partitions, with never more than two
// nodes impaired at once, while clients write throughout. Every invariant must
// hold and every acknowledged write must be readable at the end.
func TestChaos(t *testing.T) {
	c := NewKVCluster(t, 5)
	c.WaitLeader()
	rng := rand.New(rand.NewSource(99))

	stop := make(chan struct{})
	mon := c.electionMonitor(stop)
	w := c.startWorkload(3)
	time.Sleep(200 * time.Millisecond)

	// impaired[i] = "crash" or "disconnect"; at most 2 at a time so a majority
	// of 3 is always reachable and the cluster keeps making progress.
	impaired := map[int]string{}
	deadline := time.Now().Add(chaosDuration)
	for time.Now().Before(deadline) {
		if len(impaired) < 2 && rng.Intn(100) < 55 {
			// Impair a healthy node.
			i := rng.Intn(5)
			if _, bad := impaired[i]; !bad {
				if rng.Intn(2) == 0 {
					c.Crash(i)
					impaired[i] = "crash"
				} else {
					c.Disconnect(i)
					impaired[i] = "disconnect"
				}
			}
		} else if len(impaired) > 0 {
			// Recover a random impaired node.
			for i, kind := range impaired {
				if kind == "crash" {
					c.Restart(i)
				} else {
					c.Connect(i)
				}
				delete(impaired, i)
				break
			}
		}
		time.Sleep(time.Duration(50+rng.Intn(150)) * time.Millisecond)
	}

	// Recover everything.
	for i, kind := range impaired {
		if kind == "crash" {
			c.Restart(i)
		} else {
			c.Connect(i)
		}
	}
	c.Heal()

	acked := w.stopAndWait()
	close(stop)
	mon.Wait()

	c.WaitLeader()
	c.checkLogMatching()
	c.verifyAcked(acked)
}
