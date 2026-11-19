package tests

import (
	"fmt"
	"math/rand"
	"testing"
	"time"
)

// TestFigure8 reproduces the churn from the paper's Figure 8: a long run of
// leader changes while commands are being submitted. The current-term commit
// rule (plus the no-op entry) must guarantee that no applied entry ever changes
// at a given index — which the harness checker enforces on every ApplyCh. The
// cluster must also still make progress once everyone is reconnected.
func TestFigure8(t *testing.T) {
	c := NewCluster(t, 5)
	rng := rand.New(rand.NewSource(42))
	c.WaitLeader()

	connected := []bool{true, true, true, true, true}
	nup := 5

	for iter := 0; iter < 300; iter++ {
		// Submit to whoever is leader (may be nobody mid-election).
		if _, leader, ok := c.Leader(); ok {
			leader.Start([]byte(fmt.Sprintf("f%d", iter)))
		}

		// Sometimes let an election fully resolve, usually interrupt it.
		if rng.Intn(1000) < 100 {
			time.Sleep(time.Duration(rng.Int63n(int64(c.electionMax))))
		} else {
			time.Sleep(time.Duration(rng.Intn(13)) * time.Millisecond)
		}

		// Churn: drop a random connected node.
		if rng.Intn(1000) < 500 {
			i := rng.Intn(5)
			if connected[i] {
				c.Disconnect(i)
				connected[i] = false
				nup--
			}
		}
		// Never let the cluster fall below a majority for long, so the run
		// terminates: reconnect random nodes until 3 are up.
		for nup < 3 {
			i := rng.Intn(5)
			if !connected[i] {
				c.Connect(i)
				connected[i] = true
				nup++
			}
		}
	}

	// Reconnect everyone and require one final command to commit on all 5.
	for i := 0; i < 5; i++ {
		if !connected[i] {
			c.Connect(i)
		}
	}
	c.SubmitAndWait(t, []byte("figure8-final"), 5)
}
