package tests

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/atharv-pdarshi/kvraft/client"
	"github.com/atharv-pdarshi/kvraft/raft"
)

// --- network controls ---

func (c *KVCluster) toIDs(idx []int) []string {
	out := make([]string, len(idx))
	for i, v := range idx {
		out[i] = c.ids[v]
	}
	return out
}

func (c *KVCluster) Partition(a, b []int) { c.net.Partition(c.toIDs(a), c.toIDs(b)) }
func (c *KVCluster) Heal()                { c.net.Heal() }
func (c *KVCluster) SetDropRate(r float64) { c.net.SetDropRate(r) }
func (c *KVCluster) SetDelay(min, max time.Duration) { c.net.SetDelay(min, max) }

// --- inspection ---

// liveNodes returns the current node instances keyed by id.
func (c *KVCluster) liveNodes() map[string]*raft.Node {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := map[string]*raft.Node{}
	for id, n := range c.nodes {
		out[id] = n
	}
	return out
}

// snapshotAll returns each live server's state machine.
func (c *KVCluster) snapshotAll() map[string]map[string]string {
	c.mu.Lock()
	ids := make([]string, 0, len(c.servers))
	for id := range c.servers {
		ids = append(ids, id)
	}
	c.mu.Unlock()
	out := map[string]map[string]string{}
	for _, id := range ids {
		if srv := c.getServer(id); srv != nil {
			out[id] = srv.Snapshot()
		}
	}
	return out
}

// --- invariant 1: election safety (at most one leader per term) ---

// electionMonitor samples the cluster until stop is closed and fails the test
// if two leaders are ever seen sharing a term.
func (c *KVCluster) electionMonitor(stop <-chan struct{}) *sync.WaitGroup {
	wg := &sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		seen := map[uint64]string{} // term -> leader id
		ticker := time.NewTicker(10 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-stop:
				return
			case <-ticker.C:
				for id, n := range c.liveNodes() {
					if term, isLeader := n.State(); isLeader {
						if prev, ok := seen[term]; ok && prev != id {
							c.t.Errorf("ELECTION SAFETY violated: term %d had leaders %s and %s", term, prev, id)
						}
						seen[term] = id
					}
				}
			}
		}
	}()
	return wg
}

// --- invariant 2: log matching ---

// checkLogMatching verifies that whenever two live logs share a term at an
// index, their prefixes up to that index are identical.
func (c *KVCluster) checkLogMatching() {
	c.t.Helper()
	logs := map[string][]raft.LogEntry{}
	for id, n := range c.liveNodes() {
		logs[id] = n.LogView()
	}
	ids := make([]string, 0, len(logs))
	for id := range logs {
		ids = append(ids, id)
	}
	for i := 0; i < len(ids); i++ {
		for j := i + 1; j < len(ids); j++ {
			a, b := logs[ids[i]], logs[ids[j]]
			n := len(a)
			if len(b) < n {
				n = len(b)
			}
			for k := 0; k < n; k++ {
				if a[k].Term == b[k].Term {
					if string(a[k].Command) != string(b[k].Command) {
						c.t.Errorf("LOG MATCHING violated at index %d between %s and %s", k, ids[i], ids[j])
					}
				}
			}
		}
	}
}

// --- invariants 4 & 5: state-machine convergence and durability ---

// waitConverge waits until every live node's state machine equals want, then
// returns. Proves state-machine safety and (with the acked set) durability.
func (c *KVCluster) waitConverge(want map[string]string, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		snaps := c.snapshotAll()
		if len(snaps) > 0 && allMatch(snaps, want) {
			return true
		}
		time.Sleep(30 * time.Millisecond)
	}
	return false
}

func allMatch(snaps map[string]map[string]string, want map[string]string) bool {
	for _, snap := range snaps {
		for k, v := range want {
			if snap[k] != v {
				return false
			}
		}
	}
	return true
}

// --- workload: clients writing throughout a scenario ---

type workload struct {
	c       *KVCluster
	stop    chan struct{}
	wg      sync.WaitGroup
	mu      sync.Mutex
	acked   map[string]string
	history []*client.History
}

// startWorkload launches n clients that each Put a stream of their own keys and
// record every acknowledged write.
func (c *KVCluster) startWorkload(n int) *workload {
	w := &workload{c: c, stop: make(chan struct{}), acked: map[string]string{}}
	for i := 0; i < n; i++ {
		cl := c.NewClient(fmt.Sprintf("w%d", i))
		h := client.NewHistory()
		cl.SetHistory(h)
		w.history = append(w.history, h)
		w.wg.Add(1)
		go func(cl *client.Client, id int) {
			defer w.wg.Done()
			seq := 0
			for {
				select {
				case <-w.stop:
					return
				default:
				}
				key := fmt.Sprintf("w%d-k%d", id, seq)
				val := fmt.Sprintf("v%d", seq)
				ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
				err := cl.Put(ctx, key, val)
				cancel()
				if err == nil {
					w.mu.Lock()
					w.acked[key] = val
					w.mu.Unlock()
				}
				seq++
				time.Sleep(5 * time.Millisecond)
			}
		}(cl, i)
	}
	return w
}

// stopAndWait halts the workload and returns the set of acknowledged writes.
func (w *workload) stopAndWait() map[string]string {
	close(w.stop)
	w.wg.Wait()
	w.mu.Lock()
	defer w.mu.Unlock()
	out := map[string]string{}
	for k, v := range w.acked {
		out[k] = v
	}
	return out
}

// verifyAcked confirms every acknowledged write is readable with its value,
// using a fresh client. This is the durability invariant.
func (c *KVCluster) verifyAcked(acked map[string]string) {
	c.t.Helper()
	reader := c.NewClient("verifier")
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	for key, want := range acked {
		v, ok, err := reader.Get(ctx, key)
		if err != nil {
			c.t.Fatalf("durability: get %s failed: %v", key, err)
		}
		if !ok || v != want {
			c.t.Fatalf("DURABILITY violated: key %s = (%q,%v), want %q", key, v, ok, want)
		}
	}
}
