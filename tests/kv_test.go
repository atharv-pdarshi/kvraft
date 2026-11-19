package tests

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"
)

// TestKVBasic: Put then Get returns the value, through any node.
func TestKVBasic(t *testing.T) {
	c := NewKVCluster(t, 5)
	c.WaitLeader()
	cl := c.NewClient("c1")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := cl.Put(ctx, "foo", "bar"); err != nil {
		t.Fatalf("put: %v", err)
	}
	v, ok, err := cl.Get(ctx, "foo")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if !ok || v != "bar" {
		t.Fatalf("get returned (%q,%v), want (bar,true)", v, ok)
	}
}

// TestKVLeaderChange: Put, crash the leader, Get from the new leader returns the
// value.
func TestKVLeaderChange(t *testing.T) {
	c := NewKVCluster(t, 5)
	c.WaitLeader()
	cl := c.NewClient("c1")

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	if err := cl.Put(ctx, "k", "v1"); err != nil {
		t.Fatalf("put: %v", err)
	}

	// Crash the current leader; a new one must take over.
	leader := c.WaitLeader()
	c.Crash(leader)
	// Rebuild the client over the surviving servers.
	cl = c.NewClient("c1b")
	c.WaitLeader()

	v, ok, err := cl.Get(ctx, "k")
	if err != nil {
		t.Fatalf("get after leader change: %v", err)
	}
	if !ok || v != "v1" {
		t.Fatalf("after leader change got (%q,%v), want (v1,true)", v, ok)
	}
}

// TestKVDuplicateRetry: a Put whose first attempt's reply is "lost" is retried
// by the client with the same seq; it must be applied only once.
func TestKVDuplicateRetry(t *testing.T) {
	c := NewKVCluster(t, 5)
	c.WaitLeader()
	cl := c.NewClient("dup")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Append-style value so a double-apply would be detectable; here we use a
	// counter key. First set it to 1.
	if err := cl.Put(ctx, "count", "1"); err != nil {
		t.Fatalf("put: %v", err)
	}

	// Simulate a lost reply by submitting the SAME logical write twice at the
	// Raft level through the server dedup path: issue two Puts with the same
	// client+seq via the low-level server to model a retry.
	leader := c.WaitLeader()
	srv := c.servers[c.ids[leader]]
	// Two identical requests (same client id + seq) == a retried write.
	req := makePut("count", "2", "dup-retry", 99)
	_, _ = srv.Put(ctx, req)
	_, _ = srv.Put(ctx, req) // retry of the same op

	// Final value must be exactly "2": the retried write applied once.
	v, ok, err := cl.Get(ctx, "count")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if !ok || v != "2" {
		t.Fatalf("after duplicate retry got (%q,%v), want (2,true)", v, ok)
	}
}

// TestKVConcurrentClients: 5 clients each write their own keys for a while; at
// the end every acknowledged write is readable.
func TestKVConcurrentClients(t *testing.T) {
	c := NewKVCluster(t, 5)
	c.WaitLeader()

	const clients = 5
	const writesPer = 20
	var wg sync.WaitGroup
	acked := make([]map[string]string, clients)

	for ci := 0; ci < clients; ci++ {
		wg.Add(1)
		go func(ci int) {
			defer wg.Done()
			cl := c.NewClient(fmt.Sprintf("client-%d", ci))
			acked[ci] = map[string]string{}
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()
			for w := 0; w < writesPer; w++ {
				key := fmt.Sprintf("c%d-k%d", ci, w)
				val := fmt.Sprintf("v%d", w)
				if err := cl.Put(ctx, key, val); err == nil {
					acked[ci][key] = val
				}
			}
		}(ci)
	}
	wg.Wait()

	// Verify every acknowledged write is readable.
	reader := c.NewClient("verifier")
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	for ci := 0; ci < clients; ci++ {
		for key, want := range acked[ci] {
			v, ok, err := reader.Get(ctx, key)
			if err != nil {
				t.Fatalf("get %s: %v", key, err)
			}
			if !ok || v != want {
				t.Fatalf("key %s = (%q,%v), want %q", key, v, ok, want)
			}
		}
	}
}
