package tests

import (
	"fmt"
	"testing"
	"time"

	"github.com/atharv-pdarshi/kvraft/client"
	"github.com/atharv-pdarshi/kvraft/kv"
	"github.com/atharv-pdarshi/kvraft/raft"
	"github.com/atharv-pdarshi/kvraft/storage"
	"github.com/atharv-pdarshi/kvraft/transport"
)

// KVCluster is a set of Raft-backed KV servers on one in-memory network, used
// to test the key-value layer end to end through the client library.
type KVCluster struct {
	t   *testing.T
	n   int
	ids []string

	net     *transport.Network
	nodes   map[string]*raft.Node
	servers map[string]*kv.Server
	store   map[string]*storage.Memory

	electionMin time.Duration
	electionMax time.Duration
	heartbeat   time.Duration
}

// NewKVCluster builds and starts n KV nodes.
func NewKVCluster(t *testing.T, n int) *KVCluster {
	t.Helper()
	c := &KVCluster{
		t:           t,
		n:           n,
		net:         transport.NewNetwork(80 * time.Millisecond),
		nodes:       make(map[string]*raft.Node),
		servers:     make(map[string]*kv.Server),
		store:       make(map[string]*storage.Memory),
		electionMin: 150 * time.Millisecond,
		electionMax: 300 * time.Millisecond,
		heartbeat:   50 * time.Millisecond,
	}
	for i := 0; i < n; i++ {
		c.ids = append(c.ids, fmt.Sprintf("node%d", i+1))
	}
	for _, id := range c.ids {
		c.store[id] = storage.NewMemory()
		c.start(id)
	}
	t.Cleanup(c.Stop)
	return c
}

func (c *KVCluster) peersOf(self string) []string {
	var out []string
	for _, id := range c.ids {
		if id != self {
			out = append(out, id)
		}
	}
	return out
}

func (c *KVCluster) start(id string) {
	node, err := raft.NewNode(raft.Config{
		ID:                 id,
		Peers:              c.peersOf(id),
		Transport:          c.net,
		Storage:            c.store[id],
		ElectionTimeoutMin: c.electionMin,
		ElectionTimeoutMax: c.electionMax,
		HeartbeatInterval:  c.heartbeat,
	})
	if err != nil {
		c.t.Fatalf("new node %s: %v", id, err)
	}
	c.nodes[id] = node
	c.servers[id] = kv.NewServer(id, node)
	c.net.SetHandler(id, node)
}

// Crash stops a node and its server but keeps the storage for a restart.
func (c *KVCluster) Crash(i int) {
	id := c.ids[i]
	c.net.RemoveHandler(id)
	if srv := c.servers[id]; srv != nil {
		srv.Stop()
	}
	if node := c.nodes[id]; node != nil {
		node.Stop()
	}
	delete(c.servers, id)
	delete(c.nodes, id)
}

// Restart rebuilds node i from its retained storage.
func (c *KVCluster) Restart(i int) { c.start(c.ids[i]) }

// Disconnect / Connect cut and restore a node's network links.
func (c *KVCluster) Disconnect(i int) { c.net.Disconnect(c.ids[i]) }
func (c *KVCluster) Connect(i int)    { c.net.Connect(c.ids[i]) }

// Stop shuts the whole cluster down.
func (c *KVCluster) Stop() {
	for id, srv := range c.servers {
		srv.Stop()
		if node := c.nodes[id]; node != nil {
			node.Stop()
		}
	}
	c.servers = map[string]*kv.Server{}
	c.nodes = map[string]*raft.Node{}
}

// NewClient returns a client wired to every live server.
func (c *KVCluster) NewClient(clientID string) *client.Client {
	eps := map[string]client.Endpoint{}
	for id, srv := range c.servers {
		eps[id] = srv
	}
	return client.New(clientID, eps)
}

// indexOfLeader returns the index of the current leader, or -1.
func (c *KVCluster) leaderIndex() int {
	for i, id := range c.ids {
		if n := c.nodes[id]; n != nil {
			if _, isLeader := n.State(); isLeader {
				return i
			}
		}
	}
	return -1
}

// WaitLeader blocks until some node becomes leader.
func (c *KVCluster) WaitLeader() int {
	c.t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if i := c.leaderIndex(); i >= 0 {
			return i
		}
		time.Sleep(20 * time.Millisecond)
	}
	c.t.Fatal("no leader elected")
	return -1
}
