package tests

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/atharv-pdarshi/kvraft/client"
	"github.com/atharv-pdarshi/kvraft/kv"
	kvpb "github.com/atharv-pdarshi/kvraft/proto/kvpb"
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
	mu      sync.Mutex
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
	c.mu.Lock()
	c.nodes[id] = node
	c.servers[id] = kv.NewServer(id, node)
	c.mu.Unlock()
	c.net.SetHandler(id, node)
}

// Crash stops a node and its server but keeps the storage for a restart.
func (c *KVCluster) Crash(i int) {
	id := c.ids[i]
	c.net.RemoveHandler(id)
	c.mu.Lock()
	srv := c.servers[id]
	node := c.nodes[id]
	delete(c.servers, id)
	delete(c.nodes, id)
	c.mu.Unlock()
	if srv != nil {
		srv.Stop()
	}
	if node != nil {
		node.Stop()
	}
}

// getServer / getNode return the current live instance for an id, or nil.
func (c *KVCluster) getServer(id string) *kv.Server {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.servers[id]
}

func (c *KVCluster) getNode(id string) *raft.Node {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.nodes[id]
}

// Restart rebuilds node i from its retained storage.
func (c *KVCluster) Restart(i int) { c.start(c.ids[i]) }

// Disconnect / Connect cut and restore a node's network links.
func (c *KVCluster) Disconnect(i int) { c.net.Disconnect(c.ids[i]) }
func (c *KVCluster) Connect(i int)    { c.net.Connect(c.ids[i]) }

// Stop shuts the whole cluster down.
func (c *KVCluster) Stop() {
	c.mu.Lock()
	servers := c.servers
	nodes := c.nodes
	c.servers = map[string]*kv.Server{}
	c.nodes = map[string]*raft.Node{}
	c.mu.Unlock()
	for id, srv := range servers {
		srv.Stop()
		if node := nodes[id]; node != nil {
			node.Stop()
		}
	}
}

// liveEndpoint routes a client call to whichever server currently holds an id,
// so clients transparently follow crashes and restarts. A crashed node's call
// fails like a refused connection.
type liveEndpoint struct {
	c  *KVCluster
	id string
}

func (e liveEndpoint) Put(ctx context.Context, req *kvpb.PutRequest) (*kvpb.PutResponse, error) {
	srv := e.c.getServer(e.id)
	if srv == nil {
		return nil, errNodeDown
	}
	return srv.Put(ctx, req)
}
func (e liveEndpoint) Get(ctx context.Context, req *kvpb.GetRequest) (*kvpb.GetResponse, error) {
	srv := e.c.getServer(e.id)
	if srv == nil {
		return nil, errNodeDown
	}
	return srv.Get(ctx, req)
}
func (e liveEndpoint) Delete(ctx context.Context, req *kvpb.DeleteRequest) (*kvpb.DeleteResponse, error) {
	srv := e.c.getServer(e.id)
	if srv == nil {
		return nil, errNodeDown
	}
	return srv.Delete(ctx, req)
}

var errNodeDown = fmt.Errorf("node is down")

// NewClient returns a client that can reach every node id, routing to whatever
// instance is currently live.
func (c *KVCluster) NewClient(clientID string) *client.Client {
	eps := map[string]client.Endpoint{}
	for _, id := range c.ids {
		eps[id] = liveEndpoint{c: c, id: id}
	}
	return client.New(clientID, eps)
}

// indexOfLeader returns the index of the current leader, or -1.
func (c *KVCluster) leaderIndex() int {
	for i, id := range c.ids {
		if n := c.getNode(id); n != nil {
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
