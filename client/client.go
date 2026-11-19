// Package client is the key-value client library. It hides leader discovery and
// retries behind a simple Put/Get/Delete API: it picks a node, follows
// leader_hint on NOT_LEADER, and retries timeouts with the same client_id and
// seq so a write is never applied twice.
package client

import (
	"context"
	"fmt"
	"math/rand"
	"sort"
	"sync"
	"time"

	kvpb "github.com/atharv-pdarshi/kvraft/proto/kvpb"
)

// Endpoint is one node's client-facing surface. Both *kv.Server (in-memory) and
// the gRPC wrapper implement it, so the retry logic is shared.
type Endpoint interface {
	Put(ctx context.Context, req *kvpb.PutRequest) (*kvpb.PutResponse, error)
	Get(ctx context.Context, req *kvpb.GetRequest) (*kvpb.GetResponse, error)
	Delete(ctx context.Context, req *kvpb.DeleteRequest) (*kvpb.DeleteResponse, error)
}

// Client talks to a cluster of endpoints keyed by node id.
type Client struct {
	clientID  string
	endpoints map[string]Endpoint
	ids       []string

	mu     sync.Mutex
	leader string // last known leader id ("" if unknown)
	seq    uint64
	rng    *rand.Rand
}

// New builds a client over the given endpoints. clientID must be unique per
// client so the server can deduplicate that client's retries.
func New(clientID string, endpoints map[string]Endpoint) *Client {
	ids := make([]string, 0, len(endpoints))
	for id := range endpoints {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return &Client{
		clientID:  clientID,
		endpoints: endpoints,
		ids:       ids,
		rng:       rand.New(rand.NewSource(int64(len(clientID) + 1))),
	}
}

// nextSeq allocates a fresh sequence number for a new operation.
func (c *Client) nextSeq() uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.seq++
	return c.seq
}

// target returns the id to try next: the known leader, else a random node.
func (c *Client) target() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.leader != "" {
		if _, ok := c.endpoints[c.leader]; ok {
			return c.leader
		}
	}
	return c.ids[c.rng.Intn(len(c.ids))]
}

func (c *Client) setLeader(id string) {
	c.mu.Lock()
	c.leader = id
	c.mu.Unlock()
}

func (c *Client) clearLeader() {
	c.mu.Lock()
	c.leader = ""
	c.mu.Unlock()
}

// Put stores a value, retrying through leader changes until it succeeds or ctx
// is cancelled.
func (c *Client) Put(ctx context.Context, key, value string) error {
	seq := c.nextSeq()
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		id := c.target()
		ep := c.endpoints[id]
		resp, err := ep.Put(ctx, &kvpb.PutRequest{Key: key, Value: value, ClientId: c.clientID, Seq: seq})
		if err != nil {
			c.clearLeader()
			time.Sleep(20 * time.Millisecond)
			continue
		}
		switch resp.Status {
		case kvpb.Status_OK:
			c.setLeader(id)
			return nil
		case kvpb.Status_NOT_LEADER:
			c.redirect(resp.LeaderHint)
		default:
			c.clearLeader()
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// Delete removes a key, with the same retry behaviour as Put.
func (c *Client) Delete(ctx context.Context, key string) error {
	seq := c.nextSeq()
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		id := c.target()
		ep := c.endpoints[id]
		resp, err := ep.Delete(ctx, &kvpb.DeleteRequest{Key: key, ClientId: c.clientID, Seq: seq})
		if err != nil {
			c.clearLeader()
			time.Sleep(20 * time.Millisecond)
			continue
		}
		switch resp.Status {
		case kvpb.Status_OK:
			c.setLeader(id)
			return nil
		case kvpb.Status_NOT_LEADER:
			c.redirect(resp.LeaderHint)
		default:
			c.clearLeader()
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// Get reads a value, returning (value, found, error).
func (c *Client) Get(ctx context.Context, key string) (string, bool, error) {
	seq := c.nextSeq()
	for {
		if err := ctx.Err(); err != nil {
			return "", false, err
		}
		id := c.target()
		ep := c.endpoints[id]
		resp, err := ep.Get(ctx, &kvpb.GetRequest{Key: key, ClientId: c.clientID, Seq: seq})
		if err != nil {
			c.clearLeader()
			time.Sleep(20 * time.Millisecond)
			continue
		}
		switch resp.Status {
		case kvpb.Status_OK:
			c.setLeader(id)
			return resp.Value, resp.Found, nil
		case kvpb.Status_NOT_LEADER:
			c.redirect(resp.LeaderHint)
		default:
			c.clearLeader()
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// redirect points the client at the hinted leader, or clears it if the hint is
// empty or unknown.
func (c *Client) redirect(hint string) {
	if hint == "" {
		c.clearLeader()
		return
	}
	if _, ok := c.endpoints[hint]; ok {
		c.setLeader(hint)
		return
	}
	c.clearLeader()
}

// String identifies the client in logs.
func (c *Client) String() string { return fmt.Sprintf("client(%s)", c.clientID) }
