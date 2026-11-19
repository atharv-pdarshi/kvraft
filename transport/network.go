// Package transport provides the two Transport implementations a node can run
// on: an in-memory Network for tests (which can cut, delay and drop messages)
// and a gRPC transport for real clusters (Stage 8).
package transport

import (
	"context"
	"math/rand"
	"sync"
	"time"

	"github.com/atharv-pdarshi/kvraft/raft"
)

// Network is an in-memory mesh connecting several Raft nodes inside one test
// process. It implements raft.Transport. A node reaches a peer by calling the
// network, which looks up the peer's handler and calls it directly — unless the
// link is cut, the two nodes are in different partitions, or the message is
// (randomly) dropped or delayed.
type Network struct {
	mu         sync.Mutex
	handlers   map[string]raft.RPCHandler
	connected  map[string]bool
	partition  map[string]int // partition group; same group == can talk
	dropRate   float64
	delayMin   time.Duration
	delayMax   time.Duration
	reqTimeout time.Duration
	rng        *rand.Rand
}

// NewNetwork returns an empty network. reqTimeout bounds how long a call to an
// unreachable node blocks before it reports failure (modelling a dead server).
func NewNetwork(reqTimeout time.Duration) *Network {
	if reqTimeout <= 0 {
		reqTimeout = 100 * time.Millisecond
	}
	return &Network{
		handlers:   make(map[string]raft.RPCHandler),
		connected:  make(map[string]bool),
		partition:  make(map[string]int),
		reqTimeout: reqTimeout,
		rng:        rand.New(rand.NewSource(1)),
	}
}

// SetHandler registers (or replaces) a node's RPC handler and marks it
// connected. Used when a node starts or restarts.
func (nw *Network) SetHandler(id string, h raft.RPCHandler) {
	nw.mu.Lock()
	defer nw.mu.Unlock()
	nw.handlers[id] = h
	nw.connected[id] = true
	nw.partition[id] = 0
}

// RemoveHandler detaches a node (used on crash) so calls to it fail.
func (nw *Network) RemoveHandler(id string) {
	nw.mu.Lock()
	defer nw.mu.Unlock()
	delete(nw.handlers, id)
	nw.connected[id] = false
}

// Disconnect / Connect cut and restore a single node's links.
func (nw *Network) Disconnect(id string) { nw.setConnected(id, false) }
func (nw *Network) Connect(id string)    { nw.setConnected(id, true) }

// IsConnected reports whether a node currently has its links up. An observer in
// the majority cannot see a partitioned-away node, so tests use this to ignore
// a stale leader isolated in a minority.
func (nw *Network) IsConnected(id string) bool {
	nw.mu.Lock()
	defer nw.mu.Unlock()
	return nw.connected[id]
}

func (nw *Network) setConnected(id string, v bool) {
	nw.mu.Lock()
	defer nw.mu.Unlock()
	nw.connected[id] = v
}

// Partition splits the listed nodes into two groups that cannot talk to each
// other (but can within their group). Nodes not listed keep their group.
func (nw *Network) Partition(groupA, groupB []string) {
	nw.mu.Lock()
	defer nw.mu.Unlock()
	for _, id := range groupA {
		nw.partition[id] = 1
	}
	for _, id := range groupB {
		nw.partition[id] = 2
	}
}

// Heal removes all partitions, putting every node back in one group.
func (nw *Network) Heal() {
	nw.mu.Lock()
	defer nw.mu.Unlock()
	for id := range nw.partition {
		nw.partition[id] = 0
	}
}

// SetDropRate makes the network drop that fraction of messages (0..1).
func (nw *Network) SetDropRate(r float64) {
	nw.mu.Lock()
	defer nw.mu.Unlock()
	nw.dropRate = r
}

// SetDelay delays each delivered message by a random amount in [min,max].
func (nw *Network) SetDelay(min, max time.Duration) {
	nw.mu.Lock()
	defer nw.mu.Unlock()
	nw.delayMin, nw.delayMax = min, max
}

// SendRequestVote implements raft.Transport.
func (nw *Network) SendRequestVote(ctx context.Context, to string, args *raft.RequestVoteArgs) (*raft.RequestVoteReply, error) {
	h, err := nw.deliver(ctx, args.CandidateID, to)
	if err != nil {
		return nil, err
	}
	return h.RequestVote(args), nil
}

// SendAppendEntries implements raft.Transport.
func (nw *Network) SendAppendEntries(ctx context.Context, to string, args *raft.AppendEntriesArgs) (*raft.AppendEntriesReply, error) {
	h, err := nw.deliver(ctx, args.LeaderID, to)
	if err != nil {
		return nil, err
	}
	return h.AppendEntries(args), nil
}

// deliver enforces connectivity, drop and delay, then returns the target's
// handler. It never holds the network lock while sleeping or calling a handler.
func (nw *Network) deliver(ctx context.Context, from, to string) (raft.RPCHandler, error) {
	nw.mu.Lock()
	reachable := nw.reachableLocked(from, to)
	h := nw.handlers[to]
	drop := reachable && nw.rng.Float64() < nw.dropRate
	var delay time.Duration
	if reachable && nw.delayMax > 0 {
		d := nw.delayMax - nw.delayMin
		if d <= 0 {
			delay = nw.delayMin
		} else {
			delay = nw.delayMin + time.Duration(nw.rng.Int63n(int64(d)))
		}
	}
	timeout := nw.reqTimeout
	nw.mu.Unlock()

	if !reachable || h == nil || drop {
		// Model a lost message / dead peer: block until the deadline, then fail.
		return nil, waitTimeout(ctx, timeout)
	}
	if delay > 0 {
		if err := sleepCtx(ctx, delay); err != nil {
			return nil, err
		}
	}
	return h, nil
}

func (nw *Network) reachableLocked(from, to string) bool {
	if !nw.connected[from] || !nw.connected[to] {
		return false
	}
	return nw.partition[from] == nw.partition[to]
}

// waitTimeout blocks until ctx is done or the fallback timeout elapses, then
// returns a timeout error — just like an RPC to a server that never answers.
func waitTimeout(ctx context.Context, timeout time.Duration) error {
	t := time.NewTimer(timeout)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return context.DeadlineExceeded
	}
}

func sleepCtx(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}
