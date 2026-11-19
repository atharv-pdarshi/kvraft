// Package tests contains the in-memory cluster harness and every cluster-level
// test. The harness builds N Raft nodes on one shared in-memory network, lets a
// test crash, restart and partition them, and continuously checks the core
// safety property: no two nodes ever apply different commands at the same index.
package tests

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/atharv-pdarshi/kvraft/raft"
	"github.com/atharv-pdarshi/kvraft/storage"
	"github.com/atharv-pdarshi/kvraft/transport"
)

// Cluster is a set of in-memory Raft nodes wired to one Network.
type Cluster struct {
	t   *testing.T
	n   int
	ids []string

	net   *transport.Network
	nodes map[string]*raft.Node
	store map[string]raft.Storage

	// File-backed mode (used by the torn-write cluster test).
	fileMode bool
	dir      string

	electionMin time.Duration
	electionMax time.Duration
	heartbeat   time.Duration

	mu sync.Mutex
	// applied[id][index] = command last applied by that node (current instance).
	applied map[string]map[uint64][]byte
	// cmdAt[index] = the one command every node must agree on at that index.
	cmdAt map[uint64][]byte
	// expectNext[id] = next index this instance should apply (gap/repeat check).
	expectNext map[string]uint64
	stopReader map[string]chan struct{}
}

// NewCluster builds and starts n nodes backed by in-memory storage.
func NewCluster(t *testing.T, n int) *Cluster {
	return newCluster(t, n, false)
}

// NewFileCluster builds n nodes backed by real on-disk storage in a temp
// directory, so a test can corrupt files and restart.
func NewFileCluster(t *testing.T, n int) *Cluster {
	return newCluster(t, n, true)
}

func newCluster(t *testing.T, n int, fileMode bool) *Cluster {
	t.Helper()
	c := &Cluster{
		t:           t,
		n:           n,
		net:         transport.NewNetwork(80 * time.Millisecond),
		nodes:       make(map[string]*raft.Node),
		store:       make(map[string]raft.Storage),
		fileMode:    fileMode,
		electionMin: 150 * time.Millisecond,
		electionMax: 300 * time.Millisecond,
		heartbeat:   50 * time.Millisecond,
		applied:     make(map[string]map[uint64][]byte),
		cmdAt:       make(map[uint64][]byte),
		expectNext:  make(map[string]uint64),
		stopReader:  make(map[string]chan struct{}),
	}
	if fileMode {
		c.dir = t.TempDir()
	}
	for i := 0; i < n; i++ {
		c.ids = append(c.ids, fmt.Sprintf("node%d", i+1))
	}
	for _, id := range c.ids {
		if !fileMode {
			c.store[id] = storage.NewMemory()
		}
		c.start(id)
	}
	t.Cleanup(c.Stop)
	return c
}

// storageFor returns the storage a (re)starting node should use: the same
// in-memory object across restarts, or a freshly-reopened file store that reads
// whatever is currently on disk.
func (c *Cluster) storageFor(id string) raft.Storage {
	if !c.fileMode {
		return c.store[id]
	}
	st, err := storage.NewFile(filepath.Join(c.dir, id))
	if err != nil {
		c.t.Fatalf("open file storage for %s: %v", id, err)
	}
	c.store[id] = st
	return st
}

// logFilePath returns the on-disk log path for node i (file mode only).
func (c *Cluster) logFilePath(i int) string {
	return filepath.Join(c.dir, c.ids[i], "log")
}

// truncateLogTail chops n bytes off node i's on-disk log, simulating a torn
// final write. The node must be crashed (file handle closed) first.
func (c *Cluster) truncateLogTail(i, n int) {
	c.t.Helper()
	path := c.logFilePath(i)
	info, err := os.Stat(path)
	if err != nil {
		c.t.Fatalf("stat log %s: %v", path, err)
	}
	size := info.Size() - int64(n)
	if size < 0 {
		size = 0
	}
	if err := os.Truncate(path, size); err != nil {
		c.t.Fatalf("truncate log %s: %v", path, err)
	}
}

// peersOf returns every id except self.
func (c *Cluster) peersOf(self string) []string {
	var out []string
	for _, id := range c.ids {
		if id != self {
			out = append(out, id)
		}
	}
	return out
}

// start creates a fresh node instance for id using its existing storage and
// begins draining its ApplyCh through the safety checker.
func (c *Cluster) start(id string) {
	cfg := raft.Config{
		ID:                 id,
		Peers:              c.peersOf(id),
		Transport:          c.net,
		Storage:            c.storageFor(id),
		ElectionTimeoutMin: c.electionMin,
		ElectionTimeoutMax: c.electionMax,
		HeartbeatInterval:  c.heartbeat,
	}
	node, err := raft.NewNode(cfg)
	if err != nil {
		c.t.Fatalf("new node %s: %v", id, err)
	}
	c.nodes[id] = node
	c.net.SetHandler(id, node)

	c.mu.Lock()
	c.applied[id] = make(map[uint64][]byte)
	c.expectNext[id] = 1
	done := make(chan struct{})
	c.stopReader[id] = done
	c.mu.Unlock()

	go c.drain(id, node.ApplyCh(), done)
}

// drain reads committed entries from one node and feeds the checker.
func (c *Cluster) drain(id string, ch <-chan raft.ApplyMsg, done chan struct{}) {
	for {
		select {
		case <-done:
			return
		case msg, ok := <-ch:
			if !ok {
				return
			}
			c.record(id, msg)
		}
	}
}

// record enforces (a) no gaps or repeats within one node instance and (b) all
// nodes agree on the command at each index.
func (c *Cluster) record(id string, msg raft.ApplyMsg) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if want := c.expectNext[id]; msg.Index != want {
		c.t.Errorf("apply out of order on %s: got index %d, want %d", id, msg.Index, want)
	}
	c.expectNext[id] = msg.Index + 1

	if prev, ok := c.cmdAt[msg.Index]; ok {
		if !bytes.Equal(prev, msg.Command) {
			c.t.Errorf("STATE MACHINE SAFETY violated at index %d: %q vs %q",
				msg.Index, prev, msg.Command)
		}
	} else {
		c.cmdAt[msg.Index] = append([]byte(nil), msg.Command...)
	}
	c.applied[id][msg.Index] = append([]byte(nil), msg.Command...)
}

// Crash stops node i but keeps its storage, so Restart can bring it back as
// itself. Peers can no longer reach it.
func (c *Cluster) Crash(i int) {
	id := c.ids[i]
	c.mu.Lock()
	done := c.stopReader[id]
	c.mu.Unlock()
	if done != nil {
		close(done)
	}
	c.net.RemoveHandler(id)
	if node := c.nodes[id]; node != nil {
		node.Stop()
	}
	delete(c.nodes, id)
	// Release the file handle so a restart reopens from disk, as a real
	// process restart would.
	if c.fileMode {
		if st, ok := c.store[id].(*storage.File); ok {
			st.Close()
		}
	}
}

// Restart builds a new node for i from its retained storage.
func (c *Cluster) Restart(i int) {
	c.start(c.ids[i])
}

// Stop shuts the whole cluster down.
func (c *Cluster) Stop() {
	for id, node := range c.nodes {
		c.mu.Lock()
		done := c.stopReader[id]
		c.mu.Unlock()
		if done != nil {
			close(done)
			c.mu.Lock()
			c.stopReader[id] = nil
			c.mu.Unlock()
		}
		node.Stop()
	}
	c.nodes = make(map[string]*raft.Node)
	if c.fileMode {
		for _, st := range c.store {
			if f, ok := st.(*storage.File); ok {
				f.Close()
			}
		}
	}
}

// Disconnect / Connect / Partition / Heal expose the network controls by index.
func (c *Cluster) Disconnect(i int)  { c.net.Disconnect(c.ids[i]) }
func (c *Cluster) Connect(i int)     { c.net.Connect(c.ids[i]) }
func (c *Cluster) Heal()             { c.net.Heal() }
func (c *Cluster) Net() *transport.Network { return c.net }

// Partition splits the cluster by index lists.
func (c *Cluster) Partition(groupA, groupB []int) {
	toIDs := func(xs []int) []string {
		var out []string
		for _, i := range xs {
			out = append(out, c.ids[i])
		}
		return out
	}
	c.net.Partition(toIDs(groupA), toIDs(groupB))
}

// alive returns the live node instances.
func (c *Cluster) alive() []*raft.Node {
	var out []*raft.Node
	for _, n := range c.nodes {
		out = append(out, n)
	}
	return out
}
