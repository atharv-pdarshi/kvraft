package raft

import (
	"fmt"
	"math/rand"
	"sync"
	"time"
)

// Node is a single Raft server. Every field from Figure 2's "State" box lives
// here, guarded by one mutex.
type Node struct {
	mu sync.Mutex

	id        string
	peers     []string // other nodes' ids
	transport Transport
	storage   Storage
	logger    logger

	// --- Persistent state (saved before responding to RPCs) ---
	currentTerm uint64
	votedFor    string     // "" means "none"
	log         []LogEntry // log[0] is the dummy entry (term 0)

	// --- Volatile state on all servers ---
	commitIndex uint64
	lastApplied uint64
	role        Role
	leaderID    string

	// --- Volatile state on leaders (reinitialized after election) ---
	nextIndex  map[string]uint64
	matchIndex map[string]uint64

	// --- Timing ---
	electionMin  time.Duration
	electionMax  time.Duration
	heartbeat    time.Duration
	lastHeard    time.Time     // last time we heard from a leader or granted a vote
	electionWait time.Duration // current randomized election timeout
	rng          *rand.Rand

	// --- Lifecycle ---
	applyCh   chan ApplyMsg
	applyCond *sync.Cond
	stopCh    chan struct{}
	stopped   bool
	wg        sync.WaitGroup
}

// NewNode builds a node from cfg and its storage, restoring any persisted
// state. It does not start running until Start-up goroutines are launched by
// run(), which later stages add.
func NewNode(cfg Config) (*Node, error) {
	cfg.withDefaults()
	if cfg.Transport == nil {
		return nil, fmt.Errorf("raft: Config.Transport is nil")
	}
	if cfg.Storage == nil {
		return nil, fmt.Errorf("raft: Config.Storage is nil")
	}

	term, votedFor, entries, err := cfg.Storage.Load()
	if err != nil {
		return nil, fmt.Errorf("raft: load storage: %w", err)
	}

	n := &Node{
		id:          cfg.ID,
		peers:       append([]string(nil), cfg.Peers...),
		transport:   cfg.Transport,
		storage:     cfg.Storage,
		logger:      logger{l: cfg.Logger, id: cfg.ID},
		currentTerm: term,
		votedFor:    votedFor,
		log:         append([]LogEntry{{Term: 0}}, entries...), // dummy at index 0
		role:        Follower,
		nextIndex:   make(map[string]uint64),
		matchIndex:  make(map[string]uint64),
		electionMin: cfg.ElectionTimeoutMin,
		electionMax: cfg.ElectionTimeoutMax,
		heartbeat:   cfg.HeartbeatInterval,
		rng:         rand.New(rand.NewSource(seedFor(cfg.ID))),
		applyCh:     make(chan ApplyMsg, 256),
		stopCh:      make(chan struct{}),
	}
	n.applyCond = sync.NewCond(&n.mu)
	n.resetElectionTimer()

	// Stage 2 leaves the node idle (a Follower that never times out). Stage 3
	// starts the election ticker here; Stage 5 starts the applier.
	return n, nil
}

// ID returns this node's id.
func (n *Node) ID() string { return n.id }

// ApplyCh delivers committed entries, in order.
func (n *Node) ApplyCh() <-chan ApplyMsg { return n.applyCh }

// State reports the current term and whether this node believes it is leader.
func (n *Node) State() (term uint64, isLeader bool) {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.currentTerm, n.role == Leader
}

// Stop shuts the node down and waits for its goroutines to exit. It is safe to
// call more than once.
func (n *Node) Stop() {
	n.mu.Lock()
	if n.stopped {
		n.mu.Unlock()
		return
	}
	n.stopped = true
	close(n.stopCh)
	if n.applyCond != nil {
		n.applyCond.Broadcast()
	}
	n.mu.Unlock()
	n.wg.Wait()
	// Safe now that the applier (if any) has exited: no one will send again.
	close(n.applyCh)
}

// --- log helpers (caller holds n.mu) ---

func (n *Node) lastLogIndex() uint64 { return uint64(len(n.log) - 1) }
func (n *Node) lastLogTerm() uint64  { return n.log[len(n.log)-1].Term }

// persist writes the current term and vote to stable storage. Callers hold the
// lock; storage calls themselves must be durable.
func (n *Node) persistState() {
	if err := n.storage.SaveState(n.currentTerm, n.votedFor); err != nil {
		n.logger.Printf("WARN persist state: %v", err)
	}
}

// becomeFollower steps down into term. Caller holds n.mu.
func (n *Node) becomeFollower(term uint64) {
	changed := term != n.currentTerm
	n.role = Follower
	n.currentTerm = term
	if changed {
		n.votedFor = ""
		n.persistState()
	}
}

func (n *Node) resetElectionTimer() {
	span := n.electionMax - n.electionMin
	if span <= 0 {
		span = 1
	}
	n.electionWait = n.electionMin + time.Duration(n.rng.Int63n(int64(span)))
	n.lastHeard = time.Now()
}

func seedFor(id string) int64 {
	var h int64 = 1469598103934665603 // FNV-ish, deterministic per id
	for _, c := range id {
		h ^= int64(c)
		h *= 1099511628211
	}
	if h < 0 {
		h = -h
	}
	return h + time.Now().UnixNano()
}
