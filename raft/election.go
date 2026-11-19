package raft

import (
	"context"
	"sync"
	"time"
)

// run launches the node's background goroutines. Stage 3 starts the election
// ticker (drives elections and leader heartbeats). Stage 5 adds the applier.
func (n *Node) run() {
	n.wg.Add(2)
	go n.electionTicker()
	go n.heartbeatTicker()
}

// rpcTimeout bounds a single outbound RPC. It is longer than the network's
// unreachable-timeout so a dead peer fails via the transport, not via ctx.
func (n *Node) rpcTimeout() time.Duration { return n.electionMin }

// electionTicker periodically checks whether we have gone too long without
// hearing from a leader; if so (and we are not the leader) it starts an election.
func (n *Node) electionTicker() {
	defer n.wg.Done()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-n.stopCh:
			return
		case <-ticker.C:
			n.mu.Lock()
			timedOut := n.role != Leader && time.Since(n.lastHeard) >= n.electionWait
			n.mu.Unlock()
			if timedOut {
				n.startElection()
			}
		}
	}
}

// heartbeatTicker makes the leader send AppendEntries to every peer on a fixed
// interval so followers keep their election timers reset.
func (n *Node) heartbeatTicker() {
	defer n.wg.Done()
	ticker := time.NewTicker(n.heartbeat)
	defer ticker.Stop()
	for {
		select {
		case <-n.stopCh:
			return
		case <-ticker.C:
			n.mu.Lock()
			isLeader := n.role == Leader
			n.mu.Unlock()
			if isLeader {
				n.broadcastAppendEntries()
			}
		}
	}
}

// startElection turns this node into a candidate for a new term and asks every
// peer for its vote, in parallel.
func (n *Node) startElection() {
	n.mu.Lock()
	if n.stopped {
		n.mu.Unlock()
		return
	}
	n.role = Candidate
	n.currentTerm++
	n.votedFor = n.id
	n.leaderID = ""
	n.persistState()
	n.resetElectionTimer()

	term := n.currentTerm
	lastIdx := n.lastLogIndex()
	lastTerm := n.lastLogTerm()
	peers := append([]string(nil), n.peers...)
	n.logger.Printf("term %d: starting election", term)
	n.mu.Unlock()

	var votesMu sync.Mutex
	votes := 1 // vote for self
	majority := (len(peers)+1)/2 + 1

	for _, peer := range peers {
		go func(peer string) {
			args := &RequestVoteArgs{
				Term:         term,
				CandidateID:  n.id,
				LastLogIndex: lastIdx,
				LastLogTerm:  lastTerm,
			}
			ctx, cancel := context.WithTimeout(context.Background(), n.rpcTimeout())
			defer cancel()
			reply, err := n.transport.SendRequestVote(ctx, peer, args)
			if err != nil {
				return
			}

			n.mu.Lock()
			defer n.mu.Unlock()
			// Ignore stale replies: term moved on, or we are no longer a candidate.
			if n.currentTerm != term || n.role != Candidate {
				return
			}
			if reply.Term > n.currentTerm {
				n.becomeFollower(reply.Term)
				return
			}
			if !reply.VoteGranted {
				return
			}

			votesMu.Lock()
			votes++
			won := votes >= majority
			votesMu.Unlock()
			if won {
				n.becomeLeader()
			}
		}(peer)
	}
}

// becomeLeader promotes the node and seeds the leader bookkeeping. Caller holds
// n.mu and has verified it is still a candidate in the election's term.
func (n *Node) becomeLeader() {
	if n.role != Candidate {
		return
	}
	n.role = Leader
	n.leaderID = n.id
	last := n.lastLogIndex()
	for _, p := range n.peers {
		n.nextIndex[p] = last + 1
		n.matchIndex[p] = 0
	}
	n.logger.Printf("term %d: became LEADER", n.currentTerm)
	// Assert leadership immediately rather than waiting for the next tick.
	go n.broadcastAppendEntries()
}

// broadcastAppendEntries pushes the log (or a heartbeat) to every peer. Each
// peer is handled independently by sendAppendEntries, which uses that peer's
// nextIndex to decide what to send and how far to back up on a mismatch.
func (n *Node) broadcastAppendEntries() {
	n.mu.Lock()
	if n.role != Leader {
		n.mu.Unlock()
		return
	}
	peers := append([]string(nil), n.peers...)
	n.mu.Unlock()

	for _, peer := range peers {
		go n.sendAppendEntries(peer)
	}
}
