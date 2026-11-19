package raft

import (
	"context"
)

// maxEntriesPerAppend caps how many entries one AppendEntries carries, so a
// badly lagging follower catches up over several bounded messages.
const maxEntriesPerAppend = 100

// sendAppendEntries replicates to a single peer based on its nextIndex, then
// applies the reply: advancing match/next on success, or backing nextIndex up
// on a mismatch and retrying. It is always run in its own goroutine.
func (n *Node) sendAppendEntries(peer string) {
	n.mu.Lock()
	if n.role != Leader {
		n.mu.Unlock()
		return
	}
	term := n.currentTerm
	ni := n.nextIndex[peer]
	if ni < 1 {
		ni = 1
	}
	prevLogIndex := ni - 1
	prevLogTerm := n.log[prevLogIndex].Term

	end := uint64(len(n.log))
	if end > ni+maxEntriesPerAppend {
		end = ni + maxEntriesPerAppend
	}
	var entries []LogEntry
	if ni < end {
		entries = append([]LogEntry(nil), n.log[ni:end]...)
	}
	leaderCommit := n.commitIndex
	n.mu.Unlock()

	args := &AppendEntriesArgs{
		Term:         term,
		LeaderID:     n.id,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      entries,
		LeaderCommit: leaderCommit,
	}
	ctx, cancel := context.WithTimeout(context.Background(), n.rpcTimeout())
	defer cancel()
	reply, err := n.transport.SendAppendEntries(ctx, peer, args)
	if err != nil {
		return
	}

	n.mu.Lock()
	defer n.mu.Unlock()
	// Ignore stale replies.
	if n.currentTerm != term || n.role != Leader {
		return
	}
	if reply.Term > n.currentTerm {
		n.becomeFollower(reply.Term)
		return
	}

	if reply.Success {
		// Use the values we SENT: the log may have grown meanwhile.
		match := prevLogIndex + uint64(len(entries))
		if match > n.matchIndex[peer] {
			n.matchIndex[peer] = match
		}
		n.nextIndex[peer] = n.matchIndex[peer] + 1
		n.maybeAdvanceCommit()
		// More to send? Keep streaming until the follower is caught up.
		if n.nextIndex[peer] <= n.lastLogIndex() {
			go n.sendAppendEntries(peer)
		}
		return
	}

	// Mismatch: back nextIndex up using the follower's conflict hint, then retry.
	oldNext := n.nextIndex[peer]
	n.nextIndex[peer] = n.backtrackNext(peer, reply)
	if n.nextIndex[peer] < oldNext {
		go n.sendAppendEntries(peer)
	}
}

// backtrackNext computes the new nextIndex for a peer after a failed
// AppendEntries, using fast backtracking (skip a whole conflicting term at
// once) rather than decrementing by one. Caller holds n.mu.
func (n *Node) backtrackNext(peer string, reply *AppendEntriesReply) uint64 {
	if reply.ConflictTerm < 0 {
		// Follower's log was too short: jump straight to its end.
		next := reply.ConflictIndex
		if next < 1 {
			next = 1
		}
		return next
	}
	// Follower had a conflicting term. If the leader also has that term, resume
	// just after the leader's last entry of it; otherwise fall back to the
	// follower's first index of the term.
	ct := uint64(reply.ConflictTerm)
	for i := n.lastLogIndex(); i >= 1; i-- {
		if n.log[i].Term == ct {
			return i + 1
		}
		if i == 1 {
			break
		}
	}
	next := reply.ConflictIndex
	if next < 1 {
		next = 1
	}
	return next
}

// maybeAdvanceCommit recomputes the leader's commit index: an entry is
// committed once a majority stores it AND it belongs to the current term. The
// current-term check is what prevents the Figure 8 scenario from un-committing
// an entry. Caller holds n.mu.
func (n *Node) maybeAdvanceCommit() {
	if n.role != Leader {
		return
	}
	majority := (len(n.peers)+1)/2 + 1
	for N := n.lastLogIndex(); N > n.commitIndex; N-- {
		// A leader may only directly commit entries from its own term.
		if n.log[N].Term != n.currentTerm {
			continue
		}
		count := 1 // the leader stores it itself
		for _, p := range n.peers {
			if n.matchIndex[p] >= N {
				count++
			}
		}
		if count >= majority {
			n.commitIndex = N
			n.applyCond.Signal()
			break
		}
	}
}
