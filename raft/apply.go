package raft

// applier delivers committed entries to the state machine over ApplyCh, one at
// a time and strictly in index order. It waits on a condition variable until
// commitIndex moves past lastApplied, and it never holds the Raft lock while
// sending on the channel — a slow reader must not be able to freeze the node.
func (n *Node) applier() {
	defer n.wg.Done()
	n.mu.Lock()
	for {
		for !n.stopped && n.commitIndex <= n.lastApplied {
			n.applyCond.Wait()
		}
		if n.stopped {
			n.mu.Unlock()
			return
		}

		from := n.lastApplied + 1
		to := n.commitIndex
		msgs := make([]ApplyMsg, 0, to-from+1)
		for i := from; i <= to; i++ {
			msgs = append(msgs, ApplyMsg{
				Index:   i,
				Term:    n.log[i].Term,
				Command: append([]byte(nil), n.log[i].Command...),
			})
		}
		n.lastApplied = to
		n.mu.Unlock()

		for _, m := range msgs {
			select {
			case n.applyCh <- m:
			case <-n.stopCh:
				return
			}
		}
		n.mu.Lock()
	}
}
