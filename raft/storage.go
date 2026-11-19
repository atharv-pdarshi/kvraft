package raft

// Storage is the durable state a node must keep to survive a crash. Only the
// three pieces of Figure 2's "Persistent state" live here — currentTerm,
// votedFor and the log. commitIndex, lastApplied, nextIndex and matchIndex are
// rebuilt after a restart and are never saved.
//
// The interface is defined here (where it is consumed) and implemented in
// package storage: an in-memory version for tests and a file version for real
// nodes. Every mutating call must be durable (fsync'd) before it returns, so
// callers can fsync-before-reply simply by calling these.
type Storage interface {
	// SaveState persists currentTerm and votedFor atomically.
	SaveState(term uint64, votedFor string) error
	// AppendLog durably appends entries to the end of the on-disk log. The
	// persisted log holds real entries at indices 1..N; the dummy index-0
	// entry is implicit and never stored.
	AppendLog(entries []LogEntry) error
	// TruncateSuffix removes every persisted entry with index >= from.
	TruncateSuffix(from uint64) error
	// Load returns the persisted term, vote and log entries (indices 1..N).
	Load() (term uint64, votedFor string, entries []LogEntry, err error)
}
