package storage

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/atharv-pdarshi/kvraft/raft"
)

func entry(term uint64, cmd string) raft.LogEntry {
	return raft.LogEntry{Term: term, Command: []byte(cmd)}
}

func TestFileStateRoundTrip(t *testing.T) {
	dir := t.TempDir()
	f, err := NewFile(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.SaveState(7, "node3"); err != nil {
		t.Fatal(err)
	}
	f.Close()

	g, err := NewFile(dir)
	if err != nil {
		t.Fatal(err)
	}
	term, vote, _, _ := g.Load()
	if term != 7 || vote != "node3" {
		t.Fatalf("reloaded (%d,%q), want (7,node3)", term, vote)
	}
}

func TestFileLogRoundTrip(t *testing.T) {
	dir := t.TempDir()
	f, _ := NewFile(dir)
	in := []raft.LogEntry{entry(1, "a"), entry(1, "b"), entry(2, "c")}
	if err := f.AppendLog(in); err != nil {
		t.Fatal(err)
	}
	f.Close()

	g, _ := NewFile(dir)
	_, _, out, _ := g.Load()
	if len(out) != 3 || string(out[2].Command) != "c" || out[2].Term != 2 {
		t.Fatalf("reloaded %+v", out)
	}
}

func TestFileTruncateSuffix(t *testing.T) {
	dir := t.TempDir()
	f, _ := NewFile(dir)
	f.AppendLog([]raft.LogEntry{entry(1, "a"), entry(1, "b"), entry(1, "c"), entry(1, "d")})
	// Drop entries at index >= 3 (keep a,b).
	if err := f.TruncateSuffix(3); err != nil {
		t.Fatal(err)
	}
	// Append a replacement; it must land at index 3.
	f.AppendLog([]raft.LogEntry{entry(2, "C2")})
	f.Close()

	g, _ := NewFile(dir)
	_, _, out, _ := g.Load()
	if len(out) != 3 {
		t.Fatalf("want 3 entries after truncate+append, got %d: %+v", len(out), out)
	}
	if string(out[2].Command) != "C2" || out[2].Term != 2 {
		t.Fatalf("index 3 is %+v, want term 2 C2", out[2])
	}
}

// TestTornWrite cuts the last log record in half on disk; the node must still
// start, dropping only the torn record, and keep the good prefix.
func TestTornWrite(t *testing.T) {
	dir := t.TempDir()
	f, _ := NewFile(dir)
	f.AppendLog([]raft.LogEntry{entry(1, "keep1"), entry(1, "keep2"), entry(1, "torn")})
	f.Close()

	// Chop the last few bytes off the log file to simulate a half-written record.
	logPath := filepath.Join(dir, "log")
	info, err := os.Stat(logPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Truncate(logPath, info.Size()-3); err != nil {
		t.Fatal(err)
	}

	g, err := NewFile(dir)
	if err != nil {
		t.Fatalf("node failed to start after torn write: %v", err)
	}
	_, _, out, _ := g.Load()
	if len(out) != 2 || string(out[1].Command) != "keep2" {
		t.Fatalf("torn recovery kept %+v, want keep1,keep2", out)
	}
	// After recovery the file must be sound: a new append reloads cleanly.
	if err := g.AppendLog([]raft.LogEntry{entry(2, "after")}); err != nil {
		t.Fatal(err)
	}
	g.Close()
	h, _ := NewFile(dir)
	_, _, out2, _ := h.Load()
	if len(out2) != 3 || string(out2[2].Command) != "after" {
		t.Fatalf("after torn recovery + append, got %+v", out2)
	}
}

// Satisfy the compile-time check that File implements raft.Storage.
var _ raft.Storage = (*File)(nil)
