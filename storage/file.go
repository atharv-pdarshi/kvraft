package storage

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"sync"

	"github.com/atharv-pdarshi/kvraft/raft"
)

// File is a crash-safe, disk-backed raft.Storage. It keeps two files in a data
// directory:
//
//   - state: currentTerm and votedFor, rewritten atomically (temp file, fsync,
//     rename) on every change.
//   - log:   an append-only sequence of records, each framed with a length and
//     a CRC32 so a torn final write (a half-written record left by a crash) is
//     detected on load and dropped.
//
// Every mutating method fsyncs before returning, so a caller that fsyncs before
// replying to an RPC gets that guarantee simply by calling these.
type File struct {
	mu      sync.Mutex
	dir     string
	logPath string
	statePath string

	logFile *os.File
	entries []raft.LogEntry // in-memory mirror of the on-disk log (indices 1..N)

	term     uint64
	votedFor string
}

var crcTable = crc32.MakeTable(crc32.Castagnoli)

// NewFile opens (creating if needed) the data directory and loads any existing
// state and log, dropping a torn trailing log record.
func NewFile(dir string) (*File, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("storage: mkdir %s: %w", dir, err)
	}
	f := &File{
		dir:       dir,
		logPath:   filepath.Join(dir, "log"),
		statePath: filepath.Join(dir, "state"),
	}
	if err := f.loadState(); err != nil {
		return nil, err
	}
	if err := f.loadLog(); err != nil {
		return nil, err
	}
	lf, err := os.OpenFile(f.logPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return nil, fmt.Errorf("storage: open log: %w", err)
	}
	f.logFile = lf
	return f, nil
}

// --- raft.Storage ---

func (f *File) SaveState(term uint64, votedFor string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.term, f.votedFor = term, votedFor
	return f.writeStateAtomic()
}

func (f *File) AppendLog(entries []raft.LogEntry) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, e := range entries {
		if _, err := f.logFile.Write(encodeRecord(e)); err != nil {
			return fmt.Errorf("storage: append: %w", err)
		}
		f.entries = append(f.entries, e)
	}
	return f.logFile.Sync()
}

// TruncateSuffix removes entries with index >= from (1-based) by rewriting the
// log file atomically from the kept prefix.
func (f *File) TruncateSuffix(from uint64) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if from < 1 {
		from = 1
	}
	keep := int(from - 1)
	if keep >= len(f.entries) {
		return nil
	}
	f.entries = append([]raft.LogEntry(nil), f.entries[:keep]...)
	return f.rewriteLog()
}

func (f *File) Load() (uint64, string, []raft.LogEntry, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := append([]raft.LogEntry(nil), f.entries...)
	return f.term, f.votedFor, out, nil
}

// Close releases the open log file.
func (f *File) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.logFile != nil {
		err := f.logFile.Close()
		f.logFile = nil
		return err
	}
	return nil
}

// --- state file ---

func (f *File) writeStateAtomic() error {
	buf := make([]byte, 0, 32)
	var hdr [8]byte
	binary.LittleEndian.PutUint64(hdr[:], f.term)
	buf = append(buf, hdr[:]...)
	var vl [2]byte
	binary.LittleEndian.PutUint16(vl[:], uint16(len(f.votedFor)))
	buf = append(buf, vl[:]...)
	buf = append(buf, f.votedFor...)
	var crc [4]byte
	binary.LittleEndian.PutUint32(crc[:], crc32.Checksum(buf, crcTable))
	buf = append(buf, crc[:]...)

	tmp := f.statePath + ".tmp"
	if err := os.WriteFile(tmp, buf, 0o644); err != nil {
		return fmt.Errorf("storage: write state tmp: %w", err)
	}
	// Ensure the temp file's bytes hit disk before the rename.
	if tf, err := os.OpenFile(tmp, os.O_RDWR, 0o644); err == nil {
		_ = tf.Sync()
		_ = tf.Close()
	}
	if err := os.Rename(tmp, f.statePath); err != nil {
		return fmt.Errorf("storage: rename state: %w", err)
	}
	return syncDir(f.dir)
}

func (f *File) loadState() error {
	b, err := os.ReadFile(f.statePath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil // fresh node: term 0, no vote
		}
		return fmt.Errorf("storage: read state: %w", err)
	}
	if len(b) < 8+2+4 {
		return nil // too short to be valid; treat as fresh
	}
	payload := b[:len(b)-4]
	want := binary.LittleEndian.Uint32(b[len(b)-4:])
	if crc32.Checksum(payload, crcTable) != want {
		return nil // corrupt state; treat as fresh rather than crash
	}
	f.term = binary.LittleEndian.Uint64(payload[:8])
	vlen := int(binary.LittleEndian.Uint16(payload[8:10]))
	if 10+vlen <= len(payload) {
		f.votedFor = string(payload[10 : 10+vlen])
	}
	return nil
}

// --- log file ---

// encodeRecord frames one entry: [payloadLen u32][crc u32][payload].
// payload = [term u64][cmdLen u32][cmd].
func encodeRecord(e raft.LogEntry) []byte {
	payload := make([]byte, 0, 12+len(e.Command))
	var t [8]byte
	binary.LittleEndian.PutUint64(t[:], e.Term)
	payload = append(payload, t[:]...)
	var cl [4]byte
	binary.LittleEndian.PutUint32(cl[:], uint32(len(e.Command)))
	payload = append(payload, cl[:]...)
	payload = append(payload, e.Command...)

	rec := make([]byte, 8, 8+len(payload))
	binary.LittleEndian.PutUint32(rec[0:4], uint32(len(payload)))
	binary.LittleEndian.PutUint32(rec[4:8], crc32.Checksum(payload, crcTable))
	rec = append(rec, payload...)
	return rec
}

// loadLog reads the log, stopping at the first short or corrupt record (a torn
// write) and physically truncating the file to drop that tail.
func (f *File) loadLog() error {
	b, err := os.ReadFile(f.logPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("storage: read log: %w", err)
	}

	var entries []raft.LogEntry
	off := 0
	good := 0 // byte offset of the end of the last fully-valid record
	for off+8 <= len(b) {
		plen := int(binary.LittleEndian.Uint32(b[off : off+4]))
		crc := binary.LittleEndian.Uint32(b[off+4 : off+8])
		start := off + 8
		if plen < 12 || start+plen > len(b) {
			break // torn: record runs past end of file
		}
		payload := b[start : start+plen]
		if crc32.Checksum(payload, crcTable) != crc {
			break // torn/corrupt: checksum mismatch
		}
		term := binary.LittleEndian.Uint64(payload[0:8])
		clen := int(binary.LittleEndian.Uint32(payload[8:12]))
		if 12+clen > plen {
			break
		}
		cmd := append([]byte(nil), payload[12:12+clen]...)
		entries = append(entries, raft.LogEntry{Term: term, Command: cmd})
		off = start + plen
		good = off
	}
	f.entries = entries

	// If we dropped a torn tail, clean it off disk so future appends are sound.
	if good < len(b) {
		if err := os.Truncate(f.logPath, int64(good)); err != nil {
			return fmt.Errorf("storage: truncate torn log: %w", err)
		}
	}
	return nil
}

// rewriteLog atomically replaces the log file with the current entries.
func (f *File) rewriteLog() error {
	if f.logFile != nil {
		_ = f.logFile.Close()
	}
	tmp := f.logPath + ".tmp"
	tf, err := os.OpenFile(tmp, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o644)
	if err != nil {
		return fmt.Errorf("storage: rewrite open: %w", err)
	}
	for _, e := range f.entries {
		if _, err := tf.Write(encodeRecord(e)); err != nil {
			tf.Close()
			return fmt.Errorf("storage: rewrite write: %w", err)
		}
	}
	if err := tf.Sync(); err != nil {
		tf.Close()
		return err
	}
	tf.Close()
	if err := os.Rename(tmp, f.logPath); err != nil {
		return fmt.Errorf("storage: rewrite rename: %w", err)
	}
	if err := syncDir(f.dir); err != nil {
		return err
	}
	lf, err := os.OpenFile(f.logPath, os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return fmt.Errorf("storage: reopen log: %w", err)
	}
	f.logFile = lf
	return nil
}

// syncDir fsyncs a directory so a rename within it is durable.
func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}
