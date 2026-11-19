package raft

import "log"

// logger prefixes every line with the node id, so interleaved cluster logs stay
// readable. A nil underlying *log.Logger is impossible here because Config
// defaults it to one that discards output.
type logger struct {
	l  *log.Logger
	id string
}

func (lg logger) Printf(format string, args ...any) {
	lg.l.Printf("[%s] "+format, append([]any{lg.id}, args...)...)
}
