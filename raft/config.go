package raft

import (
	"io"
	"log"
	"time"
)

// Config is everything a Node needs to start. In tests use short timeouts
// (~150-300 ms elections, ~50 ms heartbeats); in production the defaults below
// are more relaxed.
type Config struct {
	ID    string   // this node's id
	Peers []string // the OTHER nodes' ids (self excluded)

	Transport Transport
	Storage   Storage

	ElectionTimeoutMin time.Duration
	ElectionTimeoutMax time.Duration
	HeartbeatInterval  time.Duration

	// Logger is where structured state-change lines go. nil discards them.
	Logger *log.Logger
}

func (c *Config) withDefaults() {
	if c.ElectionTimeoutMin == 0 {
		c.ElectionTimeoutMin = 150 * time.Millisecond
	}
	if c.ElectionTimeoutMax == 0 {
		c.ElectionTimeoutMax = 300 * time.Millisecond
	}
	if c.HeartbeatInterval == 0 {
		c.HeartbeatInterval = 50 * time.Millisecond
	}
	if c.Logger == nil {
		c.Logger = log.New(io.Discard, "", 0)
	}
}
