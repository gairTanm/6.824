package raft

import "time"

const heartbeatInterval = time.Millisecond * 1

type LogEntry struct {
	Val int
}
