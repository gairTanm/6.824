package mr

import "time"

type Job string

const (
	Map    Job = "MAP"
	Reduce Job = "REDUCE"
	Done   Job = "DONE"
)

type WorkerStatus string

const (
	wIdle       WorkerStatus = "IDLE"
	wInProgress WorkerStatus = "PROG"
	wDone       WorkerStatus = "DONE"
)

type JobStatus string

const (
	jPending   JobStatus = "PEND"
	jProgress  JobStatus = "PROG"
	jCompleted JobStatus = "COMP"
)

const TimeoutLimit = 10 * time.Second
