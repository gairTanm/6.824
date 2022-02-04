package mr

import "time"

const TempDir = "temp"

type TaskType string

const (
	Map    TaskType = "MAP"
	Reduce TaskType = "REDUCE"
	Done   TaskType = "DONE"
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
