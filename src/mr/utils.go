package mr

import "time"

const TempDir = "temp"

type TaskType string

const (
	Map    TaskType = "MAP"
	Reduce TaskType = "REDUCE"
	Done   TaskType = "DONE"
)

func (t TaskType) String() string {
	return string(t)
}

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

const TimeoutLimit = 1 * time.Second
