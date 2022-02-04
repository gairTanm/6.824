package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

//
// example to show how to declare the arguments
// and reply for an RPC.
//

// Add your RPC definitions here.

type MapJob struct {
	filename string
	status   JobStatus
}

type RequestJobArgs struct {
	WorkerId int
}

type RequestJobReply struct {
	Task     TaskType
	Filename string
	BucketId int
	TaskId   int
	nReduce  int
}

type ReportTaskArgs struct {
	WorkerId int
	Task     TaskType
	TaskId   int
}

type ReportTaskReply struct {
	CanExit bool
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
