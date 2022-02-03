package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type TaskType string

type Task struct {
	taskType TaskType
	filename string
	status   JobStatus
	workerId int
}

type Coordinator struct {
	// Your definitions here.
	nReduce      int
	nMap         int
	mTasksDone   bool
	rTasksDone   bool
	mTasks       []Task
	rTasks       []Task
	mapJobStatus map[string]JobStatus
	wStatus      map[int]WorkerStatus
	mu           sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//

func (c *Coordinator) CheckTimeout(t Task) {
	<-time.After(TimeoutLimit)
	c.mu.Lock()
	defer c.mu.Unlock()
	// TODO: check if t is still pending
	if t.status == jProgress {
		t.status = jPending
		t.workerId = -1
		fmt.Printf("%s task %d took too long, exiting\n", t.taskType, t.workerId)
	}
}

func (c *Coordinator) RequestJob(args *RequestJobArgs, reply *RequestJobReply) error {
	mapTasksCompleted := 0
	reduceTasksCompleted := 0
	c.mu.Lock()
	defer c.mu.Unlock()

	// if all map tasks haven't been completed
	if !c.mTasksDone {
		for _, mTask := range c.mTasks {
			if mTask.status == jPending {
				reply.Filename = mTask.filename
				reply.JobRecieved = Map
				mTask.status = jProgress
				go c.CheckTimeout(mTask)
				return nil
			} else if mTask.status == jCompleted {
				mapTasksCompleted++
			}
		}
		if mapTasksCompleted == c.nMap {
			c.mTasksDone = true
		}
	}

	for _, rTask := range c.rTasks {
		if rTask.status == jPending {
			reply.Filename = rTask.filename
			reply.JobRecieved = Reduce
			rTask.status = jProgress
			go c.CheckTimeout(rTask)
			return nil
		}
	}
	if reduceTasksCompleted == c.nReduce {
		c.rTasksDone = true
	}

	if c.mTasksDone && c.rTasksDone {
		// exit code
		c.Done()
	}
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	ret := true

	// Your code here.

	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//

// TODO: initialise the coordinator
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	// Your code here.
	c.nMap = len(files)
	c.nReduce = nReduce

	c.server()
	return &c
}
