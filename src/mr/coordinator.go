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

type Task struct {
	taskType TaskType
	filename string
	status   JobStatus
	workerId int
	id       int
}

type Coordinator struct {
	// Your definitions here.
	nReduce      int
	nMap         int
	mTasksDone   int
	rTasksDone   int
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

func (c *Coordinator) CheckTimeout(t *Task) {
	<-time.After(TimeoutLimit)
	c.mu.Lock()
	defer c.mu.Unlock()
	if t.status == jProgress {
		t.status = jPending
		t.workerId = -1
		fmt.Printf("%s task %d took too long, exiting\n", t.taskType, t.workerId)
	}
}

func (c *Coordinator) getTask(tasks []Task, wId int) *Task {
	for _, task := range tasks {
		if task.status == jPending {
			if task.taskType == Map {
				c.mTasksDone++
			} else if task.taskType == Reduce {
				c.rTasksDone++
			}
			task.workerId = wId
			task.status = jPending
			return &task
		}
	}
	return nil
}

func (c *Coordinator) RequestJob(args *RequestJobArgs, reply *RequestJobReply) error {
	c.mu.Lock()
	var task *Task
	wId := args.WorkerId
	// if all map tasks haven't been completed
	if c.mTasksDone < c.nMap {
		task = c.getTask(c.mTasks, wId)
	} else if c.rTasksDone < c.nReduce {
		task = c.getTask(c.rTasks, wId)
	} else {
		// give a signal to the worker to exit
		task = &Task{Done, "", jCompleted, -1, -1}
	}

	reply.Filename = task.filename
	reply.TaskId = task.id
	reply.Task = task.taskType
	reply.nReduce = c.nReduce

	c.mu.Unlock()
	go c.CheckTimeout(task)
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
