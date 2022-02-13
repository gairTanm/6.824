package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"path/filepath"
	"sync"
	"time"
)

type Task struct {
	taskType TaskType
	filename string
	status   JobStatus
	id       int
	workerId int
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
		fmt.Printf("%d %s task %d took too long, exiting\n", t.workerId, t.taskType, t.workerId)
	}
}

func (c *Coordinator) getTask(tasks []Task, wId int) *Task {
	var taskToSend *Task
	for _, task := range tasks {
		if task.status == jPending {
			fmt.Printf("%v pending\n", task.filename)
			taskToSend = &task
			taskToSend.workerId = wId
			taskToSend.status = jProgress
			return taskToSend
		}
	}
	return nil
}

func (c *Coordinator) RequestJob(args *RequestJobArgs, reply *RequestJobReply) error {
	c.mu.Lock()
	var task *Task
	wId := args.WorkerId
	fmt.Printf("worker id requesting: %d\n", args.WorkerId)

	if c.mTasksDone < c.nMap {
		task = c.getTask(c.mTasks, wId)
	} else if c.rTasksDone < c.nReduce {
		task = c.getTask(c.rTasks, wId)
	} else {
		task = &Task{Done, "", jCompleted, -1, -1}
	}

	fmt.Printf("task sent: %v\n", task)
	reply.Filename = task.filename
	reply.TaskId = task.id
	reply.Task = task.taskType
	reply.NReduce = c.nReduce
	// fmt.Printf("task id sent: %d\n", task.id)
	c.mu.Unlock()
	// go c.CheckTimeout(task)
	return nil
}

func (c *Coordinator) ReportTaskDone(args *ReportTaskArgs, reply *ReportTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// fmt.Printf("%d did %d\n", args.WorkerId, args.TaskId)
	if args.TaskId < 0 {
		return fmt.Errorf("out of bounds")
	}
	var task *Task
	if args.Task == Map {
		task = &c.mTasks[args.TaskId]
	} else if args.Task == Reduce {
		task = &c.rTasks[args.TaskId]
	} else {
		// fmt.Printf("incorrect task type: %v\n", args.Task)
		return nil
	}

	fmt.Printf("task %v with worker %v in task %v\n", task, args.WorkerId, task.workerId)

	if args.WorkerId == task.workerId && task.status == jProgress {
		task.status = jCompleted
		if args.Task == Map && c.mTasksDone < c.nMap {
			c.mTasksDone++
		} else if args.Task == Reduce && c.mTasksDone < c.nReduce {
			c.rTasksDone++
		}
	}

	reply.CanExit = (c.mTasksDone == c.nMap && c.nReduce == c.rTasksDone)

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
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.mTasksDone == c.nMap && c.rTasksDone == c.nReduce
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
	c.mTasks = make([]Task, 0, c.nMap)
	c.rTasks = make([]Task, 0, c.nReduce)

	for i := 0; i < c.nMap; i++ {
		mTask := Task{Map, files[i], jPending, i, -1}
		c.mTasks = append(c.mTasks, mTask)
	}
	for i := 0; i < nReduce; i++ {
		rTask := Task{Reduce, "", jPending, i, -1}
		c.rTasks = append(c.rTasks, rTask)
	}

	c.server()
	outFiles, _ := filepath.Glob("mr-out*")
	for _, f := range outFiles {
		if err := os.Remove(f); err != nil {
			log.Fatalf("Cannot remove file %v\n", f)
		}
	}
	err := os.RemoveAll(TempDir)
	if err != nil {
		log.Fatalf("Cannot remove temp directory %v\n", TempDir)
	}
	err = os.Mkdir(TempDir, 0755)
	if err != nil {
		log.Fatalf("Cannot create temp directory %v\n", TempDir)
	}
	return &c
}