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
	if t.taskType == Done {
		return
	}
	<-time.After(TimeoutLimit)
	c.mu.Lock()
	defer c.mu.Unlock()
	var task Task
	if t.taskType == Map {
		task = c.mTasks[t.id]
	} else if t.taskType == Reduce {
		task = c.rTasks[t.id]
	} else {
		return
	}
	if task.status == jProgress {
		fmt.Printf("took too long: %v\n", t)
		if t.taskType == Map {
			c.mTasks[t.id].status = jPending
			c.mTasks[t.id].workerId = -1
		} else {
			c.rTasks[t.id].status = jPending
			c.rTasks[t.id].workerId = -1
		}
	}
}

func (c *Coordinator) getTask(tasks []Task, wId int) *Task {
	var taskToSend *Task
	for _, task := range tasks {
		if task.status == jPending {
			fmt.Printf("%v pending\n", task)
			taskToSend = &task
			taskToSend.workerId = wId
			taskToSend.status = jProgress
			tasks[taskToSend.id] = *taskToSend
			return taskToSend
		}
	}
	return &Task{No, "", jCompleted, -1, -1}
}

func (c *Coordinator) RequestJob(args *RequestJobArgs, reply *RequestJobReply) error {
	c.mu.Lock()
	var task *Task
	wId := args.WorkerId
	// fmt.Printf("worker id requesting: %d\n", args.WorkerId)

	// fmt.Printf("mdone %v\trdone %v\n", c.mTasksDone, c.rTasksDone)
	if c.mTasksDone < c.nMap {
		task = c.getTask(c.mTasks, wId)
		// fmt.Printf("maptasks done %v\n", c.mTasksDone)
	} else if c.rTasksDone < c.nReduce {
		task = c.getTask(c.rTasks, wId)
		// fmt.Printf("reducetasks done %v\n", c.rTasksDone)
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
	go c.CheckTimeout(task)
	return nil
}

func (c *Coordinator) ReportTaskDone(args *ReportTaskArgs, reply *ReportTaskReply) error {
	c.mu.Lock()

	// fmt.Printf("reduce: %v\n", c.rTasks)

	// fmt.Printf("task done req args: %v\n", args)

	// fmt.Printf("%d did %d\n", args.WorkerId, args.TaskId)
	if args.TaskId < 0 {
		return fmt.Errorf("out of bounds")
	}
	var task *Task
	if args.Task == Map {
		// fmt.Printf("getting a map currently\n")
		task = &c.mTasks[args.TaskId]
	} else if args.Task == Reduce {
		// fmt.Printf("getting a reduce currently\n")
		task = &c.rTasks[args.TaskId]
	} else {
		// fmt.Printf("incorrect task type: %v\n", args.Task)
		return nil
	}

	// fmt.Printf("task %v with worker %v in task %v, status %v with arg task id %v\n", task, args.WorkerId, task.workerId, task.status, args.TaskId)
	// fmt.Printf("task: %v\n", task)

	if args.WorkerId == task.workerId && task.status == jProgress {
		task.status = jCompleted
		if args.Task == Map && c.mTasksDone < c.nMap {
			c.mTasksDone++
		} else if args.Task == Reduce && c.mTasksDone < c.nReduce {
			c.rTasksDone++
		}
		if args.Task == Map {
			c.mTasks[args.TaskId] = *task
		} else {
			c.rTasks[args.TaskId] = *task
		}
	}

	reply.CanExit = (c.mTasksDone == c.nMap && c.nReduce == c.rTasksDone)

	c.mu.Unlock()
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
