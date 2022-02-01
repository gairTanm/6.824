package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
)

type Status string

const (
	wIdle       Status = "IDLE"
	wInProgress Status = "PROG"
	wDone       Status = "DONE"
)

type Coordinator struct {
	// Your definitions here.
	nReduce  int
	nMap     int
	nWorkers int
	wStatus  map[int]Status
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) RequestJob(args *RequestJobArgs, reply *RequestJobReply) error {
	reply.Recieved = "MAP"
	reply.Filename = "tanmay.png"
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
	ret := false

	// Your code here.

	return ret
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
	c.nWorkers = c.nMap + c.nReduce
	c.wStatus = make(map[int]Status, c.nWorkers)
	for i := 0; i < c.nWorkers; i++ {
		c.wStatus[i] = wIdle
	}
	c.server()
	return &c
}
