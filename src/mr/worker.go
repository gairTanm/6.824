package mr

import (
	"fmt"
	"hash/fnv"
	"log"
	"net/rpc"
	"time"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	res, ok := CallRequestJob()
	for !ok {
		fmt.Printf("call failed!\n")
		time.Sleep(2 * time.Second)
		res, ok = CallRequestJob()
	}
	fmt.Printf("reply.Job %v\n", res.JobRecieved)
	if res.JobRecieved == "MAP" {
		WorkerMap(res.Filename, mapf)
	} else if res.JobRecieved == "REDUCE" {
		WorkerReduce(res.BucketId, reducef)
	} else if res.JobRecieved == "DONE" {
		fmt.Printf("all jobs done, exiting...\n")
		return
	}
}

func WorkerMap(filename string, mapf func(string, string) []KeyValue) {
	// map and write to bucket files
}

func WorkerReduce(bucketId int, reducef func(string, []string) string) {
	// read from bucket files and reduce
}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallRequestJob() (*RequestJobReply, bool) {

	// declare an argument structure.
	args := RequestJobArgs{}

	// fill in the argument(s).

	// declare a reply structure.
	reply := RequestJobReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.RequestJob", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Job %v\n", reply.Recieved)
		return &reply, true
	}
	fmt.Printf("call failed!\n")
	return nil, false
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
