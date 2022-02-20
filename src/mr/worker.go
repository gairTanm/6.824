package mr

import (
	"bufio"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"path/filepath"
	"sort"
	"time"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

var workerId int = os.Getpid()

type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

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
	for {
		reply, ok := CallRequestJob()
		if !ok {
			fmt.Printf("%d can't call request job\n", workerId)
		}
		fmt.Printf("reply: %v\n", reply)
		if reply.Task == Done {
			fmt.Printf("%d worker exiting since all tasks done\n", workerId)
			break
		} else if reply.Task == Map {
			// fmt.Printf("got a map task\n")
			WorkerMap(reply.Filename, reply.NReduce, reply.TaskId, mapf)
			CallTaskDone(Map, reply.TaskId)
		} else if reply.Task == Reduce {
			// fmt.Printf("got a reduce task\n")
			WorkerReduce(reply.TaskId, reducef)
			CallTaskDone(Reduce, reply.TaskId)
		}

		time.Sleep(500)
	}

}

func CallTaskDone(task TaskType, taskId int) (bool, bool) {
	args := ReportTaskArgs{workerId, task, taskId}
	// fmt.Printf("task done args: %v\n", args)
	reply := ReportTaskReply{}
	succ := call("Coordinator.ReportTaskDone", &args, &reply)

	return reply.CanExit, succ

}

func WorkerMap(filename string, nReduce int, taskId int, mapf func(string, string) []KeyValue) {
	// map and write to bucket files
	file, err := os.Open(filename)
	if err != nil {
		fmt.Printf("%v cannot open file", workerId)
	}

	content, err := ioutil.ReadAll(file)
	if err != nil {
		fmt.Printf("%v cannot read file", workerId)
	}
	file.Close()

	kva := mapf(filename, string(content))
	prefix := fmt.Sprintf("%v/mr-%v", TempDir, taskId)
	files := make([]*os.File, 0, nReduce)
	buffers := make([]*bufio.Writer, 0, nReduce)
	encoders := make([]*json.Encoder, 0, nReduce)

	for i := 0; i < nReduce; i++ {
		filePath := fmt.Sprintf("%v-%v-%v", prefix, i, os.Getpid())
		file, err := os.Create(filePath)
		if err != nil {
			fmt.Printf("%v cannot create file", workerId)
		}
		buf := bufio.NewWriter(file)
		files = append(files, file)
		buffers = append(buffers, buf)
		encoders = append(encoders, json.NewEncoder(buf))
	}
	for _, kv := range kva {
		idx := ihash(kv.Key) % nReduce
		err := encoders[idx].Encode(&kv)
		if err != nil {
			fmt.Printf("%v cannot encode file", workerId)
		}
	}

	for _, buf := range buffers {
		err := buf.Flush()
		if err != nil {
			fmt.Printf("%v can't flush to disk", workerId)
		}
	}

	for i, file := range files {
		file.Close()
		newPath := fmt.Sprintf("%v-%v", prefix, i)
		os.Rename(file.Name(), newPath)
	}
}

func WorkerReduce(taskId int, reducef func(string, []string) string) {
	files, err := filepath.Glob(fmt.Sprintf("%v/mr-%v-%v", TempDir, "*", taskId))
	if err != nil {
		fmt.Printf("%v can't open %v", workerId, files)
	}

	kvMap := make(map[string][]string)
	var kv KeyValue

	for _, filePath := range files {
		file, err := os.Open(filePath)
		if err != nil {
			fmt.Printf("%v can't open %v", workerId, filePath)
		}
		dec := json.NewDecoder(file)
		for dec.More() {
			dec.Decode(&kv)
			kvMap[kv.Key] = append(kvMap[kv.Key], kv.Value)
		}
	}

	keys := make([]string, 0, len(kvMap))
	for k := range kvMap {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	filePath := fmt.Sprintf("%v/mr-out-%v-%v", TempDir, taskId, os.Getpid())
	file, _ := os.Create(filePath)

	for _, k := range keys {
		reducef(k, kvMap[k])
		fmt.Fprintf(file, "%v %v\n", k, reducef(k, kvMap[k]))
	}

	file.Close()
	newPath := fmt.Sprintf("mr-out-%v", taskId)
	os.Rename(filePath, newPath)
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
	args.WorkerId = workerId

	// declare a reply structure.
	reply := RequestJobReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.RequestJob", &args, &reply)
	if ok {
		// reply.Y should be 100.
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
