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
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

const TempDir = "tmp"

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

	nReduce := GetNReduce()

	fmt.Printf("Num reduce %v", nReduce)

	task := GetTask()
	mapTask(task, mapf, nReduce)

}

func mapTask(task *RequestTaskReply, mapf func(string, string) []KeyValue, nReduce int) {
	filename := task.File
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()
	kva := mapf(filename, string(content))
	writeMapToDisk(kva, task.TaskId, nReduce)
}

func writeMapToDisk(kva []KeyValue, mapID int, nReduce int) {
	files := make([]*os.File, 0, nReduce)
	buffers := make([]*bufio.Writer, 0, nReduce)
	encoders := make([]*json.Encoder, 0, nReduce)
	prefix := fmt.Sprintf("%v/mr-%v", TempDir, mapID)

	for i := 0; i < nReduce; i++ {
		filePath := fmt.Sprintf("%v-%v-%v", prefix, i, os.Getpid())
		file, err := os.Create(filePath)
		if err != nil {
			log.Fatalf("Cannot create file %v\n", filePath)
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
			log.Fatalf("Cannot encode %v to file\n", kv)
		}
	}

	// flush file buffer to disk
	for i, buf := range buffers {
		err := buf.Flush()
		if err != nil {
			log.Fatalf("Cannot flush buffer for file: %v\n", files[i].Name())
		}
	}

	// atomically rename temp files to ensure no one observes partial files
	for i, file := range files {
		file.Close()
		newPath := fmt.Sprintf("%v-%v", prefix, i)
		err := os.Rename(file.Name(), newPath)
		if err != nil {
			log.Fatalf("Cannot rename file %v\n", file.Name())
		}
	}
}

//GetTask is used to get call the rpc GetTask of the coordinator
func GetTask() *RequestTaskReply {
	args := RequestTaskArgs{}
	reply := RequestTaskReply{}
	args.Workerid = 1
	call("Coordinator.GetTask", &args, &reply)
	return &reply
}

//GetNReduce gets the number of reduce tasks
func GetNReduce() int {
	args := GetReduceCountArgs{}
	reply := GetReduceCountReply{}
	call("Coordinator.GetNReduce", &args, &reply)
	return reply.ReduceCount
}

func ReportTaskDone() {
	args := ReportTaskDoneArgs{}
	reply := ReportTaskDoneReply{}
	call("Coordinator.ReportTaskStatus", &args, &reply)
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
