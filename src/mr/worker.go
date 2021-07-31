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

	for {

		task := GetTask()

		if task.TaskType == Map {
			fmt.Printf("Got MAP task id %v  \n", task.TaskId)
			mapTask(task, mapf, nReduce)
			ReportTaskDone(task.TaskId, Map)
		} else if task.TaskType == Reduce {
			fmt.Printf("Got REDUCE task id %v \n", task.TaskId)
			doReduce(reducef, task.TaskId)
			ReportTaskDone(task.TaskId, Reduce)
		} else {
			return
		}
	}

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

func doReduce(reducef func(string, []string) string, reduceId int) {
	files, err := filepath.Glob(fmt.Sprintf("%v/mr-%v-%v", TempDir, "*", reduceId))
	if err != nil {
		log.Fatalf("Cannot list reduce files: %v \n", err)
	}

	kvMap := make(map[string][]string)
	var kv KeyValue

	for _, filePath := range files {
		file, err := os.Open(filePath)
		if err != nil {
			log.Fatalf("Cannot open file: %v \n", filePath)
		}

		dec := json.NewDecoder(file)
		for dec.More() {
			err = dec.Decode(&kv)
			if err != nil {
				log.Fatalf("Cannot decode from file : %v \n", err)
			}
			kvMap[kv.Key] = append(kvMap[kv.Key], kv.Value)
		}
	}

	writeReduceOutput(reducef, kvMap, reduceId)
}

func writeReduceOutput(reducef func(string, []string) string,
	kvMap map[string][]string, reduceId int) {

	// sort the kv map by key
	keys := make([]string, 0, len(kvMap))
	for k := range kvMap {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	// Create temp file
	filePath := fmt.Sprintf("%v/mr-out-%v-%v", TempDir, reduceId, os.Getpid())
	file, err := os.Create(filePath)
	if err != nil {
		log.Fatalf("Cannot create file %v\n", filePath)
	}

	// Call reduce and write to temp file
	for _, k := range keys {
		v := reducef(k, kvMap[k])
		_, err := fmt.Fprintf(file, "%v %v\n", k, reducef(k, kvMap[k]))
		if err != nil {
			log.Fatalf("Cannot write mr output (%v, %v) to file", k, v)
		}
	}

	// atomically rename temp files to ensure no one observes partial files
	file.Close()
	newPath := fmt.Sprintf("mr-out-%v", reduceId)
	err = os.Rename(filePath, newPath)
	if err != nil {
		log.Fatalf("Cannot rename file %v\n", filePath)
	}
}

//GetTask is used to get call the rpc GetTask of the coordinator
func GetTask() *RequestTaskReply {
	args := RequestTaskArgs{}
	reply := RequestTaskReply{}
	args.Workerid = os.Getpid()
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

func ReportTaskDone(taskId int, taskType TaskType) {
	args := ReportTaskDoneArgs{}
	args.TaskId = taskId
	args.TaskType = taskType
	args.Workerid = os.Getpid()
	reply := ReportTaskDoneReply{}
	call("Coordinator.ReportTaskDone", &args, &reply)
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
