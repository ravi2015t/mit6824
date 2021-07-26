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

//TaskStatus enum to track the status of the task running
type TaskStatus int

//Enum to represent the staus of the task
const (
	Notstarted TaskStatus = iota
	Running
	Done
)

//TaskType is enum to track the kind of task assigned
type TaskType int

//Enum to represent the kind of task assigned
const (
	Map TaskType = iota
	Reduce
)

//Task has the status and type
type Task struct {
	id       int
	file     string
	taskType TaskType
	status   TaskStatus
}

//Coordinator is responsible for assigning map and reduce tasks
type Coordinator struct {
	mu          sync.Mutex
	nReduce     int
	mapTasks    []Task
	reduceTasks []Task
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	fmt.Println("Inside Example")
	reply.Y = args.X + 1
	return nil
}

//GetTask is used to get a task from the co ordinator by workers
func (c *Coordinator) GetTask(args *RequestTaskArgs, reply *RequestTaskReply) error {
	c.mu.Lock()
	for _, task := range c.mapTasks {
		if task.status == Notstarted {
			reply.File = task.file
			reply.TaskType = Map
			reply.TaskId = task.id
			task.status = Running
			c.mu.Unlock()
			go c.waitForTask(&task)
			return nil
		}
	}
	return nil
}

//GetNReduce is used to get a task from the co ordinator by workers
func (c *Coordinator) GetNReduce(args *GetReduceCountArgs, reply *GetReduceCountReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	reply.ReduceCount = c.nReduce
	return nil
}

//ReportTaskDone is used by workers to report that a task is done
func (c *Coordinator) ReportTaskDone(args *ReportTaskDoneArgs, reply *ReportTaskDoneReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	for _, task := range c.mapTasks {
		if task.id == args.TaskId {
			task.status = Done
			c.mu.Unlock()
			return nil
		}
	}

	return nil

}

//waitForTask is for timing out the assigned work to a worker
func (c *Coordinator) waitForTask(task *Task) error {

	<-time.After(10 * time.Second)

	c.mu.Lock()
	defer c.mu.Unlock()

	if task.status == Running {
		fmt.Println("Task not completed; Changing back the staus of task to not started")
		task.status = Notstarted
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
	c.mapTasks = make([]Task, 0, len(files))
	c.reduceTasks = make([]Task, 0, nReduce)
	c.nReduce = nReduce
	for i, f := range files {
		task := Task{i, f, Map, Notstarted}
		c.mapTasks = append(c.mapTasks, task)
	}
	// Your code here.

	c.server()
	return &c
}
