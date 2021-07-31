package mr

import (
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
	Wait
	Exit
)

//Task has the status and type
type Task struct {
	id       int
	file     string
	taskType TaskType
	status   TaskStatus
	workerId int
}

//Coordinator is responsible for assigning map and reduce tasks
type Coordinator struct {
	mu          sync.Mutex
	nReduce     int
	mapTasks    []Task
	reduceTasks []Task
	nMap        int
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

//GetTask is used to get a task from the co ordinator by workers
func (c *Coordinator) GetTask(args *RequestTaskArgs, reply *RequestTaskReply) error {
	c.mu.Lock()

	if c.nMap > 0 {
		for i := range c.mapTasks {
			if c.mapTasks[i].status == Notstarted {
				reply.File = c.mapTasks[i].file
				reply.TaskType = Map
				reply.TaskId = c.mapTasks[i].id
				c.mapTasks[i].status = TaskStatus(Running)
				c.mapTasks[i].workerId = args.Workerid
				c.mu.Unlock()
				go c.waitForTask(&c.mapTasks[i])
				return nil
			}
		}
		c.mu.Unlock()
		reply.TaskType = Wait
		return nil
	}
	if c.nReduce > 0 {
		for i := range c.reduceTasks {
			if c.reduceTasks[i].status == Notstarted {
				reply.TaskType = Reduce
				reply.TaskId = c.reduceTasks[i].id
				c.reduceTasks[i].status = TaskStatus(Running)
				c.reduceTasks[i].workerId = args.Workerid
				c.mu.Unlock()
				go c.waitForTask(&c.reduceTasks[i])
				return nil
			}
		}
		c.mu.Unlock()
		reply.TaskType = Wait
		return nil
	}
	reply.TaskId = -1
	reply.TaskType = TaskType(Done)
	c.mu.Unlock()
	return nil
}

//GetNReduce is used to get a task from the co ordinator by workers
func (c *Coordinator) GetNReduce(args *GetReduceCountArgs, reply *GetReduceCountReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	reply.ReduceCount = len(c.reduceTasks)
	return nil
}

//ReportTaskDone is used by workers to report that a task is done
func (c *Coordinator) ReportTaskDone(args *ReportTaskDoneArgs, reply *ReportTaskDoneReply) error {
	c.mu.Lock()
	if Map == args.TaskType {
		for i := range c.mapTasks {
			if c.mapTasks[i].id == args.TaskId {
				if c.mapTasks[i].workerId != args.Workerid {
					log.Print("Task was reassigned to a different worker")
					c.mu.Unlock()
					return nil
				}
				c.mapTasks[i].status = TaskStatus(Done)
				c.nMap--
				c.mu.Unlock()
				return nil
			}
		}
	} else if Reduce == args.TaskType {
		for i := range c.reduceTasks {
			if c.reduceTasks[i].id == args.TaskId {
				if c.reduceTasks[i].workerId != args.Workerid {
					log.Print("Task was reassigned to a different worker")
					c.mu.Unlock()
					return nil
				}
				c.reduceTasks[i].status = TaskStatus(Done)
				c.nReduce--
				c.mu.Unlock()
				return nil
			}
		}
	}

	c.mu.Unlock()
	return nil

}

//waitForTask is for timing out the assigned work to a worker
func (c *Coordinator) waitForTask(task *Task) error {

	<-time.After(10 * time.Second)

	c.mu.Lock()
	defer c.mu.Unlock()

	if task.status == Running {
		task.status = Notstarted
		task.workerId = -1
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

	c.mu.Lock()
	defer c.mu.Unlock()

	ret := (c.nMap == 0 && c.nReduce == 0)
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
	c.nMap = len(files)
	for i, f := range files {
		task := Task{i, f, Map, Notstarted, -1}
		c.mapTasks = append(c.mapTasks, task)
	}

	for i := 0; i < nReduce; i++ {
		task := Task{i, "", Reduce, Notstarted, -1}
		c.reduceTasks = append(c.reduceTasks, task)
	}

	err := os.RemoveAll(TempDir)
	if err != nil {
		log.Fatalf("Cannot remove temp directory %v\n", TempDir)
	}

	err = os.Mkdir(TempDir, 0755)
	if err != nil {
		log.Fatalf("Cannot create temp directory %v\n", TempDir)
	}

	c.server()
	return &c
}
