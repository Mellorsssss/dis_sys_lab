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

type MapTaskState struct {
	complete      bool
	inputFile     string
	outputFiles   []string
	executeWorker int
}

type ReduceTaskState struct {
	complete      bool
	inputFiles    []string
	outputFile    string
	executeWorker int
}

type Coordinator struct {
	// Your definitions here.
	mapTaskState    map[int]*MapTaskState
	reduceTaskState map[int]*ReduceTaskState

	workerState map[int]chan bool

	mu sync.Mutex // lock for protecting shared data

	done chan bool
}

// Your code here -- RPC handlers for the worker to call.

// Worker pings the Coordinator and ask for task
// Worker pass the id of itself
// Coordinator returns the task type and corresponding splits/regions locations
func (c *Coordinator) AskForTask(args *HeartBeatArgs, reply *HeartBeatReply) error {
	// current worker is alive
	_, prs := c.workerState[args.WorkerId]

	if !prs {
		// spawn channel for ping
		c.mu.Lock()
		c.workerState[args.WorkerId] = make(chan bool)
		c.mu.Unlock()
		// pings periodically
		go func(ping <-chan bool, done <-chan bool) {
			for {
				select {
				case <-ping:
					continue
				// task is finished, stop ping
				case <-done:
					return
				// timeout, stop ping
				case <-time.After(8.0 * time.Second):
					c.ReExecuteTask(args.WorkerId)
					c.mu.Lock()
					delete(c.workerState, args.WorkerId)
					c.mu.Unlock()
					return
				}
			}
		}(c.workerState[args.WorkerId], c.done)
	}

	c.workerState[args.WorkerId] <- true

	// allocate a map task
	c.mu.Lock()
	for taskNum, taskState := range c.mapTaskState {
		if taskState.complete {
			continue
		}

		reply = &HeartBeatReply{
			TaskType: 0,
			FileName: []string{taskState.inputFile},
			TaskNum:  taskNum,
		}
		c.mu.Unlock()
		return nil
	}
	c.mu.Unlock()

	// allocate a reduce task
	c.mu.Lock()
	for taskNum, taskState := range c.reduceTaskState {
		if taskState.complete {
			continue
		}

		reply = &HeartBeatReply{
			TaskType: 1,
			FileName: taskState.inputFiles,
			TaskNum:  taskNum,
		}
		c.mu.Unlock()
		return nil
	}
	c.mu.Unlock()

	return nil
}

// re-execute all the map tasks of specific worker

func (c *Coordinator) ReExecuteTask(WorkerId int) {
	c.mu.Lock()
	for _, taskState := range c.mapTaskState {
		if !taskState.complete {
			continue
		}

		if taskState.executeWorker == WorkerId {
			taskState.complete = false
			taskState.executeWorker = -1
			taskState.outputFiles = []string{}
		}
	}
	c.mu.Unlock()
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

	c.server()
	return &c
}
