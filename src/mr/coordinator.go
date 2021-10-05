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

func (c *Coordinator) PingWorker(WorkerId int) {
	_, prs := c.workerState[WorkerId]

	if !prs {
		// spawn channel for ping
		c.mu.Lock()
		c.workerState[WorkerId] = make(chan bool)
		c.mu.Unlock()

		// pings periodically
		go func(ping chan bool, done <-chan bool) {
			for {
				select {
				case <-ping:
					continue
				// task is finished, stop ping
				case <-done:
					return
				// timeout, stop ping
				case <-time.After(8.0 * time.Second):
					c.ReExecuteTask(WorkerId)
					c.mu.Lock()
					delete(c.workerState, WorkerId)
					close(ping)
					c.mu.Unlock()
					return
				}
			}
		}(c.workerState[WorkerId], c.done)
	}

	// ping
	c.workerState[WorkerId] <- true
}

func (c *Coordinator) HeartBeat(args *HeartBeatArgs, reply *HeartBeatReply) error {
	c.PingWorker(args.WorkerId)
	c.workerState[args.WorkerId] <- true
	// @todo: check if done
	reply.Done = false

	return nil
}

// Worker pings the Coordinator and ask for task
// Worker pass the id of itself
// Coordinator returns the task type and corresponding splits/regions locations
func (c *Coordinator) AskForTask(args *TaskArgs, reply *TaskReply) error {
	// ping current worker
	c.PingWorker(args.WorkerId)

	// allocate a map task
	c.mu.Lock()
	for taskNum, taskState := range c.mapTaskState {
		if taskState.complete {
			continue
		}

		reply.TaskType = 0
		reply.FileName = []string{taskState.inputFile}
		reply.TaskNum = taskNum
		reply.ReduceNum = len(c.reduceTaskState)

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

		reply.TaskType = 1
		reply.FileName = taskState.inputFiles
		reply.TaskNum = taskNum
		reply.ReduceNum = len(c.reduceTaskState)

		c.mu.Unlock()
		return nil
	}
	c.mu.Unlock()

	return nil
}

// re-execute all the map tasks of specific worker
func (c *Coordinator) ReExecuteTask(WorkerId int) {
	c.mu.Lock()

	// re-execute all the map tasks of WorkedId
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

func (c *Coordinator) CompleteTask(args *CompleteTaskArgs, reply *CompleteTaskReply) error {
	c.PingWorker(args.WorkerId)

	if args.TaskType == 0 {
		taskState, prs := c.mapTaskState[args.TaskNum]

		if !prs {
			log.Fatalln("Map Task %v not allocated.", args.TaskNum)
		}

		// task has been allocated to another worker
		if taskState.executeWorker != args.WorkerId {
			log.Fatalln("Map task %v not allocated to %v", args.TaskNum, args.WorkerId)
		}

		if taskState.complete {
			log.Fatalln("Re-execution of completed map tassk %v", args.TaskNum)
		}

		taskState.complete = true
		taskState.executeWorker = args.WorkerId
		taskState.outputFiles = args.FileName
		return nil
	} else if args.TaskType == 1 {
		taskState, prs := c.reduceTaskState[args.TaskNum]

		if len(args.FileName) != 1 {
			log.Fatalln("Reduce task %v has more than one output file", args.TaskNum)
		}

		if !prs {
			log.Fatalln("Reduce Task %v not allocated.", args.TaskNum)
		}

		// task has been allocated to another worker
		if taskState.executeWorker != args.WorkerId {
			log.Fatalln("Reduce task %v not allocated to %v", args.TaskNum, args.WorkerId)
		}

		if taskState.complete {
			log.Fatalln("Re-execution of completed reduce tassk %v", args.TaskNum)
		}

		taskState.complete = true
		taskState.executeWorker = args.WorkerId
		taskState.outputFile = args.FileName[0]
		return nil
	} else {
		log.Fatalln("Invalid task type %v", args.TaskType)
	}

	log.Fatalln("Should never hit")
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
		log.Fatalln("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	ret := true

	// Check if all the reduce tasks are complete
	for _, v := range c.reduceTaskState {
		if !v.complete {
			ret = false
			break
		}
	}

	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		mapTaskState:    make(map[int]*MapTaskState),
		reduceTaskState: make(map[int]*ReduceTaskState),
		workerState:     make(map[int]chan bool),
		done:            make(chan bool),
	}

	// init task states
	for index, inputSplit := range files {
		c.mapTaskState[index] = &MapTaskState{
			complete:      false,
			inputFile:     inputSplit,
			outputFiles:   nil,
			executeWorker: -1,
		}
	}

	for index := 0; index < nReduce; index++ {
		c.reduceTaskState[index] = &ReduceTaskState{
			complete:      false,
			inputFiles:    nil,
			outputFile:    "",
			executeWorker: -1,
		}
	}

	c.server()
	return &c
}
