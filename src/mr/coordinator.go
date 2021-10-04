package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"strings"
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
	mapTaskState    map[int]*MapTaskState
	reduceTaskState map[int]*ReduceTaskState

	workerState map[int]chan bool

	mu sync.Mutex

	done chan bool
}

func (c *Coordinator) PingWorker(WorkerId int) {
	c.mu.Lock()
	_, prs := c.workerState[WorkerId]

	if !prs {
		// if there is no channel for current worker(e.g., new worker or "dead" one)
		// spawn channel for ping
		c.workerState[WorkerId] = make(chan bool, 1)

		// ping for the first time, must ping before create the goroutine
		c.workerState[WorkerId] <- true

		// from now on, pings the worker periodically
		// if timeout, then re-ex all the task of this worker
		go func(ping chan bool, done <-chan bool) {
			for {
				select {
				case <-ping:
					continue
				// task is finished, stop ping
				case <-done:
					return
				// timeout, stop ping
				case <-time.After(10.0 * time.Second):
					c.ReExecuteTask(WorkerId)
					c.mu.Lock()
					delete(c.workerState, WorkerId)
					close(ping)
					c.mu.Unlock()
					return
				}
			}
		}(c.workerState[WorkerId], c.done)
	} else {
		c.workerState[WorkerId] <- true
	}

	c.mu.Unlock()

}

func (c *Coordinator) HeartBeat(args *HeartBeatArgs, reply *HeartBeatReply) error {
	c.PingWorker(args.WorkerId)
	// @todo: check if done
	reply.Done = c.Done()

	return nil
}

/*
 * Worker pings the Coordinator and ask for task
 * Worker pass the id of itself
 * Coordinator returns the task type and corresponding splits/regions locations
 */
func (c *Coordinator) AskForTask(args *TaskArgs, reply *TaskReply) error {
	// ping current worker
	c.PingWorker(args.WorkerId)

	// allocate a map task
	allMapTaskDone := true
	c.mu.Lock()
	for taskNum, taskState := range c.mapTaskState {
		if !taskState.complete {
			allMapTaskDone = false
		}

		if taskState.complete || taskState.executeWorker != -1 {
			continue
		}

		//  allocate task to current worker
		taskState.executeWorker = args.WorkerId

		reply.TaskType = 0
		reply.FileName = []string{taskState.inputFile}
		reply.TaskNum = taskNum
		reply.ReduceNum = len(c.reduceTaskState)

		c.mu.Unlock()
		return nil
	}
	c.mu.Unlock()

	if allMapTaskDone {
		// allocate a reduce task only when all the map tasks are done
		c.mu.Lock()
		for taskNum, taskState := range c.reduceTaskState {
			if taskState.complete || taskState.executeWorker != -1 {
				continue
			}

			taskState.executeWorker = args.WorkerId

			reply.TaskType = 1
			reply.FileName = taskState.inputFiles
			reply.TaskNum = taskNum
			reply.ReduceNum = len(c.reduceTaskState)

			c.mu.Unlock()
			return nil
		}
		c.mu.Unlock()
	}

	// no task allocated for the current worker
	reply.TaskType = -1
	return nil
}

// re-execute all the map tasks of specific worker
func (c *Coordinator) ReExecuteTask(WorkerId int) {
	c.mu.Lock()

	// re-execute all the map tasks of WorkedId
	for _, taskState := range c.mapTaskState {
		if taskState.executeWorker != WorkerId {
			continue
		}

		if taskState.complete {
			continue
		}

		taskState.complete = false
		taskState.executeWorker = -1
		taskState.outputFiles = []string{}

		// modity the input of the reduce tasks

	}

	for _, taskState := range c.reduceTaskState {
		if !taskState.complete && taskState.executeWorker == WorkerId {
			taskState.executeWorker = -1
		}
	}
	c.mu.Unlock()
}

func (c *Coordinator) CompleteTask(args *CompleteTaskArgs, reply *CompleteTaskReply) error {
	c.PingWorker(args.WorkerId)
	c.mu.Lock()
	defer c.mu.Unlock()

	if args.TaskType == 0 {
		taskState, prs := c.mapTaskState[args.TaskNum]

		if !prs {
			log.Fatalf("Map Task %d not allocated.", args.TaskNum)
		}

		// task has been allocated to another worker
		if taskState.executeWorker != args.WorkerId {
			log.Fatalf("Map task %d not allocated to %d", args.TaskNum, args.WorkerId)
		}

		if taskState.complete {
			log.Fatalf("Re-execution of completed map tassk %d", args.TaskNum)
		}

		taskState.complete = true
		taskState.executeWorker = args.WorkerId
		taskState.outputFiles = args.FileName

		// update the input files of reduce task
		for _, fileName := range taskState.outputFiles {
			// fileName must be the format mr-X-Y
			fileNameSlice := strings.Split(fileName, "-")
			if len(fileNameSlice) != 3 {
				log.Fatalf("Wrong map task output name format.")
			}

			// add new input file to corresponding reduce task
			reduceNum, err := strconv.Atoi(fileNameSlice[2])
			if err != nil || reduceNum < 0 || reduceNum >= len(c.reduceTaskState) {
				log.Fatalf("Wrong map partion number.")
			}

			c.reduceTaskState[reduceNum].inputFiles = append(c.reduceTaskState[reduceNum].inputFiles, fileName)
		}
		return nil
	} else if args.TaskType == 1 {
		taskState, prs := c.reduceTaskState[args.TaskNum]

		if len(args.FileName) != 1 {
			log.Fatalf("Reduce task %d has more than one output file", args.TaskNum)
		}

		if !prs {
			log.Fatalf("Reduce Task %d not allocated.", args.TaskNum)
		}

		// task has been allocated to another worker
		if taskState.executeWorker != args.WorkerId {
			log.Fatalf("Reduce task %d not allocated to %d", args.TaskNum, args.WorkerId)
		}

		if taskState.complete {
			log.Fatalf("Re-execution of completed reduce tassk %d", args.TaskNum)
		}

		taskState.complete = true
		taskState.executeWorker = args.WorkerId
		taskState.outputFile = args.FileName[0]
		return nil
	} else {
		log.Fatalf("Invalid task type %d", args.TaskType)
	}

	log.Fatalf("Should never hit")
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
	ret := true

	c.mu.Lock()
	// Check if all the reduce tasks are complete
	for _, v := range c.reduceTaskState {
		if !v.complete {
			ret = false
			break
		}
	}
	c.mu.Unlock()

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
	// give task a number(index) to indentify itself
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
			inputFiles:    []string{},
			outputFile:    "",
			executeWorker: -1,
		}
	}

	c.server()
	return &c
}
