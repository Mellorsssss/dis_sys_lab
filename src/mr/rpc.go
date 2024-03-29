package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Ping args and reply
type HeartBeatArgs struct {
	WorkerId int
}

type HeartBeatReply struct {
	Done bool
}

// AskForTask args and reply
type TaskArgs struct {
	WorkerId int
}

type TaskReply struct {
	TaskType  int // 0 for map task, 1 for reduce task
	FileName  []string
	TaskNum   int
	ReduceNum int
}

// CompleteTask args and reply
type CompleteTaskArgs struct {
	WorkerId int
	TaskType int
	FileName []string
	TaskNum  int
}

type CompleteTaskReply struct {
}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
