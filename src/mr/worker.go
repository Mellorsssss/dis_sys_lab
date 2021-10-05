package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
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

type KVs []KeyValue

// interfaces for sort
func (a KVs) Len() int           { return len(a) }
func (a KVs) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a KVs) Less(i, j int) bool { return a[i].Key < a[j].Key }

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
	// CallExample()

}

func WorkLoop(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	for {
		reply, err := WorkerAskForTask()
		if !err {
			done := runTask(reply, mapf, reducef)

			for {
				select {
				case <-done:
					fmt.Printf("%v complete task %d\n", os.Getuid(), reply.TaskNum)

					break
				case <-time.After(2 * time.Second):
					fmt.Printf("%v pings.\n", os.Getuid())
					WorkerHeatBeat()
				}
			}
		} else {
			time.Sleep(1 * time.Second)
		}
	}
}

func runTask(taskInfo TaskReply, mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) chan bool {
	done := make(chan bool)

	if taskInfo.TaskType == 0 {
		go runMapTask(taskInfo, mapf, done)
	} else if taskInfo.TaskType == 1 {
		go runReduceTask(taskInfo, reducef, done)
	} else {
		log.Fatalln("Wrong task type %v", taskInfo.TaskType)
	}

	return done
}

func runMapTask(mapTaskInfo TaskReply, mapf func(string, string) []KeyValue, done chan<- bool) {
	if mapTaskInfo.TaskType != 0 {
		log.Fatalln("Not map task.")
	}

	if len(mapTaskInfo.FileName) != 1 {
		log.Fatal("Map task has more than one input")
	}

	// read split
	file, err := os.Open(mapTaskInfo.FileName[0])
	if err != nil {
		log.Fatal("Map task fails to open split %v", mapTaskInfo.FileName[0])
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatal("Map task fails to read split %v", mapTaskInfo.FileName[0])
	}
	file.Close()

	// init intermediate buffer
	intermediateData := make([][]KeyValue, mapTaskInfo.ReduceNum)

	// execute map function to split
	kva := mapf(mapTaskInfo.FileName[0], string(content))
	for _, kv := range kva {
		partitionNum := ihash(kv.Key) % mapTaskInfo.ReduceNum
		intermediateData[partitionNum] = append(intermediateData[partitionNum], kv)
	}

	// write intermediate buffer to local disk
	for partitionNum, kvs := range intermediateData {
		if len(kvs) == 0 {
			continue
		}

		// create output file
		oname := makeMapOutput(mapTaskInfo, partitionNum)
		ofile, err := os.Create(oname)
		if err != nil {
			log.Fatal("Map output can't create file %v.", oname)
		}
		enc := json.NewEncoder(ofile)
		for _, kv := range kvs {
			err := enc.Encode(&kv)
			if err != nil {
				log.Fatal("Map output can't encode.")
			}
		}
		ofile.Close()
	}
	done <- true
}

func runReduceTask(reduceTaskInfo TaskReply, reducef func(string, []string) string, done chan<- bool) {
	if reduceTaskInfo.TaskType != 1 {
		log.Fatalln("Not reduce task.")
	}

	if len(reduceTaskInfo.FileName) < 1 {
		log.Fatal("Reduce input files less than one.")
	}

	// read all the splits and kvas
	kvas := []KeyValue{}
	for _, fileName := range reduceTaskInfo.FileName {
		file, err := os.Open(fileName)
		if err != nil {
			log.Fatal("Map task fails to open split %v", fileName)
		}

		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kvas = append(kvas, kv)
		}

		file.Close()
	}

	// sort by key
	sort.Sort(KVs(kvas))

	oname := makeReduceOutput(reduceTaskInfo)
	ofile, _ := os.Create(oname)
	// output result
	i := 0
	for i < len(kvas) {
		j := i + 1
		for j < len(kvas) && kvas[j].Key == kvas[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, kvas[k].Value)
		}
		output := reducef(kvas[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", kvas[i].Key, output)

		i = j
	}
	ofile.Close()

	done <- true
}

func makeMapOutput(mapTaskInfo TaskReply, partionNum int) string {
	return fmt.Sprintf("mr-%d-%d", mapTaskInfo.TaskNum, partionNum)
}

func makeReduceOutput(reduceTaskInfo TaskReply) string {
	return fmt.Sprintf("mr-out-%d", reduceTaskInfo.TaskNum)
}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Coordinator.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

func WorkerAskForTask() (TaskReply, bool) {
	args := TaskArgs{
		WorkerId: os.Geteuid(),
	}

	reply := TaskReply{}
	ret := call("Coordinator.AskForTask", &args, &reply)
	return reply, ret
}

func WorkerHeatBeat() {
	args := HeartBeatArgs{
		WorkerId: os.Geteuid(),
	}
	reply := HeartBeatReply{}

	call("Coordinator.HeartBeat", &args, &reply)
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
