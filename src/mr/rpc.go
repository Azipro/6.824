package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"net/rpc"
	"os"
	"strconv"
)

//
// example to show how to declare the arguments
// and reply for an RPC.
//

// type ExampleArgs struct {
// 	X int
// }

// type ExampleReply struct {
// 	Y int
// }

// Add your RPC definitions here.
// type RegisterArgs struct {
// }

// type RegisterReply struct {
// 	workerNumber WorkerNumber
// }

type GetTaskArgs struct {
	// workerNumber WorkerNumber
	// workerType   WorkerType
}

type GetTaskReply struct {
	WT     string
	Name   string
	Number int

	// task
	// map
	MapIntputFile string
	NReduce       int

	// reduce
	ReduceInputFile []string
}

type FinishTaskArgs struct {
	// workerNumber WorkerNumber
	Done bool
	WT   string

	// task
	Name   string
	Number int

	// map
	MapIntputFile  string
	NReduce        int
	MapOutputFiles []string

	// reduce
	ReduceInputFile  []string
	ReduceOutputFile string
}

type FinishTaskReply struct {
	Done bool
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}

// TCP/IP
var MRServiceName string = "mapreduce"

type MRService interface {
	GetTask(args *GetTaskArgs, reply *GetTaskReply) error
	FinishTask(args *FinishTaskArgs, reply *FinishTaskReply) error
}

func RegisterService(service MRService) error {
	return rpc.RegisterName(MRServiceName, service)
}
