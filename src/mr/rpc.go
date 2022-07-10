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

type WorkerArgs struct {
}

type MapWorkArgs struct {
	Filename string
}

type MapWorkReply struct {
}

type ReduceWorkArgs struct {
	Index int
}

type ReduceWorkReply struct {
}

// Add your RPC definitions here.
type WorkTask struct {
	IsAllTaskFinished bool   //所有task完成 worker 退出
	Wait              bool   //等会再试
	IsMapTask         bool   //true map ; false reduce
	Filename          string //map input filename
	NReduce           int
	IndexReduce       int
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
