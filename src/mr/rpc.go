package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
)
import "strconv"

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type JobType uint8

const (
	Map JobType = iota
	Reduce
)

type Args struct {
	
}

type Reply struct {
	JobType JobType
	JobID int64
	MapInput string
	ReduceInput1 []string
	ReduceInput2 [][]string
}

type SubmitMapArgs struct {
	JobType JobType
	JobID int64
	Result []KeyValue
}

type SubmitMapReply struct {
}

type SubmitReduceArgs struct {
	JobType JobType
	JobID   int64
	Result  []string
}

type SubmitReduceReply struct {
}

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.


// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
