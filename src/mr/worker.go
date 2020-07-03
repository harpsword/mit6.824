package mr

import (
	"fmt"
	"io/ioutil"
	"os"
	"time"
	"log"
	"net/rpc"
	"hash/fnv"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

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

	CallExample()
	fmt.Println("success call example")

	args := Args{}
	reply := Reply{}

	for {
		fmt.Println("---------------")
		fmt.Println("Before Get Job ", "type: ", reply.JobType, "; job id:", reply.JobID)
		call("Master.GetJob", &args, &reply)
		fmt.Println("After Get Job ", "type: ", reply.JobType, "; job id:", reply.JobID)

		if reply.JobID == -1 {
			time.Sleep(time.Second)
			continue
		}else if reply.JobID == -2 {
			return
		}
		switch reply.JobType {
		case Map:
			submitMapArgs := SubmitMapArgs{
				JobType: Map,
				JobID: reply.JobID,
				Result: mapDeal(mapf, &reply),
			}
			submitMapReply := SubmitMapReply{}
			fmt.Println("Submit Job, type Map, job id:", reply.JobID)
			call("Master.SubmitMap", &submitMapArgs, &submitMapReply)

		case Reduce:
			submitReduceArgs := SubmitReduceArgs{
				JobType: Reduce,
				JobID:   reply.JobID,
				Result:  reduceDeal(reducef, &reply),
			}
			submitReduceReply := SubmitReduceReply{}
			fmt.Println("Submit Job, type Reduce, job id:", reply.JobID)
			call("Master.SubmitReduce", &submitReduceArgs, &submitReduceReply)
		}

	}

	// uncomment to send the Example RPC to the master.
	// CallExample()
}

func mapDeal(mapf func(string, string) []KeyValue, reply *Reply) []KeyValue{
	file, err := os.Open(reply.MapInput)
	if err != nil {
		log.Fatal("filename: %v", reply.MapInput)
		log.Fatalf("cannot open %v", reply.MapInput)
	}

	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatal("cannot read %v", reply.MapInput)
	}
	return mapf(reply.MapInput, string(content))
}

func reduceDeal(reducef func(string, []string) string, reply *Reply) []string {
	result := []string{}
	for index, input1 := range reply.ReduceInput1 {
		result = append(result, reducef(input1, reply.ReduceInput2[index]))
	}
	return result
}

//
// example function to show how to make an RPC call to the master.
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
	call("Master.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
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
