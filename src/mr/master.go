package mr

import (
	"errors"
	"fmt"
	"log"
	"sort"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type JobStateType uint8
const (
	Ready JobStateType = iota
	Running
	Finished
)

type JobState struct {
	JobType JobType
	JobID uint64
	JobStateNow JobStateType
}

type MapJobState struct {
	JobState

	Input string
	Result []KeyValue
}

type ReduceJobState struct {
	JobState
	Input1 []string
	Input2 [][]string
	Result []string
}

type Master struct {
	// Your definitions here.
	sync.Mutex

	nMapJobs uint64
	nMapFinishJobs uint64

	nReduceJobs uint64
	nReduceFinishJobs uint64

	mapJobQueue *Queue
	//mapJobs map[uint64]MapJobState
	mapJobs []*MapJobState
	reduceJobQueue *Queue
	//reduceJobs map[uint64]ReduceJobState
	reduceJobs []*ReduceJobState

	timers []*time.Timer

	isFinish bool
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}


//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	fmt.Println("Listen to unix %v", sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	return m.isFinish
}


func (m *Master) GetJob(args *Args, reply *Reply) error {
	m.Lock()
	defer m.Unlock()

	if m.nMapFinishJobs < m.nMapJobs {
		// return Map Jobs
		reply.JobType = Map
		if m.mapJobQueue.IsEmpty() {
			reply.JobID = -1
		}else {
			Jobid, _ := m.mapJobQueue.Pop()
			fmt.Println("Pop Job id:", Jobid)
			reply.JobID = int64(Jobid)
			reply.MapInput = m.mapJobs[Jobid].Input
			m.mapJobs[Jobid].JobStateNow = Running
			m.timers = append(m.timers, time.AfterFunc(10 * time.Second, func(){
				if m.mapJobs[Jobid].JobStateNow == Running {
					m.mapJobQueue.Push(Jobid)
					m.mapJobs[Jobid].JobStateNow = Ready
				}
			}))
		}
	}else {
		// return Reduce Jobs
		if m.nReduceJobs == m.nReduceFinishJobs {
			// all jobs are finished
			reply.JobID = -2

		} else {
			reply.JobType = Reduce
			if m.reduceJobQueue.IsEmpty() {
				reply.JobID = -1
			} else {
				Jobid, _ := m.reduceJobQueue.Pop()
				fmt.Println("----------")
				fmt.Println("Pop Job id:", Jobid)
				reply.JobID = int64(Jobid)
				fmt.Println("Send Job id:", reply.JobID)
				reply.ReduceInput1 = m.reduceJobs[Jobid].Input1
				reply.ReduceInput2 = m.reduceJobs[Jobid].Input2
				m.reduceJobs[Jobid].JobStateNow = Running
				m.timers = append(m.timers, time.AfterFunc(10 * time.Second, func(){
					if m.reduceJobs[Jobid].JobStateNow == Running {
						m.reduceJobQueue.Push(Jobid)
						m.reduceJobs[Jobid].JobStateNow = Ready
					}
				}))
			}
		}

	}
	fmt.Println("Send Job id:", reply.JobID)
	return nil
}

// ByKey for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

func (m *Master) generateReduceJobs() {
	intermediate := []KeyValue{}
	for _, jobState := range m.mapJobs[1:] {
		intermediate = append(intermediate, jobState.Result...)
	}
	sort.Sort(ByKey(intermediate))

	for i := uint64(0); i < m.nReduceJobs; i++ {
		m.reduceJobs = append(m.reduceJobs, &ReduceJobState{
			JobState: JobState{Reduce, i, Ready},
			Input1: []string{},
			Input2: [][]string{},
			Result: []string{},
		})
		m.reduceJobQueue.Push(i+1)
	}

	for i := uint64(0); i < m.nReduceJobs; i++ {
		fmt.Println("Job id in Queue:", m.reduceJobQueue.Index(i))
	}

	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}

		Jobid := ihash(intermediate[i].Key) % int(m.nReduceJobs) + 1
		m.reduceJobs[Jobid].Input1 = append(m.reduceJobs[Jobid].Input1, intermediate[i].Key)
		m.reduceJobs[Jobid].Input2 = append(m.reduceJobs[Jobid].Input2, values)
		i = j
	}
}

func (m *Master) SubmitMap(args *SubmitMapArgs, reply *SubmitMapReply) error {
	m.Lock()
	defer m.Unlock()
	if args.JobID < 0 {
		return errors.New("Wrong Job ID, Job ID should bigger than 0!")
	}
	if m.mapJobs[args.JobID].JobStateNow == Running {
		fmt.Println("finish Map Job (id:", args.JobID, ")")
		m.nMapFinishJobs ++
		m.mapJobs[args.JobID].JobStateNow = Finished
		m.mapJobs[args.JobID].Result = args.Result

		fmt.Println("the number of finished map jobs: ", m.nMapFinishJobs, "/", m.nMapJobs)
		if m.nMapFinishJobs == m.nMapJobs {
			// finish all map jobs
			m.generateReduceJobs()
		}
	}
	return nil
}

func (m *Master) SubmitReduce(args *SubmitReduceArgs, reply *SubmitReduceReply) error {
	m.Lock()
	defer m.Unlock()
	if args.JobID < 0 {
		return errors.New("Wrong Job ID, Job ID should bigger than 0!")
	}
	if m.reduceJobs[args.JobID].JobStateNow == Running {
		fmt.Println("finish Reduce Job (id:", args.JobID, ")")
		m.nReduceFinishJobs ++
		m.reduceJobs[args.JobID].JobStateNow = Finished
		m.reduceJobs[args.JobID].Result = args.Result

		fmt.Println("the number of finished reduce jobs: ", m.nReduceFinishJobs, "/", m.nReduceJobs)
		if m.nReduceFinishJobs == m.nReduceJobs {
			m.outputResult()
			m.isFinish = true
		}
	}
	return nil
}

func (m *Master) outputResult() {
	fmt.Println("save result, lens of reducejobs:", len(m.reduceJobs))
	oname := "mr-out-0"
	ofile, _ := os.Create(oname)
	resultAll := []KeyValue{}
	for _, value := range m.reduceJobs[1:] {
		for index, key := range value.Input1 {
			resultAll = append(resultAll, KeyValue{Key: key, Value: value.Result[index]})
		}
	}
	sort.Sort(ByKey(resultAll))
	for _, value := range resultAll {
		fmt.Fprintf(ofile, "%v %v\n", value.Key, value.Value)
	}
	ofile.Close()
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{
		nMapJobs: uint64(len(files)),
		nMapFinishJobs: 0,
		nReduceJobs: uint64(nReduce),
		nReduceFinishJobs: 0,

		mapJobs:[]*MapJobState{},
		mapJobQueue: &Queue{},

		reduceJobs: []*ReduceJobState{},
		//reduceJobs: make([]*ReduceJobState, nReduce),
		reduceJobQueue: &Queue{},

		timers: []*time.Timer{},
		isFinish: false,
	}

	m.mapJobs = append(m.mapJobs, &MapJobState{})
	m.reduceJobs = append(m.reduceJobs, &ReduceJobState{})

	// add map jobs
	var i uint64 = 0
	for _, filename := range files {
		i ++
		m.mapJobQueue.Push(i)
		m.mapJobs = append(m.mapJobs, &MapJobState{
			JobState: JobState{Map, i, Ready},
			Input: filename,
			Result: nil,
		})
	}

	m.server()
	return &m
}
