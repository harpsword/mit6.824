package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/harpsword/raft/labrpc"
)

// import "bytes"
// import "../labgob"

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

type LogEntry struct {
	Index   int
	Term    int
	Command interface{}
}

type RaftState int

const (
	Follower RaftState = iota
	Candidate
	Leader
)

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm int
	votedFor    *int
	log         []LogEntry

	commitIndex int
	lastApplied int

	// assistance variable
	state RaftState
	// receive heartbeat or not
	resetElectTimer bool

	nextIndex  []int
	matchIndex []int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (2A).
	return rf.currentTerm, rf.state == Leader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

// CompareLogTerm if candidate's log is at least up-to-date as rf(receiver)'s
// log, return true
func (rf *Raft) CompareLogTerm(logTerm int, logIndex int) bool {
	length := len(rf.log)
	if length == 0 {
		return true
	}
	term := rf.log[length-1].Term

	if logTerm > term {
		return true
	} else if logTerm == term && logIndex >= length-1 {
		return true
	}
	return false
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	DPrintf("args:%+v, rf:%+v \n", args, rf)
	reply.VoteGranted = false
	if rf.state == Follower {
		rf.mu.Lock()
		rf.resetElectTimer = true
		DPrintf("resetElectTimer; rf:%+v\n", rf)
		rf.mu.Unlock()
	}
	reply.Term = rf.currentTerm
	if rf.currentTerm > args.Term {
		return
	} else if rf.currentTerm < args.Term {
		rf.mu.Lock()
		rf.currentTerm = args.Term
		rf.mu.Unlock()
	}
	if rf.votedFor == nil && rf.CompareLogTerm(args.LastLogTerm, args.LastLogIndex) {
		rf.mu.Lock()
		rf.votedFor = &args.CandidateId
		rf.mu.Unlock()
		reply.VoteGranted = true
	} else if *rf.votedFor == args.CandidateId && rf.CompareLogTerm(args.LastLogTerm, args.LastLogIndex) {
		reply.VoteGranted = true
	}
	DPrintf("defer: reply:%+v, rf:%+v \n", reply, rf)
	return
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	DPrintf("in sendRequestVote, reply:%+v\n", reply)
	return ok
}

type AppendEntriesReq struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry

	LeaderCommit int
}

type AppendEntriesResp struct {
	Term    int
	Success bool
}

func min(a, b int) int {
	if a < b {
		return a
	} else {
		return b
	}
}

func (rf *Raft) AppendEntries(req *AppendEntriesReq, resp *AppendEntriesResp) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	resp.Success = true
	resp.Term = rf.currentTerm

	rf.resetElectTimer = true
	if req.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(req.LeaderCommit, len(rf.log)-1)
		DPrintf("renew commitIndex, rf:%+v", rf)
	}
	DPrintf("req: %+v, and , rf:%+v\n", req, rf)
	DPrintf("--------------in HeartBeat: resetElectTimer; rf:%+v\n", rf)
	if req.Term < rf.currentTerm {
		resp.Success = false
		return
	} else if req.Term > rf.currentTerm {
		rf.currentTerm = req.Term
		rf.TurnToFollower()
	}

	if req.PrevLogIndex == -1 {
		resp.Success = true
		return
	}
	if req.PrevLogIndex >= len(rf.log) {
		resp.Success = false
		return
	} else if rf.log[req.PrevLogIndex].Term != req.PrevLogIndex {
		resp.Success = false
		rf.log = rf.log[:req.PrevLogIndex]
		return
	}
	// if len(rf.log) == 0 {
	// 	resp.Success = (req.PrevLogIndex == 0 || req.PrevLogIndex == -1)
	// } else if len(rf.log)-1 < req.PrevLogIndex {
	// 	resp.Success = false
	// } else if rf.log[req.PrevLogIndex].Term != req.PrevLogTerm {
	// 	// 删除PrevLogIndex之后的元素
	// 	resp.Success = false
	// 	rf.log = rf.log[:req.PrevLogIndex]
	// }
	DPrintf("AppendEntries resp: %+v", resp)
	DPrintf("AppendEntries len:%d, success:%v", len(rf.log), (req.PrevLogIndex == 0 || req.PrevLogIndex == 1))
	if resp.Success {
		// append req.Entries 到 rf.log中
		rf.log = append(rf.log, req.Entries...)
	}
	return
}

func (rf *Raft) sendAppendEntries(server int, req *AppendEntriesReq, resp *AppendEntriesResp) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", req, resp)
	return ok
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// Your code here (2B).
	if rf.state != Leader {
		return 0, rf.currentTerm, false
	}
	// sync
	rf.log = append(rf.log, LogEntry{
		Index:   len(rf.log),
		Term:    rf.currentTerm,
		Command: command,
	})
	for idx := range rf.nextIndex {
		rf.nextIndex[idx]++
	}
	rf.commitIndex++
	DPrintf("add commitIndex in leader")
	// _ = rf.SendAllAppendEntries()
	// if ok {
	// 	rf.mu.Lock()
	// 	rf.commitIndex++
	// 	rf.mu.Unlock()
	// } else {
	// 	// abandom last log
	// 	rf.mu.Lock()
	// 	rf.log = rf.log[:len(rf.log)-1]
	// 	rf.mu.Unlock()
	// }

	return len(rf.log) - 1, rf.currentTerm, true
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) Run() {
	for {
		if rf.currentTerm == 20 {
			break
		}
		if rf.dead == 1 {
			break
		}
		switch rf.state {
		case Follower:
			rf.RunAsFollower()
		case Candidate:
			rf.RunAsCandidate()
		case Leader:
			rf.RunAsLeader()
		}
		DPrintf("time:%v,output State, rf:%+v \n", time.Now(), rf)
	}
}

func (rf *Raft) TurnToFollower() {
	rf.state = Follower
	rf.votedFor = nil
}

func (rf *Raft) TurnToCandidate() {
	rf.state = Candidate
	rf.votedFor = &rf.me
}

func (rf *Raft) TurntoLeader() {
	rf.state = Leader
	rf.votedFor = nil
	tmpLen := len(rf.log)
	if tmpLen > 0 {
		for i := 0; i != len(rf.peers); i++ {
			rf.nextIndex[i] = tmpLen
			rf.matchIndex[i] = 0
		}
	}
}

func (rf *Raft) SleepForElection() {
	var t int
	t = rand.Intn(150) + 150
	time.Sleep(time.Duration(t) * time.Millisecond)
}

func (rf *Raft) RunAsFollower() {
	if rf.state != Follower {
		return
	}
	rf.SleepForElection()
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.resetElectTimer {
		DPrintf("just ruturn, rf:%+v\n", rf)
		rf.resetElectTimer = false
		return
	}

	// turn to Candidate and send message to others
	rf.TurnToCandidate()
	rf.currentTerm++
}

func (rf *Raft) sendRequestVoteDeal(server int, count *int, wg *sync.WaitGroup) {
	defer wg.Done()
	if rf.state != Candidate {
		return
	}
	req := &RequestVoteArgs{
		Term:        rf.currentTerm,
		CandidateId: rf.me,
		// LastLogIndex: rf.log[len(rf.log)-1].Index,
		// LastLogTerm:  rf.log[len(rf.log)-1].Term,
	}
	resp := &RequestVoteReply{}
	ok := rf.sendRequestVote(server, req, resp)
	if !ok {
		return
	}
	if resp.Term > rf.currentTerm {
		rf.mu.Lock()
		rf.currentTerm = resp.Term
		rf.TurnToFollower()
		rf.mu.Unlock()
		return
	}
	DPrintf("before add count: resp:%+v, count:%d\n", resp, *count)
	if resp.VoteGranted {
		DPrintf("increase count, rf:%+v \n", rf)
		rf.mu.Lock()
		*count++
		rf.mu.Unlock()
		return
	}
}

func (rf *Raft) RunAsCandidate() {
	wg := sync.WaitGroup{}
	for {
		if rf.currentTerm == 20 {
			break
		}
		count := 0
		for i := 0; i != len(rf.peers); i++ {
			if i == rf.me {
				rf.mu.Lock()
				count++
				rf.mu.Unlock()
				continue
			}
			wg.Add(1)
			go rf.sendRequestVoteDeal(i, &count, &wg)
		}
		wg.Wait()
		if rf.state == Follower {
			return
		}
		DPrintf("me:%d, get votes:%d, the number of all rafts:%d\n", rf.me, count, len(rf.peers))
		if count*2 > len(rf.peers) {
			// turn to leaders
			rf.state = Leader
			return
		}
		if rf.resetElectTimer {
			// receive effect heartbeat
			rf.TurnToFollower()
			rf.resetElectTimer = false
			return
		}
		rf.currentTerm++
		rf.votedFor = nil
		rf.SleepForElection()
	}
}

func (rf *Raft) sendAppendEntriesDeal(server int, wg *sync.WaitGroup, count *int) {
	defer wg.Done()
	var ok bool = false
	done := false
	for !done {
		DPrintf("In sendAppendEntriesDeal, Send Heartbeat to server:%d \n", server)
		if rf.state != Leader {
			return
		}
		prevIndex := rf.nextIndex[server] - 1
		DPrintf("tmpIndex:%d; rf.log's length:%d \n", prevIndex, len(rf.log))
		DPrintf("nextIndex: %v", rf.nextIndex)
		var req AppendEntriesReq
		if len(rf.log) == 0 || prevIndex < 0 {
			// HeartBeat
			req = AppendEntriesReq{
				Term:     rf.currentTerm,
				LeaderId: rf.me,

				PrevLogIndex: prevIndex,
				LeaderCommit: rf.commitIndex,
			}
		} else {
			req = AppendEntriesReq{
				Term:     rf.currentTerm,
				LeaderId: rf.me,

				PrevLogIndex: prevIndex,
				PrevLogTerm:  rf.log[prevIndex].Term,
				Entries:      rf.log[prevIndex:],
				LeaderCommit: rf.commitIndex,
			}
		}

		resp := AppendEntriesResp{}
		ok = rf.sendAppendEntries(server, &req, &resp)
		if !ok {
			return
		}
		if resp.Term > rf.currentTerm {
			rf.mu.Lock()
			rf.currentTerm = resp.Term
			rf.TurnToFollower()
			rf.mu.Unlock()
		}
		if !resp.Success {
			rf.nextIndex[server]--
		} else {
			rf.nextIndex[server] = len(rf.log)
			rf.commitIndex = len(rf.log) - 1
			done = true
		}
	}
	rf.mu.Lock()
	*count++
	rf.mu.Unlock()
	return
}

// SendAllAppendEntries return true, if majority of peers receive rpc request
func (rf *Raft) SendAllAppendEntries() bool {
	wg := sync.WaitGroup{}
	count := 0
	if rf.state != Leader {
		return false
	}
	for i := 0; i != len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		wg.Add(1)
		DPrintf("Send Heartbeat to server:%d \n", i)
		go rf.sendAppendEntriesDeal(i, &wg, &count)
	}
	wg.Wait()
	if 2*(count+1) > len(rf.peers) {
		return true
	} else {
		return false
	}
}

func (rf *Raft) RunAsLeader() {
	DPrintf("Leader: Send HeartBeat!!!\n")
	rf.SendAllAppendEntries()
	time.Sleep(150 * time.Millisecond)
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	rf.state = Follower
	rf.currentTerm = 0
	rf.votedFor = nil

	// volatile state on all servers
	rf.commitIndex = 0
	rf.lastApplied = 0

	// Volatile state on leaders
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))

	// Assistant Variable
	rf.resetElectTimer = false

	// Your initialization code here (2A, 2B, 2C).
	go rf.Run()
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}
