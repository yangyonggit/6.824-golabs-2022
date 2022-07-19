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
	//	"bytes"
	"fmt"
	"log"
	"math/rand"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labrpc"
)

/////
type logTopic string

const (
	dClient  logTopic = "CLNT"
	dCommit  logTopic = "CMIT"
	dDrop    logTopic = "DROP"
	dError   logTopic = "ERRO"
	dInfo    logTopic = "INFO"
	dLeader  logTopic = "LEAD"
	dLog     logTopic = "LOG1"
	dLog2    logTopic = "LOG2"
	dPersist logTopic = "PERS"
	dSnap    logTopic = "SNAP"
	dTerm    logTopic = "TERM"
	dTest    logTopic = "TEST"
	dTimer   logTopic = "TIMR"
	dTrace   logTopic = "TRCE"
	dVote    logTopic = "VOTE"
	dWarn    logTopic = "WARN"
)

var debugStart time.Time
var debugVerbosity int

// Retrieve the verbosity level from an environment variable
func getVerbosity() int {
	v := os.Getenv("VERBOSE")
	level := 0
	if v != "" {
		var err error
		level, err = strconv.Atoi(v)
		if err != nil {
			log.Fatalf("Invalid verbosity %v", v)
		}
	}
	return level
}
func init() {
	debugVerbosity = getVerbosity()
	debugStart = time.Now()

	log.SetFlags(log.Flags() &^ (log.Ldate | log.Ltime))
}

func LogPrint(topic logTopic, format string, a ...interface{}) {
	if debugVerbosity >= 1 {
		time := time.Since(debugStart).Microseconds()
		time /= 100
		prefix := fmt.Sprintf("%06d %v ", time, string(topic))
		format = prefix + format
		log.Printf(format, a...)
	}
}

//

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

//
// LogEntry
//
type AppendEntry struct {
	// log string
}

type ServerState int

const (
	Follower  ServerState = 0
	Candidate ServerState = 1
	Leader    ServerState = 2
)

const (
	ElectionMinWait  int = 200
	ElectionMaxWait  int = 400
	HearBeatTick     int = 150
	HeartBeatTimeOut int = 600
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

	//Persisten state on all servers
	currentTerm int
	votedFor    int // candidateId that recevived vote in current term (or null if none)
	logList     []AppendEntry

	//Volatile state on all servers
	commitIndex int
	lastApplied int

	//Volatile state on leaders
	nextIndex  []int
	matchIndex []int

	//use for logic
	lastReciveTime   time.Time
	state            ServerState
	alreadRecivedRpc bool
	voteCount        int
	replyCount       int
	missCount        int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	isleader = rf.state == Leader

	LogPrint(dInfo, "S%d isLeader %v  ----- %v", rf.me, rf.state, term)
	return term, isleader
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
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

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

type AppendEntriesArgs struct {
	Term   int
	Leader int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) requestVoteToIndex(i int, args RequestVoteArgs, reply RequestVoteReply) {
	rf.mu.Lock()
	LogPrint(dInfo, "S%v on term %v sendRequestVote to S%v", rf.me, rf.currentTerm, i)
	rf.mu.Unlock()

	ok := rf.sendRequestVote(i, &args, &reply)
	rf.mu.Lock()
	if rf.state != Candidate || rf.alreadRecivedRpc {
		rf.mu.Unlock()
		return
	}

	if !ok {
		LogPrint(dVote, "S%v Can't Recive Vote From S%v", rf.me, i)
		rf.missCount++
	} else {
		rf.replyCount++

		if reply.Term > rf.currentTerm {
			LogPrint(dVote, "S%v c to f in request. ot = %v  nt = %v", rf.me, rf.currentTerm, reply.Term)
			rf.state = Follower
			rf.currentTerm = reply.Term
			rf.votedFor = -1
			rf.voteCount = 0
			rf.missCount = 0
			rf.mu.Unlock()
			return
		}

		if reply.VoteGranted {
			rf.voteCount++
			LogPrint(dVote, "S%v get vote from S%v. Vote is %v", rf.me, i, rf.voteCount)
		}
	}

	LogPrint(dVote, "========== S%v got %v votes from %v    &&&& miss count%v", rf.me, rf.voteCount, rf.replyCount, rf.missCount)

	validCount := len(rf.peers) - rf.missCount

	if validCount < 3 && rf.voteCount == validCount {
		LogPrint(dVote, "S%v became a leader!!!!!!!!  validCount=%v voteCount=%v", rf.me, validCount, rf.voteCount)
		rf.state = Leader
	} else if rf.voteCount > validCount/2 {
		LogPrint(dVote, "S%v became a leader!!!!!!!!  validCount=%v voteCount=%v", rf.me, validCount, rf.voteCount)
		rf.state = Leader
	}
	rf.mu.Unlock()
	rf.SendAllAppendEntries()
}

//RequestOthersVoteMe
func (rf *Raft) RequestOthersVoteMe(argsList []RequestVoteArgs, replyList []RequestVoteReply) {
	for i := range rf.peers {
		if i != rf.me {
			go rf.requestVoteToIndex(i, argsList[i], replyList[i])
		}
	}

}

//Leader send Heart Beat to all
func (rf *Raft) SendAllAppendEntries() {
	for i := range rf.peers {
		if i != rf.me {
			go rf.sendAppendEntriesToIndex(i)
		}
	}
}

func (rf *Raft) sendAppendEntriesToIndex(i int) {
	rf.mu.Lock()
	if rf.state != Leader {
		rf.mu.Unlock()
		return
	}

	args := AppendEntriesArgs{}
	reply := AppendEntriesReply{}

	args.Leader = rf.me
	args.Term = rf.currentTerm

	rf.mu.Unlock()
	ok := rf.sendAppendEntries(i, &args, &reply)

	rf.mu.Lock()
	if rf.state != Leader {
		rf.mu.Unlock()
		return
	}

	if !ok {
		LogPrint(dInfo, "S%v on term %v state = %v ---- sendAppendEntries S%v time out ", rf.me, rf.currentTerm, rf.state, i)
		rf.mu.Unlock()
		return
	}

	if reply.Term > rf.currentTerm {
		LogPrint(dInfo, "S%v LLL to fff in request. ot = %v  nt = %v", rf.me, rf.currentTerm, reply.Term)
		rf.state = Follower
		rf.currentTerm = reply.Term
		rf.voteCount = 0
		rf.votedFor = -1
		rf.missCount = 0
		rf.mu.Unlock()
		return
	}
	rf.mu.Unlock()
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	LogPrint(dVote, "S%v in stage %v recive from S%v request vote in stage %v.   ----  Vote is %v", rf.me, rf.currentTerm, args.CandidateId, args.Term, rf.votedFor)

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	if args.Term > rf.currentTerm {
		if rf.state != Follower {
			rf.state = Follower
			rf.voteCount = 0
			rf.missCount = 0
		}
		rf.currentTerm = args.Term
		rf.votedFor = -1
	}

	if rf.votedFor == -1 {
		LogPrint(dVote, "S%v  vote for S%v", rf.me, args.CandidateId)
		rf.votedFor = args.CandidateId
		reply.Term = rf.currentTerm
		reply.VoteGranted = true
	} else {
		LogPrint(dVote, "S%v  already vote to -----> %v", rf.me, rf.votedFor)
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
	}
}

//
// handle append entries rpc
//
func (rf *Raft) OnAppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// LogPrint(dVote, "S%v  on term %v recive AppendEntries From Leader S%v on term %v", rf.me, rf.currentTerm, args.Leader, args.Term)
	rf.lastReciveTime = time.Now()
	rf.alreadRecivedRpc = true

	if args.Term > rf.currentTerm {
		if rf.state != Follower {
			LogPrint(dVote, "S%v on term %v. Change To term %v", rf.me, rf.currentTerm, args.Term)
			rf.state = Follower
		}
		rf.currentTerm = args.Term
	}

	if args.Term == rf.currentTerm && rf.state == Leader {
		LogPrint(dVote, "S2S, kill one. S%v on term %v. Change To term %v", rf.me, rf.currentTerm, args.Term)
		rf.state = Follower
		rf.currentTerm = args.Term
	}

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false

	} else {
		rf.currentTerm = args.Term
		reply.Term = args.Term
		reply.Success = true
	}
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
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.OnAppendEntries", args, reply)
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
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	return index, term, isLeader
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

func (rf *Raft) followerTick() {
	waitTime := rand.Intn(ElectionMaxWait-ElectionMinWait) + ElectionMinWait
	time.Sleep(time.Duration(waitTime * int(time.Millisecond)))
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state != Follower {
		return
	}

	if !rf.alreadRecivedRpc {
		rf.state = Candidate
	} else {
		now := time.Now()
		if now.Sub(rf.lastReciveTime) > time.Duration(HeartBeatTimeOut*int(time.Millisecond)) {
			rf.alreadRecivedRpc = false
			LogPrint(dVote, "S%v failed connect to Leader.", rf.me)
		}
	}
}

func (rf *Raft) candidateTick() {
	waitTime := rand.Intn(ElectionMaxWait-ElectionMinWait) + ElectionMinWait
	time.Sleep(time.Duration(waitTime * int(time.Millisecond)))
	rf.mu.Lock()

	if rf.state == Candidate {
		rf.currentTerm++
		rf.votedFor = rf.me
		rf.voteCount = 1
		rf.replyCount = 0
		rf.missCount = 0
		LogPrint(dInfo, "Try start Election By S%v on term %v", rf.me, rf.currentTerm)
		argsList := make([]RequestVoteArgs, len(rf.peers))
		replyList := make([]RequestVoteReply, len(rf.peers))
		for i := range rf.peers {
			argsList[i].Term = rf.currentTerm
			argsList[i].CandidateId = rf.me
			argsList[i].LastLogIndex = 0
			argsList[i].LastLogTerm = 0
		}
		rf.mu.Unlock()
		rf.RequestOthersVoteMe(argsList, replyList)
		return
	}

	rf.mu.Unlock()
}

func (rf *Raft) leaderTick() {
	time.Sleep(time.Duration(HearBeatTick * int(time.Millisecond)))
	// rf.mu.Lock()
	// if rf.state != Leader {
	// 	return
	// }
	// rf.mu.Unlock()
	rf.SendAllAppendEntries()

}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().

		rf.mu.Lock()
		state := rf.state
		rf.mu.Unlock()

		if state == Follower {
			rf.followerTick()
		} else if state == Candidate {
			rf.candidateTick()
		} else if state == Leader {
			rf.leaderTick()
		}
	}
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

	// Your initialization code here (2A, 2B, 2C).

	rf.dead = 0
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.commitIndex = -1
	rf.lastApplied = -1
	rf.state = Follower
	rf.alreadRecivedRpc = false
	rf.voteCount = 0
	rf.replyCount = 0
	rf.missCount = 0
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
