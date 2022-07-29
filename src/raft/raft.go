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
	"bytes"
	"fmt"
	"log"
	"math/rand"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labgob"
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

type Entry struct {
	Cmd  interface{}
	Term int
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()
	applyCh   chan ApplyMsg
	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	//Persisten state on all servers
	currentTerm int
	votedFor    int // candidateId that recevived vote in current term (or null if none)
	// log         map[int]Entry
	log []Entry

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
	lastestLog       bool
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

	LogPrint(dInfo, "S%d isLeader %v on Term %v  ----- log=%v   commitIndex=%v", rf.me, rf.state, term, rf.lengthOfLog(), rf.commitIndex)
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
	LogPrint(dPersist, "===============================================================")
	LogPrint(dPersist, "S%v is writing Persist", rf.me)

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.commitIndex)
	e.Encode(rf.lastApplied)
	e.Encode(rf.lengthOfLog())
	LogPrint(dPersist, "S%v is writing currentTerm=%v       votedFor=%v  len(rf.log)=%v",
		rf.me, rf.currentTerm, rf.votedFor, rf.lengthOfLog())
	for i := 1; i < len(rf.log); i++ {
		e.Encode(rf.log[i].Term)
		val, _ := rf.log[i].Cmd.(int)
		LogPrint(dPersist, "S%v is writing entry: term = %v   cmd = %v  ", rf.me, rf.log[i].Term, val)
		e.Encode(val)
	}
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
	LogPrint(dPersist, "===============================================================")
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

	// rf.mu.Lock()
	// defer rf.mu.Unlock()
	rf.log = make([]Entry, 1)
	LogPrint(dPersist, "---------------------------------------------------------")
	LogPrint(dPersist, "S%v is reading Persist", rf.me)
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	d.Decode(&rf.currentTerm)
	d.Decode(&rf.votedFor)
	d.Decode(&rf.commitIndex)
	d.Decode(&rf.lastApplied)
	logCount := 0
	d.Decode(&logCount)

	LogPrint(dPersist, "S%v is reading Persist TERM=%v   votedFor=%v  logCount=%v", rf.me, rf.currentTerm, rf.votedFor, logCount)
	for i := 0; i < logCount; i++ {
		entry := Entry{}
		d.Decode(&entry.Term)
		val := 0
		d.Decode(&val)
		var cmd interface{} = val
		entry.Cmd = cmd
		rf.log = append(rf.log, entry)
		LogPrint(dPersist, "S%v is reading entry term=%v   cmd=%v", rf.me, entry.Term, val)
	}
	LogPrint(dPersist, "---------------------------------------------------------")
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
	VoteGranted int // 0 - not Granted. 1 - Voted .
}

type AppendEntriesArgs struct {
	Term         int
	Leader       int
	PreLogIndex  int
	PrevLogTerm  int
	Entries      []Entry
	LeaderCommit int
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

		if reply.Term > rf.currentTerm || reply.VoteGranted < 0 {
			LogPrint(dVote, "S%v c to f in request. ot = %v  nt = %v or VoteGranted = %v ",
				rf.me, rf.currentTerm, reply.Term, reply.VoteGranted)
			// rf.state = Follower
			rf.changeStateToFollow()
			rf.currentTerm = reply.Term
			rf.votedFor = -1
			rf.voteCount = 0
			rf.missCount = 0
			if reply.VoteGranted < 0 {
				rf.lastestLog = false
			}
			rf.persist()
			rf.mu.Unlock()
			return
		}

		rf.voteCount += reply.VoteGranted
		LogPrint(dVote, "S%v get vote from S%v. Vote is %v", rf.me, i, rf.voteCount)

	}

	LogPrint(dVote, "========== S%v got %v votes from %v    &&&& miss count%v    on term=%v",
		rf.me, rf.voteCount, rf.replyCount, rf.missCount, rf.currentTerm)

	if rf.voteCount < 2 || rf.replyCount+rf.missCount+1 <= len(rf.peers)/2 {
		rf.mu.Unlock()
		return
	}

	validCount := len(rf.peers) - rf.missCount

	if rf.voteCount == len(rf.peers)-1 {
		LogPrint(dVote, "S%v became a leader!!!!!!!!  validCount=%v voteCount=%v", rf.me, validCount, rf.voteCount)
		rf.doLeaderInit()
	} else if rf.voteCount > len(rf.peers)/2 {
		LogPrint(dVote, "S%v became a leader!!!!!!!!  validCount=%v voteCount=%v", rf.me, validCount, rf.voteCount)
		rf.doLeaderInit()
	}

	rf.mu.Unlock()
	rf.SendAllAppendEntries(true)
}

func (rf *Raft) doLeaderInit() {
	LogPrint(dLeader, "S%v is Leader. Do init......", rf.me)
	rf.state = Leader
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))

	for i := range rf.peers {
		rf.nextIndex[i] = rf.lengthOfLog()
		// LogPrint(dTest, "S%v    init next index %v   ----   %v", rf.me, i, rf.nextIndex[i])
		rf.matchIndex[i] = rf.commitIndex
	}

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
func (rf *Raft) SendAllAppendEntries(empty bool) {
	for i := range rf.peers {
		if i != rf.me {
			go rf.sendAppendEntriesToIndex(i, empty)
		}
	}
}

func (rf *Raft) buildAppendEntriesArgs(i int, empty bool) *AppendEntriesArgs {
	args := AppendEntriesArgs{}
	args.Leader = rf.me
	args.Term = rf.currentTerm
	if !empty {
		logRepicationCount := rf.lengthOfLog() - rf.nextIndex[i]
		// LogPrint(dTest, "logRepicationCount %v      len->%v    next->%v", logRepicationCount, rf.lengthOfLog(), rf.nextIndex[i])
		if logRepicationCount > 0 {
			args.Entries = make([]Entry, 0)
			for i := rf.nextIndex[i] + 1; i <= rf.lengthOfLog(); i++ {
				args.Entries = append(args.Entries, rf.log[i])
			}

			// copy(args.Entries, rf.log[rf.nextIndex[i]:])
			LogPrint(dTest, "S%v build append to S%v,  rf.NextIndex->%v len of log->%v  send->%v", rf.me, i, rf.nextIndex[i], rf.lengthOfLog(), len(args.Entries))
			args.PreLogIndex = rf.nextIndex[i]
			if args.PreLogIndex > 0 {
				args.PrevLogTerm = rf.log[args.PreLogIndex].Term
			} else {
				args.PrevLogTerm = 0
			}
			args.LeaderCommit = rf.commitIndex
		} else {
			args.Entries = nil
			args.PreLogIndex = 0
			args.PrevLogTerm = 0
			args.LeaderCommit = rf.commitIndex
		}
	}
	return &args
}

func (rf *Raft) checkAppendEntriesReply(i int, empty bool, ok bool, args *AppendEntriesArgs, reply *AppendEntriesReply) {
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
		// rf.state = Follower
		rf.changeStateToFollow()
		rf.currentTerm = reply.Term
		rf.voteCount = 0
		rf.votedFor = -1
		rf.missCount = 0
		rf.persist()
		rf.mu.Unlock()
		return
	}

	if reply.Success {
		if args.Entries != nil && len(args.Entries) > 0 {
			rf.nextIndex[i] += len(args.Entries)
			rf.matchIndex[i] += len(args.Entries)
			LogPrint(dLeader, "Leader up date index ====== S%v  <--  S%v     next->%v match->%v len->%v", rf.me, i, rf.nextIndex[i], rf.matchIndex[i], rf.lengthOfLog())
		}
	} else {
		//todo
		for rf.nextIndex[i] > 0 {
			lastLogTerm := rf.log[rf.nextIndex[i]].Term
			rf.nextIndex[i]--
			if lastLogTerm != rf.log[rf.nextIndex[i]].Term {
				break
			}
		}
		// LogPrint(dTest, "S%v  --   %v     %v", rf.me, i, rf.nextIndex[i])
	}

	maxMatchIndex := 0
	for _, v := range rf.matchIndex {
		if maxMatchIndex < v {
			maxMatchIndex = v
		}
	}
	// LogPrint(dTest, "Leader S%v found maxMatchIndex = %v  and commitIndex = %v", rf.me, maxMatchIndex, rf.commitIndex)
	for maxMatchIndex > rf.commitIndex {
		count := 0
		for _, v := range rf.matchIndex {
			if v >= maxMatchIndex {
				count++
			}
		}
		LogPrint(dTest, "Leader S%v how many follower update to maxMatchIndex %v       ---- from  S%v ", rf.me, count, i)
		if maxMatchIndex > 0 && rf.isLogReplicationOK(count) && maxMatchIndex < len(rf.log) && rf.log[maxMatchIndex].Term == rf.currentTerm {
			// rf.commitIndex = maxMatchIndex
			LogPrint(dCommit, "===============================================================================")
			rf.commitLog(maxMatchIndex)
			LogPrint(dCommit, "===============================================================================")
			LogPrint(dLeader, "========> S%v Commit index up to %v", rf.me, rf.commitIndex)
			LogPrint(dLeader, "-----------------------------------------------------------------------")
			break
		}
		maxMatchIndex--
	}

	rf.mu.Unlock()
}

func (rf *Raft) isLogReplicationOK(count int) bool {
	if len(rf.peers) < 4 {
		return count > 0
	}
	return (count + 1) > len(rf.peers)/2
}

func (rf *Raft) sendAppendEntriesToIndex(i int, empty bool) {
	rf.mu.Lock()
	if rf.state != Leader {
		rf.mu.Unlock()
		return
	}

	args := rf.buildAppendEntriesArgs(i, empty)
	reply := AppendEntriesReply{}

	rf.mu.Unlock()
	ok := rf.sendAppendEntries(i, args, &reply)
	rf.checkAppendEntriesReply(i, empty, ok, args, &reply)
}

func (rf *Raft) getLastLogTerm() int {
	if len(rf.log) == 1 {
		return 0
	} else {
		return rf.log[len(rf.log)-1].Term
	}
}

func (rf *Raft) isCandidateLogLastest(index int, term int) bool {
	if term > rf.getLastLogTerm() {
		return true
	} else if term == rf.getLastLogTerm() {
		return index >= rf.lengthOfLog()
	} else {
		return false
	}
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	LogPrint(dVote, "S%v in stage %v recive from S%v request vote in stage %v.   ----  Vote is %v   ==> %v",
		rf.me, rf.currentTerm, args.CandidateId, args.Term, rf.votedFor, rf.isCandidateLogLastest(args.LastLogIndex, args.LastLogTerm))

	//不是最新的，一票否决
	if !rf.isCandidateLogLastest(args.LastLogIndex, args.LastLogTerm) {
		reply.Term = rf.currentTerm
		reply.VoteGranted = -len(rf.peers)
		return
	}

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = 0
		return
	}

	if args.Term > rf.currentTerm {
		if rf.state != Follower {
			// rf.state = Follower
			rf.changeStateToFollow()
			rf.voteCount = 0
			rf.missCount = 0
		}
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.persist()
	}

	//If votedFor is null or candidateId, and candidate’s log is at
	//least as up-to-date as receiver’s log, grant vot
	LogPrint(dTest, "LastLogIndex = %v    lastTerm = %v   curTerm = %v  len(log) =%v logTerm = %v  ---->  %v",
		args.LastLogIndex, args.LastLogTerm, rf.currentTerm, rf.lengthOfLog(), rf.getLastLogTerm(), rf.isCandidateLogLastest(args.LastLogIndex, args.LastLogTerm))

	if rf.votedFor == -1 {
		LogPrint(dVote, "S%v  vote for S%v", rf.me, args.CandidateId)
		rf.votedFor = args.CandidateId
		reply.Term = rf.currentTerm
		reply.VoteGranted = 1
	} else {
		LogPrint(dVote, "S%v  already vote to -----> %v", rf.me, rf.votedFor)
		reply.Term = rf.currentTerm
		reply.VoteGranted = 0
	}
	rf.persist()
}

//remove 0 item
func (rf *Raft) lengthOfLog() int {
	return len(rf.log) - 1
}

func (rf *Raft) changeStateToFollow() {
	rf.state = Follower
	LogPrint(dTest, "S%v  ------ remove uncommit log commit%v   len%v", rf.me, rf.commitIndex, rf.lengthOfLog())
	if rf.commitIndex < rf.lengthOfLog()-1 {
		rf.log = rf.log[:rf.commitIndex+1]
	}
	rf.persist()
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
	rf.lastestLog = true

	//common check
	if args.Term > rf.currentTerm {
		if rf.state != Follower {
			LogPrint(dInfo, "S%v on term %v. ======> Change To term %v. Leader is %v", rf.me, rf.currentTerm, args.Term, args.Leader)
			// rf.state = Follower
			rf.changeStateToFollow()
		}
		rf.currentTerm = args.Term
		rf.persist()
	}

	if args.Term == rf.currentTerm && rf.state == Leader {
		LogPrint(dInfo, "S2S, kill one. S%v on term %v. Change To term %v", rf.me, rf.currentTerm, args.Term)
		// rf.state = Follower
		rf.changeStateToFollow()
		rf.currentTerm = args.Term
		rf.persist()
	}

	if rf.state == Candidate {
		// rf.state = Follower
		rf.changeStateToFollow()
		rf.currentTerm = args.Term
		reply.Term = rf.currentTerm
		reply.Success = false
		rf.persist()
		return
	}

	//task 1.Reply false if term < currentTerm
	if args.Term < rf.currentTerm {
		LogPrint(dInfo, "S%v on term %v recive S%v Leader on term %v ---  AppendEntries , leader term is behind -- failed check 1 ",
			rf.me, rf.currentTerm, args.Leader, args.Term)
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	// if args.Entries == nil || len(args.Entries) == 0 {
	// 	reply.Term = rf.currentTerm
	// 	reply.Success = true
	// 	return
	// }

	//task 2 Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm
	if args.PreLogIndex > 0 && args.PrevLogTerm > 0 &&
		(args.PreLogIndex > rf.lengthOfLog() || (args.PreLogIndex <= rf.lengthOfLog() && rf.log[args.PreLogIndex].Term != args.PrevLogTerm)) {
		LogPrint(dInfo, "S%v on term %v.PreLogIndex=>%v, PrevLogTerm=>%v,rf.lengthOfLog()=>%v,  preLog not exist  -- failed check 2 ",
			rf.me, rf.currentTerm, args.PreLogIndex, args.PrevLogTerm, rf.lengthOfLog())
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	//task 3:If an existing entry conflicts with a new one (same index
	//but different terms), delete the existing entry and all that
	//follow it (§5.3
	//task 5:Append any new entries not already in the log
	if args.Entries != nil && len(args.Entries) > 0 {
		for i, entry := range args.Entries {
			index := i + args.PreLogIndex + 1
			if index < len(rf.log) {
				rf.log[index] = entry
			} else {
				rf.log = append(rf.log, entry)
			}
			LogPrint(dCommit, "S%v add log %v ->>>>>>>>>>>>>>>>>>>>>> update by leader   S%v cmd --> %v ||||  curTerm %v   leaderTerm %v",
				rf.me, index, args.Leader, entry.Cmd, args.Term, rf.currentTerm)
		}
		rf.persist()
	}

	//task 5,If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)

	// LogPrint(dTest, "S%v  ===>  args.LeaderCommit -> %v      rf.commitIndex -> %v", rf.me, args.LeaderCommit, rf.commitIndex)
	if args.LeaderCommit > rf.commitIndex {
		minValue := 0
		if args.LeaderCommit < rf.lengthOfLog() {
			minValue = args.LeaderCommit
		} else {
			minValue = rf.lengthOfLog()
		}
		LogPrint(dCommit, "----------------------------------------------------------------------")
		rf.commitLog(minValue)
		LogPrint(dCommit, "----------------------------------------------------------------------")
		LogPrint(dLeader, "S%v Commit index up to %v --- by update to leader, rf.lengthOfLog()->%v", rf.me, rf.commitIndex, rf.lengthOfLog())
		// rf.commitIndex = math.Min(args.LeaderCommit, rf.lengthOfLog()-1)
	}

	rf.currentTerm = args.Term
	reply.Term = args.Term
	reply.Success = true
	rf.persist()
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
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	isLeader = rf.state == Leader
	if isLeader {
		entry := Entry{Cmd: command, Term: rf.currentTerm}
		rf.log = append(rf.log, entry)
		index = rf.lengthOfLog()
		// // lastIndex++
		// // rf.log[lastIndex] = entry
		rf.persist()
		LogPrint(dCommit, "=========================================================================== %v", command)
		LogPrint(dCommit, "S%v Leader add rf log write to index %v  on Term ------>%v", rf.me, index, rf.currentTerm)
	} else {
		return index, term, isLeader
	}
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

func (rf *Raft) commitTick() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.commitIndex > rf.lastApplied {
		rf.lastApplied++
		// todo aplply log[lastApplied] to state machine

	}
	rf.persist()
}

func (rf *Raft) commitLog(index int) {
	for i := rf.commitIndex + 1; i <= index; i++ {
		msg := ApplyMsg{}
		msg.CommandValid = true
		msg.Command = rf.log[i].Cmd
		msg.CommandIndex = i
		LogPrint(dCommit, "commitLog  S%v   ------ commit id -> %v     cmd->%v   cmdTerm->%v   ||||||||||  curTerm ->%v",
			rf.me, i, msg.Command, rf.log[i].Term, rf.currentTerm)
		rf.applyCh <- msg
	}

	rf.commitIndex = index
	rf.persist()
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

	if rf.state == Candidate && rf.lastestLog {
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
			lastIndex := rf.lengthOfLog()
			argsList[i].LastLogIndex = lastIndex
			if lastIndex > 0 {
				argsList[i].LastLogTerm = rf.log[lastIndex].Term
			} else {
				argsList[i].LastLogTerm = 0
			}

		}
		rf.persist()
		rf.mu.Unlock()
		rf.RequestOthersVoteMe(argsList, replyList)
		// waitTime = rand.Intn(ElectionMaxWait-ElectionMinWait) + ElectionMinWait
		// waitTime = ElectionMaxWait
		// time.Sleep(time.Duration(waitTime * int(time.Millisecond)))
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
	rf.SendAllAppendEntries(false)

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
		rf.commitTick()
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
	rf.applyCh = applyCh
	rf.dead = 0
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.state = Follower
	rf.alreadRecivedRpc = false
	rf.voteCount = 0
	rf.replyCount = 0
	rf.missCount = 0
	rf.lastestLog = true
	// rf.log = map[int]Entry{}
	rf.log = make([]Entry, 1)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
