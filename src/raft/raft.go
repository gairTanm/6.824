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

	"math/rand"
	"os"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labrpc"
)

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

var peerId = os.Getpid()

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
// A Go object implementing a single Raft peer.
//

type State string

const (
	Leader    State = "L"
	Candidate State = "C"
	Follower  State = "F"
)

type Raft struct {
	mu           sync.Mutex          // Lock to protect shared access to this peer's state
	peers        []*labrpc.ClientEnd // RPC end points of all peers
	persister    *Persister          // Object to hold this peer's persisted state
	me           int                 // this peer's index into peers[]
	dead         int32               // set by Kill()
	currentTerm  int                 // 2A start
	votedFor     int                 //
	log          []*LogEntry         //
	isLeader     bool                //
	state        State
	electionTime time.Time
	heartbeatCh  chan (bool) // buffered channel
	//								//2A end
	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isLeader bool
	// Your code here (2A).
	rf.mu.Lock()
	term = rf.currentTerm
	isLeader = rf.isLeader
	rf.mu.Unlock()
	return term, isLeader
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

func max(a int, b int) int {
	if a > b {
		return a
	}
	return b
}

func (rf *Raft) resetElectionTimer() {
	t := time.Now()
	electionTimeout := time.Duration(150+rand.Intn(150)) * time.Millisecond
	rf.electionTime = t.Add(electionTimeout)
}

func (rf *Raft) ConvertToLeader() {
	rf.state = Leader
}

func (rf *Raft) ConvertToCandidate() {
	rf.state = Candidate
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.resetElectionTimer()
}

func (rf *Raft) ConvertToFollower() {
	rf.state = Follower
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	currentTerm, _ := rf.GetState()
	reply.Term = currentTerm
	if args.Term < currentTerm {
		reply.VoteGranted = false
	} else {
		reply.VoteGranted = true
		rf.mu.Lock()
		rf.currentTerm = max(currentTerm, args.Term)
		rf.mu.Unlock()
	}
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	currentTerm, _ := rf.GetState()

	rf.heartbeatCh <- true

	if args.Term < currentTerm {
		reply.Success = false
		reply.Term = currentTerm
	} else {
		rf.mu.Lock()
		rf.currentTerm = args.Term
		rf.mu.Unlock()
		reply.Success = true
		reply.Term = args.Term
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
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

func (rf *Raft) BroadcastHeartbeats() {
	var repliesMu sync.Mutex
	var replies []AppendEntriesReply
	currentTerm, _ := rf.GetState()
	args := AppendEntriesArgs{currentTerm, rf.me, nil}

	var wg sync.WaitGroup
	// fmt.Printf("args sent for election start: %v\n", args)
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		wg.Add(1)
		go func(server int, args *AppendEntriesArgs) {
			defer wg.Done()
			reply := &AppendEntriesReply{}
			rf.sendAppendEntries(server, args, reply)
			repliesMu.Lock()
			replies = append(replies, *reply)
			repliesMu.Unlock()

		}(i, &args)
	}
	wg.Wait()
	for i := 0; i < len(replies); i++ {
		if replies[i].Term > currentTerm {
			rf.mu.Lock()
			rf.isLeader = false
			rf.currentTerm = replies[i].Term
			rf.mu.Unlock()
		}
	}
}

func (rf *Raft) StartElection() {
	var repliesMu sync.Mutex
	var replies []RequestVoteReply
	rf.mu.Lock()
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.mu.Unlock()
	currentTerm, _ := rf.GetState()
	args := RequestVoteArgs{currentTerm, rf.me, -1, -1}
	var wg sync.WaitGroup
	// fmt.Printf("args sent for election start: %v\n", args)
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		wg.Add(1)
		go func(server int, args *RequestVoteArgs) {
			reply := &RequestVoteReply{}
			rf.sendRequestVote(server, args, reply)
			repliesMu.Lock()
			replies = append(replies, *reply)
			repliesMu.Unlock()
			wg.Done()
		}(i, &args)
	}
	wg.Wait()
	votes := 0
	// fmt.Printf("replies %v\n", replies)
	for i := 0; i < len(replies); i++ {
		if replies[i].VoteGranted {
			votes++
		}
	}
	if votes*2 > len(rf.peers) {
		rf.mu.Lock()
		rf.isLeader = true
		rf.mu.Unlock()
		go rf.BroadcastHeartbeats()
	}
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

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
// Your code here to check if a leader election should
// be started and to randomize sleeping time using
// time.Sleep().
//
func (rf *Raft) Server() {
	for rf.killed() == false {
		electionTimeout := time.Millisecond * time.Duration(rand.Intn(151)+150)
		// electionTimeout := time.Second * time.Duration(rand.Intn(10))
		_, isLeader := rf.GetState()
		// Debug(dInfo, "%v's term: %v\n", rf.me, currentTerm)
		if isLeader {
			<-time.After(heartbeatInterval)
			Debug(dInfo, "%v is the leader\n", rf.me)
			rf.BroadcastHeartbeats()
			continue
		} else {
			// wait for a heartbeat receive or start an election whichever is earlier
			select {
			case <-rf.heartbeatCh:
				continue
			case <-time.After(electionTimeout):
				// start a new election
				Debug(dInfo, "%v starting a new election\n", rf.me)
				rf.StartElection()
				continue
			}
		}
	}
	// Debug(dInfo, "%v's status: %v, %v", rf.me, rf.isLeader, rf.currentTerm)
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
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.isLeader = false
	rf.heartbeatCh = make(chan bool, 1)
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.Server()

	return rf
}
