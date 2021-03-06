package raft

type AppendEntriesArgs struct {
	Term     int // leader's term
	LeaderId int
	Entries  []LogEntry
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

//
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
