package raft

import "labrpc"
import "time"
import "sync"


type Raft struct {
	mu        sync.Mutex
	peers     []*labrpc.ClientEnd
	persister *Persister
	me        int // index into peers[]
	applyCh   chan ApplyMsg

	timer 	  *time.Timer
	isLeader 	bool

	currentTerm 	int
	votedFor 		int
	log 			[]LogEntry
	commitIndex 	int
	lastApplied 	int

	nextIndex 		[]int
	matchIndex		[]int
	killed			bool

	lastIncludedIndex  	int
	lastIncludedTerm 	int

}

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
//
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

//
// example RequestVote RPC arguments structure.
//
type RequestVoteArgs struct {
	// Your data here.
	Term 		int
	CandidateId 	int
	LastLogIndex	int
	LastLogTerm		int
}

//
// example RequestVote RPC reply structure.
//
type RequestVoteReply struct {
	// Your data here.
	Term 		int
	VoteGranted 	int
}

type SnapshotArgs struct {
	Term 	int
	LeaderId 	int
	LastIncludedIndex 	int
	LastIncludedTerm 	int
	Snapshot  []byte
}
type SnapshotReply struct {
	Term 	int
}

type AppendArgs struct {
	Term 		int
	LeaderId 	int
	PrevLogIndex 	int
	PrevLogTerm 	int
	Entries 	[]LogEntry
	LeaderCommit 	int
}

type AppendReply struct {
	Term 		int
	Success 	bool
	Jump 		int
}

type LogEntry struct {
	Term 		int
	Index 		int
	Command 	interface{}
}
