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

import "labrpc"
import "time"
import "math/rand"
import "sync"
import "fmt"
import "bytes"
import "encoding/gob"

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	var term int
	var isleader bool
	// Your code here.
	term = rf.currentTerm
	isleader = rf.isLeader
	return term, isleader
}

func (rf *Raft) persist() {
	// Your code here.
	 w := new(bytes.Buffer)
	 e := gob.NewEncoder(w)
	 e.Encode(rf.currentTerm)
	 e.Encode(rf.votedFor)
	 e.Encode(rf.log)
	 data := w.Bytes()
	 rf.persister.SaveRaftState(data)
}

func (rf *Raft) readPersist(data []byte) {
	// Your code here.
	 r := bytes.NewBuffer(data)
	 d := gob.NewDecoder(r)
	 d.Decode(&rf.currentTerm)
	 d.Decode(&rf.votedFor)
	 d.Decode(&rf.log)
}

func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.timer.Reset(randomTime())
	reply.VoteGranted = 0
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		return 
	}
	if args.Term > rf.currentTerm {
		rf.update(args.Term)
	}
	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		curTerm := rf.log[len(rf.log)-1].Term
		if args.LastLogTerm > curTerm || (args.LastLogTerm == curTerm && args.LastLogIndex+1 >= len(rf.log)) {
			reply.VoteGranted = 1
			rf.votedFor = args.CandidateId
		}
	}
	rf.persist()
}


func (rf *Raft) AppendEntries(args AppendArgs, reply *AppendReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	reply.Success = false
	reply.Jump = args.PrevLogIndex
	rf.timer.Reset(randomTime())
	if args.Term < rf.currentTerm {
		return
	}
	rf.isLeader = false
	if args.Term > rf.currentTerm {
		rf.update(args.Term)
	}
	if args.PrevLogIndex < len(rf.log) && args.PrevLogTerm == rf.log[args.PrevLogIndex].Term {
		reply.Success = true
		rf.log = append(rf.log[:args.PrevLogIndex+1], args.Entries...)
		if args.LeaderCommit > rf.commitIndex {
			if len(rf.log)-1 > args.LeaderCommit {
				rf.commitIndex = args.LeaderCommit 
			}else {
				rf.commitIndex = len(rf.log)-1
			}
		}
	}else if args.PrevLogIndex >= len(rf.log){
		reply.Jump = len(rf.log)
	}else if args.PrevLogTerm != rf.log[args.PrevLogIndex].Term {
		for i := args.PrevLogIndex; i > 0; i-- {
			if rf.log[i].Term != rf.log[args.PrevLogIndex].Term {
				reply.Jump = i+1
				break
			}
		}
	}
	rf.persist()
}

func (rf *Raft) sendRequestVote(server int, args RequestVoteArgs, reply *RequestVoteReply) bool {
	rf.votedFor = rf.me
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}


//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	isLeader := true
	if rf.isLeader == false {
		isLeader := false
		return -1, -1, isLeader
	}
	rf.timer.Reset(randomTime())

	index := len(rf.log)
	term := rf.currentTerm
	logEntry := LogEntry{}
	logEntry.Term = rf.currentTerm
	logEntry.Command = command
	rf.log = append(rf.log, logEntry)

	rf.persist()
	return index, term, isLeader
}

//
func (rf *Raft) Kill() {
	// Your code here, if desired.
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.killed = true
}


func randomTime() (time.Duration) {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	t := r.Intn(150) + 150
	return time.Millisecond * time.Duration(t);
}

func (rf *Raft) update(term int) {
	rf.currentTerm = term
	rf.isLeader = false
	rf.votedFor = -1
	rf.persist()
}

func (rf *Raft) sendLog(to int) {
	rf.mu.Lock()
	if rf.isLeader == false {
		rf.mu.Unlock()
		return
	}
	if rf.nextIndex[to] > 1 {
		rf.nextIndex[to] --
	}
	a := AppendArgs{}	
	a.Term = rf.currentTerm
	a.LeaderId = rf.me
	a.PrevLogIndex = rf.nextIndex[to] - 1
	a.PrevLogTerm = rf.log[a.PrevLogIndex].Term
	a.Entries = rf.log[rf.nextIndex[to]:]
	a.LeaderCommit = rf.commitIndex

	b := AppendReply{}

	if rf.isLeader {
		rf.mu.Unlock()
		rf.peers[to].Call("Raft.AppendEntries", a, &b)
		if b.Term > rf.currentTerm {
			rf.update(b.Term)
			return
		}
		rf.mu.Lock()
		if rf.isLeader == false {
			rf.mu.Unlock()
			return
		}
		if b.Success {
			if rf.matchIndex[to] < a.PrevLogIndex + len(a.Entries) {
				rf.matchIndex[to] = a.PrevLogIndex + len(a.Entries)
			}
			if rf.nextIndex[to] < rf.matchIndex[to] + 1 {
				rf.nextIndex[to] = rf.matchIndex[to] + 1 
			}
			for com := len(rf.log)-1; com > 0; com-- {
				cnt := 0
				for i := 0; i < len(rf.peers); i++ {
					if i != rf.me && rf.matchIndex[i] >= com && rf.log[com].Term == rf.currentTerm {
						cnt++
					}
				}
				if cnt*2+1 >= len(rf.peers) && rf.commitIndex < com {
					rf.commitIndex = com 
					break
				}
			}
			rf.persist()
			rf.mu.Unlock()
			return
		}
		if (b.Jump > 0) {
			if rf.nextIndex[to] > b.Jump {
				rf.nextIndex[to] = b.Jump
			}
			defer rf.sendLog(to)
		}
		rf.mu.Unlock()
			
	} else {
		rf.mu.Unlock()
	}
	rf.mu.Lock()
	rf.persist()
	rf.mu.Unlock()
}

func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	rf.applyCh = applyCh
	rf.isLeader = false
	rf.votedFor = -1
	rf.commitIndex = 0

	logEntry := LogEntry{}
	logEntry.Term = -1
	rf.log = append(rf.log, logEntry)
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))

	rf.timer = time.NewTimer(randomTime())
	go func() {
		for !rf.killed {
			time.Sleep(time.Millisecond * 10)
			rf.mu.Lock()
			for ; rf.lastApplied < rf.commitIndex; {
				rf.lastApplied++
				app := ApplyMsg{}
				app.Index = rf.lastApplied
				app.Command = rf.log[app.Index].Command
				applyCh <- app
			}
			rf.mu.Unlock()
		}
	}()
	go func() {
		for !rf.killed {
			<- rf.timer.C
			if rf.isLeader {
				continue
			}
			rf.mu.Lock()
			fmt.Println("begin voted ", me, rf.currentTerm)
			var waitgroup sync.WaitGroup
			rf.currentTerm ++
			rf.votedFor = me
			orgTerm := rf.currentTerm
			rf.persist()
			n := len(peers)
			cnt := 0
			for i := 0; i < n; i++ {
				if i != me {
					waitgroup.Add(1)
					go func(to int) {
						args := RequestVoteArgs{}
						args.Term = rf.currentTerm
						args.CandidateId = me
						args.LastLogIndex = len(rf.log)-1
						args.LastLogTerm = rf.log[len(rf.log)-1].Term
						reply := new(RequestVoteReply)
						waitgroup.Done()
						rf.sendRequestVote(to, args, reply)
						rf.mu.Lock()
						if reply.Term > rf.currentTerm {
							rf.update(reply.Term)
						}
						if reply.VoteGranted > 0 {
							cnt ++
						}
						rf.mu.Unlock()
					}(i)
				}
			}
			waitgroup.Wait() 
			rf.mu.Unlock()
			round := 10
			timeout := randomTime()
			for i := 0; orgTerm == rf.currentTerm && i < round; i++ {
				if cnt * 2 + 1 >= n {
					break;
				}
				time.Sleep(timeout / time.Duration(round))
			}
			rf.mu.Lock()
			rf.timer.Reset(randomTime())
			if orgTerm != rf.currentTerm {
				rf.mu.Unlock()
				continue
			}
			if cnt * 2 + 1 >= n {
				rf.isLeader = true
				for i := 0; i < len(rf.peers); i++ {
					rf.nextIndex[i] = len(rf.log)
					rf.matchIndex[i] = 0
				}
			}else {
				rf.votedFor = -1
			}
			rf.mu.Unlock()
		}
	}()
	go func() {
		for !rf.killed {
			time.Sleep(time.Millisecond * 50)
			if rf.isLeader {
				n := len(peers)
				for i := 0; i < n; i++ {
					if i != me {
						go func(to int) {
							rf.sendLog(to)
						}(i)
					}
				}
			}
		}
	}()

	// initialize from state persisted before a crash
	rf.mu.Lock()
	rf.readPersist(persister.ReadRaftState())
	rf.mu.Unlock()

	return rf
}
