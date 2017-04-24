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
import "bytes"
import "encoding/gob"
import "fmt"

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

func (rf *Raft) DiscardLogBefore(lastSnapIndex int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//DPrintf("before %d %d %d", rf.lastIncludedIndex, rf.lastIncludedTerm, len(rf.log))
	if rf.lastIncludedIndex < lastSnapIndex {
		index := lastSnapIndex - rf.lastIncludedIndex - 1
		rf.lastIncludedIndex = lastSnapIndex
		rf.lastIncludedTerm = rf.log[index].Term
		rf.log = rf.log[index+1:]
	}
	rf.persist()
	//DPrintf("ok %d %d %d", rf.lastIncludedIndex, rf.lastIncludedTerm, len(rf.log))
}

func (rf *Raft) persist() {
	// Your code here.
	 w := new(bytes.Buffer)
	 e := gob.NewEncoder(w)
	 e.Encode(rf.currentTerm)
	 e.Encode(rf.votedFor)
	 e.Encode(rf.log)
	 e.Encode(rf.lastIncludedIndex)
	 e.Encode(rf.lastIncludedTerm)
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
	 d.Decode(&rf.lastIncludedIndex)
	 d.Decode(&rf.lastIncludedTerm)
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
		var curTerm int
		if len(rf.log) > 0 {
			curTerm = rf.log[len(rf.log)-1].Term
		}else {
			curTerm = rf.lastIncludedTerm
		}
		if args.LastLogTerm > curTerm || (args.LastLogTerm == curTerm && args.LastLogIndex >= len(rf.log) + rf.lastIncludedIndex) {
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
	if args.PrevLogIndex < rf.lastIncludedIndex {
		reply.Success = true // not gonna happen?
		return
	}

	if args.PrevLogIndex == rf.lastIncludedIndex || 
		(args.PrevLogIndex <= len(rf.log) + rf.lastIncludedIndex && args.PrevLogTerm == rf.log[args.PrevLogIndex-rf.lastIncludedIndex-1].Term) {
		reply.Success = true
		rf.log = append(rf.log[:args.PrevLogIndex-rf.lastIncludedIndex], args.Entries...)
		if args.LeaderCommit > rf.commitIndex {
			if len(rf.log)+rf.lastIncludedIndex > args.LeaderCommit {
				rf.commitIndex = args.LeaderCommit 
			}else {
				rf.commitIndex = len(rf.log)+rf.lastIncludedIndex
			}
		}
	}else if args.PrevLogIndex > len(rf.log) + rf.lastIncludedIndex{
		reply.Jump = len(rf.log) + rf.lastIncludedIndex + 1
	}else if args.PrevLogTerm != rf.log[args.PrevLogIndex-rf.lastIncludedIndex-1].Term {
		var i int
		for i = args.PrevLogIndex-rf.lastIncludedIndex-1; i >= 0; i-- {
			if rf.log[i].Term != rf.log[args.PrevLogIndex-rf.lastIncludedIndex-1].Term {
				break
			}
		}
		reply.Jump = rf.lastIncludedIndex + i + 2
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
//fmt.Println(rf.me, command.(int))
	index := len(rf.log) + 1 + rf.lastIncludedIndex
	term := rf.currentTerm
	logEntry := LogEntry{}
	logEntry.Term = rf.currentTerm
	logEntry.Command = command
	logEntry.Index = index
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

func (rf *Raft) InitallSnapshotRPC(args SnapshotArgs, reply *SnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm || rf.lastIncludedIndex >= args.LastIncludedIndex {
		return
	}
	rf.persister.snapshot = args.Snapshot
	if args.LastIncludedIndex <= rf.lastIncludedIndex + len(rf.log) && args.LastIncludedTerm == rf.log[args.LastIncludedIndex-rf.lastIncludedIndex-1].Term {
		rf.log = rf.log[args.LastIncludedIndex-rf.lastIncludedIndex:]
	}else {
		rf.log = make([]LogEntry, 0)
	}
	rf.lastIncludedTerm = args.LastIncludedTerm
	rf.lastIncludedIndex = args.LastIncludedIndex
	if rf.commitIndex < rf.lastIncludedIndex {
		rf.commitIndex = rf.lastIncludedIndex
		rf.lastApplied = rf.lastIncludedIndex
		app := ApplyMsg{}
		app.UseSnapshot = true
		app.Snapshot = args.Snapshot
		rf.applyCh <- app
	}
}

func (rf *Raft) sendLog(to int) {
	rf.mu.Lock()
	if rf.isLeader == false {
		rf.mu.Unlock()
		return
	}
	if rf.nextIndex[to] != len(rf.log)+1 {
		DPrintf("rf.nextIndex %d, %d, %d, %d %b", rf.nextIndex[to], len(rf.log), rf.me, to, (rf.nextIndex[to] <= rf.lastIncludedIndex))
		DPrintf("%d %d  %d", rf.me, rf.isLeader, rf.currentTerm)
	}
	a := AppendArgs{}	
	a.Term = rf.currentTerm
	a.LeaderId = rf.me
	a.PrevLogIndex = rf.nextIndex[to] - 1
	if rf.nextIndex[to] <= rf.lastIncludedIndex {
		args := SnapshotArgs{}
		args.Term = rf.currentTerm
		args.LeaderId = rf.me
		args.LastIncludedIndex = rf.lastIncludedIndex
		args.LastIncludedTerm = rf.lastIncludedTerm
		args.Snapshot = rf.persister.snapshot
		reply := SnapshotReply{}
		rf.mu.Unlock()
		rf.peers[to].Call("Raft.InitallSnapshotRPC", args, &reply)
		rf.mu.Lock()
		if reply.Term > rf.currentTerm {
			rf.update(reply.Term)
		}
		rf.mu.Unlock()
		return
	}
	if rf.nextIndex[to] > 1 + rf.lastIncludedIndex {
		a.PrevLogTerm = rf.log[a.PrevLogIndex-rf.lastIncludedIndex-1].Term
	}else {
		a.PrevLogTerm = rf.lastIncludedTerm
	}
	a.Entries = rf.log[rf.nextIndex[to]-rf.lastIncludedIndex-1:]
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
			for com := len(rf.log)-1; com >= 0; com-- {
				cnt := 0
				for i := 0; i < len(rf.peers); i++ {
					if i != rf.me && rf.matchIndex[i] >= com+1+rf.lastIncludedIndex && rf.log[com].Term == rf.currentTerm {
						cnt++
					}
				}
				if cnt*2+1 >= len(rf.peers) && rf.commitIndex < com+1+rf.lastIncludedIndex {
					rf.commitIndex = com+1+rf.lastIncludedIndex 
					break
				}
			}
			if len(a.Entries) > 0 {
			//fmt.Println("send to ", rf.me, to, a.PrevLogIndex, len(a.Entries), len(rf.log), rf.commitIndex)
			}
			rf.persist()
			rf.mu.Unlock()
		}else if (b.Jump > 0) {
			if rf.nextIndex[to] > b.Jump {
				rf.nextIndex[to] = b.Jump
			}
			rf.persist()
			rf.mu.Unlock()
			rf.sendLog(to)
		}else {
			rf.mu.Unlock()
		}
			
	} else {
		rf.mu.Unlock()
	}
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

	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))

	rf.timer = time.NewTimer(randomTime())
	// initialize from state persisted before a crash
	rf.mu.Lock()
	rf.readPersist(persister.ReadRaftState())
	rf.mu.Unlock()

	rf.lastApplied = rf.lastIncludedIndex

	go func() {
		for !rf.killed {
			time.Sleep(time.Millisecond * 10)
			for ; rf.lastApplied < rf.commitIndex; {
				rf.mu.Lock()
				rf.lastApplied++
				app := ApplyMsg{}
				app.Index = rf.lastApplied
				app.Command = rf.log[app.Index - rf.lastIncludedIndex - 1].Command
				app.UseSnapshot = false
				rf.mu.Unlock()
				applyCh <- app
				//fmt.Println("commit", rf.me, app.Command.(int))
			}
			//rf.mu.Unlock()
		}
	}()
	go func() {
		for !rf.killed {
			<- rf.timer.C
			if rf.isLeader {
				continue
			}
			rf.mu.Lock()
			DPrintf("begin voted %d %d", me, rf.currentTerm)
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
						if len(rf.log) > 0 {
							args.LastLogIndex = rf.log[len(rf.log)-1].Index
							args.LastLogTerm = rf.log[len(rf.log)-1].Term
						}else {
							args.LastLogIndex = rf.lastIncludedIndex
							args.LastLogTerm = rf.lastIncludedTerm
						}
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
			timeout := randomTime()/10
			for i := 0; orgTerm == rf.currentTerm && i < round; i++ {
				if cnt * 2 + 1 >= n {
					break;
				}
				time.Sleep(timeout / time.Duration(round))
			}
			rf.mu.Lock()
			DPrintf("get vote %d %d %d", cnt, rf.me, rf.currentTerm)
			rf.timer.Reset(randomTime())
			if orgTerm != rf.currentTerm {
				rf.mu.Unlock()
				continue
			}
			if cnt * 2 + 1 >= n {
				rf.isLeader = true
				for i := 0; i < len(rf.peers); i++ {
					rf.nextIndex[i] = rf.lastIncludedIndex + len(rf.log) + 1
					rf.matchIndex[i] = 0
				}
				if false {
					fmt.Println("					change leader", me)
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

	

	return rf
}
