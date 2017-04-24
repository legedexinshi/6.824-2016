package raftkv

import (
	"encoding/gob"
	"labrpc"
	"log"
	"raft"
	"fmt"
	"time"
	"bytes"
	"sync"
)

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}


type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Key   string
	Value string
	Op    string
	SeqNum 	int
}

type RaftKV struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	persister *raft.Persister

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	Map 	map[string]string
	commitMap	map[int]int
	dealMap 	map[int]bool
	getMap 		map[int]string
	lastSnapIndex 	int
}

func (kv *RaftKV) Apply(index int, seqNum int) (string, bool) {
	if index > len(kv.commitMap) {
		return "", false
	}
	kv.mu.Lock()
	defer kv.mu.Unlock()
	debug := 0
	if debug > 0 {
		fmt.Println(index, len(kv.commitMap))
	}
	if index > len(kv.commitMap) || kv.commitMap[index] != seqNum {
		return "", false
	}
	ans := kv.getMap[seqNum]
	kv.getMap[seqNum] = ""
	return ans, true
}
 
func (kv *RaftKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	new_args := Op{}
	new_args.Key = args.Key
	new_args.Op = "Get"
	new_args.SeqNum = args.SeqNum
	index, term, isLeader := kv.rf.Start(new_args)
	reply.WrongLeader = !isLeader
	if isLeader {
		for {
			time.Sleep(time.Millisecond * 1)
			ans, ok := kv.Apply(index, args.SeqNum)
			if ok {
				reply.Value = ans
				return
			}
			_term, _isLeader := kv.rf.GetState()
			if _isLeader == false || _term != term {
				reply.WrongLeader = true
				return 
			}
		}
		//fmt.Println("get leader ", kv.me)
	}
}

func (kv *RaftKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	new_args := Op{}
	new_args.Key = args.Key
	new_args.Value = args.Value
	new_args.Op = args.Op
	new_args.SeqNum = args.SeqNum
	index, term, isLeader := kv.rf.Start(new_args)
	//fmt.Println("commit ", kv.me, index, isLeader)
	reply.WrongLeader = !isLeader
	if isLeader {
		for {
			//fmt.Println("loop", index)
			time.Sleep(time.Millisecond * 1)
			_, ok := kv.Apply(index, args.SeqNum)
			if ok {
				return
			}
			_term, _isLeader := kv.rf.GetState()
			if _isLeader == false || _term != term {
				reply.WrongLeader = true
				return 
			}
		}
		//fmt.Println("get leader ", kv.me)
	}
}

//
// the tester calls Kill() when a RaftKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *RaftKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *RaftKV) readSnapShot(data []byte) {
	 r := bytes.NewBuffer(data)
	 d := gob.NewDecoder(r)
	 d.Decode(&kv.Map)
	 d.Decode(&kv.dealMap)
	 d.Decode(&kv.getMap)
	 d.Decode(&kv.commitMap)
	 d.Decode(&kv.lastSnapIndex)
}

func (kv *RaftKV) saveSnapShot() {
	 w := new(bytes.Buffer)
	 e := gob.NewEncoder(w)
	 e.Encode(kv.Map)
	 e.Encode(kv.dealMap)
	 e.Encode(kv.getMap)
	 e.Encode(kv.commitMap)
	 e.Encode(kv.lastSnapIndex)
	 data := w.Bytes()
	 kv.persister.SaveSnapshot(data)
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots with persister.SaveSnapshot(),
// and Raft should save its state (including log) with persister.SaveRaftState().
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *RaftKV {
	// call gob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	gob.Register(Op{})
	kv := new(RaftKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.persister = persister
	// Your initialization code here.

	kv.Map = make(map[string]string)
	kv.dealMap = make(map[int]bool)
	kv.getMap = make(map[int]string)
	kv.commitMap = make(map[int]int)

	kv.readSnapShot(persister.ReadSnapshot())

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	go func() {
		for {
			msg := <- kv.applyCh
			kv.mu.Lock()
			if msg.UseSnapshot {
				kv.readSnapShot(persister.ReadSnapshot())
				kv.mu.Unlock()
				continue
			}
			command := msg.Command.(Op)
			kv.commitMap[msg.Index] = command.SeqNum

			args := command
			seq := args.SeqNum
			if args.Op == "Get" {
				kv.getMap[seq] = kv.Map[args.Key]
			}
			if kv.dealMap[seq] == true {
				kv.mu.Unlock()
				continue
			}
			kv.dealMap[seq] = true
			if args.Op == "Put" {
				kv.Map[args.Key] = args.Value
			}else if args.Op == "Append" {
				kv.Map[args.Key] += args.Value
			}
			kv.mu.Unlock()
		}
	}()
	if maxraftstate != -1 {
		go func() {
			for {
				time.Sleep(time.Millisecond * 10)
				if kv.persister.RaftStateSize() > maxraftstate {
					kv.mu.Lock()
					DPrintf("delete", kv.me)
					DPrintf("", kv.persister.RaftStateSize(), maxraftstate)
					kv.saveSnapShot()
					kv.lastSnapIndex = len(kv.commitMap)
					kv.rf.DiscardLogBefore(kv.lastSnapIndex)
					DPrintf("", kv.persister.RaftStateSize(), maxraftstate)
					kv.mu.Unlock()
				}
			}
		}()
	}
	return kv
}
