package raftkv

import "labrpc"
import "crypto/rand"
import "math/big"
import "sync"
import "fmt"


type Clerk struct {
	servers []*labrpc.ClientEnd
	seqNum 	int
	mu 		sync.Mutex
	lastIdx 	int
	// You will have to modify this struct.
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("RaftKV.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {
	ck.mu.Lock()
	//seq := ck.seqNum
	seq := int(nrand())
	ck.seqNum++
	ck.mu.Unlock()
	debug := 0
	if debug > 0 {
		fmt.Println("								Get ", key)
	}
	// You will have to modify this function.
	i := ck.lastIdx
	for {
		args := GetArgs{}
		args.SeqNum = seq
		args.Key = key
		reply := GetReply{}
		i++
		i %= len(ck.servers)
		//fmt.Println("Start ", i)
		ok := ck.servers[i].Call("RaftKV.Get", &args, &reply)
		//fmt.Println("End ", i)
		if ok == true && reply.WrongLeader == false {
			//fmt.Println("quit get", i, key, reply.Value, seq)
			ck.lastIdx = i
			return reply.Value
		}
	}
	return ""
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("RaftKV.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	ck.mu.Lock()
	seq := int(nrand())
	//seq := ck.seqNum
	ck.seqNum++
	ck.mu.Unlock()
	debug := 0
	if debug > 0 {
		fmt.Println("										PutAppend ", key, value, op)
	}
	i := ck.lastIdx
	for {
		args := PutAppendArgs{}
		args.SeqNum = seq
		args.Key = key
		args.Value = value
		args.Op = op
		reply := PutAppendReply{}
		i+=2
		i %= len(ck.servers)
		//fmt.Println("Start ", i)
		ok := ck.servers[i].Call("RaftKV.PutAppend", &args, &reply)
		//fmt.Println("End ", i)
		if ok == true && reply.WrongLeader == false {
			//fmt.Println("quit putappend", i, op, key, value)
			ck.lastIdx = i
			return
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
