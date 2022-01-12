package kvraft

import (
	"log"
	"sync"
	"sync/atomic"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
)

const Debug = false
const ERROR = true

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

func Error(format string, a ...interface{}) (n int, err error) {
	if ERROR {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	OpType int
	Key    string
	Value  string
}

type KVStore interface {
	Get(string) string
	Put(string, string)
	Append(string, string)
	Data() []byte
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	store KVStore
	sub   map[chan<- raft.ApplyMsg]bool
}

func (kv *KVServer) registerMsgListener(ch chan<- raft.ApplyMsg) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if _, ok := kv.sub[ch]; ok {
		Error("chan has been registered")
		panic("chan has been registered")
	}

	kv.sub[ch] = true
}

func (kv *KVServer) cancelMsgListener(ch chan<- raft.ApplyMsg) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if _, ok := kv.sub[ch]; !ok {
		Error("chan has no sub")
		panic("chan has no sub")
	}

	close(ch)
	delete(kv.sub, ch)
}

func (kv *KVServer) broadcastMsg() {
	for appmsg := range kv.applyCh {
		if appmsg.CommandValid {
			kv.mu.Lock()
			for ch := range kv.sub {
				select {
				case ch <- appmsg:
					continue
				default:
					continue
				}
			}
			kv.mu.Unlock()
		}
		// TODO: deal with snapshot msg
	}
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	if kv.killed() {
		reply.Err = ErrWrongLeader
		return
	}

	term, isLeader := kv.rf.GetState()
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	DPrintf("leader %v Get in term %v", kv.me, term)

	// start the agree
	ch := make(chan raft.ApplyMsg, MsgChanLen)
	cmd := Op{
		OpType: GET,
		Key:    args.Key,
	}
	kv.registerMsgListener(ch)
	index, term, ok := kv.rf.Start(cmd)
	if !ok {
		reply.Err = ErrWrongLeader
		kv.cancelMsgListener(ch)
		return
	}

	// check if cmd is commited
	for msg := range ch {
		if msg.CommandIndex == index {
			newTerm, isStillLeader := kv.rf.GetState()
			if !isStillLeader || newTerm != term || msg.Command != cmd {
				reply.Err = ErrWrongLeader
				kv.cancelMsgListener(ch)
				return
			}

			reply.Value = kv.store.Get(args.Key)
			reply.Err = OK
			kv.cancelMsgListener(ch)
			return
		} else if msg.CommandIndex > index {
			Error("server %v miss the msg", kv.me)
			panic("server miss the msg")
		}
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	if kv.killed() {
		reply.Err = ErrWrongLeader
		DPrintf("server %v is killed", kv.me)
		return
	}

	term, isLeader := kv.rf.GetState()
	if !isLeader {
		DPrintf("sever %v is not leader in term %v", kv.me, term)
		reply.Err = ErrWrongLeader
		return
	}
	DPrintf("leader %v PutAppend in term %v", kv.me, term)

	// start the agree
	ch := make(chan raft.ApplyMsg, MsgChanLen)
	var optype int
	if args.Op == "Put" {
		optype = PUT
	} else if args.Op == "Append" {
		optype = APPEND
	} else {
		panic("unkown op type")
	}
	cmd := Op{
		OpType: optype,
		Key:    args.Key,
		Value:  args.Value,
	}
	kv.registerMsgListener(ch)
	index, term, ok := kv.rf.Start(cmd)
	if !ok {
		reply.Err = ErrWrongLeader
		DPrintf("server %v fail to start the cmd", kv.me)
		kv.cancelMsgListener(ch)
		return
	}

	// check if cmd is commited
	for msg := range ch {
		if msg.CommandIndex == index {
			newTerm, isStillLeader := kv.rf.GetState()
			if !isStillLeader || newTerm != term {
				DPrintf("server %v is not leader any more", kv.me)
				reply.Err = ErrWrongLeader
				kv.cancelMsgListener(ch)
				return
			}

			if msg.Command != cmd {
				DPrintf("server %v get wrong cmd %v compared with cmd %v ", kv.me, msg.Command, cmd)
				reply.Err = ErrWrongLeader
				kv.cancelMsgListener(ch)
				return
			}
			if optype == PUT {
				kv.store.Put(args.Key, args.Value)
			} else if optype == APPEND {
				kv.store.Append(args.Key, args.Value)
			} else {
				panic("unknown op")
			}
			reply.Err = OK
			kv.cancelMsgListener(ch)
			return
		} else if msg.CommandIndex > index {
			Error("server %v miss the msg", kv.me)
			panic("server miss the msg")
		}
	}
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.store = MakeMapStore()
	kv.sub = make(map[chan<- raft.ApplyMsg]bool)

	// long-running goroutine
	go kv.broadcastMsg()
	return kv
}
