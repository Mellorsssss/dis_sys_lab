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
const ERROR = false
const INFO = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf("[DEBUG]"+format, a...)
	}
	return
}

func Info(format string, a ...interface{}) (n int, err error) {
	if INFO {
		log.Printf("[INFO]"+format, a...)
	}
	return
}

func Error(format string, a ...interface{}) (n int, err error) {
	if ERROR {
		log.Printf("[ERROR]"+format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	OpType       int
	Key          string
	Value        string
	SerialNumber int64
	Id           string
}

type Response struct {
	SerialNumber int64
	Value        string
	OpType       int
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
	//sub       map[chan<- raft.ApplyMsg]bool

	sub       map[Op]func(raft.ApplyMsg)
	clientMap map[string]Response
}

func (kv *KVServer) registerMsgListener(op Op, fn func(raft.ApplyMsg)) bool {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if _, ok := kv.sub[op]; ok {
		Error("chan has been registered")
		return false
	}

	kv.sub[op] = fn
	return true
}

func (kv *KVServer) cancelMsgListener(op Op) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if _, ok := kv.sub[op]; !ok {
		Error("chan has no sub")
		panic("chan has no sub")
	}

	delete(kv.sub, op)
}

// isExecuted return true if op with sn has been executed before
func (kv *KVServer) isExecuted(id string, sn int64) (bool, Response) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	v, exist := kv.clientMap[id]
	if !exist {
		return false, Response{}
	}

	if v.SerialNumber < sn {
		return false, Response{}
	} else if v.SerialNumber == sn {
		return true, v
	} else {
		return true, Response{} // for any before requests, just response anything
		// client must have handled the correct reponse before
	}
}

// memorizeOp latest op of client id
func (kv *KVServer) memorizeOp(id string, sn int64, r Response) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	v, exist := kv.clientMap[id]
	if exist {
		if v.SerialNumber != sn-1 {
			Error("cilent %v send op out of order: before:%v, cur:%v ", id, v.SerialNumber, sn)
			panic("client send op out of order")
		}
	}
	kv.clientMap[id] = r
}

// execOp executes op in statemachine
func (kv *KVServer) execOp(op Op) {
	ok, _ := kv.isExecuted(op.Id, op.SerialNumber)
	if ok {
		return
	}
	kv.mu.Lock()
	if op.OpType == GET {
		value := kv.store.Get(op.Key)
		kv.mu.Unlock()
		kv.memorizeOp(op.Id, op.SerialNumber, Response{op.SerialNumber, value, op.OpType})
		return
	} else if op.OpType == PUT {
		kv.store.Put(op.Key, op.Value)
		kv.mu.Unlock()
		kv.memorizeOp(op.Id, op.SerialNumber, Response{op.SerialNumber, "", op.OpType})
	} else if op.OpType == APPEND {
		kv.store.Append(op.Key, op.Value)
		kv.mu.Unlock()
		kv.memorizeOp(op.Id, op.SerialNumber, Response{op.SerialNumber, "", op.OpType})
	} else {
		kv.mu.Unlock()
		panic("exec wrong type of op")
	}
}

// broadcastMsg process msg from applyCh
// and broadcast to subs
func (kv *KVServer) broadcastMsg() {
	for appmsg := range kv.applyCh {
		if kv.killed() {
			return
		}

		if appmsg.CommandValid {
			// execute the cmd
			op := appmsg.Command.(Op)
			kv.execOp(op)

			// broadcast to all the subscribers
			kv.mu.Lock()
			fn, ok := kv.sub[op]
			kv.mu.Unlock()

			if ok {
				fn(appmsg) // execute the callback function
			}
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

	ok, r := kv.isExecuted(args.Id, args.SerialNumber)
	if ok {
		DPrintf("leader %v Get but commit before by %v", kv.me, args.Id)
		reply.Err = OK
		reply.Value = r.Value
		return
	}

	// start the agree
	done := make(chan struct{})
	cmd := Op{
		OpType:       GET,
		Key:          args.Key,
		Value:        "",
		SerialNumber: args.SerialNumber,
		Id:           args.Id,
	}
	kv.registerMsgListener(cmd, func(msg raft.ApplyMsg) {
		newTerm, isStillLeader := kv.rf.GetState()
		if !isStillLeader || newTerm != term || msg.Command != cmd {
			reply.Err = ErrWrongLeader
			kv.cancelMsgListener(cmd)
			done <- struct{}{}
			return
		}

		reply.Value = kv.store.Get(args.Key)
		reply.Err = OK
		kv.cancelMsgListener(cmd)
		done <- struct{}{}
		return
	})

	_, _, ok = kv.rf.Start(cmd)
	if !ok {
		reply.Err = ErrWrongLeader
		kv.cancelMsgListener(cmd)
		return
	}
	// wait for the msg is applied

	<-done
	DPrintf("leader %v Get \"%v\" : \"%v\" in term %v", kv.me, cmd.Key, reply.Value, term)
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

	ok, _ := kv.isExecuted(args.Id, args.SerialNumber)
	if ok {
		DPrintf("leader %v Get but commit before by %v", kv.me, args.Id)
		reply.Err = OK
		return
	}

	// start the agree
	done := make(chan struct{})
	var optype int
	if args.Op == "Put" {
		optype = PUT
	} else if args.Op == "Append" {
		optype = APPEND
	} else {
		panic("unkown op type")
	}
	cmd := Op{
		OpType:       optype,
		Key:          args.Key,
		Value:        args.Value,
		SerialNumber: args.SerialNumber,
		Id:           args.Id,
	}
	kv.registerMsgListener(cmd, func(msg raft.ApplyMsg) {
		newTerm, isStillLeader := kv.rf.GetState()
		if !isStillLeader || newTerm != term || msg.Command != cmd {
			reply.Err = ErrWrongLeader
			kv.cancelMsgListener(cmd)
			done <- struct{}{}
			return
		}

		reply.Err = OK
		kv.cancelMsgListener(cmd)
		done <- struct{}{}
		return
	})

	_, _, ok = kv.rf.Start(cmd)
	if !ok {
		reply.Err = ErrWrongLeader
		kv.cancelMsgListener(cmd)
		return
	}
	// wait for the msg is applied

	<-done
	DPrintf("leader %v PutAppend \"%v\" : \"%v\" in term %v", kv.me, cmd.Key, cmd.Value, term)
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
	kv.sub = make(map[Op]func(raft.ApplyMsg))
	kv.clientMap = make(map[string]Response)

	// long-running goroutine
	go kv.broadcastMsg()
	return kv
}
