/* interface of shardkv */

package shardkv

import (
	"sync"
	"sync/atomic"

	"6.824/kvraft"
	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"6.824/shardctrler"
)

type Op struct {
	OpType       int // PUT/ APPEND/ GET
	Key          string
	Value        string
	SerialNumber int64
	Id           string
}

type Migration struct {
	Shard int    // the shard to migrate
	Data  []byte // store data
}

type Response struct {
	SerialNumber int64
	Value        string
	OpType       int
}

type KVRPCHandler func(raft.ApplyMsg, string)

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	ctrlers      []*labrpc.ClientEnd
	maxraftstate int                // snapshot if log grows this big
	ck           *shardctrler.Clerk // communicate with ctrlers

	// must hold lock
	gid       int
	cfg       shardctrler.Config     // latest config, fetch periodically
	shards    map[int]kvraft.KVStore // shard id -> data, all data from different shards
	dead      int32                  // true if shardkv is dead
	sub       map[int]KVRPCHandler   // msg_id -> handler
	clientMap map[string]Response    // ck_id -> latest response
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here
	if kv.killed() {
		reply.Err = ErrWrongLeader
		return
	}

	term, isLeader := kv.rf.GetState()
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	shard := key2shard(args.Key)
	kv.mu.Lock()
	if !kv.isInShardsUnlocked(shard) {
		DPrintf("leader %v,%v doesn't have shard %v(%v)", kv.me, kv.gid, shard, kv.shards)
		kv.mu.Unlock()
		reply.Err = ErrWrongGroup
		return
	}
	kv.mu.Unlock()

	ok, r := kv.isDuplicatedOp(args.Id, args.SerialNumber)
	if ok {
		DPrintf("leader %v Get but commit before by %v", kv.me, args.Id)
		reply.Err = OK
		reply.Value = r.Value
		return
	}

	// start the agree
	done := make(chan struct{}, 1)
	cmd := Op{
		OpType:       GET,
		Key:          args.Key,
		Value:        "",
		SerialNumber: args.SerialNumber,
		Id:           args.Id,
	}

	kv.mu.Lock()
	index, _, ok := kv.rf.Start(cmd)
	if !ok {
		reply.Err = ErrWrongLeader
		kv.mu.Unlock()
		return
	}

	kv.registerHandler(index, func(msg raft.ApplyMsg, value string) {
		newTerm, isStillLeader := kv.rf.GetState()
		if !isStillLeader || newTerm != term || msg.Command != cmd {
			reply.Err = ErrWrongLeader
			kv.removeHandler(index)
			done <- struct{}{}
			return
		}

		reply.Value = value
		reply.Err = OK
		kv.removeHandler(index)
		done <- struct{}{}
	})
	kv.mu.Unlock()
	// wait for the msg is applied

	<-done
	DPrintf("leader %v,%v Get \"%v\" : \"%v\" in term %v", kv.me, kv.gid, cmd.Key, reply.Value, term)
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
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

	shard := key2shard(args.Key)
	kv.mu.Lock()
	if !kv.isInShardsUnlocked(shard) {
		DPrintf("leader <%v, %v> doesn't have shard %v(%v)", kv.me, kv.gid, shard, kv.shards)
		reply.Err = ErrWrongGroup
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()

	DPrintf("leader %v PutAppend in term %v", kv.me, term)

	ok, _ := kv.isDuplicatedOp(args.Id, args.SerialNumber)
	if ok {
		DPrintf("leader %v Get but commit before by %v", kv.me, args.Id)
		reply.Err = OK
		return
	}

	// start the agree
	done := make(chan struct{}, 1)
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

	kv.mu.Lock()
	index, _, ok := kv.rf.Start(cmd)
	if !ok {
		reply.Err = ErrWrongLeader
		kv.mu.Unlock()
		return
	}

	kv.registerHandler(index, func(msg raft.ApplyMsg, _ string) {
		newTerm, isStillLeader := kv.rf.GetState()
		if !isStillLeader || newTerm != term || msg.Command != cmd {
			reply.Err = ErrWrongLeader
			kv.removeHandler(index)
			done <- struct{}{}
			return
		}

		reply.Err = OK
		kv.removeHandler(index)
		done <- struct{}{}
	})
	kv.mu.Unlock()
	// wait for the msg is applied

	<-done
	DPrintf("leader %v,%v PutAppend \"%v\" : \"%v\" in term %v", kv.me, kv.gid, cmd.Key, cmd.Value, term)
}

//
// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *ShardKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
	atomic.StoreInt32(&kv.dead, 1)
	DPrintf("server %v is killed", kv.me)
}

//
// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardctrler.
//
// pass ctrlers[] to shardctrler.MakeClerk() so you can send
// RPCs to the shardctrler.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use ctrlers[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.ctrlers = ctrlers

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	kv.ck = shardctrler.MakeClerk(kv.ctrlers) // never change during lifetime, used to communicate with ctrlers
	kv.cfg = kv.ck.Query(0)                   // fetch the frist config for recover
	kv.shards = make(map[int]kvraft.KVStore)

	kv.clientMap = make(map[string]Response)
	kv.sub = make(map[int]KVRPCHandler)

	go kv.fetchConfig() // periodically fetch latest config
	go kv.loop()
	return kv
}
