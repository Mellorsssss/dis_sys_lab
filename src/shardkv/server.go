/* interface of shardkv */

package shardkv

import (
	"sync"
	"sync/atomic"

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

type Response struct {
	SerialNumber int64
	Value        string
	OpType       int
}

type KVRPCContext struct {
	value string
	Err   Res
}
type KVRPCHandler func(raft.ApplyMsg, KVRPCContext)

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
	gid          int
	cfg          shardctrler.Config   // current config, fetch periodically
	shards       map[int]*ShardStore  // shard id -> data, all data from different shards
	shards_state map[int]int          // shard id -> Valid/Pushing/GC, only Valid state can serve rq
	dead         int32                // true if shardkv is dead
	sub          map[int]KVRPCHandler // msg_id -> handler
	persister    *raft.Persister
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
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
	// four cases:
	// | shard in config | has valid shard | solve
	// | true            | true            | full check
	// | true            | false           | add op, not check(since no shard)
	// | false           | true            | full check(still as normal shard)
	// | false           | false           | drop
	if !kv.shardInConfig(shard) && !kv.hasValidShard(shard) {
		reply.Err = ErrWrongGroup
		DPrintf("leader %v,%v doesn't have shard %v", kv.me, kv.gid, shard)
		return
	}

	if kv.hasValidShard(shard) {
		ok, r := kv.isDuplicatedOpInShard(shard, args.Id, args.SerialNumber)
		if ok {
			DPrintf("leader %v Get in shard %v but commit before by %v", kv.me, shard, args.Id)
			reply.Err = OK
			reply.Value = r.Value
			return
		}
	}

	// start the agree
	done := make(chan bool, 1)
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

	kv.registerHandler(index, func(msg raft.ApplyMsg, ctx KVRPCContext) {
		newTerm, isStillLeader := kv.rf.GetState()
		if !isStillLeader || newTerm != term || msg.Command != cmd {
			reply.Err = ErrWrongLeader
			kv.removeHandler(index)
			done <- false
			return
		}

		if ctx.Err == Succ {
			reply.Value = ctx.value
			reply.Err = OK
			kv.removeHandler(index)
			done <- true
		} else if ctx.Err == WrongGroup {
			reply.Err = ErrWrongGroup
			kv.removeHandler(index)
			done <- false
		} else {
			panic("wrong err")
		}
	})
	kv.mu.Unlock()
	// wait for the msg is applied

	res := <-done
	if res {
		DPrintf("%v Get \"%v\" : \"%v\" in term %v", kv.shardkvInfo(), cmd.Key, reply.Value, term)
	}
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
		DPrintf("%v is not leader in term %v", kv.shardkvInfo(), term)
		reply.Err = ErrWrongLeader
		return
	}

	shard := key2shard(args.Key)
	// four cases:
	// | shard in config | has valid shard | solve
	// | true            | true            | full check
	// | true            | false           | add op, not check(since no shard)
	// | false           | true            | full check(still as normal shard)
	// | false           | false           | drop
	if !kv.shardInConfig(shard) && !kv.hasValidShard(shard) {
		reply.Err = ErrWrongGroup
		return
	}

	if kv.hasValidShard(shard) {
		ok, _ := kv.isDuplicatedOpInShard(shard, args.Id, args.SerialNumber)
		if ok {
			DPrintf("leader %v Get in shard %v but commit before by %v", kv.me, shard, args.Id)
			reply.Err = OK
			return
		}
	}

	// start the agree
	done := make(chan bool, 1)
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

	kv.registerHandler(index, func(msg raft.ApplyMsg, ctx KVRPCContext) {
		newTerm, isStillLeader := kv.rf.GetState()
		if !isStillLeader || newTerm != term || msg.Command != cmd {
			reply.Err = ErrWrongLeader
			kv.removeHandler(index)
			done <- false
			return
		}

		if ctx.Err == Succ {
			reply.Err = OK
			kv.removeHandler(index)
			done <- true
		} else if ctx.Err == WrongGroup {
			reply.Err = ErrWrongGroup
			kv.removeHandler(index)
			done <- false
		} else {
			panic("wrong err")
		}
	})
	kv.mu.Unlock()
	// wait for the msg is applied

	res := <-done
	if res {
		DPrintf("%v PutAppend \"%v\" : \"%v\" in term %v", kv.shardkvInfo(), cmd.Key, cmd.Value, term)
	}
}

func (kv *ShardKV) MultiMigrate(args *MultiMigrateArgs, reply *MultiMigrateReply) {
	if kv.killed() {
		reply.Err = ErrWrongLeader
		return
	}

	term, isLeader := kv.rf.GetState()
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	// duplicated shard transition
	has_new_shard := false
	kv.mu.Lock()
	for shard := range args.ShardData {
		_, ok := kv.shards[shard]
		if !ok || kv.shards_state[shard] != Valid { // no such valid shard
			has_new_shard = true
			break
		}
	}

	if !has_new_shard {
		if args.CfgNum < kv.cfg.Num { // detect old config
			reply.Err = ErrOldShard
			kv.mu.Unlock()
			return
		} else {
			reply.Err = OK
			kv.mu.Unlock()
			return
		}
	}

	// start the agree
	done := make(chan struct{}, 1)
	index, _, ok := kv.rf.Start(MultiMigrationOp{term, kv.gid, args.CfgNum, false, args.ShardData, args.ShardMem})
	if !ok {
		reply.Err = ErrWrongLeader
		kv.mu.Unlock()
		return
	}

	kv.registerHandler(index, func(msg raft.ApplyMsg, ctx KVRPCContext) {
		op := msg.Command.(MultiMigrationOp)
		newTerm, isStillLeader := kv.rf.GetState()
		if !isStillLeader || newTerm != term {
			reply.Err = ErrOldShard
			kv.removeHandler(index)
			done <- struct{}{}
			return
		}
		kv.mu.Lock()
		cur_cfg_num := kv.cfg.Num
		kv.mu.Unlock()
		if op.Cfgnum < cur_cfg_num {
			reply.Err = ErrOldShard
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
	all_shards := []int{}
	for shard := range args.ShardData {
		all_shards = append(all_shards, shard)
	}

	DPrintf("server <%v, %v> receive shards %v succ", kv.me, kv.gid, all_shards)
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
	DPrintf("server %v,%v is killed", kv.me, kv.gid)
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
	// labgob.Register(MigrationOp{})
	labgob.Register(MultiMigrationOp{})
	labgob.Register(GCShardOp{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.ctrlers = ctrlers
	kv.persister = persister

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	kv.ck = shardctrler.MakeClerk(kv.ctrlers) // never change during lifetime, used to communicate with ctrlers
	kv.cfg = kv.ck.Query(0)

	kv.sub = make(map[int]KVRPCHandler)
	kv.readPersist(kv.persister.ReadSnapshot())

	go kv.fetchConfigLoop() // periodically fetch latest config
	go kv.loop()
	DPrintf("server %v, %v restart", kv.me, kv.gid)
	return kv
}
