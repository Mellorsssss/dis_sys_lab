/* core impl of shardkv
   - event loop
   - rpc handler register/remove
   - configureation fetch/change
*/

package shardkv

import (
	"fmt"
	"sync/atomic"

	"6.824/raft"
	"6.824/shardctrler"
)

// MigrationCtx is the data of migrating
type MigrationCtx struct {
	Shard     int    // the shards to migrate
	Data      []byte // store data
	Mem       map[int]Response
	Gid       int // target id(valid when Sending is true)
	ConfigNum int
}

type MultiMigrationCtx struct {
	ShardData map[int][]byte // shard -> shard data
	ShardMem  map[int]map[string]Response
	Gid       int
	ConfigNum int
}

// type MigrationOp struct {
// 	Term    int // current term
// 	Gid     int // target gid
// 	Cfgnum  int
// 	Shard   int    // shard to move
// 	Sending bool   // true for sending shard
// 	Data    []byte // if sending is false, then the data of store
// 	Mem     map[int]Response
// }

type MultiMigrationOp struct {
	Term      int // current term
	Gid       int // target gid
	Cfgnum    int
	Sending   bool           // true for sending shard
	ShardData map[int][]byte // shard -> shard data
	ShardMem  map[int]map[string]Response
}

type GCShardOp struct {
	Shards []int
	CfgNum int
}

type ConfigOp struct {
	Cfg shardctrler.Config // config to apply
}

func (kv *ShardKV) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// loop process msg from applyCh, e.g, client-request/migration
func (kv *ShardKV) loop() {
	snapshotIndex := 0
	for appmsg := range kv.applyCh {

		if kv.killed() {
			return
		}

		if appmsg.CommandValid {
			switch appmsg.Command.(type) {
			case Op:
				// execute the cmd
				op := appmsg.Command.(Op)
				value, res := kv.execOp(op)

				// go func(appmsg raft.ApplyMsg, ctx KVRPCContext) {
				kv.mu.Lock()
				fn, ok := kv.sub[appmsg.CommandIndex]
				kv.mu.Unlock()

				if ok {
					fn(appmsg, KVRPCContext{value, res})
				}
				// }(appmsg, KVRPCContext{value, res})

			case MultiMigrationOp:
				kv.execMultiMigrate(appmsg.Command.(MultiMigrationOp))
				kv.mu.Lock()
				fn, ok := kv.sub[appmsg.CommandIndex]
				kv.mu.Unlock()
				if ok {
					fn(appmsg, KVRPCContext{"", 0})
				}
			case GCShardOp:
				kv.execGC(appmsg.Command.(GCShardOp))
			case ConfigOp:
				kv.execConfig(appmsg.Command.(ConfigOp))
			default:
				err := fmt.Sprintf("Wrong msg:%v", appmsg)
				panic(err)
			}

			if kv.maxraftstate != -1 && kv.persister.RaftStateSize() > int(float32(kv.maxraftstate)*RaftSizeThreshold) {
				kv.rf.Snapshot(appmsg.CommandIndex, kv.persist())
				snapshotIndex = raft.MaxInt(snapshotIndex, appmsg.CommandIndex)
			}
		} else if appmsg.SnapshotValid { // CondInstallSnapshot
			if kv.rf.CondInstallSnapshot(appmsg.SnapshotTerm, appmsg.SnapshotIndex, appmsg.Snapshot) {
				kv.readPersist(kv.persister.ReadSnapshot())
				snapshotIndex = raft.MaxInt(snapshotIndex, appmsg.SnapshotIndex)
			} else {
			}
		}
	}
}

// registerHandler register fn as handler of index
func (kv *ShardKV) registerHandler(index int, fn KVRPCHandler) bool {
	if _, ok := kv.sub[index]; ok {
		Error("chan has been registered")
		return false
	}

	kv.sub[index] = fn
	return true
}

// removeHandler remove the handler of index
func (kv *ShardKV) removeHandler(index int) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if _, ok := kv.sub[index]; !ok {
		return
	}

	delete(kv.sub, index)
}

// isDuplicated return true if op with sn has been executed before
func (kv *ShardKV) isDuplicatedOpInShard(shard int, id string, sn int64) (bool, Response) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	v, exist := kv.shards[shard].mem[id]
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

// memorizeOp memorize the op for duplicating detecting
func (kv *ShardKV) memorizeOpInShard(shard int, id string, sn int64, r Response) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if v, ok := kv.shards[shard].mem[id]; ok {
		if v.SerialNumber >= r.SerialNumber {
			panic(fmt.Sprintf("%v get old op to memorize in shard %v(%v < %v)", kv.shardkvInfo(), shard, v.SerialNumber, r.SerialNumber))
		}
	}
	kv.shards[shard].mem[id] = r
}

// hasValidShard return ture if kv can process shard
func (kv *ShardKV) hasValidShard(shard int) bool {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	_, ok := kv.shards[shard]
	if !ok {
		return false
	}

	return kv.shards_state[shard] == Valid
}

func (kv *ShardKV) shardInConfig(shard int) bool {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	return kv.cfg.Shards[shard] == kv.gid
}

// execOp executes op in statemachine
// if shard is not valid, just return WrongGroup
func (kv *ShardKV) execOp(op Op) (string, Res) {
	shard := key2shard(op.Key)
	if !kv.hasValidShard(shard) {
		DPrintf("%v doesn't have shard %v now(%v)", kv.shardkvInfo(), shard, kv.shardInfo())
		return "", WrongGroup
	}

	ok, v := kv.isDuplicatedOpInShard(shard, op.Id, op.SerialNumber)
	if ok {
		DPrintf("%v: op is duplicated: %v in shard %v", kv.shardkvInfo(), op.SerialNumber, shard)
		return v.Value, Succ
	}

	latest_ck_mem, ok := kv.shards[shard].mem[op.Id]
	latest_ck_sq := 0
	if ok {
		latest_ck_sq = int(latest_ck_mem.SerialNumber)
	}
	kv.mu.Lock()

	switch op.OpType {
	case GET:
		value := kv.shards[shard].store.Get(op.Key)
		kv.mu.Unlock()
		kv.memorizeOpInShard(shard, op.Id, op.SerialNumber, Response{op.SerialNumber, value, op.OpType})

		DPrintf("%v exec GET: \"%v\", %v(while %v)", kv.shardkvInfo(), op.Key, op.SerialNumber, latest_ck_sq)
		return value, Succ
	case PUT:
		kv.shards[shard].store.Put(op.Key, op.Value)
		kv.mu.Unlock()
		kv.memorizeOpInShard(shard, op.Id, op.SerialNumber, Response{op.SerialNumber, "", op.OpType})
		DPrintf("%v exec PUT: \"%v,%v\",%v(while %v)", kv.shardkvInfo(), op.Key, op.Value, op.SerialNumber, latest_ck_sq)
		return "", Succ
	case APPEND:
		kv.shards[shard].store.Append(op.Key, op.Value)
		kv.mu.Unlock()
		kv.memorizeOpInShard(shard, op.Id, op.SerialNumber, Response{op.SerialNumber, "", op.OpType})
		DPrintf("%v exec APPEND: \"%v,%v\",%v(while %v)", kv.shardkvInfo(), op.Key, op.Value, op.SerialNumber, latest_ck_sq)
		return "", Succ
	default:
		kv.mu.Unlock()
		panic("exec wrong type of op")
	}
}
