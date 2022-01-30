/* core impl of shardkv
   - event loop
   - rpc handler register/remove
   - configureation fetch/change
*/

package shardkv

import (
	"fmt"
	"sync/atomic"
	"time"

	"6.824/kvraft"
	"6.824/raft"
)

func (kv *ShardKV) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// loop process msg from applyCh, e.g, client-request/migration
func (kv *ShardKV) loop() {
	for appmsg := range kv.applyCh {
		if kv.killed() {
			return
		}

		if appmsg.CommandValid {
			switch appmsg.Command.(type) {
			case Op:
				// execute the cmd
				op := appmsg.Command.(Op)
				value := kv.execOp(op)

				// broadcast to all the subscribers(handlers)
				// non-blocking
				go func(appmsg raft.ApplyMsg, value string) {
					kv.mu.Lock()
					fn, ok := kv.sub[appmsg.CommandIndex]
					kv.mu.Unlock()

					if ok {
						fn(appmsg, value)
					}
				}(appmsg, value)
			case Migration:
				continue
			default:
				err := fmt.Sprintf("Wrong msg:%v", appmsg)
				panic(err)
			}

		}

	}
}

// fetchConfig fetches the latest config
// and changes the shard
func (kv *ShardKV) fetchConfig() {
	// update the config atomically
	for !kv.killed() {
		kv.mu.Lock()
		ncfg := kv.ck.Query(-1)
		if kv.cfg.Num == 0 && ncfg.Num != 0 {
			DPrintf("server %v get the first config", kv.me)
			kv.cfg = ncfg
			// add initial store without waiting for it

			for shard, gid := range kv.cfg.Shards {
				if gid == kv.gid {
					_, ok := kv.shards[shard]
					if !ok {
						DPrintf("server <%v, %v> get shard %v", kv.me, kv.gid, shard)
						kv.shards[shard] = kvraft.MakeMapStore()
					}
				}
			}
			kv.mu.Unlock()
			time.Sleep(ServerConfigUpdatePeriod * time.Millisecond)
		} else {
			kv.cfg = ncfg
			// todo: migrate the shard(only the leader)
			for shard := range kv.shards {
				if shard >= len(kv.cfg.Shards) {
					err := fmt.Sprintf("server %v has shard which range out:%v > %v", kv.me, shard, len(kv.cfg.Shards))
					panic(err)
				}

				if kv.cfg.Shards[shard] != kv.gid {
					DPrintf("server %v, %v removes shard %v", kv.me, kv.gid, shard)
					delete(kv.shards, shard)
				}
			}

			kv.mu.Unlock()
			time.Sleep(ServerConfigUpdatePeriod * time.Millisecond)
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
func (kv *ShardKV) isDuplicatedOp(id string, sn int64) (bool, Response) {
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

// memorizeOp memorize the op for duplicating detecting
func (kv *ShardKV) memorizeOp(id string, sn int64, r Response) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	kv.clientMap[id] = r
}

// isInShards return true if shard is in kv's shards
func (kv *ShardKV) isInShardsUnlocked(shard int) bool {
	_, ok := kv.shards[shard]
	return ok
}

// execOp executes op in statemachine
func (kv *ShardKV) execOp(op Op) string {
	ok, v := kv.isDuplicatedOp(op.Id, op.SerialNumber)
	if ok {
		return v.Value
	}

	shard := key2shard(op.Key)
	kv.mu.Lock()
	if !kv.isInShardsUnlocked(shard) { // key is not in current shard
		DPrintf("server %v: %v is not in %v's shard", kv.me, op.Key, kv.gid)
		kv.mu.Unlock()
		return ""
	}

	switch op.OpType {
	case GET:
		value := kv.shards[shard].Get(op.Key)
		kv.mu.Unlock()
		kv.memorizeOp(op.Id, op.SerialNumber, Response{op.SerialNumber, value, op.OpType})
		return value
	case PUT:
		kv.shards[shard].Put(op.Key, op.Value)
		kv.mu.Unlock()
		kv.memorizeOp(op.Id, op.SerialNumber, Response{op.SerialNumber, "", op.OpType})
		return ""
	case APPEND:
		kv.shards[shard].Append(op.Key, op.Value)
		kv.mu.Unlock()
		kv.memorizeOp(op.Id, op.SerialNumber, Response{op.SerialNumber, "", op.OpType})
		return ""
	default:
		kv.mu.Unlock()
		panic("exec wrong type of op")
	}
}

func (kv *ShardKV) execMigrate(m Migration) {
	// todo: migrate for every server
}
