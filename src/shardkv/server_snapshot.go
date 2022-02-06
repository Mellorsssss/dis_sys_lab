package shardkv

import (
	"bytes"
	"fmt"

	"6.824/kvraft"
	"6.824/labgob"
	"6.824/raft"
)

/* snapshot library */

type SnapShotData struct {
	ShardData  map[int][]byte
	Mem        map[int]map[string]Response
	ShardState map[int]int
}

func (kv *ShardKV) dumpShardDataUnlocked() map[int][]byte {
	data := make(map[int][]byte) // shard id -> shard store serialized data
	for shard, v := range kv.shards {
		data[shard] = cloneBytes(v.store.Data())
	}

	return data
}

func (kv *ShardKV) dumpMemUnlocked() map[int]map[string]Response {
	data := make(map[int]map[string]Response) // shard id -> shard mem serialized data
	for shard, v := range kv.shards {
		data[shard] = copySingleShardMem(v.mem)
	}

	return data
}

// loadShardUnlocked load snapshot data into ShardStore
func (kv *ShardKV) loadShardUnlocked(sn *SnapShotData) {
	if len(sn.Mem) != len(sn.ShardData) {
		err := fmt.Sprintf("fail to load shard data of %v", kv.shardkvInfo())
		panic(err)
	}

	kv.shards_state = sn.ShardState
	// re-send the shards not be gc
	for shard := range kv.shards_state {
		if kv.shards_state[shard] == Pushing {
			kv.shards_state[shard] = RePushing
		}
	}

	kv.shards = make(map[int]*ShardStore)
	for shard, sharddata := range sn.ShardData {
		kv.shards[shard] = &ShardStore{}
		kv.shards[shard].store = kvraft.MakeMapStore()
		kv.shards[shard].store.Load(sharddata)
		kv.shards[shard].mem = sn.Mem[shard]
	}
}

func (kv *ShardKV) persist() []byte {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	ss := SnapShotData{
		ShardData:  kv.dumpShardDataUnlocked(),
		Mem:        kv.dumpMemUnlocked(),
		ShardState: kv.shards_state,
	}

	sb := new(bytes.Buffer)
	senc := labgob.NewEncoder(sb)
	err := senc.Encode(ss)
	if err != nil {
		Error("server %v persist fails.", kv.me)
		panic(err)
	}
	return sb.Bytes()
}

func (kv *ShardKV) readPersist(data []byte) {
	Error("server %v begins to read persist", kv.me)
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if data == nil || len(data) < 1 {
		kv.shards_state = make(map[int]int)
		return
	} else {
		sbuffer := bytes.NewBuffer(data)
		var rfsnapshot raft.SnapShotData
		var snapshot SnapShotData
		rdec := labgob.NewDecoder(sbuffer)
		if rdec.Decode(&rfsnapshot) != nil {
			panic("Decode raft snapshot error")
		}

		sdec := labgob.NewDecoder(bytes.NewBuffer(rfsnapshot.Data))
		if sdec.Decode(&snapshot) != nil {
			Error("snapshot of %v decodes fail", kv.me)
			panic("snapshot decodes fails.")
		}

		kv.loadShardUnlocked(&snapshot)
	}
}
