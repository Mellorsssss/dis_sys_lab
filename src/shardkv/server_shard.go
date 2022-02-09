package shardkv

import (
	"fmt"
	"time"

	"6.824/kvraft"
	"6.824/shardctrler"
)

// ShardStore contains the data and duplicate detection
type ShardStore struct {
	store kvraft.KVStore      // key-value pairs
	mem   map[string]Response // ck id -> latest response
}

// query the Config 1 to init shards
func (kv *ShardKV) initShards() {
	cfg := kv.ck.Query(1)
	for ; cfg.Num != 1; cfg = kv.ck.Query(1) {
		time.Sleep(100 * time.Millisecond)
	}

	kv.mu.Lock()
	kv.applyConfigUnlocked(cfg)
	kv.mu.Unlock()
}

// fetchConfig fetches the latest config and changes the shard
// for the first config, just add the empty store;
// for the later config change, remove the leaving shards
// anytime, if shards are ready, change kv.cfg to latest config
// and if ck's rq's Num is larger than kv.cfg, don't serve it(todo: optimize)
func (kv *ShardKV) fetchConfigLoop() {
	// if no shards(means no snapshot), query the first config
	if kv.cfg.Num == 0 {
		kv.initShards()
	}

	// update the config atomically
	// start the migrate if necessary
	for !kv.killed() {
		kv.mu.Lock()
		ccfg := kv.cfg

		if ncfg, ok := kv.exsitConfig(ccfg.Num + 1); ok { // exsit new config, start migrate
			// move all the leaving shards
			shards_to_move := []int{}
			for shard := range kv.shards {
				if shard >= len(kv.cfg.Shards) {
					err := fmt.Sprintf("server %v has shard which range out:%v > %v", kv.me, shard, len(kv.cfg.Shards))
					panic(err)
				}

				if ncfg.Shards[shard] != kv.gid && kv.shards_state[shard] != Pushing { // only valid shard or repushing shard should be pushed
					shards_to_move = append(shards_to_move, shard)
				}
			}

			if len(shards_to_move) > 1 {
				kv.removeMultiShardUnlocked(shards_to_move, ncfg.Shards[shards_to_move[0]], ncfg.Num)
			}

			kv.cfg = ncfg // change to newst config
			kv.mu.Unlock()
			time.Sleep(ServerConfigUpdatePeriod * time.Millisecond)
		}
	}
}

func (kv *ShardKV) removeMultiShardUnlocked(shards []int, gid, cfgnum int) {
	if kv.killed() {
		return
	}

	term, isLeader := kv.rf.GetState()
	if !isLeader {
		return
	}

	// place-holder args, fill them when executing the op
	shards_map := make(map[int][]byte)
	shards_mem := make(map[int]map[string]Response)
	for _, v := range shards {
		shards_map[v] = nil
		shards_mem[v] = nil
	}

	kv.rf.Start(MultiMigrationOp{term, gid, cfgnum, true, shards_map, shards_mem})
	DPrintf("leader %v, %v start to remove shards %v", kv.me, kv.gid, shards)
}

func (kv *ShardKV) gcMultiShardUnlocked(shards []int) {
	if kv.killed() {
		return
	}

	_, isLeader := kv.rf.GetState()
	if !isLeader {
		return
	}

	kv.rf.Start(GCShardOp{Shards: shards})
	DPrintf("leader %v start to gc shards %v", kv.shardkvInfo(), shards)
}

func (kv *ShardKV) applyConfigUnlocked(cfg shardctrler.Config) {
	if kv.killed() {
		return
	}

	_, isLeader := kv.rf.GetState()
	if !isLeader {
		return
	}

	kv.rf.Start(ConfigOp{cfg})
	DPrintf("leader %v start to apply config %v", kv.shardkvInfo(), cfg.Num)
}

func copyShardData(src map[int][]byte) map[int][]byte {
	dst := make(map[int][]byte)
	for shard, data := range src {
		ndata := make([]byte, len(data))
		copy(ndata, data)
		dst[shard] = ndata
	}

	return dst
}

func copyShardMem(src map[int]map[string]Response) map[int]map[string]Response {
	dst := make(map[int]map[string]Response)
	for shard, data := range src {
		dst[shard] = make(map[string]Response)
		for ckid, r := range data {
			dst[shard][ckid] = r
		}
	}

	return dst
}

func copySingleShardMem(src map[string]Response) map[string]Response {
	dst := make(map[string]Response)
	for ckid, r := range src {
		dst[ckid] = r
	}
	return dst
}

// execMigrate keep migrating a shard until success
func (kv *ShardKV) execMultiMigrate(m MultiMigrationOp) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if m.Sending {
		copysharddata := copyShardData(m.ShardData)
		copyshardmem := copyShardMem(m.ShardMem)

		shards_to_send := []int{}
		// if the shard should still be sending
		for shard := range copysharddata {
			_, ok := kv.shards[shard]
			if !ok {
				delete(copysharddata, shard)
				delete(copyshardmem, shard)
				continue
			}

			shards_to_send = append(shards_to_send, shard)
			kv.shards_state[shard] = Pushing
			goaldata := kv.shards[shard].store.Data()
			newdata := make([]byte, len(goaldata))
			copy(newdata, goaldata)
			copysharddata[shard] = newdata
			copyshardmem[shard] = copySingleShardMem(kv.shards[shard].mem)
		}

		DPrintf("%v exec migrate :%v", kv.shardkvInfo(), shards_to_send)

		// asynchronous handler:
		// G1 exec migrate migrate(pushing)
		// G1 -> G2 (no blocking)
		// G2 apply and reply(pulling)
		// G1 see reply, start and apply GC op
		go func() {
			kv.multimigrateHandler(MultiMigrationCtx{copysharddata, copyshardmem, m.Gid, m.Cfgnum})
			// debug output
			kv.mu.Lock()
			all_shards := []int{}
			for shard := range kv.shards {
				all_shards = append(all_shards, shard)
			}
			kv.mu.Unlock()

			shards_gc := []int{}
			for shard := range copysharddata {
				shards_gc = append(shards_gc, shard)
			}

			kv.mu.Lock()
			if len(shards_gc) > 0 {
				kv.gcMultiShardUnlocked(shards_gc)
			}

			DPrintf("%v succ send the shard %v, still have %v shards:%v ", kv.shardkvInfo(), len(kv.shards), all_shards)
			kv.mu.Unlock()
		}()

	} else {
		install_shards := []int{}
		for shard, data := range m.ShardData {
			// exist and valid: pass
			// exist and old: replace
			// nonexist: add
			_, ok := kv.shards[shard]
			if ok && kv.shards_state[shard] == Valid {
				continue
			}
			// replace/add shard
			_, memok := m.ShardMem[shard]
			if !memok {
				err := fmt.Sprintf("%v receive shard %v without mem", kv.shardkvInfo(), shard)
				panic(err)
			}
			install_shards = append(install_shards, shard)
			kv.shards[shard] = &ShardStore{kvraft.MakeMapStore(), copySingleShardMem(m.ShardMem[shard])}
			kv.shards[shard].store.Load(data)
			kv.shards_state[shard] = Valid
		}
		if len(install_shards) > 0 {
			DPrintf("%v successfully install the shards %v", kv.shardkvInfo(), install_shards)
		}
	}
}

func (kv *ShardKV) execGC(gc GCShardOp) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	shards_gc := []int{}
	for _, shard := range gc.Shards {
		_, ok := kv.shards_state[shard]
		if !ok {
			_, ok = kv.shards[shard]
			if ok {
				err := fmt.Sprintf("%v has no state about shard %v", kv.shardkvInfo(), shard)
				panic(err)
			}
			continue
		}

		if kv.shards_state[shard] != Pushing && kv.shards_state[shard] != RePushing {
			continue
			// err := fmt.Sprintf("%v has shard %v to be gc but not in pushing state:%v", kv.shardkvInfo(), shard, kv.shards_state[shard])
			// panic(err)
		}

		delete(kv.shards, shard)
		delete(kv.shards_state, shard)
		shards_gc = append(shards_gc, shard)
	}
	DPrintf("%v gc shard %v", kv.shardkvInfo(), shards_gc)
}

func (kv *ShardKV) execConfig(cfg ConfigOp) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if cfg.Cfg.Num <= kv.cfg.Num {
		err := fmt.Sprintf("%v try to apply an old config:%v< %v", kv.shardkvInfo(), cfg.Cfg.Num, kv.cfg.Num)
		panic(err)
	}

	if cfg.Cfg.Num != kv.cfg.Num+1 {
		err := fmt.Sprintf("%v try to apply an too later config:%v > %v + 1", kv.shardkvInfo(), cfg.Cfg.Num, kv.cfg.Num)
		panic(err)
	}

	kv.cfg = cfg.Cfg
}

// move multi-shards one time
// no lock is needed
func (kv *ShardKV) multimigrateHandler(ctx MultiMigrationCtx) {

	shards := []int{}
	for shard := range ctx.ShardData {
		shards = append(shards, shard)
	}

	DPrintf("%v begins to migrate shards %v to %v", kv.shardkvInfo(), shards, ctx.Gid)

	// prepare args
	args := MultiMigrateArgs{}
	args.ShardData = ctx.ShardData
	args.ShardMem = ctx.ShardMem
	args.CfgNum = ctx.ConfigNum
	args.Gid = ctx.Gid

	// send rpcs until success
	for !kv.killed() {
		kv.mu.Lock()
		servers, ok := kv.cfg.Groups[args.Gid]
		kv.mu.Unlock()

		if ok {
			// try each server for the shard.
			for si := 0; si < len(servers); si++ {
				srv := kv.make_end(servers[si])
				var reply MultiMigrateReply
				ok := srv.Call("ShardKV.MultiMigrate", &args, &reply)
				if ok && (reply.Err == OK || reply.Err == ErrOldShard) {
					all_shards := []int{}
					for shard := range args.ShardData {
						all_shards = append(all_shards, shard)
					}
					DPrintf("server %v, %v translate shard %v to gid %v", kv.me, kv.gid, all_shards, args.Gid)
					return
				}
				if ok && (reply.Err == ErrWrongGroup) {
					panic("shouldn't wrong group")
				}
				// ... not ok, or ErrWrongLeader
			}
		}
		time.Sleep(ClientRPCPeriod * time.Millisecond)
	}
}
