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
func (kv *ShardKV) initShardsUnlocked(cfg shardctrler.Config) {
	kv.shards = make(map[int]*ShardStore)
	for shard, gid := range cfg.Shards {
		if gid == kv.gid {
			kv.shards[shard] = &ShardStore{kvraft.MakeMapStore(), make(map[string]Response)}
			kv.shards_state[shard] = Valid
			DPrintf("%v get initial shard %v", kv.shardkvInfo(), shard)
		}
	}
}

// fetchConfig fetches the latest config and changes the shard
// for the first config, just add the empty store;
// for the later config change, remove the leaving shards
// anytime, if shards are ready, change kv.cfg to latest config
// and if ck's rq's Num is larger than kv.cfg, don't serve it(todo: optimize)
func (kv *ShardKV) fetchConfigLoop() {
	// update the config atomically
	// start the migrate if necessary
	for !kv.killed() {
		kv.mu.Lock()
		ccfg := kv.cfg

		if ncfg, ok := kv.exsitConfig(ccfg.Num + 1); ok { // exsit new config, start migrate
			if ncfg.Num == 1 { // initial situation, just apply it
				DPrintf("%v begin to init...", kv.shardkvInfo())
				kv.applyConfigUnlocked(ncfg)
			} else {

				shards_need_push := false
				shards_need_pull := false

				// detect if shards to move have been moved
				shard_gid_map := make(map[int][]int)
				for shard := range kv.shards {
					if shard >= len(kv.cfg.Shards) {
						err := fmt.Sprintf("server %v has shard which range out:%v > %v", kv.me, shard, len(kv.cfg.Shards))
						panic(err)
					}

					gid := ncfg.Shards[shard]
					if gid != kv.gid && kv.shards_state[shard] != Pushing { // only valid shard or repushing shard should be pushed
						if _, ok := shard_gid_map[gid]; !ok {
							shard_gid_map[gid] = []int{}
						}
						shard_gid_map[gid] = append(shard_gid_map[gid], shard)
					}

					if gid != kv.gid {
						shards_need_push = true
					}
				}

				for gid1, shards := range shard_gid_map {
					kv.removeMultiShardUnlocked(shards, gid1, ncfg.Num)
				}

				// detect if all shards needed in this config is ready
				for shard, gid := range ncfg.Shards {
					if gid == kv.gid {
						if v, ok := kv.shards_state[shard]; !ok || v != Valid {
							shards_need_pull = true
							break
						}
						if _, ok := kv.shards[shard]; !ok {
							panic(fmt.Sprintf("%v has the state of shard%v but no shard", kv.shardkvInfo(), shard))
						}
					}
				}

				if !shards_need_pull && !shards_need_push { // shards are ready
					kv.applyConfigUnlocked(ncfg)
				}

				if shards_need_push {
					Info("%v(%v) needs to push shards: %v(%v)", kv.shardkvInfo(), kv.cfg.Num, kv.shardInfoUnlocked(), ncfg.Shards)
				}

				if shards_need_pull {
					Info("%v(%v) needs to pull shards:%v(%v)", kv.shardkvInfo(), kv.cfg.Num, kv.shardInfoUnlocked(), ncfg.Shards)
				}
			}
		}
		kv.mu.Unlock()
		time.Sleep(ServerConfigUpdatePeriod * time.Millisecond)
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
	Info("leader %v, %v start to remove shards %v", kv.me, kv.gid, shards)
}

func (kv *ShardKV) gcMultiShardUnlocked(shards []int, cfgnum int) {
	if kv.killed() {
		return
	}

	_, isLeader := kv.rf.GetState()
	if !isLeader {
		return
	}

	kv.rf.Start(GCShardOp{Shards: shards, CfgNum: cfgnum})
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
	DPrintf("leader %v start to apply config %v:%v", kv.shardkvInfo(), cfg.Num, cfg.Shards)
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
		// only leader should send shards
		// follower just update the shards state
		_, isLeader := kv.rf.GetState()
		if !isLeader {
			for shard := range m.ShardData {
				if _, ok := kv.shards[shard]; ok {
					kv.shards_state[shard] = Pushing
				}
			}
			return
		}

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

		Info("%v exec send migrate :%v in %v", kv.shardkvInfo(), shards_to_send, m.Cfgnum)

		// asynchronous handler:
		// G1 exec migrate migrate(pushing)
		// G1 -> G2 (no blocking)
		// G2 apply and reply(pulling)
		// G1 see reply, start and apply GC op
		go func() {
			kv.multimigrateHandler(MultiMigrationCtx{copysharddata, copyshardmem, m.Gid, m.Cfgnum})
			// debug output
			shards_gc := []int{}

			for shard := range copysharddata {
				shards_gc = append(shards_gc, shard)
			}

			kv.mu.Lock()
			if len(shards_gc) > 0 {
				kv.gcMultiShardUnlocked(shards_gc, m.Cfgnum)
			}
			kv.mu.Unlock()
		}()

	} else {
		if m.Cfgnum <= kv.cfg.Num {
			if kv.ConfigOutOfDateUnlocked(m.Cfgnum) {
				Info("%v(%v) get old shards from %v", kv.shardkvInfo(), kv.cfg.Num, m.Cfgnum)
				return
			}
			return
		}
		if m.Cfgnum > kv.cfg.Num+1 {
			err := fmt.Sprintf("shit: %v > %v +1", m.Cfgnum, kv.cfg.Num)
			panic(err)
		}
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

	if gc.CfgNum <= kv.cfg.Num {
		Info("%v get old gc in %v(%v), just drop", kv.shardkvInfo(), gc, gc.CfgNum, kv.cfg.Num)
		return
	}

	shards_gc := []int{}
	o_shards_gc := []int{}
	for _, shard := range gc.Shards {
		o_shards_gc = append(o_shards_gc, shard)
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
	Info("%v gc shard %v(goal:%v)", kv.shardkvInfo(), shards_gc, o_shards_gc)
}

func (kv *ShardKV) execConfig(cfg ConfigOp) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if cfg.Cfg.Num <= kv.cfg.Num {
		return
	}

	if cfg.Cfg.Num != kv.cfg.Num+1 {
		err := fmt.Sprintf("%v try to apply an too later config:%v > %v + 1", kv.shardkvInfo(), cfg.Cfg.Num, kv.cfg.Num)
		panic(err)
	}

	kv.cfg = cfg.Cfg
	if kv.cfg.Num == 1 {
		kv.initShardsUnlocked(kv.cfg)
	}
}

// move multi-shards one time
// no lock is needed
func (kv *ShardKV) multimigrateHandler(ctx MultiMigrationCtx) bool {

	shards := []int{}
	kv.mu.Lock()
	cfg := kv.ck.Query(ctx.ConfigNum)
	kv.mu.Unlock()
	for shard := range ctx.ShardData {
		shards = append(shards, shard)
	}

	DPrintf("%v begins to migrate shards %v to %v of %v", kv.shardkvInfo(), shards, ctx.Gid, ctx.ConfigNum)

	// prepare args
	args := MultiMigrateArgs{}
	args.ShardData = ctx.ShardData
	args.ShardMem = ctx.ShardMem
	args.CfgNum = ctx.ConfigNum
	args.Gid = ctx.Gid

	// send rpcs until success
	for !kv.killed() {
		kv.mu.Lock()
		servers, ok := cfg.Groups[args.Gid]
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
					return true
				}
				if ok && (reply.Err == ErrOldShard) {
					return false
				}
				if ok && (reply.Err == ErrWrongGroup) {
					panic("shouldn't wrong group")
				}
				// ... not ok, or ErrWrongLeader
			}
		} else {
			panic("shit, noservers")
		}
		time.Sleep(ClientRPCPeriod * time.Millisecond)
	}
	return false
}
