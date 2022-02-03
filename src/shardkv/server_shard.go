package shardkv

import (
	"fmt"
	"time"

	"6.824/kvraft"
)

// query the Config 1 to init shards
func (kv *ShardKV) initShards() {
	cfg := kv.ck.Query(1)
	for ; cfg.Num != 1; cfg = kv.ck.Query(1) {
		DPrintf("initing...")
		time.Sleep(100 * time.Millisecond)
	}

	kv.mu.Lock()
	for shard, gid := range cfg.Shards {
		if gid == kv.gid {
			kv.shards[shard] = kvraft.MakeMapStore()
			DPrintf("server %v,%v get %v", kv.me, kv.gid, shard)
		}
	}
	kv.mu.Unlock()
}

// fetchConfig fetches the latest config and changes the shard
// for the first config, just add the empty store;
// for the later config change, remove the leaving shards
// anytime, if shards are ready, change kv.cfg to latest config
// and if ck's rq's Num is larger than kv.cfg, don't serve it(todo: optimize)
func (kv *ShardKV) fetchConfigLoop() {
	// update the config atomically
	kv.initShards()
	for !kv.killed() {
		kv.mu.Lock()
		ncfg := kv.ck.Query(-1)

		// move all the leaving shards
		for shard := range kv.shards {
			if shard >= len(kv.cfg.Shards) {
				err := fmt.Sprintf("server %v has shard which range out:%v > %v", kv.me, shard, len(kv.cfg.Shards))
				panic(err)
			}

			if ncfg.Shards[shard] != kv.gid {
				kv.removeShardUnlocked(shard, ncfg.Shards[shard], ncfg.Num)
			}
		}

		kv.cfg = ncfg // change to newst config
		kv.mu.Unlock()
		time.Sleep(ServerConfigUpdatePeriod * time.Millisecond)
		// }
	}
}

func (kv *ShardKV) removeShardUnlocked(shard, gid, cfgnum int) {
	if kv.killed() {
		return
	}

	term, isLeader := kv.rf.GetState()
	if !isLeader {
		return
	}
	kv.rf.Start(MigrationOp{term, gid, cfgnum, shard, true, nil})
	DPrintf("leader %v, %v start to remove shard %v", kv.me, kv.gid, shard)
}

// execMigrate keep migrating a shard until success
func (kv *ShardKV) execMigrate(m MigrationOp) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if m.Sending {
		DPrintf("server %v,%v exec send shard %v", kv.me, kv.gid, m.Shard)

		// if the shard should still be sending
		_, ok := kv.shards[m.Shard]
		if !ok {
			return
		}
		kv.migrateHandler(MigrationCtx{m.Shard, kv.shards[m.Shard].Data(), m.Gid, m.Cfgnum})
		delete(kv.shards, m.Shard) // move successfully, remove shard

		// debug output
		all_shards := []int{}
		for shard := range kv.shards {
			all_shards = append(all_shards, shard)
		}

		DPrintf("server %v,%v succ send the shard %v, still have %v shards:%v ", kv.me, kv.gid, m.Shard, len(kv.shards), all_shards)
	} else {
		_, ok := kv.shards[m.Shard]
		if ok { // server already has the shard
			DPrintf("server %v,%v already has install the shard %v", kv.me, kv.gid, m.Shard)
			return
		}

		// install the shard
		DPrintf("server %v,%v successfully install the shard %v", kv.me, kv.gid, m.Shard)
		kv.shards[m.Shard] = kvraft.MakeMapStore()
		kv.shards[m.Shard].Load(m.Data)
	}
}

// migrateHandler send shards to leader until move shard success
func (kv *ShardKV) migrateHandler(ctx MigrationCtx) {
	DPrintf("server <%v,%v> begins to migrate shard  %v", kv.me, kv.gid, ctx.Shard)

	// prepare args
	args := MigrateArgs{}
	args.Data = ctx.Data
	args.Shard = ctx.Shard
	args.CfgNum = ctx.ConfigNum
	args.Gid = ctx.Gid

	// send rpcs until success
	for !kv.killed() {
		DPrintf("server %v, %v sends shard to %v", kv.me, kv.gid, args.Gid)
		if servers, ok := kv.cfg.Groups[args.Gid]; ok {
			// try each server for the shard.
			for si := 0; si < len(servers); si++ {
				srv := kv.make_end(servers[si])
				var reply MigrateReply
				ok := srv.Call("ShardKV.Migrate", &args, &reply)
				if ok && (reply.Err == OK || reply.Err == ErrOldShard) {
					DPrintf("server %v, %v translate shard %v to gid %v", kv.me, kv.gid, args.Shard, args.Gid)
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
