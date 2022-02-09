package shardkv

//
// client code to talk to a sharded key/value service.
//
// the client first talks to the shardctrler to find out
// the assignment of shards (keys) to groups, and then
// talks to the group that holds the key's shard.
//

import (
	"crypto/rand"
	"math/big"
	"time"

	"6.824/labrpc"
	"6.824/shardctrler"
	"github.com/segmentio/ksuid"
)

//
// which shard is a key in?
// please use this function,
// and please do not change it.
//
func key2shard(key string) int {
	shard := 0
	if len(key) > 0 {
		shard = int(key[0])
	}
	shard %= shardctrler.NShards
	return shard
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func genUUID() string {
	id := ksuid.New()
	return id.String()
}

type Clerk struct {
	sm       *shardctrler.Clerk
	config   shardctrler.Config
	make_end func(string) *labrpc.ClientEnd

	// prevLeader int    // last leader seen
	prevSerNum int64  // last serial number
	id         string // client unique id
	// mu         sync.Mutex
}

//
// the tester calls MakeClerk.
//
// ctrlers[] is needed to call shardctrler.MakeClerk().
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs.
//
func MakeClerk(ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.sm = shardctrler.MakeClerk(ctrlers)
	ck.make_end = make_end
	ck.config = ck.sm.Query(-1)

	ck.prevSerNum = 0
	ck.id = genUUID()
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
// You will have to modify this function.
//
func (ck *Clerk) Get(key string) string {
	// DPrintf("ck %v begin get \"%v\"", ck.id, key)

	// prepare args
	args := GetArgs{}
	args.Key = key
	ck.prevSerNum++ // sn increase
	args.SerialNumber = ck.prevSerNum
	args.Id = ck.id
	args.CfgNum = ck.config.Num

	// send rpcs until success
	for {
		// showConfigInfo(&ck.config)
		shard := key2shard(key)
		gid := ck.config.Shards[shard]
		if servers, ok := ck.config.Groups[gid]; ok {
			// try each server for the shard.
			for si := 0; si < len(servers); si++ {
				srv := ck.make_end(servers[si])
				var reply GetReply
				ok := srv.Call("ShardKV.Get", &args, &reply)
				if ok && (reply.Err == OK || reply.Err == ErrNoKey) {
					// DPrintf("ck %v get \"%v\": \"%v\"", ck.id, key, reply.Value)
					return reply.Value
				}
				if ok && (reply.Err == ErrWrongGroup) {
					DPrintf("ck %v get \"%v\" fail with sn %v", ck.id, key, reply.Value, args.SerialNumber)
					break
				}
				// ... not ok, or ErrWrongLeader
			}
		}
		time.Sleep(ClientRPCPeriod * time.Millisecond)
		// ask controler for the latest configuration.
		ck.config = ck.sm.Query(-1)
		args.CfgNum = ck.config.Num
	}
}

//
// shared by Put and Append.
// You will have to modify this function.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	args := PutAppendArgs{}
	args.Key = key
	args.Value = value
	args.Op = op

	ck.prevSerNum++ // sn increase
	args.SerialNumber = ck.prevSerNum
	args.Id = ck.id
	args.CfgNum = ck.config.Num

	for {
		// showConfigInfo(&ck.config)
		shard := key2shard(key)
		gid := ck.config.Shards[shard]
		if servers, ok := ck.config.Groups[gid]; ok {
			for si := 0; si < len(servers); si++ {
				srv := ck.make_end(servers[si])
				var reply PutAppendReply
				ok := srv.Call("ShardKV.PutAppend", &args, &reply)
				if ok && reply.Err == OK {
					return
				}

				if ok && reply.Err == ErrWrongGroup {
					DPrintf("ck %v putappend fail with sn %v", ck.id, args.SerialNumber)
					break
				}
				// ... not ok, or ErrWrongLeader
			}
		}
		time.Sleep(ClientRPCPeriod * time.Millisecond)
		// ask controler for the latest configuration.
		ck.config = ck.sm.Query(-1)
		args.CfgNum = ck.config.Num
	}
}

func (ck *Clerk) Put(key string, value string) {
	// DPrintf("ck %v begin put :< \"%v\", \"%v\">", ck.id, key, value)
	ck.PutAppend(key, value, "Put")
	// DPrintf("ck %v successfully put :< \"%v\", \"%v\">", ck.id, key, value)
}
func (ck *Clerk) Append(key string, value string) {
	// DPrintf("ck %v begin append :< \"%v\", \"%v\">", ck.id, key, value)
	ck.PutAppend(key, value, "Append")
	// DPrintf("ck %v successfully append :< \"%v\", \"%v\">", ck.id, key, value)
}
