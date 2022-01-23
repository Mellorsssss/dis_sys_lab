package shardctrler

//
// Shardctrler clerk.
//

import (
	"crypto/rand"
	"math/big"
	"sync"
	"time"

	"6.824/labrpc"
	"github.com/segmentio/ksuid"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// Your data here.
	prevLeader int    // last leader seen
	prevSerNum int64  // last serial number
	id         string // client unique id
	mu         sync.Mutex
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

// gen unique id of clerk
func genUUID() string {
	id := ksuid.New()
	return id.String()
}

func genSerialNumber() int64 {
	return nrand()
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// Your code here.
	ck.prevLeader = NoLeader
	ck.prevSerNum = genSerialNumber()
	ck.id = genUUID()
	Info("start %v client", ck.id)
	return ck
}

func (ck *Clerk) Query(num int) Config {
	args := &QueryArgs{}
	args.Num = num

	// maintain the info
	ck.mu.Lock()
	curLeader := ck.prevLeader
	sn := ck.prevSerNum + 1
	ck.prevSerNum = sn // serial number increase
	ck.mu.Unlock()

	args.Id = ck.id
	args.SerialNumber = sn

	if curLeader != NoLeader {
		var reply QueryReply
		ok := ck.servers[curLeader].Call("ShardCtrler.Query", args, &reply)
		if ok && !reply.WrongLeader {
			return reply.Config
		}
	}

	for {
		// try each known server.
		for ind, srv := range ck.servers {
			var reply QueryReply
			ok := srv.Call("ShardCtrler.Query", args, &reply)
			if ok && !reply.WrongLeader {
				ck.mu.Lock()
				ck.prevLeader = ind
				ck.mu.Unlock()
				return reply.Config
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Join(servers map[int][]string) {
	args := &JoinArgs{}
	args.Servers = servers

	// maintain the info
	ck.mu.Lock()
	curLeader := ck.prevLeader
	sn := ck.prevSerNum + 1
	ck.prevSerNum = sn // serial number increase
	ck.mu.Unlock()
	args.Id = ck.id
	args.SerialNumber = sn
	if curLeader != NoLeader {
		var reply JoinReply
		ok := ck.servers[curLeader].Call("ShardCtrler.Join", args, &reply)
		if ok && !reply.WrongLeader {
			return
		}
	}
	for {
		// try each known server.
		for ind, srv := range ck.servers {
			var reply JoinReply
			ok := srv.Call("ShardCtrler.Join", args, &reply)
			if ok && !reply.WrongLeader {
				ck.mu.Lock()
				ck.prevLeader = ind
				ck.mu.Unlock()
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Leave(gids []int) {
	args := &LeaveArgs{}
	// Your code here.
	args.GIDs = gids

	// maintain the info
	ck.mu.Lock()
	curLeader := ck.prevLeader
	sn := ck.prevSerNum + 1
	ck.prevSerNum = sn // serial number increase
	ck.mu.Unlock()
	args.Id = ck.id
	args.SerialNumber = sn
	if curLeader != NoLeader {
		var reply LeaveReply
		ok := ck.servers[curLeader].Call("ShardCtrler.Leave", args, &reply)
		if ok && !reply.WrongLeader {
			return
		}
	}

	for {
		// try each known server.
		for ind, srv := range ck.servers {
			var reply LeaveReply
			ok := srv.Call("ShardCtrler.Leave", args, &reply)
			if ok && !reply.WrongLeader {
				ck.mu.Lock()
				ck.prevLeader = ind
				ck.mu.Unlock()
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Move(shard int, gid int) {
	args := &MoveArgs{}
	// Your code here.
	args.Shard = shard
	args.GID = gid

	// maintain the info
	ck.mu.Lock()
	curLeader := ck.prevLeader
	sn := ck.prevSerNum + 1
	ck.prevSerNum = sn // serial number increase
	ck.mu.Unlock()
	args.Id = ck.id
	args.SerialNumber = sn
	if curLeader != NoLeader {
		var reply MoveReply
		ok := ck.servers[curLeader].Call("ShardCtrler.Move", args, &reply)
		if ok && !reply.WrongLeader {
			return
		}
	}

	for {
		// try each known server.
		for ind, srv := range ck.servers {
			var reply MoveReply
			ok := srv.Call("ShardCtrler.Move", args, &reply)
			if ok && !reply.WrongLeader {
				ck.mu.Lock()
				ck.prevLeader = ind
				ck.mu.Unlock()
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}
