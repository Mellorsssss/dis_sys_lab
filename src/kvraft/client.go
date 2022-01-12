package kvraft

import (
	"crypto/rand"
	"math/big"
	"sync"
	"time"

	"6.824/labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	prevLeader int // last leader seen
	mu         sync.Mutex
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.prevLeader = NoLeader
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {
	defer func() {
		DPrintf("ck get %v succ", key)
	}()

	curLeader := NoLeader
	ck.mu.Lock()
	if ck.prevLeader != NoLeader {
		curLeader = ck.prevLeader
	}
	ck.mu.Unlock()

	// use the known possible leader to send rpc first
	if curLeader != NoLeader {
		ok, value := ck.tryGet(key, curLeader)
		if ok {
			DPrintf("ck use cached server info")
			ck.mu.Lock()
			ck.prevLeader = curLeader
			ck.mu.Unlock()

			return value
		}
	}

	// send rpc until success
	succ := false
	for !succ {
		for ind := range ck.servers {
			ok, value := ck.tryGet(key, ind)
			if ok {
				ck.mu.Lock()
				ck.prevLeader = ind
				ck.mu.Unlock()

				return value
			}
		}
	}
	return ""
}

// tryGet sends Get rpc to server with timeout
// return true if server is leader(and the value get)
// or return false(value must be "") if timeout or server isn't leader
func (ck *Clerk) tryGet(key string, server int) (bool, string) {
	args := &GetArgs{
		Key: key,
	}
	reply := &GetReply{}

	reach := make(chan bool)
	go func() {
		ok := ck.servers[server].Call("KVServer.Get", args, reply)
		reach <- ok
	}()

	select {
	case succ := <-reach:
		if !succ || reply.Err == ErrWrongLeader {
			return false, ""
		}

		if reply.Err == ErrNoKey {
			return true, ""
		}
		return true, reply.Value

	case <-time.After(time.Duration(RpcTimeout) * time.Millisecond):
		return false, ""
	}
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	defer func() {
		DPrintf("ck %v (%v, %v) succ", op, key, value)
	}()

	curLeader := NoLeader
	ck.mu.Lock()
	if ck.prevLeader != NoLeader {
		curLeader = ck.prevLeader
	}
	ck.mu.Unlock()

	// use the known possible leader to send rpc first
	if curLeader != NoLeader {
		ok := ck.tryPutAppend(key, value, op, curLeader)
		if ok {
			ck.mu.Lock()
			ck.prevLeader = curLeader
			ck.mu.Unlock()
			return
		}
	}

	// send rpc until success
	succ := false
	for !succ {
		for ind := range ck.servers {
			ok := ck.tryPutAppend(key, value, op, ind)
			if ok {
				ck.mu.Lock()
				ck.prevLeader = ind
				ck.mu.Unlock()
				return
			}
		}
	}
}

func (ck *Clerk) tryPutAppend(key, value, op string, server int) bool {
	args := &PutAppendArgs{
		Key:   key,
		Value: value,
		Op:    op,
	}
	reply := &PutAppendReply{}

	reach := make(chan bool)
	go func() {
		ok := ck.servers[server].Call("KVServer.PutAppend", args, reply)
		reach <- ok
	}()

	select {
	case succ := <-reach:
		if !succ {
			DPrintf("not succ")
			return false
		}
		if reply.Err == ErrWrongLeader {
			DPrintf("wrong leader")
			return false
		}

		if reply.Err == ErrNoKey {
			Error("PutAppend shouldn't have key error")
			return true
		}

		return true

	case <-time.After(time.Duration(RpcTimeout) * time.Millisecond):
		DPrintf("timeout")
		return false
	}
}

func (ck *Clerk) Put(key string, value string) {
	DPrintf("clerk put (%v, %v)", key, value)
	ck.PutAppend(key, value, "Put")
	DPrintf("clerk put (%v, %v) succ", key, value)
}
func (ck *Clerk) Append(key string, value string) {
	DPrintf("clerk append (%v, %v)", key, value)
	ck.PutAppend(key, value, "Append")
	DPrintf("clerk append (%v, %v) succ", key, value)
}
