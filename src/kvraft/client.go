package kvraft

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
	// You will have to modify this struct.
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
	// You'll have to add code here.
	ck.prevLeader = NoLeader
	ck.prevSerNum = genSerialNumber()
	ck.id = genUUID()
	Info("start %v client", ck.id)
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
		DPrintf("ck %v Get succ.", ck.id)
	}()
	ck.mu.Lock()
	curLeader := ck.prevLeader
	sn := ck.prevSerNum + 1
	ck.prevSerNum = sn // serial number increase
	ck.mu.Unlock()

	// try to connect to known possible leader first
	if curLeader != NoLeader {
		ok, value := ck.tryGet(key, curLeader, sn)
		if ok {
			DPrintf("ck %v use cached server info %v", ck.id, curLeader)
			ck.mu.Lock()
			ck.prevLeader = curLeader
			ck.mu.Unlock()
			return value
		}

		ck.mu.Lock()
		ck.prevLeader = NoLeader
		ck.mu.Unlock()
	}

	// send rpc until success
	succ := false
	for !succ {
		for ind := range ck.servers {
			ok, value := ck.tryGet(key, ind, sn)
			if ok {
				ck.mu.Lock()
				ck.prevLeader = ind
				ck.mu.Unlock()
				return value
			}
			time.Sleep(time.Duration(RequestInterval) * time.Millisecond)
		}
	}
	return ""
}

// tryGet sends Get rpc to server with timeout
// return true if server is leader(and the value get)
// or return false(value must be "") if timeout or server isn't leader
func (ck *Clerk) tryGet(key string, server int, serialnumber int64) (bool, string) {
	args := &GetArgs{
		Key:          key,
		SerialNumber: serialnumber,
		Id:           ck.id,
	}
	reply := &GetReply{}

	reach := make(chan bool, 1)
	go func(ch chan<- bool) {
		ok := ck.servers[server].Call("KVServer.Get", args, reply)
		ch <- ok
	}(reach)

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
	ck.mu.Lock()
	curLeader := ck.prevLeader
	sn := ck.prevSerNum + 1
	ck.prevSerNum = sn // serial number increase
	ck.mu.Unlock()

	// use the known possible leader to send rpc first
	if curLeader != NoLeader {
		ok := ck.tryPutAppend(key, value, op, curLeader, sn)
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
			ok := ck.tryPutAppend(key, value, op, ind, sn)
			if ok {
				ck.mu.Lock()
				ck.prevLeader = ind
				ck.mu.Unlock()
				return
			}
			time.Sleep(time.Duration(RequestInterval) * time.Millisecond)
		}
	}
}

func (ck *Clerk) tryPutAppend(key, value, op string, server int, sn int64) bool {
	args := &PutAppendArgs{
		Key:          key,
		Value:        value,
		Op:           op,
		SerialNumber: sn,
		Id:           ck.id,
	}
	reply := &PutAppendReply{}

	reach := make(chan bool, 1)
	go func(ch chan<- bool) {
		ok := ck.servers[server].Call("KVServer.PutAppend", args, reply)
		ch <- ok
	}(reach)

	select {
	case succ := <-reach:
		if !succ {
			return false
		}
		if reply.Err == ErrWrongLeader {
			return false
		}

		if reply.Err == ErrNoKey {
			Error("PutAppend shouldn't have key error")
			return true
		}

		return true

	case <-time.After(time.Duration(RpcTimeout) * time.Millisecond):
		return false
	}
}

func (ck *Clerk) Put(key string, value string) {
	DPrintf("ck %v put (%v, %v)", ck.id, key, value)
	ck.PutAppend(key, value, "Put")
	DPrintf("ck %v put (%v, %v) succ", ck.id, key, value)
}
func (ck *Clerk) Append(key string, value string) {
	DPrintf("ck %v append (%v, %v)", ck.id, key, value)
	ck.PutAppend(key, value, "Append")
	DPrintf("ck %v append (%v, %v) succ", ck.id, key, value)
}
