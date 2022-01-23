package shardctrler

import (
	"sync"
	"sync/atomic"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
)

type Op struct {
	OpType       int         // JOIN/LEAVE/MOVE/QUERY
	Args         interface{} // payload of JOIN/LEAVE/MOVE/QUERY ops
	SerialNumber int64       // monotonically increasing for every client
	Id           string      // client id
}

type KVRPCHandler func(raft.ApplyMsg, interface{}) // handle the msg from raft

type Response struct {
	SerialNumber int64
	Value        interface{}
	OpType       int
}

type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	dead int32 // set by Kill()

	sub       map[int]KVRPCHandler // msg_id -> corresponding handler
	clientMap map[string]Response  // client_request_id -> latest response

	configNum int         // monotonically increasing
	gsid      int         // monotonically increasing id used to sort the gid
	gsmap     map[int]int // gid -> gsid

	configs []Config // indexed by config num
}

// procApplyMsg process msg from applyCh
// and broadcast to subs
func (sc *ShardCtrler) procApplyMsg() {
	for appmsg := range sc.applyCh {
		if sc.killed() {
			return
		}

		if appmsg.CommandValid {
			// execute the cmd
			op := appmsg.Command.(Op)
			value := sc.execOp(op)

			// broadcast to all the subscribers
			// non-blocking
			go func(appmsg raft.ApplyMsg, value interface{}) {
				sc.mu.Lock()
				fn, ok := sc.sub[appmsg.CommandIndex]
				sc.mu.Unlock()

				if ok {
					fn(appmsg, value)
				}
			}(appmsg, value)

		} else if appmsg.SnapshotValid { // CondInstallSnapshot
			panic("should be no snapshot")
		}

	}
}

func (sc *ShardCtrler) rpcTemplate(args interface{}, reply interface{}, fail func(reply interface{}), succ func(reply interface{}, r *Response),
	getIdSn func(args interface{}) (string, int64), getOp func(args interface{}) Op) {
	if sc.killed() {
		fail(reply)
		return
	}

	term, isLeader := sc.rf.GetState()
	if !isLeader {
		fail(reply)
		return
	}

	id, sn := getIdSn(args)
	ok, r := sc.isExecuted(id, sn)
	if ok {
		succ(reply, &r)
		return
	}

	// start the agree
	done := make(chan struct{}, 1)
	cmd := getOp(args)

	// start agree and register handler to the msg
	sc.mu.Lock()
	index, _, ok := sc.rf.Start(cmd)
	if !ok {
		fail(reply)
		sc.mu.Unlock()
		return
	}

	sc.registerMsgListener(index, func(msg raft.ApplyMsg, value interface{}) {
		// make sure msg's context is consistent
		newTerm, isStillLeader := sc.rf.GetState()
		if !isStillLeader || newTerm != term {
			fail(reply)
			sc.cancelMsgListener(index)
			done <- struct{}{}
			return
		}

		succ(reply, nil)

		// remove msg's handler
		sc.cancelMsgListener(index)
		done <- struct{}{}
	})
	sc.mu.Unlock()

	// wait for the msg is applied
	<-done
}

func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	if sc.killed() {
		reply.Err = ErrWrongLeader
		reply.WrongLeader = true
		return
	}

	term, isLeader := sc.rf.GetState()
	if !isLeader {
		reply.Err = ErrWrongLeader
		reply.WrongLeader = true
		return
	}

	ok, _ := sc.isExecuted(args.Id, args.SerialNumber)
	if ok {
		DPrintf("leader %v Join before.", sc.me)
		reply.Err = OK
		reply.WrongLeader = false
		return
	}

	// start the agree
	done := make(chan struct{}, 1)
	servers := make(map[int][]string) // map are referenced as args
	copyMap(servers, args.Servers)
	cmd := Op{
		OpType:       JOIN,
		Args:         servers,
		SerialNumber: args.SerialNumber,
		Id:           args.Id,
	}

	// start agree and register handler to the msg
	sc.mu.Lock()
	index, _, ok := sc.rf.Start(cmd)
	if !ok {
		reply.Err = ErrWrongLeader
		reply.WrongLeader = true
		sc.mu.Unlock()
		return
	}

	sc.registerMsgListener(index, func(msg raft.ApplyMsg, value interface{}) {
		// make sure msg's context is consistent
		newTerm, isStillLeader := sc.rf.GetState()
		if !isStillLeader || newTerm != term {
			reply.Err = ErrWrongLeader
			reply.WrongLeader = true
			sc.cancelMsgListener(index)
			done <- struct{}{}
			return
		}

		reply.Err = OK
		reply.WrongLeader = false

		// remove msg's handler
		sc.cancelMsgListener(index)
		done <- struct{}{}
	})
	sc.mu.Unlock()

	// wait for the msg is applied
	<-done
	DPrintf("leader %v Join in term %v", sc.me, term)
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	if sc.killed() {
		reply.Err = ErrWrongLeader
		reply.WrongLeader = true
		return
	}

	term, isLeader := sc.rf.GetState()
	if !isLeader {
		reply.Err = ErrWrongLeader
		reply.WrongLeader = true
		return
	}

	ok, _ := sc.isExecuted(args.Id, args.SerialNumber)
	if ok {
		DPrintf("leader %v Leave before.", sc.me)
		reply.Err = OK
		reply.WrongLeader = false
		return
	}

	// start the agree
	done := make(chan struct{}, 1)
	gids := make([]int, len(args.GIDs))
	copy(gids, args.GIDs)
	cmd := Op{
		OpType:       LEAVE,
		Args:         gids,
		SerialNumber: args.SerialNumber,
		Id:           args.Id,
	}

	// start agree and register handler to the msg
	sc.mu.Lock()
	index, _, ok := sc.rf.Start(cmd)
	if !ok {
		reply.Err = ErrWrongLeader
		reply.WrongLeader = true
		sc.mu.Unlock()
		return
	}

	sc.registerMsgListener(index, func(msg raft.ApplyMsg, value interface{}) {
		// make sure msg's context is consistent
		newTerm, isStillLeader := sc.rf.GetState()
		if !isStillLeader || newTerm != term {
			reply.Err = ErrWrongLeader
			reply.WrongLeader = true
			sc.cancelMsgListener(index)
			done <- struct{}{}
			return
		}

		reply.Err = OK
		reply.WrongLeader = false

		// remove msg's handler
		sc.cancelMsgListener(index)
		done <- struct{}{}
	})
	sc.mu.Unlock()

	// wait for the msg is applied
	<-done
	DPrintf("leader %v Leave in term %v", sc.me, term)
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	if sc.killed() {
		reply.Err = ErrWrongLeader
		reply.WrongLeader = true
		return
	}

	term, isLeader := sc.rf.GetState()
	if !isLeader {
		reply.Err = ErrWrongLeader
		reply.WrongLeader = true
		return
	}

	ok, _ := sc.isExecuted(args.Id, args.SerialNumber)
	if ok {
		DPrintf("leader %v Move before.", sc.me)
		reply.Err = OK
		reply.WrongLeader = false
		return
	}

	// start the agree
	done := make(chan struct{}, 1)
	cmd := Op{
		OpType:       MOVE,
		Args:         GSPair{args.GID, args.Shard},
		SerialNumber: args.SerialNumber,
		Id:           args.Id,
	}

	// start agree and register handler to the msg
	sc.mu.Lock()
	index, _, ok := sc.rf.Start(cmd)
	if !ok {
		reply.Err = ErrWrongLeader
		reply.WrongLeader = true
		sc.mu.Unlock()
		return
	}

	sc.registerMsgListener(index, func(msg raft.ApplyMsg, value interface{}) {
		// make sure msg's context is consistent
		newTerm, isStillLeader := sc.rf.GetState()
		if !isStillLeader || newTerm != term {
			reply.Err = ErrWrongLeader
			reply.WrongLeader = true
			sc.cancelMsgListener(index)
			done <- struct{}{}
			return
		}

		reply.Err = OK
		reply.WrongLeader = false

		// remove msg's handler
		sc.cancelMsgListener(index)
		done <- struct{}{}
	})
	sc.mu.Unlock()

	// wait for the msg is applied
	<-done
	DPrintf("leader %v Move in term %v", sc.me, term)
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	if sc.killed() {
		reply.Err = ErrWrongLeader
		reply.WrongLeader = true
		return
	}

	term, isLeader := sc.rf.GetState()
	if !isLeader {
		reply.Err = ErrWrongLeader
		reply.WrongLeader = true
		return
	}

	ok, v := sc.isExecuted(args.Id, args.SerialNumber)
	if ok {
		DPrintf("leader %v Query before.", sc.me)
		reply.Err = OK
		reply.WrongLeader = false
		reply.Config = v.Value.(Config)
		return
	}

	// start the agree
	done := make(chan struct{}, 1)
	cmd := Op{
		OpType:       QUERY,
		Args:         args.Num,
		SerialNumber: args.SerialNumber,
		Id:           args.Id,
	}

	// start agree and register handler to the msg
	sc.mu.Lock()
	index, _, ok := sc.rf.Start(cmd)
	if !ok {
		reply.Err = ErrWrongLeader
		reply.WrongLeader = true
		sc.mu.Unlock()
		return
	}

	sc.registerMsgListener(index, func(msg raft.ApplyMsg, value interface{}) {
		// make sure msg's context is consistent
		newTerm, isStillLeader := sc.rf.GetState()
		if !isStillLeader || newTerm != term {
			reply.Err = ErrWrongLeader
			reply.WrongLeader = true
			sc.cancelMsgListener(index)
			done <- struct{}{}
			return
		}

		reply.Err = OK
		reply.WrongLeader = false
		reply.Config = value.(Config)

		// remove msg's handler
		sc.cancelMsgListener(index)
		done <- struct{}{}
	})
	sc.mu.Unlock()

	// wait for the msg is applied
	<-done
	DPrintf("leader %v Query in term %v", sc.me, term)
}

//
// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sc *ShardCtrler) Kill() {
	atomic.StoreInt32(&sc.dead, 1)
	sc.rf.Kill()
	// Your code here, if desired.
}

func (sc *ShardCtrler) killed() bool {
	z := atomic.LoadInt32(&sc.dead)
	return z == 1
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	labgob.Register(map[int][]string{})
	labgob.Register(GSPair{})

	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	// Your code here.
	sc.configNum = 0
	sc.gsid = 0

	sc.sub = make(map[int]KVRPCHandler)
	sc.clientMap = make(map[string]Response)
	sc.gsmap = make(map[int]int)
	go sc.procApplyMsg()

	return sc
}
