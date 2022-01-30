package shardkv

//
// Sharded key/value server.
// Lots of replica groups, each running Raft.
// Shardctrler decides which group serves each shard.
// Shardctrler may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongGroup  = "ErrWrongGroup"
	ErrWrongLeader = "ErrWrongLeader"
)

// client params
const (
	ClientRPCPeriod = 100 // time between round of rpc sending
)

// server params
const (
	ServerConfigUpdatePeriod = 100
)

const (
	GET    = 0
	PUT    = 1
	APPEND = 2
)

type Err string

// Put or Append
type PutAppendArgs struct {
	// You'll have to add definitions here.
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// mark client
	SerialNumber int64
	Id           string // client id
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// mark client
	SerialNumber int64
	Id           string // client id
}

type GetReply struct {
	Err   Err
	Value string
}
