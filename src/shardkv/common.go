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
	ErrOldShard    = "ErrOldShard"
	ErrFailTrans   = "ErrFailTrans"
)

// client params
const (
	ClientRPCPeriod = 100 // time between round of rpc sending
)

// server params
const (
	ServerConfigUpdatePeriod = 100
	ServerRPCPeriod          = 100
	RaftSizeThreshold        = 0.8
)

const (
	GET    = 0
	PUT    = 1
	APPEND = 2
)

type Err string
type Res int

const (
	Succ       = 0
	WrongGroup = 1
)

// shard state
const (
	Valid     = 1
	Pushing   = 2
	RePushing = 3
)

// Put or Append
type PutAppendArgs struct {
	// You'll have to add definitions here.
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// mark client
	SerialNumber int64
	Id           string // client id
	CfgNum       int
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// mark client
	SerialNumber int64
	Id           string // client id
	CfgNum       int
}

type GetReply struct {
	Err   Err
	Value string
}

// type MigrateArgs struct {
// 	Shard  int
// 	Data   []byte
// 	CfgNum int
// 	Gid    int
// }

// type MigrateReply struct {
// 	Err Err
// }

type MultiMigrateArgs struct {
	ShardData map[int][]byte
	ShardMem  map[int]map[string]Response
	CfgNum    int
	Gid       int
}

type MultiMigrateReply struct {
	Err Err
}
