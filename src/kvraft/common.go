package kvraft

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
)

const (
	RpcTimeout        = 200
	NoLeader          = -1
	NoneSerialNumber  = -1
	RequestInterval   = 50
	RaftSizeThreshold = 1.2
)

const (
	GET    = 0
	PUT    = 1
	APPEND = 2
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	SerialNumber int64
	Id           string // client id
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	SerialNumber int64
	Id           string // client id
}

type GetReply struct {
	Err   Err
	Value string
}
