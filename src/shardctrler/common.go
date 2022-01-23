package shardctrler

//
// Shard controler: assigns shards to replication groups.
//
// RPC interface:
// Join(servers) -- add a set of groups (gid -> server-list mapping).
// Leave(gids) -- delete a set of groups.
// Move(shard, gid) -- hand off one shard from current owner to gid.
// Query(num) -> fetch Config # num, or latest config if num==-1.
//
// A Config (configuration) describes a set of replica groups, and the
// replica group responsible for each shard. Configs are numbered. Config
// #0 is the initial configuration, with no groups and all shards
// assigned to group 0 (the invalid group).
//
// You will need to add fields to the RPC argument structs.
//

// The number of shards.
const NShards = 10

// A configuration -- an assignment of shards to groups.
// Please don't change this.
type Config struct {
	Num    int              // config number
	Shards [NShards]int     // shard -> gid
	Groups map[int][]string // gid -> servers[]
}

const (
	OK             = "OK"
	ErrWrongLeader = "ErrWrongLeader"
)

const (
	JOIN  = 0
	LEAVE = 1
	MOVE  = 3
	QUERY = 4
)

const (
	RpcTimeout       = 200
	NoLeader         = -1
	NoneSerialNumber = -1
	RequestInterval  = 50
)

type Err string

type JoinArgs struct {
	Servers      map[int][]string // new GID -> servers mappings
	SerialNumber int64            // client request id
	Id           string           // client id
}

type JoinReply struct {
	WrongLeader bool
	Err         Err
}

type LeaveArgs struct {
	GIDs         []int
	SerialNumber int64  // client request id
	Id           string // client id
}

type LeaveReply struct {
	WrongLeader bool
	Err         Err
}

type MoveArgs struct {
	Shard        int
	GID          int
	SerialNumber int64  // client request id
	Id           string // client id
}

type MoveReply struct {
	WrongLeader bool
	Err         Err
}

type QueryArgs struct {
	Num          int    // desired config number
	SerialNumber int64  // client request id
	Id           string // client id
}

type QueryReply struct {
	WrongLeader bool
	Err         Err
	Config      Config
}
