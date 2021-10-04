package raft

const (
	INIT_LOG_SIZE = 16
	NONE_VOTE     = -1
	NONE_IND      = 0
	NONE_TERM     = 0
	ELECTION_MIN  = 150
	ELECTION_MAX  = 300
	HEARTBEAT_DUR = 110
)
