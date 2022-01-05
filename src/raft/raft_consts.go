package raft

const (
	INIT_LOG_SIZE    = 16
	NONE_VOTE        = -1
	NONE_IND         = 0
	NONE_TERM        = 0
	ELECTION_MIN     = 150
	ELECTION_MAX     = 300
	HEARTBEAT_DUR    = 110
	NONE_LEADER      = -1
	SNAPSHOTINTERVAL = 100
	RPLICATE_DUR     = 100
	AEBUFFER_LEN = 10
)

const (
	GREATER_TERM = 0
	EQ_TERM      = 1
	SMALLER_TERM = 2
)
