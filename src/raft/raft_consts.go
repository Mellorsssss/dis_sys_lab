package raft

const (
	INIT_LOG_SIZE    = 16
	NONE_VOTE        = -1
	NONE_IND         = 0
	NONE_TERM        = 0
	ELECTION_MIN     = 200
	ELECTION_MAX     = 300
	HEARTBEAT_DUR    = 150
	NONE_LEADER      = -1
	SNAPSHOTINTERVAL = 100
	RPLICATE_DUR     = 100
	AEBUFFER_LEN     = 1
	AEBATCH_SIZE     = 3
)

const (
	GREATER_TERM = 0
	EQ_TERM      = 1
	SMALLER_TERM = 2
)
