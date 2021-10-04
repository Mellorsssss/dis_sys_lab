package raft

// log replication
type AppendEntriesArgs struct {
	// Your data here (2A, 2B).
	Term        int
	ID          int
	PrevLogInd  int
	PrevLogTerm int
	Logs        []Log
	CommitInd   int
}

type AppendEntriesReply struct {
	// Your data here (2A).
	Term    int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.NotifyMsg()
	reply.Term = rf.term

	update := rf.updateTerm(args.Term)
	if !update {
		reply.Success = false
		return
	}

	// @TODO: 2B
	reply.Success = true
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}
