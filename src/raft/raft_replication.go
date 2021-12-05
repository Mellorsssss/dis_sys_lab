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

	// update term(may change state of current node)
	update := rf.updateTerm(args.Term)
	if update == SMALLER_TERM {
		reply.Success = false
		return
	} else if update == EQ_TERM { // @TODO: brain-split, there could be multiple leader?
		if rf.leaderId == NONE_LEADER { // follow new leader
			rf.leaderId = args.ID
			Info("%v think %v is the leader of term %v", rf.me, args.ID, rf.term)
		}
	} else {
		rf.leaderId = args.ID
		Info("%v think %v is the leader of term %v", rf.me, args.ID, rf.term)
	}

	// @TODO: 2B
	reply.Success = true
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}
