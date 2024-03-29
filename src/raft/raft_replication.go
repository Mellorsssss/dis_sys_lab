package raft

// log replication

type AppendEntriesArgs struct {
	Term        int
	ID          int
	PrevLogInd  int
	PrevLogTerm int
	Logs        []Log
	CommitInd   int
}

type AppendEntriesReply struct {
	Term    int
	Success bool

	// valid if there is conflict
	CIndex int // first index store for the conflicting term
	CTerm  int // conflicting term
}

// getCurrentTermFirstLog return the index of the ind of first log
// in current term
// must hold rf.mu.Lock
func (rf *Raft) getCurrentTermFirstLog(pos int) int {
	i := pos
	for ; i > 0; i-- {
		if rf.logs[i].Term != rf.logs[i-1].Term {
			return rf.logs[i].Index
		}
	}

	return rf.logs[i].Index
}

// getLastLogInTerm returns the position of the first log entry after term
// return -1 if there isn't such a log
// must hold rf.mu.Lock()
func (rf *Raft) getFirstLogAfterTerm(term int) int {
	if rf.snapshots.LastIncludedTerm > term {
		return -1
	}

	// corner case of snapshot
	if rf.snapshots.LastIncludedIndex == term && len(rf.logs) != 0 && rf.logs[0].Term != term {
		return 0
	}

	for ind := 1; ind < len(rf.logs); ind++ {
		if rf.logs[ind-1].Term == term && rf.logs[ind].Term != term {
			return ind
		}
	}

	return -1
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if len(args.Logs) != 0 { // filter heartbeat
		DPrintf("AE: leader %v to follower %v [prevInd: %v, prevTerm: %v, commitID:%v]", args.ID, rf.me, args.PrevLogInd, args.PrevLogTerm, args.CommitInd)
	}
	reply.Term = rf.term
	reply.CIndex = NONE_IND
	reply.CTerm = NONE_TERM

	// 1. update term(may change state of current node)
	// deal with the leaderID
	update := rf.updateTerm(args.Term)
	if update == SMALLER_TERM {
		reply.Success = false
		return
	} else if update == EQ_TERM { // @TODO: brain-split, there could be multiple leader?
		if rf.leaderId == NONE_LEADER { // follow new leader
			rf.leaderId = args.ID
		} else if rf.leaderId != args.ID {
			Error("AE: %v has another leader in term %v diff from %v", rf.me, rf.term, args.ID)
			reply.Success = false
			return
		}
	} else {
		rf.leaderId = args.ID
	}
	// only notify when rf makes sure leader sends AE
	rf.NotifyMsg()
	DPrintf("AE: follower %v get from leader %v", rf.me, args.ID)

	// 2. check if rf has the log with PrevLogInd & PrevLogTerm
	var indMatch int
	var ind int
	if args.PrevLogInd == rf.snapshots.LastIncludedIndex && args.PrevLogTerm == rf.snapshots.LastIncludedTerm {
		indMatch = 0
		ind = 0
	} else if args.PrevLogInd < rf.snapshots.LastIncludedIndex {
		// find the first log match
		DPrintf("AE: prefix in the %v's snapshot[index:%v]", rf.me, rf.snapshots.LastIncludedIndex)
		indMatch = 0
		ind = -1

		// align to the same log
		for _ind, log := range args.Logs {
			if log.Index == rf.snapshots.LastIncludedIndex {
				ind = _ind + 1
				break
			}
		}

		if ind == -1 { // snapshot already have all the logs
			DPrintf("AE: %v's snapshot already has all the logs from %v", rf.me, args.ID)
			reply.Success = true
			return
		}
	} else {
		indMatch = rf.GetLogWithIndex(args.PrevLogInd)
		if indMatch == -1 {
			reply.Success = false
			reply.CIndex = rf.GetLastLogIndex() + 1
			Error("AE: %v has no log with index %v(last is %v) from leader %v in term %v", rf.me, args.PrevLogInd, reply.CIndex-1, args.ID, args.Term)
			return
		}

		if rf.logs[indMatch].Term != args.PrevLogTerm {
			reply.Success = false
			reply.CIndex = rf.getCurrentTermFirstLog(indMatch)
			reply.CTerm = rf.logs[indMatch].Term
			DPrintf("AE: %v has with index %v diff from leader %v in term %v", rf.me, args.PrevLogInd, args.ID, args.Term)
			return
		}

		indMatch++
		ind = 0
	}

	// 3. append the logs and remove the conflict logs
	for ; ind < len(args.Logs) && indMatch < len(rf.logs); ind++ {
		if args.Logs[ind].Index != rf.logs[indMatch].Index {
			DPrintf("AE: log mismatch between leader %v(%v) and %v(%v) in term %v", args.ID, args.Logs[ind].Index, rf.me, rf.logs[indMatch].Index, args.Term)
			reply.Success = false
			reply.CIndex = rf.getCurrentTermFirstLog(ind)
			reply.CTerm = rf.logs[indMatch].Term
			return
		}

		if args.Logs[ind].Term != rf.logs[indMatch].Term {
			rf.logs = rf.logs[0:indMatch]
			rf.persist()
			break
		}

		indMatch++
	}
	if ind < len(args.Logs) {
		rf.logs = append(rf.logs, args.Logs[ind:]...)
		rf.persist()
	}

	// 4. update the commitInd
	if args.CommitInd > rf.commitInd {
		oldCommitInd := rf.commitInd
		if len(args.Logs) == 0 { // TODO: may be should prove it?
			rf.commitInd = MaxInt(rf.commitInd, MinInt(args.CommitInd, args.PrevLogInd))
		} else {
			rf.commitInd = MaxInt(rf.commitInd, MinInt(args.Logs[len(args.Logs)-1].Index, args.CommitInd))
		}

		DPrintf("AE: follower %v change commitId from %v to %v(compared to %v)", rf.me, oldCommitInd, rf.commitInd, args.CommitInd)
		rf.appCond.Signal() // check if should apply msg
	}
	reply.Success = true
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	if rf.killed() {
		return false
	}
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	if !PROFILE {
		return ok
	}
	rf.mu.Lock()
	rf.AECount++
	rf.mu.Unlock()
	return ok
}

/*
   must hold rf.mu.Lock()
   should only be called by leader
*/
func (rf *Raft) updateCommitIndexOfLeader() {
	// TODO: optimize the impletation

	if len(rf.logs) == 0 || rf.logs[len(rf.logs)-1].Term != rf.term {
		return
	}

	thresh := len(rf.peers)/2 + 1
	for ind := len(rf.logs) - 1; ind >= 0 && rf.logs[ind].Term == rf.term; ind-- {
		tot := 1
		for server, _ := range rf.peers {
			if server == rf.me {
				continue
			}
			if rf.matchInd[server] >= rf.logs[ind].Index {
				tot++
			}

			if tot >= thresh {
				break
			}
		}

		if tot >= thresh {
			if rf.logs[ind].Index > rf.commitInd {
				DPrintf("leader %v commit change from %v to %v", rf.me, rf.commitInd, rf.logs[ind].Index)
				rf.commitInd = rf.logs[ind].Index
				rf.appCond.Signal()
				rf.TriggerAppendToAll() // commit udpate info
			}
			return
		}
	}

}

func (rf *Raft) replicateOnCommand(server, term int) {
	// only one thread sends log to prevent redundant rpcs
	rf.mu.Lock()
	logLen := len(rf.logs)
	ssLastInd := rf.snapshots.LastIncludedIndex
	if !rf.IsStillLeader(term) /*|| len(rf.logs) != logLen || rf.snapshots.LastIncludedIndex != ssLastInd */ {
		rf.mu.Unlock()
		return
	}

	// follower is lagging, send the snapshot
	if rf.snapshots.LastIncludedIndex >= rf.nextInd[server] {
		Info("leader %v send snapshot to %v", rf.me, server)
		go rf.InstallSnapShot(server, term, rf.snapshots.LastIncludedIndex, rf.snapshots.LastIncludedTerm)
		rf.mu.Unlock()
		return
	}

	var prevLogInd int
	var prevLogTerm int
	logsToSend := []Log{}

	// check if there are logs to send
	if len(rf.logs) != 0 && rf.logs[len(rf.logs)-1].Index < rf.nextInd[server] || len(rf.logs) == 0 {
		prevLogTerm, prevLogInd = rf.GetLastLogInfo()
	} else { // may be some log to send
		indToSend := rf.GetLogWithIndex(rf.nextInd[server])
		if indToSend == -1 { // no log to send now
			prevLogInd, prevLogTerm = rf.GetLastLogInfo()
		} else {
			logsToSend = append([]Log{}, rf.logs[indToSend:len(rf.logs)]...)

			// get the prevLog info
			if indToSend == 0 {
				prevLogInd = rf.snapshots.LastIncludedIndex
				prevLogTerm = rf.snapshots.LastIncludedTerm
			} else {
				prevLogInd, prevLogTerm = rf.logs[indToSend-1].Index, rf.logs[indToSend-1].Term
			}
		}

	}

	args := &AppendEntriesArgs{
		term,
		rf.me,
		prevLogInd,
		prevLogTerm,
		logsToSend,
		rf.commitInd,
	}

	reply := &AppendEntriesReply{}
	rf.mu.Unlock()

	DPrintf("AE: leader %v begin to send AE to %v in term %v", rf.me, server, term)

	ok := rf.sendAppendEntries(server, args, reply)
	if !ok { // re-try in next iter
		Error("AE: leader %v sends AE to %v fail, re try", rf.me, server)
		rf.TriggerAppend(server) // re try
		return
	}

	rf.mu.Lock()
	if !rf.IsStillLeader(term) || len(rf.logs) != logLen || rf.snapshots.LastIncludedIndex != ssLastInd {
		rf.mu.Unlock()
		return
	}

	if reply.Success {
		// update nextInd and maxInd to the biggest possible one
		if len(args.Logs) > 0 {
			rf.nextInd[server] = MaxInt(args.Logs[len(args.Logs)-1].Index+1, rf.nextInd[server])
			rf.matchInd[server] = MaxInt(args.Logs[len(args.Logs)-1].Index, rf.matchInd[server])
		} else {
			rf.matchInd[server] = MaxInt(args.PrevLogInd, rf.matchInd[server])
			rf.nextInd[server] = MaxInt(args.PrevLogInd+1, rf.nextInd[server])
		}

		DPrintf("AE: leader %v update %v's nextInd, matchInd to [%v, %v]", rf.me, server, rf.nextInd[server], rf.matchInd[server])

		// check if there is a chance to update the commit
		rf.updateCommitIndexOfLeader()

		// finish send log, just return
		rf.mu.Unlock()
		return
	}

	update := rf.updateTerm(reply.Term)
	if update != GREATER_TERM { // decrease the nextInd and retry
		if reply.CTerm == NONE_TERM { // no conflicting term
			rf.nextInd[server] = MinInt(reply.CIndex, rf.nextInd[server])
		} else {
			logInd := rf.getFirstLogAfterTerm(reply.CTerm)
			if logInd != -1 {
				rf.nextInd[server] = MinInt(logInd, rf.nextInd[server])
			} else {
				rf.nextInd[server] = MinInt(reply.CIndex, rf.nextInd[server])
			}
		}
		if rf.nextInd[server] <= 0 {
			Error("%v has error in nextInd", rf.me)
			rf.mu.Unlock()
			return
		}

		// re-try in next iter
		rf.mu.Unlock()
		// time.Sleep(time.Duration(RPLICATE_DUR) * time.Millisecond)
		rf.TriggerAppend(server) // re try
	} else {
		rf.mu.Unlock()
		return
	}
}

func (rf *Raft) TriggerAppend(server int) {
	select {
	case rf.AEChs[server] <- struct{}{}:
		return
	default:
		return
	}
}

func (rf *Raft) TriggerAppendToAll() {
	for ind := range rf.peers {
		if ind == rf.me {
			continue
		}

		rf.TriggerAppend(ind)
	}
}
