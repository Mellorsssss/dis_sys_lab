package raft

import "time"

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
	if rf.snapshots.LastIncludedTerm > term{
		return -1
	}

	// corner case of snapshot
	if rf.snapshots.LastIncludedIndex == term && len(rf.logs) != 0 && rf.logs[0].Term != term {
		return 0
	}

	for ind := 1; ind < len(rf.logs); ind++ {
		if rf.logs[ind-1].Term == term && rf.logs[ind].Term != term{
			return ind
		}
	}

	return -1
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if len(args.Logs) != 0 { // filter heartbeat
		DPrintf("leader %v send AE to follower %v [prevInd: %v, prevTerm: %v, commitID:%v]", args.ID, rf.me, args.PrevLogInd, args.PrevLogTerm, args.CommitInd)
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
			Error("%v has another leader in term %v diff from %v", rf.me, rf.term, args.ID)
			reply.Success = false
			return
		}
	} else {
		rf.leaderId = args.ID
	}
	// only notify when rf makes sure leader sends AE
	rf.NotifyMsg()

	// 2. check if rf has the log with PrevLogInd & PrevLogTerm
	var indMatch int
	var ind int
	if args.PrevLogInd == rf.snapshots.LastIncludedIndex && args.PrevLogTerm == rf.snapshots.LastIncludedTerm {
		indMatch = 0
		ind = 0
	} else if args.PrevLogInd < rf.snapshots.LastIncludedIndex {
		// find the first log match
		DPrintf("prefix in the %v's snapshot[index:%v]", rf.me, rf.snapshots.LastIncludedIndex)
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
			DPrintf("%v's snapshot already has all the logs from %v", rf.me, args.ID)
			reply.Success = true
			return
		}
	} else {
		indMatch = rf.GetLogWithIndex(args.PrevLogInd)
		if indMatch == -1 {
			reply.Success = false
			reply.CIndex = rf.GetLastLogIndex() + 1
			Error("%v has no log with index %v(last is %v) from leader %v in term %v", rf.me, args.PrevLogInd, reply.CIndex-1, args.ID, args.Term)
			return
		}

		if rf.logs[indMatch].Term != args.PrevLogTerm {
			reply.Success = false
			reply.CIndex = rf.getCurrentTermFirstLog(indMatch)
			reply.CTerm = rf.logs[indMatch].Term
			DPrintf("%v has with index %v diff from leader %v in term %v", rf.me, args.PrevLogInd, args.ID, args.Term)
			return
		}

		indMatch++
		ind = 0
	}

	// 3. append the logs and remove the conflict logs
	for ; ind < len(args.Logs) && indMatch < len(rf.logs); ind++ {
		if args.Logs[ind].Index != rf.logs[indMatch].Index {
			DPrintf("log mismatch between leader %v(%v) and %v(%v) in term %v", args.ID, args.Logs[ind].Index, rf.me, rf.logs[indMatch].Index, args.Term)
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
		if len(args.Logs) == 0 { // TODO: may be should prove it?
			rf.commitInd = MaxInt(rf.commitInd, MinInt(args.CommitInd, args.PrevLogInd))
		} else {
			rf.commitInd = MaxInt(rf.commitInd, MinInt(args.Logs[len(args.Logs)-1].Index, args.CommitInd))
		}

		DPrintf("follower %v change commitId to %v(compared to %v)", rf.me, rf.commitInd, args.CommitInd)
		rf.appCond.Signal() // check if should apply msg
	}
	reply.Success = true
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
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
			}
			return
		}
	}

}

func (rf *Raft) agree(command interface{}) {
	rf.mu.Lock()
	term := rf.term
	rf.mu.Unlock()

	// send AE to all peers until all of them are sync to the leader
	// use goroutine to send AE until success
	for ind := range rf.peers {
		_ind := ind
		if _ind == rf.me {
			continue
		}

		go rf.replicateOnCommand(_ind, term, true)
		continue
	}
}

// precondition: sending[server] == false
// postcondition: sending[server] == false
func (rf *Raft) replicateOnCommand(server, term int, retry bool) {
	//rf.mu.Lock()
	//if rf.sending[server] {
	//	rf.mu.Unlock()
	//	return
	//}
	//rf.sending[server] = true
	//rf.mu.Unlock()

	// unique context of this goroutine
	rf.mu.Lock()
	logLen := len(rf.logs)
	ssLastInd := rf.snapshots.LastIncludedIndex
	ssLastTerm := rf.snapshots.LastIncludedTerm
	rf.mu.Unlock()

	// only one thread sends log to prevent redundant rpcs
	for !rf.killed() && retry{
		rf.mu.Lock()
		if !rf.IsStillLeader(term) || len(rf.logs) != logLen || rf.snapshots.LastIncludedIndex != ssLastInd {
			rf.sending[server] = false
			rf.mu.Unlock()
			return
		}

		// check if there are logs to send
		if len(rf.logs) != 0 && rf.logs[len(rf.logs)-1].Index < rf.nextInd[server] {
			DPrintf("%v has no log to send to %v", rf.me, server)
			rf.sending[server] = false
			rf.mu.Unlock()
			return
		}

		// follower is lagging, send the snapshot
		if rf.snapshots.LastIncludedIndex >= rf.nextInd[server] {
			DPrintf("%v sends snapshot to %v, because sp lastInd:%v, nextInd[%v] = %v", rf.me, server, rf.snapshots.LastIncludedIndex, server, rf.nextInd[server])
			rf.mu.Unlock()
			rf.InstallSnapShot(server, term, ssLastInd, ssLastTerm)
			// @TODO: improve the performance? time.sleep is not so elegant
			continue
		}

		// no logs to send
		if len(rf.logs) == 0 {
			DPrintf("%v has no log to send to %v", rf.me, server)
			rf.sending[server] = false
			rf.mu.Unlock()
			return
		}

		// get the logs to send
		indToSend := rf.GetLogWithIndex(rf.nextInd[server])
		if indToSend == -1 { // no log to send now
			rf.sending[server] = false
			rf.mu.Unlock()
			return
		}
		logsToSend := append([]Log{}, rf.logs[indToSend:len(rf.logs)]...)

		// get the prevLog info
		var prevLogInd int
		var prevLogTerm int
		if indToSend == 0 {
			prevLogInd = rf.snapshots.LastIncludedIndex
			prevLogTerm = rf.snapshots.LastIncludedTerm
		} else {
			prevLogInd, prevLogTerm = rf.logs[indToSend-1].Index, rf.logs[indToSend-1].Term
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

		DPrintf("leader %v try to send AE to %v", rf.me, server)

		ok := rf.sendAppendEntries(server, args, reply)
		if !ok { // re-try in next iter
			Error("leader %v sends AE to %v fail, re try", rf.me, server)
			time.Sleep(time.Duration(RPLICATE_DUR) * time.Millisecond)
			continue
		}

		rf.mu.Lock()
		if !rf.IsStillLeader(term) /* || len(rf.logs) != logLen || rf.snapshots.LastIncludedIndex != ssLastInd */ {
			rf.sending[server] = false
			rf.mu.Unlock()
			return
		}

		if reply.Success {
			// update nextInd and maxInd to the biggest possible one
			rf.nextInd[server] = MaxInt(args.Logs[len(args.Logs)-1].Index+1, rf.nextInd[server])
			rf.matchInd[server] = MaxInt(args.Logs[len(args.Logs)-1].Index, rf.matchInd[server])
			DPrintf("agree: leader %v update %v's nextInd, matchInd to [%v, %v]", rf.me, server, rf.nextInd[server], rf.matchInd[server])

			// check if there is a chance to update the commit
			rf.updateCommitIndexOfLeader()

			// finish send log, just return
			rf.sending[server] = false
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
				rf.sending[server] = false
				rf.mu.Unlock()
				return
			}

			// re-try in next iter
			rf.mu.Unlock()
			// time.Sleep(time.Duration(RPLICATE_DUR) * time.Millisecond)
			select {
			case rf.AEChs[server] <- struct{}{}:
				continue
			default:
				continue
			}
		} else {
			rf.sending[server] = false
			rf.mu.Unlock()
			return
		}
	}
}
