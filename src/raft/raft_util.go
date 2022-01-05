package raft

// GetLastLogInfo return the term and index of last log
// must hold rf.mu.Lock()
func (rf *Raft) GetLastLogInfo() (int, int) {
	if len(rf.logs) == 0 {
		return rf.snapshots.LastIncludedTerm, rf.snapshots.LastIncludedIndex
	}

	return rf.logs[len(rf.logs)-1].Term, rf.logs[len(rf.logs)-1].Index
}

// GetLastLogIndex return the index of last log
// must hold the rf.mu.Lock()
func (rf *Raft) GetLastLogIndex() int {
	if len(rf.logs) == 0 {
		return rf.snapshots.LastIncludedIndex
	}

	return rf.logs[len(rf.logs)-1].Index
}

// GetLogWithIndex return the ind in logs of the log
// with index
// must hold the rf.mu.Lock()
// return the index in rf.logs of log with index
// return -2 if not match
func (rf *Raft) GetLogWithIndex(index int) int {
	// @TODO: optimize the search algorithm
	if len(rf.logs) == 0 {
		return -1
	}

	for ind, v := range rf.logs {
		if v.Index == index {
			return ind
		} else if v.Index > index {
			return -1
		}
	}

	return -1
}

// IsStillLeader checks if still the leader in this term
// must hold the rf.mu.Lock()
// check if rf is still the leader and in the same term
func (rf *Raft) IsStillLeader(term int) bool {
	return rf.leaderId == rf.me && rf.term == term && !rf.killed()
}

// GetLogToSend return the ind(in the logs), index and term of the
// log to be sent to server
// must hold rf.mu.Lock()
func (rf *Raft) GetLogToSend(server int) (int, int, int) {
	indToSend := rf.GetLogWithIndex(rf.nextInd[server])
	if indToSend == -1 {
		if len(rf.logs) != 0 {
			// check if log to send has been covered by the snapshot
			if rf.snapshots.LastIncludedIndex >= rf.nextInd[server] {
				return -1, rf.snapshots.LastIncludedIndex, rf.snapshots.LastIncludedTerm
			}
			return -1, rf.logs[len(rf.logs)-1].Index, rf.logs[len(rf.logs)-1].Term
		} else {
			return -1, rf.snapshots.LastIncludedIndex, rf.snapshots.LastIncludedTerm
		}
	}
	// get the prevLog info
	var prevLogInd int
	var prevLogTerm int
	if indToSend == 0 {
		prevLogInd = rf.snapshots.LastIncludedIndex
		prevLogTerm = rf.snapshots.LastIncludedTerm
	} else {
		prevLogInd, prevLogTerm = rf.logs[indToSend-1].Index, rf.logs[indToSend-1].Term
	}

	return indToSend, prevLogInd, prevLogTerm
}

// updateTerm try to update current term
// MUST HOLD rf.mu.Lock()
// called when the term of current raft peer is
// updated
// clear the voteFor, and reboot the ticker if
// leader
func (rf *Raft) updateTerm(newTerm int) int {
	if newTerm < rf.term {
		return SMALLER_TERM
	}

	if newTerm == rf.term {
		return EQ_TERM
	}
	DPrintf("%v 's term get updated from %v to %v", rf.me, rf.term, newTerm)
	rf.term = newTerm
	rf.vote = NONE_VOTE
	rf.persist()

	if rf.leaderId == rf.me {
		rf.leaderId = NONE_LEADER
		DPrintf("%v down to a follower in term %v", rf.me, rf.term)
		go rf.ticker()
	} else {
		rf.leaderId = NONE_LEADER
		DPrintf("%v has no leader in term %v", rf.me, rf.term)
	}

	return GREATER_TERM
}
