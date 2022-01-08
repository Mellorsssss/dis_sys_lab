package raft

// leader election

type InstallSnapShotArgs struct {
	Term        int
	ID          int
	LastLogInd  int
	LastLogTerm int
	Data        []byte
}

type InstallSnapShotReply struct {
	Term int
}

func (rf *Raft) InstallSnapShotRPC(args *InstallSnapShotArgs, reply *InstallSnapShotReply) {
	rf.mu.Lock()

	DPrintf("%v receive installsnapshot rpc from leader %v", rf.me, args.ID)
	reply.Term = rf.term

	// update term(may change state of current node)
	// deal with the leaderID
	update := rf.updateTerm(args.Term)
	if update == SMALLER_TERM {
		rf.mu.Unlock()
		return
	} else if update == EQ_TERM { // @TODO: brain-split, there could be multiple leader?
		if rf.leaderId == NONE_LEADER { // follow new leader
			rf.leaderId = args.ID
		} else if rf.leaderId != args.ID {
			Error("%v has another leader in term %v diff from %v", rf.me, rf.term, args.ID)
			rf.mu.Unlock()
			return
		}
	} else {
		rf.leaderId = args.ID
	}

	if rf.snapshots.LastIncludedIndex >= args.LastLogInd {
		rf.mu.Unlock()
		return
	}

	ind := rf.GetLogWithIndex(args.LastLogInd)
	if ind == -1 {
		rf.logs = []Log{}
	} else {
		rf.logs = rf.logs[ind+1:]
	}

	rf.mu.Unlock()
	// apply the snapshot
	rf.applySnapShot(args.Data, args.LastLogInd, args.LastLogTerm)
	<-rf.snapshotCh
	DPrintf("%v install snapshot success", rf.me)
}

// must hold rf.mu.Lock()
func (rf *Raft) applySnapShot(SnapShotData []byte, SnapShotIndex int, SnapShotTerm int) {
	rf.appCh <- ApplyMsg{
		false,
		nil,
		NONE_IND,
		true,
		SnapShotData,
		SnapShotTerm,
		SnapShotIndex,
	}
}

func (rf *Raft) sendInstallSnapShotRPC(server int, args *InstallSnapShotArgs, reply *InstallSnapShotReply) bool {
	if rf.killed() {
		return false
	}
	ok := rf.peers[server].Call("Raft.InstallSnapShotRPC", args, reply)
	if !PROFILE {
		return ok
	}
	rf.mu.Lock()
	rf.ISCount++
	rf.mu.Unlock()
	return ok
}

// InstallSnapShot sends install snapshot to server
// make sure only one goroutine is installing snapshot
// to prevent deadlock
func (rf *Raft) InstallSnapShot(server, term int) {
	rf.mu.Lock()
	if !rf.IsStillLeader(term) || rf.sending[server] {
		rf.mu.Unlock()
		return
	}
	rf.sending[server] = true
	rf.mu.Unlock()

	for !rf.killed() {
		rf.mu.Lock()
		if !rf.IsStillLeader(term) {
			rf.sending[server] = false
			rf.mu.Unlock()
			return
		}

		args := InstallSnapShotArgs{
			rf.term,
			rf.me,
			rf.snapshots.LastIncludedIndex,
			rf.snapshots.LastIncludedTerm,
			rf.snapshots.Data,
		}
		reply := InstallSnapShotReply{}
		rf.mu.Unlock()

		ok := rf.sendInstallSnapShotRPC(server, &args, &reply)
		if !ok {
			DPrintf("%v send installsnapshot rpc fail, re try", rf.me)
			continue
		}
		DPrintf("succ: %v install snapshot in term %v to %v", rf.me, term, server)

		rf.mu.Lock()

		// check if still leader
		if !rf.IsStillLeader(args.Term) {
			rf.sending[server] = false
			rf.mu.Unlock()
			return
		}

		update := rf.updateTerm(reply.Term)
		if update != GREATER_TERM { // send snapshot successful
			rf.nextInd[server] = MaxInt(rf.nextInd[server], rf.snapshots.LastIncludedIndex+1)
			rf.matchInd[server] = MaxInt(rf.matchInd[server], rf.snapshots.LastIncludedIndex)
			rf.sending[server] = false
			rf.mu.Unlock()
			return
		}

		rf.mu.Unlock()
	}
}
