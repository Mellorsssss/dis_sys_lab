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
	defer rf.mu.Unlock()

	reply.Term = rf.term

	// update term(may change state of current node)
	// deal with the leaderID
	update := rf.updateTerm(args.Term)
	if update == SMALLER_TERM {
		return
	} else if update == EQ_TERM { // @TODO: brain-split, there could be multiple leader?
		if rf.leaderId == NONE_LEADER { // follow new leader
			rf.leaderId = args.ID
		} else if rf.leaderId != args.ID {
			Error("%v has another leader in term %v diff from %v", rf.me, rf.term, args.ID)
			return
		}
	} else {
		rf.leaderId = args.ID
	}

	if rf.snapshots.LastIncludedIndex >= args.LastLogInd {
		return
	}

	// rf.snapshots = SnapShotData{
	// 	LastIncludedIndex: args.LastLogInd,
	// 	LastIncludedTerm:  args.LastLogTerm,
	// 	Data:              args.Data,
	// }

	ind := rf.GetLogWithIndex(args.LastLogInd)
	if ind == -1 || ind+1 == len(rf.logs) {
		rf.logs = []Log{}
	} else {
		rf.logs = rf.logs[ind+1:]
	}

	// apply the snap shot
	rf.applySnapShot(args.Data, args.LastLogInd, args.LastLogTerm)
	// rf.lastApply = MaxInt(rf.lastApply, args.LastLogInd)
	// rf.commitInd = MaxInt(rf.commitInd, args.LastLogInd)
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
	ok := rf.peers[server].Call("Raft.InstallSnapShotRPC", args, reply)
	return ok
}

func (rf *Raft) InstallSnapShot(server, term, lastInd, lastTerm int) {
	for !rf.killed() {
		rf.mu.Lock()
		if !rf.IsStillLeader(term) {
			rf.mu.Unlock()
			return
		}

		if rf.snapshots.LastIncludedIndex != lastInd || rf.snapshots.LastIncludedTerm != lastTerm {
			Error("%v's snapshot has been updated from [index:%v, term:%v] to [index:%v, term:%v]", rf.me, lastInd, lastTerm, rf.snapshots.LastIncludedIndex, rf.snapshots.LastIncludedTerm)
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

		DPrintf("%v install snapshot[ind:%v, term:%v] in term %v to %v", rf.me, lastInd, lastTerm, term, server)
		ok := rf.sendInstallSnapShotRPC(server, &args, &reply)
		if !ok {
			continue
		}

		rf.mu.Lock()

		// check if still leader
		if !rf.IsStillLeader(args.Term) {
			rf.mu.Unlock()
			return
		}

		update := rf.updateTerm(reply.Term)
		if update != GREATER_TERM { // send snapshot successful
			rf.mu.Unlock()
			return
		}

		rf.mu.Unlock()
	}

}
