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
	// deal with the leaderID
	update := rf.updateTerm(args.Term)
	if update == SMALLER_TERM {
		reply.Success = false
		return
	} else if update == EQ_TERM { // @TODO: brain-split, there could be multiple leader?
		if rf.leaderId == NONE_LEADER { // follow new leader
			rf.leaderId = args.ID
			Info("%v think %v is the leader of term %v", rf.me, args.ID, rf.term)
		} else if rf.leaderId != args.ID {
			Error("%v has another leader in term %v diff from %v", rf.me, rf.term, args.ID)
			return
		}
	} else {
		rf.leaderId = args.ID
		Info("%v think %v is the leader of term %v", rf.me, args.ID, rf.term)
	}

	// check if rf has the log with PrevLogInd & PrevLogTerm
	var indMatch int
	if args.PrevLogInd == NONE_IND && args.PrevLogTerm == NONE_TERM {
		indMatch = 0
	} else {
		indMatch = rf.GetLogWithIndex(args.PrevLogInd)
		if indMatch == -1 {
			reply.Success = false
			DPrintf("%v has no log with index %v from leader %v in term %v", rf.me, args.PrevLogInd, args.ID, args.Term)
			return
		}

		if rf.logs[indMatch].Term != args.PrevLogTerm {
			reply.Success = false
			DPrintf("%v has with index %v diff from leader %v in term %v", rf.me, args.PrevLogInd, args.ID, args.Term)
			return
		}

		indMatch++
	}

	// check and append the logs to rf
	ind := 0
	for ; ind < len(args.Logs) && indMatch < len(rf.logs); ind++ {
		if args.Logs[ind].Index != rf.logs[indMatch].Index {
			Error("log mismatch between leader %v and %v in term %v", args.ID, rf.me, args.Term)
			reply.Success = false
			return
		}

		if args.Logs[ind].Term != rf.logs[indMatch].Term {
			rf.logs = rf.logs[0:indMatch]
			break
		}

		indMatch++
	}
	if ind < len(args.Logs) {
		rf.logs = append(rf.logs, args.Logs[ind:]...)
	}

	// update the commitInd
	if args.CommitInd > rf.commitInd {
		if len(args.Logs) == 0 { // TODO: may be should prove it?
			rf.commitInd = MaxInt(rf.commitInd, MinInt(args.CommitInd, args.PrevLogInd))
		} else {
			rf.commitInd = MinInt(rf.commitInd, args.Logs[len(args.Logs)-1].Index)
		}

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
			}
			rf.appCond.Signal()
			return
		}
	}

}

func (rf *Raft) agree(command interface{}) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	term := rf.term

	// send AE to all peers until all of them are sync to the leader
	// use goroutine to send AE until success
	for ind, _ := range rf.peers {
		_ind := ind
		if _ind == rf.me {
			continue
		}

		if !rf.IsStillLeader(term) {
			return
		}

		// check if there are logs to send
		if len(rf.logs) == 0 || rf.logs[len(rf.logs)-1].Index < rf.nextInd[_ind] {
			Error("%v has no log to send to %v", rf.me, _ind)
			continue
		}

		// get the logs to send
		indToSend := rf.GetLogWithIndex(rf.nextInd[_ind])
		if indToSend == -1 {
			return
		}
		logsToSend := append([]Log{}, rf.logs[indToSend:len(rf.logs)]...)

		// get the prevLog info
		var prevLogInd int
		var prevLogTerm int
		if indToSend == 0 {
			prevLogInd = NONE_IND
			prevLogTerm = NONE_TERM
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
		go func(i int, _rf *Raft, _args *AppendEntriesArgs) {
			for !_rf.killed() {
				_rf.mu.Lock()
				if !_rf.IsStillLeader(_args.Term) {
					_rf.mu.Unlock()
					return
				}
				_rf.mu.Unlock()

				_reply := &AppendEntriesReply{}
				ok := _rf.sendAppendEntries(i, _args, _reply)
				if !ok {
					continue
				}

				_rf.mu.Lock()
				if !_rf.IsStillLeader(_args.Term) {
					_rf.mu.Unlock()
					return
				}

				if _reply.Success {
					_rf.nextInd[i] = MaxInt(_args.Logs[len(_args.Logs)-1].Index+1, _rf.nextInd[i])
					_rf.matchInd[i] = MaxInt(_args.Logs[len(_args.Logs)-1].Index, _rf.matchInd[i])
					DPrintf("agree: leader %v update %v's nextInd, matchInd to [%v, %v]", _rf.me, i, _rf.nextInd[i], _rf.matchInd[i])
					// check if there is a chance to update the commit
					_rf.updateCommitIndexOfLeader()
					_rf.mu.Unlock()
					return
				}

				update := _rf.updateTerm(_reply.Term)
				if update != GREATER_TERM { // decrease the nextInd and retry
					if _rf.nextInd[i] != _args.Logs[0].Index{ // nextInd has changed
						_rf.mu.Unlock()
						continue
					}

					_rf.nextInd[i]--
					if _rf.nextInd[i] <= 0 {
						Error("%v has error in nextInd", _rf.me)
						_rf.mu.Unlock()
						return
					}
					// prepare the args again(since some information may changed)
					_indToSend := _rf.GetLogWithIndex(_rf.nextInd[i])
					if _indToSend == -1 {
						_rf.mu.Unlock()
						return
					}
					_args.Logs = append(_args.Logs[:0], _rf.logs[_indToSend:len(_rf.logs)]...)
					if _indToSend == 0 {
						_args.PrevLogInd, _args.PrevLogTerm = NONE_IND, NONE_TERM
					} else {
						_args.PrevLogInd, _args.PrevLogTerm = _rf.logs[_indToSend-1].Index, _rf.logs[_indToSend-1].Term
					}
				} else {
					_rf.mu.Unlock()
					return
				}
				_rf.mu.Unlock()
			}
		}(_ind, rf, args)
	}
}
