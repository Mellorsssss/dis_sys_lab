package raft

// leader election

type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term        int
	ID          int
	LastLogInd  int
	LastLogTerm int
}

type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.term
	update := rf.updateTerm(args.Term)
	if update == SMALLER_TERM {
		reply.VoteGranted = false
		return
	}

	if rf.vote == NONE_VOTE || rf.vote == args.ID {
		var LastLogInd int
		var LastLogTerm int
		if len(rf.logs) == 0 {
			LastLogInd = NONE_IND
			LastLogTerm = NONE_VOTE
		} else {
			LastLogInd = rf.logs[len(rf.logs)-1].Index
			LastLogTerm = rf.logs[len(rf.logs)-1].Term
		}

		if args.LastLogTerm > LastLogTerm || (args.LastLogTerm == LastLogTerm && args.LastLogInd >= LastLogInd) {
			reply.VoteGranted = true
			rf.vote = args.ID
			rf.NotifyMsg()
		} else {
			reply.VoteGranted = false
		}
	}
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) startNewElection() {
	// inc term, reset leaderId, vote itself
	rf.mu.Lock()
	rf.term++
	rf.leaderId = NONE_LEADER
	term := rf.term // check if term has changed later
	rf.vote = rf.me
	rf.mu.Unlock()

	voteCount := 1
	voteTot := len(rf.peers) - 1
	leaderThresh := len(rf.peers)/2 + 1 // more than half

	rf.mu.Lock()
	Info("peer %v start election in term %v", rf.me, rf.term)
	rf.mu.Unlock()

	// send RV to all the peers
	notifyVote := make(chan RequestVoteReply, len(rf.peers)-1) // let GC close it

	lastTerm, lastInd := rf.GetLastLogInfo() // shouldn't receive any new log
	args := &RequestVoteArgs{
		term,
		rf.me,
		lastTerm,
		lastInd,
	}
	for ind, _ := range rf.peers {
		if ind == rf.me {
			continue
		}

		reply := &RequestVoteReply{}
		_ind := ind
		go func(sender *Raft, i int, ch chan<- RequestVoteReply, args *RequestVoteArgs, reply *RequestVoteReply, ind int) {
			ok := sender.sendRequestVote(i, args, reply)
			if !ok {
				Error("RV from %v to %v fails", sender.me, i)
				ch <- RequestVoteReply{NONE_TERM, false}
				return
			}

			ch <- *reply
		}(rf, _ind, notifyVote, args, reply, rf.me)
	}

	// count votes
	for v := range notifyVote {
		if v.VoteGranted {
			voteCount++
		}

		rf.mu.Lock()
		if rf.term != term { // term has changed, shouldn't election
			rf.mu.Unlock()
			return
		}

		if v.Term > rf.term { // update term and become follower
			rf.term = v.Term
			rf.mu.Unlock()
			return
		}

		if voteCount >= leaderThresh { // become leader, timer will be shut
			rf.leaderId = rf.me
			go rf.heartbeat() // heartbeat
			Info("%v is leader in term %v", rf.me, term)
			rf.mu.Unlock()
			return
		}
		rf.mu.Unlock()
		voteTot--
		if voteTot <= 0 { // alreday get all votes
			break
		}
	}
	Info("%v get %v votes in term %v", rf.me, voteCount, term)
}
