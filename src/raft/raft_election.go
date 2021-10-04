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
	if !update {
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

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) startNewElection() {
	rf.mu.Lock()
	rf.term++
	term := rf.term
	rf.vote = rf.me
	rf.mu.Unlock()

	voteCount := 1
	voteTot := len(rf.peers) - 1
	leaderThresh := len(rf.peers)/2 + 1 // more than half

	rf.mu.Lock()
	DPrintf("[INFO] peer %v start election in term %v", rf.me, rf.term)
	rf.mu.Unlock()
	// send RV to all the peers
	notifyVote := make(chan RequestVoteReply, len(rf.peers)-1)

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
				DPrintf("[FAILURE] RV from %v to %v fails", sender.me, i)
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
			rf.isLeader = true
			go rf.heartbeat() // heartbeat
		}
		rf.mu.Unlock()
		voteTot--
		if voteTot <= 0 { // alreday get all votes
			break
		}
	}
	DPrintf("[INFO] %v get %v votes in term %v", rf.me, voteCount, term)
}
