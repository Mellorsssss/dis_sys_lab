package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"bytes"

	"6.824/labgob"

	//	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labrpc"
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type Log struct {
	Index   int         // index for compaction since logs will be truncted
	Term    int         // log term
	Command interface{} // command content
}

type SnapShotData struct {
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	/* persistent data */
	logs []Log // logs
	term int   // cur term
	vote int   // vote for

	/* violate data */
	commitInd int // commitIndex
	lastApply int // last applied

	/* apply msg notify channel */
	appCond sync.Cond
	appCh   chan ApplyMsg

	nextInd  []int // next Indexes
	matchInd []int // match Indexes

	leaderId int // id of current term's leader

	/* notifyMsg for AE or RV */
	notifyMsg chan struct{} // notify when recv RE/RV
	sending   []bool        // indicating if try to replicate to server

	snapshots SnapShotData // snapshot data
}

// GetState return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.killed() {
		return NONE_TERM, false
	}

	return rf.term, rf.leaderId == rf.me
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	buffer := new(bytes.Buffer)
	enc := labgob.NewEncoder(buffer)
	enc.Encode(rf.term)
	enc.Encode(rf.vote)
	enc.Encode(rf.logs)
	data := buffer.Bytes()

	sbuffer := new(bytes.Buffer)
	senc := labgob.NewEncoder(sbuffer)
	senc.Encode(rf.snapshots)
	sdata := sbuffer.Bytes()

	rf.persister.SaveStateAndSnapshot(data, sdata)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte, sdata []byte) {
	if data == nil || len(data) < 1 || sdata == nil { // bootstrap without any state?
		rf.term = NONE_TERM
		rf.vote = NONE_VOTE
		rf.logs = []Log{}
		rf.snapshots = SnapShotData{
			LastIncludedIndex: NONE_IND,
			LastIncludedTerm:  NONE_TERM,
			Data:              []byte{},
		}
		return
	}
	buffer := bytes.NewBuffer(data)
	dec := labgob.NewDecoder(buffer)
	var term int
	var vote int
	var logs []Log

	if dec.Decode(&term) != nil {
		Error("Decode term error.")
		return
	}

	if dec.Decode(&vote) != nil {
		Error("Decode vote error.")
		return
	}

	if dec.Decode(&logs) != nil {
		Error("Decode logs error.")
		return
	}

	rf.term = term
	rf.vote = vote
	rf.logs = logs

	sbuffer := bytes.NewBuffer(sdata)
	var snapshot SnapShotData
	sdec := labgob.NewDecoder(sbuffer)
	if sdec.Decode(&snapshot) != nil {
		Error("Decode snapshot error")
		return
	}

	rf.snapshots = snapshot
}

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {
	Error("CondInstallSnapshot is called")
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.snapshots.LastIncludedIndex >= lastIncludedIndex { // has newer snapshot
		return false
	}

	if rf.lastApply > lastIncludedIndex { // apply new msg
		return false
	}

	rf.snapshots = SnapShotData{
		LastIncludedIndex: lastIncludedIndex,
		LastIncludedTerm:  lastIncludedTerm,
		Data:              snapshot,
	}

	ind := rf.GetLogWithIndex(lastIncludedIndex)
	if ind == -1 || ind+1 == len(rf.logs) {
		rf.logs = []Log{}
	} else {
		rf.logs = rf.logs[ind+1:]
	}

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	DPrintf("snapshot is called %v to %v", index, rf.me)
	rf.mu.Lock()
	DPrintf("%v get lock in snapshot", rf.me)
	defer rf.mu.Unlock()
	if len(rf.logs) == 0 || rf.logs[len(rf.logs)-1].Index < index {
		Error("No index match when installing snapshot.")
		rf.snapshots = SnapShotData{
			LastIncludedIndex: index,
			LastIncludedTerm:  0,
			Data:              snapshot,
		}
		return
	}

	ind := rf.GetLogWithIndex(index)
	if ind == -1 {
		Error("Install an old snapshot")
	}
	rf.snapshots = SnapShotData{
		LastIncludedIndex: index,
		LastIncludedTerm:  rf.logs[ind].Term,
		Data:              snapshot,
	}
	DPrintf("%v install snapshot[index:%v, term:%v]", rf.me, index, rf.snapshots.LastIncludedTerm)

	if ind+1 != len(rf.logs) {
		rf.logs = rf.logs[ind+1:] // trim logs
	}
	rf.commitInd = MaxInt(rf.commitInd, index)
	rf.lastApply = MaxInt(rf.lastApply, index)
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1

	// Your code here (2B).
	term, isLeader := rf.GetState()
	if !isLeader {
		return index, term, isLeader
	}

	rf.mu.Lock()
	DPrintf("%v start %v", rf.me, command)
	// append command to leader's own logs
	newLog := Log{}
	if len(rf.logs) == 0 {
		index = rf.snapshots.LastIncludedIndex + 1
	} else {
		index = rf.logs[len(rf.logs)-1].Index + 1
	}
	newLog.Index = index
	newLog.Command = command
	newLog.Term = term

	rf.logs = append(rf.logs, newLog)

	rf.persist()

	rf.mu.Unlock()

	go rf.agree(command)

	return index, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
// when leader becomes a follower, it needs to call ticker() mannually
func (rf *Raft) ticker() {
	rand.Seed(time.Now().UnixNano())
	max := ELECTION_MAX
	min := ELECTION_MIN
	for !rf.killed() {
		// leader doesn't need timer
		timeToSleep := rand.Intn(max-min) + min
		time.Sleep(time.Duration(timeToSleep) * time.Millisecond)

		_, becomeLeader := rf.GetState()
		if becomeLeader {
			return
		}

		select {
		case <-rf.notifyMsg: // got msg during timer
			rf.mu.Lock()
			if rf.leaderId != NONE_LEADER {
				Info("%v follows to %v in term %v", rf.me, rf.leaderId, rf.term)
			}
			rf.mu.Unlock()
			continue
		default: // start new election
			go rf.startNewElection()
		}
	}
}

// The applier will apply the command to the applyCh
func (rf *Raft) applier(appCh chan ApplyMsg) {
	rf.appCond.L.Lock()
	for !rf.killed() {
		// wait until should apply new msg
		rf.mu.Lock()
		for rf.commitInd == rf.lastApply {
			rf.mu.Unlock()
			rf.appCond.Wait()
			rf.mu.Lock()
		}
		rf.mu.Unlock()

		// apply new msg
		rf.mu.Lock()
		DPrintf("%v begin apply new msgs.", rf.me)
		indToCommit := rf.GetLogWithIndex(rf.lastApply + 1)
		if indToCommit == -1 {
			Error("Log should be commited is None in %v in term %v", rf.me, rf.term)
			rf.mu.Unlock()
			return
		}
		for rf.commitInd > rf.lastApply {
			rf.lastApply++
			DPrintf("%v apply msg %v[%v] %v in term %v", rf.me, rf.lastApply, indToCommit, rf.logs[indToCommit].Command, rf.term)
			rf.appCh <- ApplyMsg{
				true,
				rf.logs[indToCommit].Command,
				rf.logs[indToCommit].Index,
				false,
				[]byte{},
				0,
				0,
			}
			indToCommit++
		}
		rf.mu.Unlock()
	}
}

func (rf *Raft) heartbeat() {
	rf.mu.Lock()
	term := rf.term
	rf.mu.Unlock()
	for !rf.killed() {
		time.Sleep(time.Duration(HEARTBEAT_DUR) * time.Millisecond)

		// check if still leader
		rf.mu.Lock()
		if !rf.IsStillLeader(term) {
			rf.mu.Unlock()
			return
		}

		rf.updateCommitIndexOfLeader()

		for ind, _ := range rf.peers {
			if ind == rf.me {
				continue
			}

			_ind := ind
			_, prevLogInd, prevLogTerm := rf.GetLogToSend(ind)
			args := &AppendEntriesArgs{
				term,
				rf.me,
				prevLogInd,
				prevLogTerm,
				[]Log{},
				rf.commitInd,
			}
			reply := &AppendEntriesReply{}
			go func(i int, _args *AppendEntriesArgs, _reply *AppendEntriesReply) {
				rf.mu.Lock()
				if !rf.IsStillLeader(_args.Term) {
					rf.mu.Unlock()
					return
				}
				rf.mu.Unlock()

				ok := rf.sendAppendEntries(i, _args, _reply)
				if !ok {
					return
				}

				// use the reply to check if there are new terms
				rf.mu.Lock()
				if !rf.IsStillLeader(_args.Term) {
					rf.mu.Unlock()
					return
				}

				if _reply.Success { // there could be an update
					rf.matchInd[i] = MaxInt(_args.PrevLogInd, rf.matchInd[i])
					rf.updateCommitIndexOfLeader()
					DPrintf("heartbeat: leader %v update %v's nextInd, matchInd to [%v, %v]", rf.me, i, rf.nextInd[i], rf.matchInd[i])
					rf.mu.Unlock()
					return
				}

				// handle the failure situation, down to follower
				// or retry with new nextInd
				update := rf.updateTerm(_reply.Term)
				if update != GREATER_TERM {
					if rf.nextInd[i] != _args.PrevLogInd+1 { // nextInd is not the same value in the context(must be less)
						rf.mu.Unlock()
						return
					}

					Error("leader %v update nextInd[%v] from %v to  %v", rf.me, i, rf.nextInd[i], MinInt(_reply.CIndex, rf.nextInd[i]))
					rf.nextInd[i] = MinInt(_reply.CIndex, rf.nextInd[i])
					if rf.nextInd[i] <= 0 {
						Error("heart: %v has error in nextInd as %v", rf.me, rf.nextInd[i])
					}
				}
				rf.mu.Unlock()
			}(_ind, args, reply)
		}

		rf.mu.Unlock()
	}
}

// *** MUST HOLD rf.mu.Lock() ***
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

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	rf.commitInd = 0
	rf.lastApply = 0

	rf.appCond = *sync.NewCond(&sync.Mutex{})
	rf.appCh = applyCh

	// re-init after election
	rf.nextInd = make([]int, len(peers))
	rf.matchInd = make([]int, len(peers))

	rf.leaderId = NONE_LEADER

	rf.notifyMsg = make(chan struct{}, 5)

	rf.sending = make([]bool, len(peers))
	for i := 0; i < len(rf.sending); i++ {
		rf.sending[i] = false
	}

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState(), persister.ReadSnapshot())

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.applier(applyCh)
	return rf
}

/* call when grant vote or get AE */
func (rf *Raft) NotifyMsg() {
	select {
	case rf.notifyMsg <- struct{}{}:
		return
	default:
		return
	}
}

// no lock should be held
// return the term and index of last log
func (rf *Raft) GetLastLogInfo() (int, int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if len(rf.logs) == 0 {
		return rf.snapshots.LastIncludedTerm, rf.snapshots.LastIncludedIndex
	}

	return rf.logs[len(rf.logs)-1].Term, rf.logs[len(rf.logs)-1].Index
}

// **must hold the rf.mu.Lock() ***
func (rf *Raft) GetLastLogIndex() int {
	if len(rf.logs) == 0 {
		return rf.snapshots.LastIncludedIndex
	}

	return rf.logs[len(rf.logs)-1].Index
}

// **must hold the rf.mu.Lock() **
// return the index in rf.logs of log with index
// return -1 if not match
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

/*
must hold the rf.mu.Lock()
check if rf is still the leader and in the same term
*/
func (rf *Raft) IsStillLeader(term int) bool {
	return rf.leaderId == rf.me && rf.term == term && !rf.killed()
}

//
//   must hold rf.mu.Lock()
//   only can be called from leader
//	get the log info to send to server
//   return the index, prevLogInd, prevLogTerm
//
func (rf *Raft) GetLogToSend(server int) (int, int, int) {
	indToSend := rf.GetLogWithIndex(rf.nextInd[server])
	if indToSend == -1 {
		if len(rf.logs) != 0 {
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
