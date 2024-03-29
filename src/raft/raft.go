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
	"fmt"

	"6.824/labgob"

	//	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labrpc"
)

// ApplyMsg is used to communicate to the service
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

// Log is the log entry in raft
type Log struct {
	Index   int         // index for compaction since logs will be truncted
	Term    int         // log term
	Command interface{} // command content
}

// SnapShotData represents a snapshot
type SnapShotData struct {
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

// Raft is a Go object implementing a single Raft peer.
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

	AEChs []chan struct{} // notify when retry on peer is needed

	snapshots  SnapShotData  // snapshot data
	snapshotCh chan struct{} // notify when cond install snapshot success

	/* profile info */
	AECount int
	RVCount int
	ISCount int
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

// persist save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	buffer := new(bytes.Buffer)
	enc := labgob.NewEncoder(buffer)
	enc.Encode(rf.term)
	enc.Encode(rf.vote)
	enc.Encode(rf.logs)
	data := buffer.Bytes()

	var sdata []byte
	if rf.snapshots.LastIncludedIndex == NONE_IND && rf.snapshots.LastIncludedTerm == NONE_TERM {
		sdata = []byte{}
	} else {
		snapshotbuffer := new(bytes.Buffer)
		senc := labgob.NewEncoder(snapshotbuffer)
		senc.Encode(rf.snapshots)
		sdata = snapshotbuffer.Bytes()
	}

	rf.persister.SaveStateAndSnapshot(data, sdata)
}

// readPersist restore previously persisted state.
func (rf *Raft) readPersist(data []byte, sdata []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		rf.term = NONE_TERM
		rf.vote = NONE_VOTE
		rf.logs = []Log{}
	} else {
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
	}

	if sdata == nil || len(sdata) < 1 {
		// use snapshot to redirect the beginning of the logs
		rf.snapshots = SnapShotData{
			LastIncludedIndex: NONE_IND,
			LastIncludedTerm:  NONE_TERM,
			Data:              []byte{},
		}
		rf.commitInd = 0
		rf.lastApply = 0
	} else {
		sbuffer := bytes.NewBuffer(sdata)
		var snapshot SnapShotData
		sdec := labgob.NewDecoder(sbuffer)
		if sdec.Decode(&snapshot) != nil {
			panic("Decode snapshot error")
		}

		rf.snapshots = snapshot
		rf.commitInd = snapshot.LastIncludedIndex
		rf.lastApply = snapshot.LastIncludedIndex
	}
}

// CondInstallSnapshot get called when A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {
	Info("CondInstallSnapshot: to %v", rf.me)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer func() {
		rf.snapshotCh <- struct{}{}
	}()

	if rf.snapshots.LastIncludedIndex > lastIncludedIndex { // has newer snapshot
		DPrintf("CondInstallSnapshot: %v get old snapshot, refuse condinstall", rf.me)
		return false
	}

	if rf.snapshots.LastIncludedIndex == lastIncludedIndex {
		if rf.snapshots.LastIncludedTerm != lastIncludedTerm {
			panic("wrong snapshot")
		}
		return true
	}

	if rf.lastApply > lastIncludedIndex { // apply new msg
		DPrintf("CondInstallSnapshot: %v apply newer(%v) than condsnapshot(%v)", rf.me, rf.lastApply, lastIncludedIndex)
		return false
	}

	rf.snapshots = SnapShotData{
		LastIncludedIndex: lastIncludedIndex,
		LastIncludedTerm:  lastIncludedTerm,
		Data:              snapshot,
	}
	rf.commitInd = MaxInt(rf.commitInd, rf.snapshots.LastIncludedIndex)
	rf.lastApply = MaxInt(rf.lastApply, rf.snapshots.LastIncludedIndex)

	Info("CondInstallSnapshot: to %v, lastInd->%v, commitInd -> %v, lastApply -> %v, logs:%v", rf.me, lastIncludedIndex, rf.commitInd, rf.lastApply, rf.logs)
	ind := rf.GetLogWithIndex(lastIncludedIndex)
	Info("trimmed start is %v", ind)
	if ind == -1 || ind+1 == len(rf.logs) {
		rf.logs = []Log{}
	} else {
		rf.logs = rf.logs[ind+1:]
	}

	if len(rf.logs) != 0 {
		Info("CondInstallSnapshot: %v's log is %v after installing snapshot(%v)", rf.me, rf.logs, lastIncludedIndex)
	} else {
		Info("CondInstallSnapshot: %v's log is none", rf.me)

	}
	rf.persist()

	rf.appCond.Signal() // to send the blocked msgs
	return true
}

// Snapshot called when the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	Info("snapshot is called %v to %v", index, rf.me)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if (len(rf.logs) == 0 && rf.snapshots.LastIncludedIndex < index) || rf.logs[len(rf.logs)-1].Index < index {
		panic("No index match when installing snapshot.")
		// rf.snapshots = SnapShotData{
		// 	LastIncludedIndex: index,
		// 	LastIncludedTerm:  0,
		// 	Data:              snapshot,
		// }
		// return
	}

	ind := rf.GetLogWithIndex(index)
	if ind == -1 {
		Error("Install an old snapshot")
		return
	}
	rf.snapshots = SnapShotData{
		LastIncludedIndex: index,
		LastIncludedTerm:  rf.logs[ind].Term,
		Data:              snapshot,
	}
	Info("%v install snapshot[index:%v, term:%v]", rf.me, index, rf.snapshots.LastIncludedTerm)

	oldLen := len(rf.logs)
	if ind+1 <= len(rf.logs) {
		rf.logs = rf.logs[ind+1:] // trim logs
	} else {
		panic("wrong ind")
	}
	newLen := len(rf.logs)
	if oldLen == newLen {
		panic("logs hasn't been trimmed")
	}

	if len(rf.logs) != 0 {
		DPrintf("%v's log is %v after installing snapshot", rf.me, rf.logs)
	}
	rf.commitInd = MaxInt(rf.commitInd, index)
	rf.lastApply = MaxInt(rf.lastApply, index)
	DPrintf("Snapshot: comitInd -> %v, lastApply -> %v", rf.commitInd, rf.lastApply)
	rf.appCond.Signal() // to send the blocked msgs
	rf.persist()
}

// Start called when the service using Raft (e.g. a k/v server) wants to start
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
	// Info("%v start %v[%v] at term %v", rf.me, newLog.Index, newLog.Command, newLog.Term)

	rf.logs = append(rf.logs, newLog)

	rf.persist()

	for ind := range rf.peers {
		if ind == rf.me {
			continue
		}

		if rf.nextInd[ind] == newLog.Index || newLog.Index <= rf.nextInd[ind]+AEBATCH_SIZE {
			rf.TriggerAppend(ind)
		}
	}
	rf.mu.Unlock()

	return index, term, isLeader
}

// Kill will set rf.dead as 1 atomically
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
	rf.appCond.Signal() // release applier
	Info("%v is killed", rf.me)

	if PROFILE {
		rf.mu.Lock()
		Profile("%v profile: RV:%v, AE:%v, IS:%v", rf.me, rf.RVCount, rf.AECount, rf.ISCount)
		rf.mu.Unlock()
	}
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// ticker starts a new election if this peer hasn't received
// heartbeats recently.
// when leader becomes a follower, it needs to call ticker() manually
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
			continue
		default: // start new election
			go rf.startNewElection()
		}
	}
}

// applier will apply the command to the applyCh
func (rf *Raft) applier(appCh chan ApplyMsg) {
	go func() {
		for !rf.killed() {
			time.Sleep(time.Millisecond * time.Duration(300))
			rf.appCond.Signal()
		}
	}()
	applyInterval := 10
	rf.appCond.L.Lock()
	for !rf.killed() {
		// wait until should apply new msg
		rf.mu.Lock()
		for rf.commitInd == rf.lastApply {
			Info("%v give up commit: %v, lastApply: %v, snapshotInd:%v", rf.me, rf.commitInd, rf.lastApply, rf.snapshots.LastIncludedIndex)
			rf.mu.Unlock()
			rf.appCond.Wait()
			if rf.killed() { // make sure not stall here
				return
			}
			rf.mu.Lock()
		}
		Info("%v begins commit: %v, lastApply: %v, snapshotInd:%v", rf.me, rf.commitInd, rf.lastApply, rf.snapshots.LastIncludedIndex)

		// apply new msg
		indToCommit := rf.GetLogWithIndex(rf.lastApply + 1)
		if indToCommit == -1 {
			Info("applier: Log should be commited(%v) is None in %v with logs %v", rf.lastApply+1, rf.me, rf.logs)
			err := fmt.Sprintf("applier: Log should be commited is None in %v in term %v", rf.me, rf.term)
			panic(err)
			// rf.mu.Unlock()
			// return
		}

		block := false
		for rf.commitInd > rf.lastApply && !block {
			rf.lastApply++
			select {
			case rf.appCh <- ApplyMsg{
				true,
				rf.logs[indToCommit].Command,
				rf.logs[indToCommit].Index,
				false,
				[]byte{},
				0,
				0,
			}:
				// Info("applier: %v apply msg %v[%v]  in term %v", rf.me, rf.lastApply, indToCommit, rf.term)
				indToCommit++
			case <-time.After(time.Duration(applyInterval) * time.Millisecond):
				rf.lastApply--
				Info("applie: appch of %v is blocked to send %v in term %v", rf.me, rf.lastApply+1, rf.term)
				block = true
			}
		}

		rf.mu.Unlock()
		if block { // hang-up applier until next apply(or endless loop)
			rf.appCond.Wait()
			DPrintf("applier: blocked appch of %v turn to normal", rf.me)
		}
	}
}

func (rf *Raft) startHeartbeat() {
	rf.mu.Lock()
	term := rf.term
	rf.mu.Unlock()

	for ind := range rf.peers {
		if ind == rf.me {
			continue
		}

		go rf.heartbeatWorker(ind, term)
	}
}

// heartbeat worker for every single peer
func (rf *Raft) heartbeatWorker(server, term int) {
	rand.Seed(time.Now().UnixNano())
	// heartbeat at the beginning
	go rf.replicateOnCommand(server, term)
	for !rf.killed() {
		// leader doesn't need timer
		rf.mu.Lock()
		if !rf.IsStillLeader(term) {
			rf.mu.Unlock()
			return
		}
		rf.mu.Unlock()
		select {
		case <-rf.AEChs[server]:
			go rf.replicateOnCommand(server, term)
		case <-time.After(time.Duration(HEARTBEAT_DUR) * time.Millisecond): // heartbeat
			DPrintf("heartbeat: leader %v send to follower %v", rf.me, server)
			go rf.replicateOnCommand(server, term)
		}
	}
}

// Make a raft server.the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	rf.appCond = *sync.NewCond(&sync.Mutex{})
	rf.appCh = applyCh

	// re-init after election
	rf.nextInd = make([]int, len(peers))
	rf.matchInd = make([]int, len(peers))

	rf.leaderId = NONE_LEADER

	rf.notifyMsg = make(chan struct{}, 5)

	rf.snapshotCh = make(chan struct{}, 5)

	rf.sending = make([]bool, len(peers))
	for i := 0; i < len(rf.sending); i++ {
		rf.sending[i] = false
	}

	rf.AEChs = make([]chan struct{}, len(peers))
	for i := 0; i < len(rf.AEChs); i++ {
		if i == rf.me {
			continue
		}

		rf.AEChs[i] = make(chan struct{}, AEBUFFER_LEN)
	}

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState(), persister.ReadSnapshot())

	// initialize profile info
	rf.AECount = 0
	rf.RVCount = 0
	rf.ISCount = 0

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.applier(applyCh)
	return rf
}
