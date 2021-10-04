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

	nextInd  []int // next Indexes
	matchInd []int // match Indexes

	isLeader bool // true if is leader

	/* notifyMsg for AE or RV */
	notifyMsg chan struct{} // notify when recv RE/RV
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// @TODO: check killed
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.term, rf.isLeader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

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
	term := -1
	isLeader := true

	// Your code here (2B).

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
			DPrintf("[INFO] %v get AE/RV in term %v", rf.me, rf.term)
			rf.mu.Unlock()
			continue
		default: // start new election
			go rf.startNewElection()
		}
	}
}

func (rf *Raft) heartbeat() {
	for !rf.killed() {
		_, isLeader := rf.GetState()
		if !isLeader {
			return
		}

		time.Sleep(time.Duration(HEARTBEAT_DUR) * time.Millisecond)
		rf.mu.Lock()
		args := &AppendEntriesArgs{
			rf.term,
			rf.me,
			0,
			0,
			nil,
			rf.commitInd,
		}
		rf.mu.Unlock()
		for ind, _ := range rf.peers {
			if ind == rf.me {
				continue
			}

			reply := &AppendEntriesReply{}
			go rf.sendAppendEntries(ind, args, reply)
		}
	}
}

// *** MUST HOLD rf.mu.Lock() ***
// called when the term of current raft peer is
// updated
// clear the voteFor, and reboot the ticker if
// leader
// return true if update successfully
func (rf *Raft) updateTerm(newTerm int) bool {
	if newTerm <= rf.term {
		return false
	}

	DPrintf("[INFO] %v 's term get updated from %v to %v", rf.me, rf.term, newTerm)
	rf.term = newTerm
	if rf.isLeader {
		rf.isLeader = false
		go rf.ticker()
	}

	rf.vote = NONE_VOTE

	return true
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

	rf.term = NONE_TERM
	rf.vote = NONE_VOTE
	rf.logs = []Log{}

	rf.commitInd = 0
	rf.lastApply = 0

	rf.nextInd = make([]int, len(peers))
	rf.matchInd = make([]int, len(peers))

	rf.notifyMsg = make(chan struct{}, 5)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

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

func (rf *Raft) GetLastLogInfo() (int, int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if len(rf.logs) == 0 {
		return NONE_TERM, NONE_IND
	}

	return rf.logs[len(rf.logs)-1].Term, rf.logs[len(rf.logs)-1].Index
}
