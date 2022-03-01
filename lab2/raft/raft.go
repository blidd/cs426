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
	"log"
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
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
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
	currentTerm int
	votedFor    int // candidateId that received vote in current term
	log         []*LogEntry

	commitIndex int
	lastApplied int

	nextIndex  []int // serverId : index of next log entry to send to it
	matchIndex []int // serverId : index of highest log entry replicated on it

	// my stored state
	electionTimeout *time.Ticker
	state           int
}

// possible states
const (
	Follower  = iota
	Candidate = iota
	Leader    = iota
)

type LogEntry struct {
	Term    int
	Command interface{}
}

func (rf *Raft) GetLastLogEntry() *LogEntry {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if len(rf.log) > 0 {
		return rf.log[len(rf.log)]
	} else {
		return &LogEntry{Term: -1, Command: nil}
	}
}

func (rf *Raft) IsMajority(x int) bool {
	majority := float64(len(rf.peers) / 2)
	return x >= int(majority)
}

func (rf *Raft) ResetElectionTimeout(floor, ceil int) {
	num := rand.Intn(ceil-floor) + floor
	rf.electionTimeout = time.NewTicker(time.Duration(num) * time.Millisecond)
	// rf.electionTimeout = time.NewTicker(1 * time.Second)
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	return rf.currentTerm, rf.state == Leader
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
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {

	log.Printf("peer id: %d    votedFor: %v      requestFrom: %d", rf.me, rf.votedFor, args.CandidateId)
	// Your code here (2A, 2B).
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		return
	} else {
		rf.currentTerm = args.Term
	}
	if rf.votedFor < 0 || rf.votedFor == args.CandidateId {
		// check if the candidate's log is up-to-date
		if args.LastLogTerm >= rf.GetLastLogEntry().Term &&
			args.LastLogIndex >= len(rf.log)-1 {
			rf.votedFor = args.CandidateId
			reply.VoteGranted = true
		} else {
			reply.VoteGranted = false
		}
	} else {
		reply.VoteGranted = false
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

// AppendEntries RPC args
type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []*LogEntry
	LeaderCommit int
}

// AppendEntries RPC reply
type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		reply.Success = false
		return
	} else {
		rf.currentTerm = args.Term
	}
	// if !rf.MatchLogEntry(args.PrevLogIndex, args.PrevLogTerm) {
	// 	reply.Success = false
	// 	return
	// }

	rf.ResetElectionTimeout(500, 1000)

}

func (rf *Raft) MatchLogEntry(prevLogIndex, prevLogTerm int) bool {
	if prevLogIndex >= len(rf.log) {
		return false
	}
	if rf.log[prevLogIndex].Term == prevLogTerm {
		return true
	}
	return false
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

func (rf *Raft) Candidate() {
	rf.currentTerm++
	votes := 1
	rf.votedFor = rf.me
	rf.ResetElectionTimeout(500, 1000)

	args := &RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogTerm:  rf.GetLastLogEntry().Term,
		LastLogIndex: len(rf.log) - 1,
	}
	replyCh := make(chan RequestVoteReply, len(rf.peers))

	// send RequestVote RPCs to all other servers
	for id, peer := range rf.peers {
		if id != rf.me { // skip sending rpc to myself
			go func(peer *labrpc.ClientEnd) {
				reply := &RequestVoteReply{}
				if ok := peer.Call("Raft.RequestVote", args, reply); ok {
					log.Printf("peer id: %d    reply: %v     from: %d", rf.me, reply, id)
					replyCh <- *reply
				} else {
					// TODO: handle error
				}
			}(peer)
		}
	}

	for {
		select {
		case reply := <-replyCh:
			if reply.VoteGranted {
				votes++
				// if votes received from majority of servers: become leader
				if rf.IsMajority(votes) {
					log.Printf("leader:  %d    votes: %d        term: %d", rf.me, votes, rf.currentTerm)
					rf.state = Leader
					return
				}
			} else if reply.Term > rf.currentTerm {
				// if the term returned by the peer server is higher than our
				// current term, we are not eligible to be leader. If their
				// current term is higher, that means we have missing committed
				// entries from that term. So we update our current term and
				// become Followers.
				rf.state = Follower
				rf.currentTerm = reply.Term
				return
			}
		case <-rf.electionTimeout.C:
			// election timed out. A new election will start after
			// we return to ticker() in the same Candidate state.
			return
		}
	}

}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for !rf.killed() {
		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().

		if rf.state == Follower {

			rf.ResetElectionTimeout(500, 1000)
			// TODO: if I reset election timeout, will this refresh??
			<-rf.electionTimeout.C // wait on channel, see if a timeout occurs
			log.Printf("becoming candidate: %d", rf.me)
			rf.state = Candidate // if the election timeout occurs, convert to candidate

		} else if rf.state == Candidate {
			rf.Candidate()

		} else if rf.state == Leader {
			// LEADER LOGIC

		} else {
			log.Fatalf("Something went very wrong. State is %v", rf.state)
		}
	}
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

	// Your initialization code here (2A, 2B, 2C).
	rf.currentTerm = 0
	rf.votedFor = -1 // -1 represents null
	rf.log = make([]*LogEntry, 0)

	rf.commitIndex = 0
	rf.lastApplied = 0

	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))

	rf.electionTimeout = time.NewTicker(time.Second)
	rf.state = Follower

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
