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
	"fmt"
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

	applyCh chan ApplyMsg

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm int32
	votedFor    int32 // candidateId that received vote in current term
	log         []*LogEntry

	commitIndex int
	lastApplied int

	nextIndex  []int // serverId : index of next log entry to send to it
	matchIndex []int // serverId : index of highest log entry replicated on it

	// my stored state
	state           int32
	appendEntriesCh chan struct{}
	voteGrantedCh   chan struct{}
}

// possible states
const (
	Follower         = iota
	Candidate        = iota
	LeaderTransition = iota
	Leader           = iota
)

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

func (rf *Raft) GetCurrentTerm() int32     { return atomic.LoadInt32(&rf.currentTerm) }
func (rf *Raft) IncrementCurrentTerm()     { atomic.AddInt32(&rf.currentTerm, 1) }
func (rf *Raft) SetCurrentTerm(term int32) { atomic.StoreInt32(&rf.currentTerm, term) }

func (rf *Raft) GetVotedFor() int32          { return atomic.LoadInt32(&rf.votedFor) }
func (rf *Raft) SetVotedFor(candidate int32) { atomic.StoreInt32(&rf.votedFor, candidate) }

func (rf *Raft) GetServerState() int32      { return atomic.LoadInt32(&rf.state) }
func (rf *Raft) SetServerState(state int32) { atomic.StoreInt32(&rf.state, state) }

type LogEntry struct {
	Term    int
	Command interface{}
}

// return index of entry and pointer to entry itself
func (rf *Raft) GetLastLogEntry() *LogEntry {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if len(rf.log) > 0 {
		return rf.log[len(rf.log)-1]
	} else {
		return &LogEntry{Term: -1, Command: nil}
	}
}

func (rf *Raft) GetLastLogIndex() int { // TODO: make this thread safe
	return len(rf.log) - 1
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

func (rf *Raft) IsMajority(x int) bool {
	return x > len(rf.peers)/2
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	return int(rf.GetCurrentTerm()), rf.GetServerState() == Leader
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
	Term         int32
	CandidateId  int32
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int32
	VoteGranted bool
	Reason      string
	Id          int
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// log.Printf("%d->%d requestVote term: %d", args.CandidateId, rf.me, rf.GetCurrentTerm())
	// log.Printf("peer id: %d    votedFor: %v      requestFrom: %d    term: %d      candidate: %d", rf.me, rf.votedFor, args.CandidateId, rf.currentTerm, args.CandidateId)
	// Your code here (2A, 2B).
	reply.Term = rf.GetCurrentTerm()
	if args.Term < rf.GetCurrentTerm() {
		reply.VoteGranted = false
		reply.Reason = fmt.Sprintf("candidate term %d < %d", args.Term, reply.Term)
		return
	} else if args.Term > reply.Term {
		rf.SetCurrentTerm(args.Term)
		rf.SetVotedFor(-1)
		rf.SetServerState(Follower)
		log.Printf("becoming follower %d - received RequestVote RPC from %d w term %d > %d", rf.me, args.CandidateId, args.Term, reply.Term)
	}

	if rf.GetVotedFor() < 0 || rf.GetVotedFor() == args.CandidateId {
		// log.Printf("shouldnt be in here")
		// log.Printf("peer id: %d    votedFor: %v      requestFrom: %d    term: %d      candidate: %d", rf.me, rf.votedFor, args.CandidateId, rf.currentTerm, args.CandidateId)

		// 5.4.1 election restriction: check if the candidate's log is up-to-date
		// if args.LastLogTerm >= rf.GetLastLogEntry().Term &&
		// 	args.LastLogIndex >= len(rf.log)-1 {
		if CheckUpToDate(
			args.LastLogTerm,
			args.LastLogIndex,
			rf.GetLastLogEntry().Term,
			rf.GetLastLogIndex(),
		) {
			rf.SetVotedFor(args.CandidateId)
			reply.VoteGranted = true
			rf.voteGrantedCh <- struct{}{}
		} else {
			reply.VoteGranted = false
			reply.Reason = fmt.Sprintf("log is not up to date")
		}
	} else {
		reply.VoteGranted = false
		reply.Reason = fmt.Sprintf("already voted for %d", rf.GetVotedFor())
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
	Term         int32
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []*LogEntry
	LeaderCommit int
}

// AppendEntries RPC reply
type AppendEntriesReply struct {
	Term        int32
	Success     bool
	NumAppended int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	DPrintf("inside appendEntries %d->%d term:%d", args.LeaderId, rf.me, rf.GetCurrentTerm())
	reply.Term = rf.GetCurrentTerm()
	if args.Term < rf.GetCurrentTerm() {
		// log.Printf("%d->%d term failed: leader term %d < my term %d", args.LeaderId, rf.me, args.Term, rf.GetCurrentTerm())
		reply.Success = false
		return
	} else if args.Term > rf.GetCurrentTerm() {
		rf.SetCurrentTerm(args.Term)
		rf.SetVotedFor(-1)
		rf.SetServerState(Follower)
		log.Printf("becoming follower %d - received AE RPC from %d w term %d > %d", rf.me, args.LeaderId, args.Term, reply.Term)
	}
	if !rf.MatchLogEntry(args.PrevLogIndex, args.PrevLogTerm) {
		// log.Printf("%d->%d log match failed", args.LeaderId, rf.me)
		reply.Success = false
		rf.appendEntriesCh <- struct{}{}
		return
	}

	var entryIdx int
	for entryIdx = 0; entryIdx < len(args.Entries); entryIdx++ {
		curr := args.PrevLogIndex + 1 + entryIdx
		// log has no existing entries at idx curr, so just break
		if curr >= len(rf.log) {
			break
		}
		// log has entry at idx curr; check if conflicts with new entry
		if rf.log[curr].Term != args.Entries[entryIdx].Term {
			rf.log = rf.log[:curr]
			break
		}
	}

	rf.log = append(rf.log, args.Entries[entryIdx:]...)
	reply.NumAppended = len(args.Entries[entryIdx:])
	// log.Printf("(%d) log: %v", rf.me, rf.log)
	// rf.DumpLog()

	// log.Printf("%d:%d ?> %d:%d", args.LeaderId, args.LeaderCommit, rf.me, rf.commitIndex)
	// log.Printf("id:%d last log: %v", rf.me, rf.log[len(rf.log)-1])
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, len(rf.log)-1)
	}

	// log.Printf("inside AppendEntries %d->%d log:%v", args.LeaderId, rf.me, rf.GetLastLogEntry())

	// ONLY SEND MESSAGE IF THE LEADER IS LEGIT.
	reply.Success = true
	rf.appendEntriesCh <- struct{}{}
}

func (rf *Raft) DumpLog() {
	logEntries := make([]int, len(rf.log))
	for i, entry := range rf.log {
		logEntries[i] = entry.Term
	}
	log.Printf("(%d) log: %+v", rf.me, logEntries)
}

func min(x, y int) int {
	if x < y {
		return x
	} else {
		return y
	}
}

func RandomTimeout(floor, ceil int) <-chan time.Time {
	ms := rand.Intn(ceil-floor) + floor
	return time.After(time.Duration(ms) * time.Millisecond)
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
	var term int
	var isLeader bool
	if term, isLeader = rf.GetState(); !isLeader {
		return 0, 0, false
	}

	// Your code here (2B).
	entry := &LogEntry{
		Term:    term,
		Command: command,
	}

	// TODO: lock this down. or we will get race conditions.
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.log = append(rf.log, entry)
	rf.matchIndex[rf.me] = len(rf.log) - 1
	// log.Printf("(%d) added %+v to log at idx %d", rf.me, entry, len(rf.log)-1)

	return len(rf.log) - 1, term, isLeader
}

func (rf *Raft) Candidate() {

	rf.IncrementCurrentTerm()
	rf.SetVotedFor(int32(rf.me))
	votes := 1

	args := &RequestVoteArgs{
		Term:         rf.GetCurrentTerm(),
		CandidateId:  int32(rf.me),
		LastLogTerm:  rf.GetLastLogEntry().Term,
		LastLogIndex: len(rf.log) - 1,
	}
	replyCh := make(chan RequestVoteReply, len(rf.peers))

	// send RequestVote RPCs to all other servers
	for id, peer := range rf.peers {
		if id != rf.me { // skip sending rpc to myself
			go func(peer *labrpc.ClientEnd, id int) {
				reply := &RequestVoteReply{}
				termSent := rf.GetCurrentTerm()
				if ok := peer.Call("Raft.RequestVote", args, reply); ok {
					// SGTR: guard against term confusion
					if rf.GetCurrentTerm() != termSent {
						return
					}
					// log.Printf("RequestVote worked: %d -> %d   reply: %v", rf.me, id, reply)
					reply.Id = id
					replyCh <- *reply
				} else {
					// TODO: handle error
					// log.Printf("RequestVote failed: %d -> %d")
					replyCh <- *reply
				}
			}(peer, id)
		}
	}

	for {
		select {
		case <-rf.appendEntriesCh:
			// we just received an appendEntries from the leader so we concede
			// our candidacy and revert to followers
			// log.Printf("Becoming follower: %d", rf.me)
			rf.SetServerState(Follower)
			log.Printf("(%d) becoming follower - received AE RPC as candidate", rf.me)

			return
		case <-rf.voteGrantedCh:
			rf.SetServerState(Follower)
			log.Printf("(%d) becoming follower - voted for another candidate", rf.me)
			return

		case reply := <-replyCh:
			log.Printf("(%d->%d) requestVote: %+v", rf.me, reply.Id, reply)
			if reply.VoteGranted {
				votes++
				// if votes received from majority of servers: become leader
				if rf.IsMajority(votes) {
					// log.Printf("leader:  %d    votes: %d        term: %d", rf.me, votes, rf.currentTerm)
					rf.SetServerState(LeaderTransition)
					return
				}
			} else if reply.Term > rf.GetCurrentTerm() {
				// if the term returned by the peer server is higher than our
				// current term, we are not eligible to be leader. If their
				// current term is higher, that means we could have missing
				// committed entries from that term. So we update our current
				// term and become Followers.

				rf.SetVotedFor(-1)
				rf.SetServerState(Follower)
				log.Printf("becoming follower %d - during candidacy, %d had higher term %d > %d", rf.me, reply.Id, reply.Term, rf.GetCurrentTerm())
				rf.SetCurrentTerm(reply.Term)
				return
			}

		case <-RandomTimeout(500, 1000):
			// election timed out. A new election will start after
			// we return to ticker() in the same Candidate state.
			log.Printf("(%d) election timeout, try again!", rf.me)
			return
		}
	}

}

func CheckUpToDate(log1Term, log1Index, log2Term, log2Index int) bool {
	if log1Term > log2Term {
		return true
	}
	if log1Term == log2Term {
		return log1Index >= log2Index
	}
	return false
}

func (rf *Raft) CanCommit(n int) bool {

	numReplicated := 0
	for _, idx := range rf.matchIndex {
		// log.Printf("matchIdx[%d] = %d", i, idx)
		if idx >= n {
			numReplicated++
		}
	}
	// log.Printf("numReplicated: %d", numReplicated)
	if numReplicated < (len(rf.peers)/2)+1 {
		return false
	}
	// log.Printf("given term: %d != my term: %d", rf.log[n].Term, int(rf.currentTerm))
	if rf.log[n].Term != int(rf.currentTerm) {
		return false
	}

	// log.Printf("success replicated")
	return true
}

// func (rf *Raft) TryCommit(ch <-chan AppendEntriesReply) {
// 	for {
// 		select {
// 		case reply := <-ch:

// 			if reply.Term > rf.GetCurrentTerm() {
// 				rf.SetCurrentTerm(reply.Term)
// 				rf.SetVotedFor(-1)
// 				rf.SetServerState(Follower)
// 				log.Printf("becoming follower %d", rf.me)
// 				return
// 			}

// 			n := rf.commitIndex + 1
// 			// log.Printf("------- n: %d -------", n)
// 			// lastIdx, _ := rf.GetLastLogEntry()
// 			for n < len(rf.log) {
// 				if rf.CanCommit(n) {
// 					rf.commitIndex = n
// 					// log.Printf("committed by leader")
// 					// log.Printf("id:%d last_log: %v", rf.me, rf.log[len(rf.log)-1])
// 					n++
// 				} else {
// 					break
// 				}
// 			}
// 		case <-time.After(50 * time.Millisecond):
// 			return
// 		}
// 	}
// }

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for !rf.killed() {
		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().

		// log.Printf("(%d) commitIndex: %d, lastApplied: %d", rf.me, rf.commitIndex, rf.lastApplied)

		for rf.commitIndex > rf.lastApplied {
			rf.lastApplied++
			rf.applyCh <- ApplyMsg{
				CommandValid: true,
				Command:      rf.log[rf.lastApplied].Command,
				CommandIndex: rf.lastApplied,
			}
			// log.Printf("(%d) applied %v at idx %d", rf.me, rf.log[rf.lastApplied].Command, rf.lastApplied)
		}
		// rf.DumpLog()

		switch rf.GetServerState() {
		case Follower:
			select {
			case <-rf.appendEntriesCh:
				// if we get a message to reset timeout, just refresh by doing nothing
				// and going into the next iteration of the loop
				// log.Printf("%d acknowledged AppendEntries", rf.me)
				continue
			case <-rf.voteGrantedCh:
				// (Student Guide) reset timer if I grant a vote
				continue
			case <-RandomTimeout(500, 1000):
				log.Printf("becoming candidate: %d", rf.me)
				// if the election timeout occurs, convert to candidate
				rf.SetServerState(Candidate)
			}

		case Candidate:
			rf.Candidate()

		case LeaderTransition:
			for i := 0; i < len(rf.peers); i++ {
				// log.Printf("log: %d", len(rf.log))
				rf.nextIndex[i] = len(rf.log)
				if i == rf.me {
					rf.matchIndex[i] = len(rf.log) - 1
				} else {
					rf.matchIndex[i] = 0
				}

				// log.Printf("(%d) reset nextIndex[%d]: %d", rf.me, i, len(rf.log))
			}
			rf.SetServerState(Leader)
			log.Printf("(%d) becoming leader in term %d", rf.me, rf.GetCurrentTerm())

		case Leader:
			// log.Printf("looped")
			// log.Printf("commitIndex: %d", rf.commitIndex)
			// replyCh := make(chan AppendEntriesReply, len(rf.peers))

			for id, peer := range rf.peers {
				if id != rf.me {
					var args *AppendEntriesArgs
					// log.Printf("(%d->%d) appending")
					// log.Printf("%d->%d next:%d log:%v", rf.me, id, rf.nextIndex[id], rf.log)

					if len(rf.log)-1 >= rf.nextIndex[id] { // actually append entries
						// log.Printf("%d->%d appending, nextIndex[%d] = %d", rf.me, id, id, rf.nextIndex[id])
						prevLogIndex := rf.nextIndex[id] - 1 // TODO: get length of log safely
						args = &AppendEntriesArgs{
							Term:         rf.GetCurrentTerm(),
							LeaderId:     rf.me,
							PrevLogIndex: prevLogIndex,
							PrevLogTerm:  rf.log[prevLogIndex].Term, // TODO: get prev log term safely
							Entries:      rf.log[prevLogIndex+1:],
							LeaderCommit: rf.commitIndex,
						}
						entries := make([]interface{}, len(args.Entries))
						for i := 0; i < len(args.Entries); i++ {
							entries[i] = args.Entries[i].Command
						}
						// log.Printf("appending %d->%d args: %+v", rf.me, id, args)
						// log.Printf("entries: %v", entries)
					} else { // just a heartbeat message
						// log.Printf("%d->%d heartbeat, nextIndex[%d] = %d", rf.me, id, id, rf.nextIndex[id])
						prevLogIndex := rf.nextIndex[id] - 1 // TODO: get length of log safely
						args = &AppendEntriesArgs{
							Term:         rf.GetCurrentTerm(),
							LeaderId:     rf.me,
							PrevLogIndex: prevLogIndex,
							PrevLogTerm:  rf.log[prevLogIndex].Term,
							Entries:      []*LogEntry{},
							LeaderCommit: rf.commitIndex,
						}
						// log.Printf("heartbeat %d->%d args: %+v", rf.me, id, args)
					}

					go func(peer *labrpc.ClientEnd, id int, args *AppendEntriesArgs) {
						reply := &AppendEntriesReply{
							Success:     false,
							NumAppended: 0,
						}
						termSent := rf.GetCurrentTerm()
						if ok := peer.Call("Raft.AppendEntries", args, reply); ok {
							// log.Printf("%d->%d got reply: %+v", id, rf.me, reply)

							// SGTR: guard against term confusion
							if rf.GetCurrentTerm() != termSent {
								return
							}

							if reply.Term > rf.GetCurrentTerm() {
								rf.SetCurrentTerm(reply.Term)
								rf.SetVotedFor(-1)
								rf.SetServerState(Follower)
								log.Printf("becoming follower %d - encountered higher termm when AE RPC", rf.me)
								return
							}

							// n := rf.commitIndex + 1
							n := len(rf.log) - 1
							for n > rf.commitIndex {
								// log.Printf("(%d) n: %d len: %d", rf.me, n, len(rf.log))
								if rf.CanCommit(n) {
									rf.commitIndex = n
									return
								}
								n--
							}

							if reply.Success {
								rf.nextIndex[id] += reply.NumAppended // TODO: safety
								rf.matchIndex[id] = args.PrevLogIndex + len(args.Entries)
								// log.Printf("%d->%d AE commands: %v terms: %v, nextIndex[%d] = %d", id, rf.me, commands, terms, id, rf.nextIndex[id])
							} else {
								if reply.Term <= rf.GetCurrentTerm() {
									rf.nextIndex[id]--
								}
							}
							// } else {
							// 	log.Printf("AppendEntries failed: %d->%d", rf.me, id)
						}
					}(peer, id, args)
				}
			}

			// rf.TryCommit(replyCh)
			time.Sleep(100 * time.Millisecond)

		default:
			log.Fatalf("Something went very wrong. State is %v", rf.GetServerState())
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

	rf.applyCh = applyCh

	// Your initialization code here (2A, 2B, 2C).
	rf.currentTerm = 0
	rf.votedFor = -1 // -1 represents null
	rf.log = make([]*LogEntry, 0)
	rf.log = append(rf.log, &LogEntry{Term: 0, Command: nil})

	rf.commitIndex = 0
	rf.lastApplied = 0

	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))

	rf.state = Follower
	rf.appendEntriesCh = make(chan struct{})
	rf.voteGrantedCh = make(chan struct{})

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
