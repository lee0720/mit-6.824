package raft

import (
	"math/rand"
	"time"
)

func (rf *Raft) timeElapsed(lastResetTimer int64) int64 {
	return (time.Now().UnixNano() - lastResetTimer) / time.Hour.Milliseconds()
}

func (rf *Raft) timerElection() {
	for {
		rf.mu.Lock()
		if rf.role != Leader && rf.timeElapsed(rf.lastResetElectionTimer) > rf.timeoutElection {
			rf.timerElectionChan <- true
		}
		rf.mu.Unlock()
		time.Sleep(time.Millisecond * 10)
	}
}

func (rf *Raft) timerHeartbeat() {
	for {
		rf.mu.Lock()
		if rf.role == Leader && rf.timeElapsed(rf.lastResetHeartbeatTimer) > rf.timeoutHeartbeat {
			rf.timerHeartbeatChan <- true
		}
		rf.mu.Unlock()
		time.Sleep(time.Millisecond * 10)
	}
}

func (rf *Raft) resetTimerElection() {
	rand.Seed(time.Now().UnixNano())
	rf.timeoutElection = rf.timeoutHeartbeat*5 + rand.Int63n(150)
	rf.lastResetElectionTimer = time.Now().UnixNano()
}

func (rf *Raft) resetTimerHeartbeat() {
	rf.lastResetHeartbeatTimer = time.Now().UnixNano()
}

func (rf *Raft) roleSwitch(role Role) {
	switch role {
	case Follower:
		rf.role = Follower
		rf.votedFor = -1
	case Candidate:
		rf.role = Candidate
		rf.currentTerm++
		rf.votedFor = rf.me
		rf.resetTimerElection()
	case Leader:
		rf.role = Leader
		rf.resetTimerHeartbeat()
	}
}

func (rf *Raft) startElection() {
	rf.mu.Lock()
	rf.roleSwitch(Candidate)
	votes := 1
	rf.mu.Unlock()

	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}

		go func(id int) {
			rf.mu.Lock()
			args := RequestVoteArgs{
				Term:        rf.currentTerm,
				CandidateID: rf.me,
			}
			rf.mu.Unlock()

			var reply RequestVoteReply
			if rf.sendRequestVote(id, &args, &reply) {
				rf.mu.Lock()
				defer rf.mu.Unlock()
				if rf.currentTerm != args.Term {
					return
				}
				if reply.VoteGranted {
					votes += 1
					if votes > len(rf.peers)/2 && rf.role == Candidate {
						rf.roleSwitch(Leader)
						rf.mu.Unlock()
						rf.broadcastHeartbeat()
						rf.mu.Lock()
					}

				} else {
					if rf.currentTerm < reply.Term {
						rf.roleSwitch(Follower)
						rf.currentTerm = reply.Term
					}
				}
			}

		}(i)
	}

}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term        int
	CandidateID int
	// LastLogIndex int
	// LastLogTerm  int
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

type LogEntry struct {
}
type AppendEntriesArgs struct {
	Term         int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {

	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.resetTimerHeartbeat()

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	if args.Term > rf.currentTerm || rf.role != Follower {
		rf.roleSwitch(Follower)
		rf.currentTerm = args.Term
	}
	reply.Term = rf.currentTerm
	reply.Success = true
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term > rf.currentTerm {
		rf.roleSwitch(Follower)
		rf.currentTerm = args.Term
	}

	if args.Term < rf.currentTerm || (rf.votedFor != -1 && rf.votedFor != args.CandidateID) {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	if rf.votedFor == -1 || rf.votedFor == args.CandidateID {
		reply.Term = rf.currentTerm
		reply.VoteGranted = true
		rf.votedFor = args.CandidateID
		rf.resetTimerElection()
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
	return rf.peers[server].Call("Raft.RequestVote", args, reply)
}

func (rf *Raft) sendAppendEntries(id int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	return rf.peers[id].Call("Raft.AppendEntries", args, reply)
}

func (rf *Raft) broadcastHeartbeat() {
	rf.mu.Lock()
	if rf.role != Leader {
		rf.mu.Unlock()
		return
	}
	rf.resetTimerHeartbeat()
	rf.mu.Unlock()

	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		go func(id int) {
			rf.mu.Lock()
			args := AppendEntriesArgs{
				Term: rf.currentTerm,
			}
			rf.mu.Unlock()

			reply := AppendEntriesReply{}
			if rf.sendAppendEntries(id, &args, &reply) {
				rf.mu.Lock()
				defer rf.mu.Unlock()
				// check role whether changed during broadcasting
				if rf.role != Leader || rf.currentTerm != args.Term {
					return
				}
				if !reply.Success && reply.Term > rf.currentTerm {
					rf.roleSwitch(Follower)
					rf.currentTerm = reply.Term
					return
				}
			}
		}(i)
	}
}
