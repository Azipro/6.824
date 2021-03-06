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
	// 持久化
	log         []Log // local logs
	currentTerm int
	votedFor    int

	// 非持久化
	commitIndex int // 最后commit的槽位号
	lastApplied int // 最后apply的槽位号, lastApplied <= commitIndex, 小于就执行对应槽位号的Command, 然后++

	// leader (选举后初始化)
	nextIndex  []int // 记录每个服务器的下一个槽位号
	matchIndex []int // 记录每个服务器最后的已经复制的槽位号

	// client回复
	applyCh chan ApplyMsg

	state           int // 0: follower, 1: candidate, 2: leader, -1: killed
	heartBeat       time.Duration
	electionTimeout time.Duration
	lastActiveTime  time.Time
}

type Log struct {
	command interface{} // 槽位: 这条log执行的操作
	term    int         // 槽位: 任期号
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	term = rf.currentTerm
	if rf.state == 2 {
		isleader = true
	}

	return term, isleader
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
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
// 投票时需要用到的依据
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int // 候选者的任期号
	CandidateId  int // 候选者的index
	LastLogIndex int // 候选者最后的槽位号
	LastLogTerm  int // 候选者最后的槽位号的任期号
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int  // 当前任期号, 给候选者更新信息
	VoteGranted bool // true表示赞同票
}

type AppendEntriesArgs struct {
	Term           int           // leader任期号
	LeaderIndex    int           // leader索引号, follower用来重定向client
	PreLogIndex    int           // leader当前槽位号的前一个位置
	PreLogTerm     int           // leader当前槽位号的前一个位置的任期号
	EntriesCommand []interface{} // 用来给follower更新与leader的log不一致的尾端
	EntriesTerm    []int
	LeaderCommit   int  // leader的commitIndex
	HeartBeat      bool // 是否是心跳检测
}

type AppendEntriesReply struct {
	Term    int  // 当前任期号, 给leader更新信息
	Success bool // PreLogIndex和PreLogTerm匹配成功为true

	// 用来更新log的额外信息
	XTerm  int // follower中与leader冲突的log对应的任期号
	XIndex int // follower中, 对应任期号为XTerm的第一条Log条目的槽位号
	XLen   int // 如果follower在对应位置没有Log, 那么XTerm会返回-1, XLen表示空白的Log槽位数
}

//
// example RequestVote RPC handler.
//
// 决定是否要投出这票, 每个任期只投一票
// 1. 候选人最后一条Log条目的任期号 > 本地最后一条Log条目的任期号
// 2. 或者, 候选人最后一条Log条目的任期号 = 本地最后一条Log条目的任期号, 且候选人的Log记录长度 ≥ 本地Log记录的长度
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	candidateTerm := args.Term
	candidateId := args.CandidateId
	candidateLastLogIndex := args.LastLogIndex
	candidateLastLogTerm := args.LastLogTerm

	rf.mu.Lock()
	defer rf.mu.Unlock()

	// candidateTerm < rf.currentTerm: 请求者任期号更小, 直接反对
	// (candidateTerm == rf.currentTerm && rf.votedFor != -1 && rf.votedFor != candidateId): 如果任期相同, 且投过票给其他服务器, 直接反对
	if candidateTerm < rf.currentTerm || (candidateTerm == rf.currentTerm && rf.votedFor != -1 && rf.votedFor != candidateId) {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		// DPrintf("```[%d]``` does not vote for %d at term[%d], because candidateTerm = %d || rf.votedFor == %d", rf.me, candidateId, rf.currentTerm, candidateTerm, rf.votedFor)
		return
	}

	// if len(rf.log) == 0 { // 下面两种条件的边界情况
	// 	reply.VoteGranted = true
	// }
	if candidateLastLogTerm > rf.log[len(rf.log)-1].term {
		reply.VoteGranted = true
	} else if candidateLastLogTerm == rf.log[len(rf.log)-1].term && candidateLastLogIndex >= (len(rf.log)-1) {
		reply.VoteGranted = true
	} else { // 其他情况, 直接投反对票
		reply.VoteGranted = false
		DPrintf("```[%d]`` does not vote for %d at term[%d], because other situation", rf.me, candidateId, rf.currentTerm)
	}

	if reply.VoteGranted {
		rf.votedFor = candidateId      // 防止同一任期重复投
		rf.currentTerm = candidateTerm // 投票成功才更新
		rf.state = 0
		rf.resetTicker()
		DPrintf("---[%d]--- vote for %d at term[%d]", rf.me, rf.votedFor, rf.currentTerm)
	}
	reply.Term = rf.currentTerm
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	leaderTerm := args.Term
	// leaderIndex := args.LeaderIndex
	preLogIndex := args.PreLogIndex
	preLogTerm := args.PreLogTerm
	entriesCommand := args.EntriesCommand
	entriesTerm := args.EntriesTerm
	leaderCommit := args.LeaderCommit
	heartBeat := args.HeartBeat

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if leaderTerm < rf.currentTerm { // 这次请求的term比当前服务器记录的任期号小, 说明是旧的无效请求
		reply.Success = false
		reply.Term = rf.currentTerm
		return
	}

	rf.state = 0                // 转为follower
	rf.currentTerm = leaderTerm // 更新当前任期号

	if heartBeat { // 心跳检测, 重置选举定时器
		// follower的log不一定和leader同步了, 这里设置为[rf.commitIndex < len(rf.log) && rf.commitIndex <= leaderCommit]
		commitI := 0
		for commitI+rf.commitIndex < len(rf.log)-1 && commitI+rf.commitIndex < leaderCommit {
			if len(rf.log) > preLogIndex && rf.log[preLogIndex].term == preLogTerm && ((len(entriesTerm) == 0) || (len(entriesTerm) > commitI && commitI+preLogIndex+1 < len(rf.log) && entriesTerm[commitI] == rf.log[commitI+preLogIndex+1].term && entriesCommand[commitI] == rf.log[commitI+preLogIndex+1].command)) {
				commitI++ // 应该等同步了log之后再++
			} else {
				break
			}
		}
		rf.commitIndex = commitI + rf.commitIndex
		rf.resetTicker()
		// DPrintf("---[%d]--- receive a HeartBeat from [%d] (leaderTerm: %d)", rf.me, leaderIndex, leaderTerm)
	} else {
		logLen := len(rf.log)

		// 处理leader的log边界情况就可以, 本地log的边界情况考虑在else里面
		// if logLen == 0 { // follower还没有任何log
		// 	if preLogIndex == -1 {
		// 		for _, entry := range entries {
		// 			rf.log = append(rf.log, entry.(Log))
		// 		}
		// 		rf.commitIndex = leaderCommit

		// 		reply.Success = true
		// 	} else {
		// 		reply.XTerm = -1
		// 		reply.XLen = preLogIndex - (logLen - 1)
		// 	}
		// }
		// 将log的有效起始位改为1, log[0] = {command: nil, term: 0}, 不需要特判了
		// if preLogIndex == 0 { // leader中只有一个log(理论上看上去是一个, 但其实由于快速恢复, entries可能携带了后面很多log), 直接覆盖
		// 	rf.log = make([]Log, len(entriesTerm)+1) // 长度要多1, log[0]不记录
		// 	for i, _ := range entriesTerm {
		// 		rf.log[i+1] = Log{
		// 			command: entriesCommand[i],
		// 			term:    entriesTerm[i],
		// 		}
		// 	}
		// 	rf.commitIndex = leaderCommit

		// 	reply.Success = true
		// }
		if (logLen-1) >= preLogIndex && rf.log[preLogIndex].term == preLogTerm { // 匹配
			laterRPC := true
			if logLen-preLogIndex-1 >= len(entriesTerm) {
				for i := range entriesTerm {
					if rf.log[i+preLogIndex+1].command != entriesCommand[i] || rf.log[i+preLogIndex+1].term != entriesTerm[i] {
						laterRPC = false
						break
					}
				}
			} else {
				laterRPC = false
			}
			if laterRPC {
				// 延迟的RPC, 直接返回true
				// 不需要处理
				DPrintf("---[%d]--- recevie a later rpc with {%v %v}", rf.me, entriesTerm, entriesCommand)
			} else {
				// 不确定是否需要覆盖掉follower中的log, 从preLogIndex + 1的槽位开始赋值
				// preLogIndex + 1 就是不需要覆盖的长度, preLogIndex + 1 + len(entries)是更新完rf.log的长度
				// DPrintf("[%d] log is {%v}", rf.me, rf.log)
				rf.log = rf.log[:preLogIndex+1]
				for i, _ := range entriesTerm {
					rf.log = append(rf.log, Log{
						command: entriesCommand[i],
						term:    entriesTerm[i],
					})
				}
				DPrintf("[%d] log is {%v} after append", rf.me, rf.log)
			}

			reply.Success = true
		} else {
			reply.Success = false

			if (logLen - 1) < preLogIndex { // rf.log[preLogIndex] 为空, 下一次会转换为下面else的情况或上面匹配的情况
				reply.XTerm = -1
				reply.XLen = preLogIndex - (logLen - 1)
			} else { // 返回冲突的任期号和起始槽位
				oldterm := rf.log[preLogIndex].term
				index := preLogIndex - 1
				for ; index >= 0; index-- {
					if oldterm != rf.log[index].term {
						break
					}
				}
				reply.XTerm = oldterm
				reply.XIndex = index + 1
			}
		}
		rf.resetTicker()
	}
	reply.Term = rf.currentTerm
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

// 发送投票请求
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// leader执行, 发送AppendEntries(执行操作或心跳检测)请求
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// 无锁化, 这里有很多rpc操作, 很费时
func (rf *Raft) Election(logLen int, currentTerm int, me int, lastLogTerm int, peers []*labrpc.ClientEnd) bool {
	voteCount := make(chan bool, len(peers))
	args := RequestVoteArgs{
		Term:         currentTerm,
		CandidateId:  me,
		LastLogIndex: logLen - 1,
		LastLogTerm:  lastLogTerm,
	}

	// ctx, cancel := context.WithCancel(context.Background())
	// defer cancel()

	for index := range peers {
		if index == me {
			voteCount <- true
			continue
		}
		reply := RequestVoteReply{}
		go func(index int, args RequestVoteArgs, reply RequestVoteReply) {
			ok := rf.sendRequestVote(index, &args, &reply)
			if ok {
				voteCount <- reply.VoteGranted
			} else {
				voteCount <- false
			}
		}(index, args, reply)
	}

	count := 0
	granted := 0
	success := len(peers) / 2                      // granted中包括了自己的一票
	for granted <= success && count < len(peers) { // 赞同票超过半数, 或已经全部接收完毕
		select {
		// 这里有问题, 假设三个服务器, 一个宕机了, 两个差不多的时候变为Candidate, 都投票给自己, 两台服务器就会同时在这里等待第三台服务器的投票
		// 因为第三台已经宕机, 所以会同时退出; 因为调度策略的问题, 这时候Scheduler在等待rf.state=0, 再由ticker设置为1,
		// 然后重新进行选举, 这两台服务器几乎是同时的, 很容易陷入分割选票（Split Vote）
		// 1. 把超时时间设置到比选举定时器的时间小. 但必须比一个rpc来回时间大.
		// 2. 退出后重新设置超时时间.
		case <-time.After(time.Millisecond * 100): // 200ms之后还有服务器没响应, 直接退出
			return false
		case v := <-voteCount:
			count++
			if v {
				granted++
			}
		}
	}
	DPrintf("***[%d]*** receive %d vote from all %d at term[%d]", me, granted, count, currentTerm)
	// DPrintf("[%d] receive %d vote, success = %d", me, granted, success)
	return granted > success
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
	index := -1       // 这个command放在Log中的槽位号
	term := -1        // 当前任期号
	isLeader := false // 是否是leader

	// Your code here (2B).
	rf.mu.Lock()
	term = rf.currentTerm

	me := rf.me
	currentTerm := rf.currentTerm

	if rf.state == 2 {
		rf.mu.Unlock()
		isLeader = true
		index = rf.AppendLog(command)
		DPrintf("***[%d]*** append a log at log[%d] with term [%d], Command is {%v}", me, index, currentTerm, command)
	} else {
		rf.mu.Unlock()
	}

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
	rf.mu.Lock()
	rf.state = -1
	DPrintf("***[%d]*** excute Kill", rf.me)
	rf.mu.Unlock()
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for !rf.killed() {

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		time.Sleep(time.Millisecond * 5) // 每5ms检查一次
		rf.mu.Lock()
		// DPrintf("%d --- %d", rf.electionTimeout, time.Duration(time.Since(rf.lastActiveTime).Milliseconds()))
		if rf.state != 2 && rf.electionTimeout <= time.Duration(time.Since(rf.lastActiveTime).Milliseconds()) { // 超时
			// DPrintf("---[%d]--- become Candidate", rf.me)
			rf.state = 1
			rf.mu.Unlock()
		} else {
			rf.mu.Unlock()
		}
	}
}

func (rf *Raft) resetTicker() {
	rand.Seed(time.Now().UnixMilli())
	rf.electionTimeout = time.Duration(200 + rand.Intn(200))
	rf.lastActiveTime = time.Now()
}

// 所有服务器都需要执行的协程
// 1. 执行过半服务器相应的log中的Command
// 2. 相应RPC时, 如果term > currentTerm, 设置currentTerm = term, 转为follower
func (rf *Raft) Server() {
	for {
		rf.mu.Lock()
		if rf.state != -1 {
			if rf.lastApplied == rf.commitIndex {
				rf.mu.Unlock()
				time.Sleep(time.Millisecond * rf.heartBeat)
				continue
			}

			// TODO: 执行rf.log[rf.lastApplied].Command
			rf.lastApplied++

			// if rf.currentTerm == rf.log[rf.lastApplied].term {
			rf.applyCh <- ApplyMsg{
				CommandValid: true,
				CommandIndex: rf.lastApplied,
				Command:      rf.log[rf.lastApplied].command,
			}
			DPrintf("---[%d]--- commit log [%v]", rf.me, rf.log[rf.lastApplied])
			// }

			rf.mu.Unlock()
		} else {
			rf.mu.Unlock()
		}

	}
}

// 当该服务器为follower时, rpc函数就默认是follower的工作了
// 1. 响应candidate和leader发来的rpc
// 2. 如果electionTimeout到期, 转为candidate
func (rf *Raft) Follower() {
	// TODO
}

// 1. 刚转变为candidate时, 开始Election: 增加currentTerm; 投票给自己; 重置elctionTimeout; 发送RequestVote给所有服务器
// 2. 如果收到赞同票过半数, 转为leader
// 3. 如果收到新的AppendEntries从其他新的leader, 转为follower
// 4. 如果Election超时, retry
func (rf *Raft) Candidate() {
	rf.mu.Lock()

	rf.resetTicker()
	rf.votedFor = rf.me
	rf.currentTerm++

	DPrintf("---[%d]--- become Candidate with term[%d]", rf.me, rf.currentTerm)

	logLen := len(rf.log)
	currentTerm := rf.currentTerm
	me := rf.me
	lastLogTerm := -1
	peers := rf.peers
	if logLen != 0 {
		lastLogTerm = rf.log[logLen-1].term
	}
	rf.mu.Unlock()

	ok := rf.Election(logLen, currentTerm, me, lastLogTerm, peers)
	rf.mu.Lock()
	if ok && rf.state == 1 && rf.currentTerm == currentTerm { // 可能这次选举已经过时
		rf.state = 2
		DPrintf("---[%d]--- become Leader with term[%d]", rf.me, rf.currentTerm)
	} else {
		if rf.votedFor == rf.me && rf.state == 1 && rf.currentTerm == currentTerm { // 这次投票失败, 回退任期号
			DPrintf("---[%d]--- candidate become Leader falied with term[%d], rf.currentTerm--", rf.me, rf.currentTerm)
			rf.currentTerm--
			rf.votedFor = -1
		}
		// rf.resetTicker()
		rf.state = 0
	}
	rf.mu.Unlock()
}

// 当该服务器为Leader时
// 1. 刚当选leader时, 发送一次AppendEntries(heartbeat)
// 2. 接收client请求, 先写本地log, 返回client(但无结果), 发送AppendEntries, 过半数就提交, 成功信息通过chanel发送给client
// 3. 如果本地最后一个log > nextIndex[i] 发送AppendEntries到服务器i, 成功: 更新nextIndex[i]和matchIndex[i], 失败: 减小nextIndex[i], retry
// 4. 如果过半的服务器matchIndex >= N, 并且 log[N] == currentTerm, 更新commitIndex = N
func (rf *Raft) Leader(stop chan struct{}) {
	stopHeartBeat := make(chan struct{})
	stopCommit := make(chan struct{})

	rf.initLeader()
	go rf.HeartBeat(stopHeartBeat)
	// 当选leader, 先同步一次log
	// go rf.syncLog()

	// TODO: 接收client请求
	// Start()

	// 检查可以commit的log
	go rf.CommitLog(stopCommit)

	<-stop
	stopHeartBeat <- struct{}{}
	stopCommit <- struct{}{}
	DPrintf("^^^^^^^^^^^^Leader[%d] Closed^^^^^^^^^^^^", rf.me)
}

func (rf *Raft) initLeader() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// DPrintf("[%d] begin initLeader", rf.me)

	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))

	logLen := len(rf.log)

	for index := range rf.nextIndex {
		rf.nextIndex[index] = logLen
		rf.matchIndex[index] = logLen - 1
	}
}

func (rf *Raft) syncLog() {
	rf.mu.Lock()

	// DPrintf("~~~~~~~~~~~~~ [Sync Log] ~~~~~~~~~~~~~")

	peers := rf.peers
	me := rf.me

	args := AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderIndex:  rf.me,
		LeaderCommit: rf.commitIndex,
		HeartBeat:    false,
	}
	reply := AppendEntriesReply{}

	tmpNextIndex := make([]int, len(rf.nextIndex))
	copy(tmpNextIndex, rf.nextIndex)
	tmpLog := make([]Log, len(rf.log))
	copy(tmpLog, rf.log)

	rf.mu.Unlock()

	for index := range peers {
		if index == me {
			continue
		}

		go func(index int, args AppendEntriesArgs, reply AppendEntriesReply, nextIndex int, tmpLog []Log) {
			oldNextIndex := nextIndex
			for {
				entriesLen := len(tmpLog) - nextIndex
				entriesCommand := make([]interface{}, entriesLen)
				entriesTerm := make([]int, entriesLen)
				for i := 0; i < entriesLen; i++ {
					entriesCommand[i] = tmpLog[nextIndex+i].command
					entriesTerm[i] = tmpLog[nextIndex+i].term
				}
				args.PreLogIndex = nextIndex - 1
				args.PreLogTerm = tmpLog[args.PreLogIndex].term
				args.EntriesCommand = entriesCommand
				args.EntriesTerm = entriesTerm

				ok := rf.sendAppendEntries(index, &args, &reply)

				if ok {
					if reply.Success {
						rf.mu.Lock()
						if rf.nextIndex[index] == oldNextIndex { // 可能被修改过
							rf.nextIndex[index] = len(tmpLog)
							rf.matchIndex[index] = len(tmpLog) - 1
						}
						rf.mu.Unlock()
						return
					} else {
						rf.mu.Lock()
						if rf.nextIndex[index] == oldNextIndex && rf.state == 2 {
							rf.mu.Unlock()
							if reply.XTerm == -1 { // 情况一: follower在PreLogIndex处空白
								nextIndex = nextIndex - reply.XLen
							} else {
								hasSameTerm := -1
								for i := reply.XIndex; i < len(tmpLog); i++ {
									if reply.XTerm == tmpLog[i].term {
										hasSameTerm = i
									}
								}
								if hasSameTerm == -1 { // 情况二: follower与leader中没有相同的Xterm, 直接将nextIndex设置到XIndex
									nextIndex = reply.XIndex
								} else { // 情况三: follower与leader中有相同的Xterm, 将自己本地记录的S1的nextIndex设置到本地在XTerm位置的Log条目后面
									nextIndex = hasSameTerm + 1
								}
							}
						} else {
							rf.mu.Unlock()
						}
					}
				} else {
					return
				}
			}
		}(index, args, reply, tmpNextIndex[index], tmpLog)
	}
}

func (rf *Raft) AppendLog(command interface{}) int {
	rf.mu.Lock()

	index := rf.nextIndex[rf.me]
	rf.matchIndex[rf.me] = index
	rf.nextIndex[rf.me]++
	rf.log = append(rf.log, Log{command: command, term: rf.currentTerm})

	rf.mu.Unlock()

	go rf.syncLog() // 异步执行同步log, client需要立刻返回

	return index
}

func (rf *Raft) CommitLog(stop chan struct{}) {
	for {
		select {
		case <-stop:
			return
		case <-time.After(rf.heartBeat * time.Millisecond):
			rf.mu.Lock()
			if rf.state == 2 {
				nextCommit := rf.commitIndex + 1
				do := true
				for do && nextCommit < len(rf.log) {
					count := 0

					if rf.log[nextCommit].term != rf.currentTerm {
						nextCommit++
						continue
					}

					for index := range rf.peers {
						if rf.matchIndex[index] >= nextCommit {
							count++
						}
					}

					if count > len(rf.peers)/2 && rf.log[nextCommit].term == rf.currentTerm {
						rf.commitIndex = nextCommit
						nextCommit++
						// rf.applyCh <- ApplyMsg{
						// 	CommandValid: true,
						// 	CommandIndex: rf.commitIndex,
						// 	Command:      rf.log[rf.commitIndex].command,
						// }
						// DPrintf("%v", ApplyMsg{
						// 	CommandValid: true,
						// 	CommandIndex: rf.commitIndex,
						// 	Command:      rf.log[rf.commitIndex].command,
						// })
					} else {
						do = false
					}
				}
			}
			rf.mu.Unlock()
		}
	}
}

func (rf *Raft) HeartBeat(stop chan struct{}) {
	for {
		select {
		case <-stop:
			return
		case <-time.After(rf.heartBeat * time.Millisecond):
			rf.mu.Lock()
			if rf.state == 2 {
				rf.mu.Unlock()
				for index := range rf.peers {
					rf.mu.Lock()
					if index == rf.me {
						rf.mu.Unlock()
						continue
					}
					args := AppendEntriesArgs{
						Term:         rf.currentTerm,
						LeaderIndex:  rf.me,
						PreLogIndex:  rf.nextIndex[index] - 1,
						PreLogTerm:   rf.log[rf.nextIndex[index]-1].term,
						LeaderCommit: rf.commitIndex,
						HeartBeat:    true,
					}
					reply := AppendEntriesReply{}

					// 带上rf.nextIndex[index]之后的log
					entriesLen := len(rf.log) - rf.nextIndex[index]
					entriesCommand := make([]interface{}, entriesLen)
					entriesTerm := make([]int, entriesLen)
					for i := 0; i < entriesLen; i++ {
						entriesCommand[i] = rf.log[rf.nextIndex[index]+i].command
						entriesTerm[i] = rf.log[rf.nextIndex[index]+i].term
					}
					args.EntriesCommand = entriesCommand
					args.EntriesTerm = entriesTerm

					rf.mu.Unlock()
					go func(index int, args AppendEntriesArgs, reply AppendEntriesReply) {
						rf.sendAppendEntries(index, &args, &reply)
						// if ok {
						// 	rf.mu.Lock()
						// 	if reply.Term > rf.currentTerm {
						// 		rf.state = 0
						// 		DPrintf("---[%d]--- send HeartBeat to [%d], but currentTerm is smaller than [%d]", rf.me, index, reply.Term)
						// 	}
						// 	rf.mu.Unlock()
						// }
					}(index, args, reply)
				}
			} else {
				rf.mu.Unlock()
			}
		}
	}
}

func (rf *Raft) Scheduler() {
	// stopCandidate := make(chan struct{})
	stopLeader := make(chan struct{})

	leaderIsRunning := false
	candidateIsRunning := false
	for {
		rf.mu.Lock()
		switch rf.state {
		case 0: // follower
			rf.mu.Unlock()

			if leaderIsRunning {
				stopLeader <- struct{}{}
				leaderIsRunning = false
			}

			if candidateIsRunning {
				candidateIsRunning = false
			}

		case 1: // candidate
			rf.mu.Unlock()

			if leaderIsRunning {
				stopLeader <- struct{}{}
				leaderIsRunning = false
			}

			if !candidateIsRunning {
				go rf.Candidate()
				candidateIsRunning = true
			}

		case 2: // leader
			rf.mu.Unlock()

			if !leaderIsRunning {
				go rf.Leader(stopLeader)
				leaderIsRunning = true
			}

			if candidateIsRunning {
				candidateIsRunning = false
			}

		case -1: // killed
			if candidateIsRunning {
				candidateIsRunning = false
				DPrintf("+++[%d]+++ be killed when it is a candidate", rf.me)
			}
			if leaderIsRunning {
				stopLeader <- struct{}{}
				leaderIsRunning = false
				DPrintf("+++[%d]+++ be killed when it is a leader", rf.me)
			}
			rf.mu.Unlock()
		default:
			rf.mu.Unlock()
			log.Fatal("wrong state")
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
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.state = 0
	rf.votedFor = -1
	rf.currentTerm = 0
	rf.applyCh = applyCh
	rf.log = make([]Log, 1) // log[0] 占位
	rf.log[0].term = 0
	rf.log[0].command = nil
	// rf.nextIndex = make([]int, len(rf.peers))
	// rf.matchIndex = make([]int, len(rf.peers))

	rf.lastActiveTime = time.Now()
	rf.electionTimeout = time.Duration(200 + rand.Intn(200)) // 选举定时器的超时时间为[200 , 400)ms
	rf.heartBeat = time.Duration(50)                         // 心跳间隔为50ms

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	// go rf.testRpc()
	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.Server()
	go rf.Scheduler()
	// go rf.PrintLogs()

	return rf
}

func (rf *Raft) PrintLogs() {
	for {
		time.Sleep(time.Second * 2)
		rf.mu.Lock()
		fmt.Printf("[%d] ===Log=== %v ===Log===\n", rf.me, rf.log)
		rf.mu.Unlock()
	}
}

func (rf *Raft) testRpc() {
	// wg := sync.WaitGroup{}
	// wg.Add(100)
	t := time.Now()
	for i := 0; i < 100; i++ {
		if i == rf.me {
			continue
		}
		// go func(i int) {
		// fmt.Println(i)
		start := time.Now()
		args := AppendEntriesArgs{
			Term:         i,
			LeaderIndex:  0,
			PreLogIndex:  0,
			PreLogTerm:   0,
			LeaderCommit: 0,
			HeartBeat:    true,
		}
		reply := AppendEntriesReply{}
		rf.sendAppendEntries(i%3, &args, &reply)
		fmt.Printf("[%d] : allcost:%f   cost: %v\n", i%3, time.Since(t).Seconds(), time.Since(start))
		// 	wg.Done()
		// }(i)
	}
	// wg.Wait()
}
