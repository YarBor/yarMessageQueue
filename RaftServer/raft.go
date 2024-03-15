package RaftServer

import (
	"MqServer/RaftServer/Persister"
	//	"bytes"
	"bytes"
	"context"
	"fmt"
	"log"
	"math/rand"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	MyLogger "MqServer/Log"
	labPack "MqServer/RaftServer/Pack"
)

func (rf *Raft) checkFuncDone(_ string) func() {
	return func() {}
	/* used to check deadlock questions  */
	//t := time.Now().UnixMilli()
	//i := make(chan bool, 1)
	//i2 := make(chan bool, 1)
	//go func() {
	//	r.Dolog(-1, t, FuncName+" GO")
	//	i2 <- true
	//	for {
	//		select {
	//		case <-time.After(HeartbeatTimeout * 2 * time.Millisecond):
	//			r.Dolog(-1, "\n", t, "!!!!\t", FuncName+" MayLocked\n")
	//		case <-i:
	//			close(i)
	//			return
	//		}
	//	}
	//}()
	//<-i2
	//close(i2)
	//return func() {
	//	i <- true
	//	r.Dolog(-1, t, FuncName+" return", time.Now().UnixMilli()-t, "ms")
	//}
}

var (
	LevelLeader    = int32(3)
	LevelCandidate = int32(2)
	LevelFollower  = int32(1)

	commitChanSize   = int32(100)
	HeartbeatTimeout = 400 * time.Millisecond
	voteTimeOut      = 100

	LogCheckBeginOrReset = 0
	LogCheckAppend       = 1
	LogCheckStore        = 2
	LogCheckIgnore       = 3
	LogCheckSnap         = 5

	UpdateLogLines = 200
)

func (rf *Raft) Dolog(index int, i ...interface{}) {
	if index == -1 {
		MyLogger.TRACE(append([]interface{}{interface{}(fmt.Sprintf("%-35s", fmt.Sprintf("{Level:%d}[T:%d]Server[%d]-[nil]", atomic.LoadInt32(&rf.level), atomic.LoadInt32(&rf.term), rf.me)))}, i...)...)
	} else {
		MyLogger.TRACE(append([]interface{}{interface{}(fmt.Sprintf("%-35s", fmt.Sprintf("{Level:%d}[T:%d]Server[%d]-[%d]", atomic.LoadInt32(&rf.level), atomic.LoadInt32(&rf.term), rf.me, index)))}, i...)...)
	}
}

type RequestArgs struct {
	SelfTerm     int32
	LastLogIndex int32
	LastLogTerm  int32
	Time         time.Time
	SelfIndex    int32
	CommitIndex  int32
	Msg          []*LogData
}
type RequestReply struct {
	// Your data here (2A).
	PeerSelfTerm     int32
	PeerLastLogIndex int32
	PeerLastLogTerm  int32
	ReturnTime       time.Time
	IsAgree          bool
	PeerCommitIndex  int32
	LogDataMsg       *LogData
}

type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
	CommandTerm  int

	SnapshotValid bool
	SnapshotTerm  int
	SnapshotIndex int
	Snapshot      []byte
}

type LogData struct {
	Msg           *ApplyMsg
	LastTimeIndex int
	LastTimeTerm  int
	SelfIndex     int
	SelfTermNow   int
}

func (a *ApplyMsg) string() string {
	return fmt.Sprintf("%+v", *a)
}
func (a *LogData) string() string {
	return fmt.Sprintf(" %+v[this.Msg:%s] ", *a, a.Msg.string())
}

type Log struct {
	Msgs    []*ApplyMsg
	MsgRwMu sync.RWMutex
}

type RaftPeer struct {
	C                *ClientEnd
	modeLock         sync.Mutex
	BeginHeartBeat   chan struct{}
	StopHeartBeat    chan struct{}
	JumpHeartBeat    chan struct{}
	SendHeartBeat    chan struct{}
	logIndexTermLock sync.Mutex
	logIndex         int32
	lastLogTerm      int32
	lastTalkTime     int64
	commitIndex      int32
}

func (R *RaftPeer) updateLastTalkTime() {
	atomic.StoreInt64(&R.lastTalkTime, time.Now().UnixMicro())
}
func (R *RaftPeer) isTimeOut() bool {
	return (time.Now().UnixMicro() - atomic.LoadInt64(&R.lastTalkTime)) > (HeartbeatTimeout * 2).Microseconds()
}
func (rf *Raft) checkOutLeaderOnline() bool {
	if rf.level != LevelLeader {
		return false
	}
	Len := len(rf.raftPeers)
	count := 1
	for i := range rf.raftPeers {
		if i != rf.me && !rf.raftPeers[i].isTimeOut() {
			count++
			if count > Len/2 {
				return true
			}
		}
	}
	return false
}

type MsgStore struct {
	msgs  []*LogData
	owner int
	term  int32
	mu    sync.Mutex
}

func (s *MsgStore) string() string {
	var str []byte
	for _, ld := range s.msgs {
		str = append(str, []byte(ld.string())...)
		str = append(str, []byte("\n\t")...)
	}
	str = append(str, []byte(fmt.Sprintf("\n\towner: %d", s.owner))...)
	return string(str)
}

type Raft struct {
	mu        sync.Mutex
	peers     []*ClientEnd
	persister *Persister.Persister
	me        int
	dead      int32

	isLeaderAlive int32
	level         int32

	commitIndex      int32
	commitIndexMutex sync.Mutex

	term            int32
	termLock        sync.Mutex
	timeOutChan     chan struct{}
	levelChangeChan chan struct{}
	raftPeers       []RaftPeer
	commandLog      Log

	commitChan          chan int32
	pMsgStore           *MsgStore // nil
	pMsgStoreCreateLock sync.Mutex

	KilledChan chan bool

	applyChan chan ApplyMsg

	logSize int64
	wg      sync.WaitGroup
}

func (rf *Raft) getCommitIndex() int32 {
	rf.commitIndexMutex.Lock()
	defer rf.commitIndexMutex.Unlock()
	return rf.getCommitIndexUnsafe()
}

func (rf *Raft) getCommitIndexUnsafe() int32 {
	return atomic.LoadInt32(&rf.commitIndex)
}
func (rf *Raft) tryUpdateCommitIndex(i int32) {
	rf.commitChan <- i
}

// because of persistent The caller needs exclusive rf.commands.msgs (lock)
func (rf *Raft) justSetCommitIndex(i int32) {
	rf.commitIndexMutex.Lock()
	defer rf.commitIndexMutex.Unlock()
	rf.commitIndex = i
}

func (rf *Raft) setCommitIndex(i int32) {
	rf.commitIndexMutex.Lock()
	defer rf.commitIndexMutex.Unlock()
	rf.setCommitIndexUnsafe(i)
}

func (rf *Raft) setCommitIndexUnsafe(i int32) {
	rf.commitIndex = i
}
func (rf *Raft) GetSnapshot() *ApplyMsg {
	rf.commandLog.MsgRwMu.RLock()
	defer rf.commandLog.MsgRwMu.RUnlock()
	return rf.getSnapshotUnsafe()
}

// unsafe
func (rf *Raft) getSnapshotUnsafe() *ApplyMsg {
	if rf.commandLog.Msgs[0].Snapshot != nil {
		rf.Dolog(-1, fmt.Sprintf("Get SnapShot index[%d] , term[%d]", rf.commandLog.Msgs[0].SnapshotIndex, rf.commandLog.Msgs[0].SnapshotTerm))
	}
	return rf.commandLog.Msgs[0]
}

func (rf *Raft) getLogIndex() int32 {
	rf.commandLog.MsgRwMu.RLock()
	defer rf.commandLog.MsgRwMu.RUnlock()
	return rf.getLogIndexUnsafe()
}
func (rf *Raft) getLogIndexUnsafe() int32 {
	if rf.commandLog.Msgs[len(rf.commandLog.Msgs)-1].SnapshotValid {
		return int32(rf.commandLog.Msgs[len(rf.commandLog.Msgs)-1].SnapshotIndex)
	} else {
		return int32(rf.commandLog.Msgs[len(rf.commandLog.Msgs)-1].CommandIndex)
	}
}

func (rf *Raft) getTerm() int32 {
	rf.termLock.Lock()
	defer rf.termLock.Unlock()
	return rf.term
}
func (rf *Raft) setTerm(i int32) {
	rf.termLock.Lock()
	defer rf.termLock.Unlock()
	rf.setTermUnsafe(i)
}
func (rf *Raft) setTermUnsafe(i int32) {
	rf.term = i
}
func (rf *Raft) beginSendHeartBeat() {
	for i := range rf.raftPeers {
		if i != rf.me {
			select {
			case rf.raftPeers[i].BeginHeartBeat <- struct{}{}:
			default:
			}
		}
	}
}
func (rf *Raft) stopSendHeartBeat() {
	for i := range rf.raftPeers {
		if i != rf.me {
			select {
			case rf.raftPeers[i].StopHeartBeat <- struct{}{}:
			default:
			}
		}
	}
}
func (rf *Raft) registeHeartBeat(index int) {
	defer rf.wg.Done()
	for !rf.killed() {
	restart:
		select {
		case <-rf.KilledChan:
			return
		case <-rf.raftPeers[index].BeginHeartBeat:
		}
		for {
			select {
			case <-rf.KilledChan:
				return
			case <-rf.raftPeers[index].StopHeartBeat:
				goto restart
			case <-rf.raftPeers[index].JumpHeartBeat:
				continue
			case <-time.After(HeartbeatTimeout):
			case <-rf.raftPeers[index].SendHeartBeat:
			}
			if rf.getLevel() != LevelLeader {
				goto restart
			}
			rf.goSendHeartBeat(index)
		}
	}
}
func (rf *Raft) goSendHeartBeat(index int) bool {
	arg := RequestArgs{SelfIndex: int32(rf.me)}
	rpl := RequestReply{}
	arg.LastLogIndex, arg.LastLogTerm = rf.getLastLogData()
	arg.SelfTerm = rf.getTerm()
	arg.Time = time.Now()
	arg.CommitIndex = rf.getCommitIndex()
	arg.Msg = nil
	// do call
	ok := rf.call(index, "RaftServer.HeartBeat", &arg, &rpl)
	if !ok {
		return false
	}
	if ok && !rpl.IsAgree && (rpl.PeerSelfTerm > rf.getTerm() || rpl.PeerLastLogIndex > rf.getLogIndex() || rpl.PeerLastLogTerm > arg.LastLogTerm) {
		rf.Dolog(index, "r.HeartBeatErrr", rf.getLogIndex(), "heartbeat return false Going to be Follower", arg.string(), rpl.string())
		rf.changeToFollower(&rpl)
	} else {
		func() {
			rf.raftPeers[index].logIndexTermLock.Lock()
			defer rf.raftPeers[index].logIndexTermLock.Unlock()
			if rpl.PeerLastLogTerm > rf.raftPeers[index].lastLogTerm {
				rf.raftPeers[index].lastLogTerm = rpl.PeerLastLogTerm
				rf.raftPeers[index].logIndex = rpl.PeerLastLogIndex
			} else if rpl.PeerLastLogTerm == rf.raftPeers[index].lastLogTerm {
				rf.raftPeers[index].logIndex = rpl.PeerLastLogIndex
			}
			rf.raftPeers[index].commitIndex = rpl.PeerCommitIndex
		}()
		if rpl.PeerLastLogIndex < arg.LastLogIndex || rpl.PeerLastLogTerm < arg.LastLogTerm {
			rf.tryleaderUpdatePeer(index, &LogData{Msg: nil, LastTimeIndex: int(arg.LastLogIndex), LastTimeTerm: int(arg.LastLogTerm)})
		}
	}
	return true
}
func (rf *Raft) Ping() bool {
	if rf.getLevel() != LevelLeader {
		return false
	}
	finish := make(chan bool, len(rf.raftPeers))
	for index := range rf.raftPeers {
		if index != rf.me {
			rf.wg.Add(1)
			go func(i int) {
				select {
				case rf.raftPeers[i].JumpHeartBeat <- struct{}{}:
				default:
				}
				finish <- rf.goSendHeartBeat(i)
				rf.Dolog(i, "ping received Peer Alive")
				rf.wg.Done()
			}(index)
		}
	}
	result := make(chan bool, len(rf.raftPeers))
	rf.wg.Add(1)
	go func() {
		flag := 1
		IsSend := false
		for i := 0; i < len(rf.raftPeers)-1; i++ {
			if <-finish {
				flag++
			}
			if !IsSend && flag > len(rf.raftPeers)/2 {
				result <- true
				IsSend = true
			}
		}
		close(finish)
		close(result)
		rf.wg.Done()
	}()
	return <-result
}
func (rf *Raft) changeToLeader() {
	rf.Dolog(-1, "Going to Be Leader")
	atomic.StoreInt32(&rf.isLeaderAlive, 1)
	rf.setLevel(LevelLeader)
	select {
	case rf.levelChangeChan <- struct{}{}:
	default:
	}
	for i := range rf.raftPeers {
		if i == rf.me {
			rf.raftPeers[i].logIndexTermLock.Lock()
			rf.raftPeers[rf.me].logIndex, rf.raftPeers[rf.me].lastLogTerm = rf.getLastLogData()
			rf.raftPeers[i].logIndexTermLock.Unlock()
		} else {
			rf.raftPeers[i].logIndexTermLock.Lock()
			rf.raftPeers[i].lastLogTerm, rf.raftPeers[i].logIndex = 0, 0
			rf.raftPeers[i].logIndexTermLock.Unlock()
		}
	}
	rf.beginSendHeartBeat()
}
func (rf *Raft) changeToCandidate() {
	rf.Dolog(-1, "Going to Be Candidate")
	rf.setLevel(LevelCandidate)
	select {
	case rf.levelChangeChan <- struct{}{}:
	default:
	}
}
func (rf *Raft) changeToFollower(rpl *RequestReply) {
	rf.Dolog(-1, "Going to Be Follower")
	if rf.getLevel() == LevelLeader {
		rf.stopSendHeartBeat()
	}
	rf.setLevel(LevelFollower)
	if rpl != nil {
		if rpl.PeerSelfTerm > rf.getTerm() {
			rf.setTerm(rpl.PeerSelfTerm)
		}
	}
	select {
	case rf.levelChangeChan <- struct{}{}:
	default:
	}
}

func (rf *Raft) HeartBeat(arg *RequestArgs, rpl *RequestReply) {
	if rf.killed() {
		return
	}
	defer func(tmpArg *RequestArgs, tmpRpl *RequestReply) {
		rf.Dolog(int(tmpArg.SelfIndex), "REPLY Heartbeat", "\t\n Arg:", tmpArg.string(), "\t\n Rpl:", tmpRpl.string())
	}(arg, rpl)

	rpl.PeerLastLogIndex, rpl.PeerLastLogTerm = rf.getLastLogData()
	rpl.PeerSelfTerm = rf.getTerm()
	rpl.ReturnTime = time.Now()

	rpl.IsAgree = arg.LastLogTerm >= rpl.PeerLastLogTerm && (arg.LastLogTerm > rpl.PeerLastLogTerm || arg.LastLogIndex >= rpl.PeerLastLogIndex)
	if !rpl.IsAgree {
		return
	}
	if arg.SelfTerm > rpl.PeerSelfTerm {
		rpl.PeerSelfTerm = arg.SelfIndex
		rf.setTerm(arg.SelfTerm)
	}
	select {
	case rf.timeOutChan <- struct{}{}:
	default:
	}

	if rf.getLevel() == LevelLeader {
		rpll := *rpl
		rpll.PeerSelfTerm = arg.SelfTerm
		rf.changeToFollower(&rpll)
	}
	if arg.Msg == nil || len(arg.Msg) == 0 {
	} else {
		//for i := range arg.Msg {
		//	rf.Dolog(int(arg.SelfIndex), "Try LOAD-Log", arg.Msg[i].string())
		//}
		rpl.LogDataMsg = rf.updateMsgs(arg.Msg)
		rf.Dolog(-1, "Request Update Log Data To Leader", rpl.LogDataMsg)
		rpl.PeerLastLogIndex, rpl.PeerLastLogTerm = rf.getLastLogData()
	}
	if arg.LastLogTerm > rpl.PeerLastLogTerm {
	} else if arg.CommitIndex > rf.getCommitIndex() {
		rf.tryUpdateCommitIndex(arg.CommitIndex)
	}
	rpl.PeerCommitIndex = rf.getCommitIndex()
}

func (rf *Raft) GetState() (int, bool) {
	return int(rf.getTerm()), rf.getLevel() == LevelLeader
}

func (rf *Raft) persist(snapshot *ApplyMsg) {
	defer rf.checkFuncDone("persist")()

	rf.commandLog.MsgRwMu.Lock()
	defer rf.commandLog.MsgRwMu.Unlock()
	rf.persistUnsafe(snapshot)

}

type CommandPersistNode struct {
	Term    int64
	Command interface{}
	Index   int64
}

// 此函数不保证对log的操作的原子性
func (rf *Raft) persistUnsafe(snapshot *ApplyMsg) {
	defer rf.checkFuncDone("persistUnsafe")()
	if snapshot != nil {
		if snapshot.SnapshotIndex < rf.commandLog.Msgs[0].SnapshotIndex {
			return
		}
	}

	buffer := bytes.Buffer{}
	encoder := labPack.NewEncoder(&buffer)
	err := encoder.Encode(rf.getCommitIndexUnsafe())
	if err != nil {
		log.Fatal("Failed to encode CommitIndex: ", err)
	}
	err = encoder.Encode(atomic.LoadInt32(&rf.term))
	if err != nil {
		log.Fatal("Failed to encode Term: ", err)
	}
	i := rf.commandLog.Msgs[0].Snapshot
	rf.commandLog.Msgs[0].Snapshot = nil
	err = encoder.Encode(rf.commandLog.Msgs)
	if err != nil {
		log.Fatal("Failed to encode Msgs: ", err)
	}
	rf.commandLog.Msgs[0].Snapshot = i
	encodedData := buffer.Bytes() // 获取编码后的数据

	if rf.commandLog.Msgs[0] != nil && rf.commandLog.Msgs[0].Snapshot != nil {
		rf.persister.Save(encodedData, rf.commandLog.Msgs[0].Snapshot) // 保存数据到持久化存储
	} else {
		rf.persister.Save(encodedData, nil) // 保存数据到持久化存储
	}

	atomic.StoreInt64(&rf.logSize, int64(rf.persister.RaftStateSize()))
}

// Unsafe
func showMsgS(rf []*ApplyMsg) string {
	str := "\n"
	for i := range rf {
		str += rf[i].string() + "\n"
	}
	return str
}

func (rf *Raft) RaftSize() int64 {
	return atomic.LoadInt64(&rf.logSize)
}

func (rf *Raft) readPersist(data []byte) {
	defer rf.checkFuncDone("readPersist")()
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}

	r := bytes.NewBuffer(data)
	d := labPack.NewDecoder(r)

	var term int32
	var commitedIndex int32
	var msgs []*ApplyMsg

	if err := d.Decode(&commitedIndex); err != nil {
		log.Fatal("Failed to decode CommitIndex: ", err)
	}
	if err := d.Decode(&term); err != nil {
		log.Fatal("Failed to decode Term: ", err)
	}
	if err := d.Decode(&msgs); err != nil {
		log.Fatal("Failed to decode Msgs: ", err)
	}

	rf.term = term
	if commitedIndex > 0 {
		rf.commitIndex = commitedIndex
	} else {
		rf.commitIndex = 0
	}
	rf.commandLog.Msgs = msgs

	// 记录输出
	output := fmt.Sprintf("Decoded CommitIndex: %v, Term: %v, Msgs: %v",
		rf.commitIndex,
		rf.term,
		rf.commandLog.Msgs)
	rf.Dolog(-1, "Persist Load - "+output)
	rf.commandLog.Msgs[0].Snapshot = rf.persister.ReadSnapshot()
}

// unsafe
func (rf *Raft) GetTargetCacheIndex(index int) int {
	if len(rf.commandLog.Msgs) == 0 {
		// return -1
		panic("len(rf.commandLog.Msgs) == 0")
	}
	LastLog := rf.commandLog.Msgs[len(rf.commandLog.Msgs)-1]
	IndexReturn := 0
	if LastLog.SnapshotValid {
		IndexReturn = len(rf.commandLog.Msgs) - (LastLog.SnapshotIndex - index) - 1
	} else {
		IndexReturn = len(rf.commandLog.Msgs) - (LastLog.CommandIndex - index) - 1
	}
	if !(IndexReturn > 0 && IndexReturn < len(rf.commandLog.Msgs)) {
		rf.Dolog(-1, fmt.Sprintf("Try to get index %d return %d rf.Msgs(len(%d) , lastIndex(%d))", index, IndexReturn, len(rf.commandLog.Msgs), (rf.commandLog.Msgs[len(rf.commandLog.Msgs)-1].CommandIndex)))
	}
	return IndexReturn
}

func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	defer rf.checkFuncDone("Snapshot")()

	rf.commandLog.MsgRwMu.Lock()
	defer rf.commandLog.MsgRwMu.Unlock()
	rf.SnapshotUnsafe(index, -1, snapshot)
}

func (rf *Raft) SnapshotUnsafe(index int, InputSnapShotTerm int, snapshot []byte) {
	// Your code here (2D).
	defer rf.checkFuncDone("SnapshotUnsafe")()
	rf.Dolog(-1, fmt.Sprintf("Try save snapshot Index:%v  data :%v", index, snapshot))
	if (InputSnapShotTerm != -1 && InputSnapShotTerm < rf.getSnapshotUnsafe().SnapshotTerm) || index <= rf.getSnapshotUnsafe().SnapshotIndex {
		return
	}

	inputIndexSCacheIndex := rf.GetTargetCacheIndex(index)

	var newMagsHead *ApplyMsg
	if inputIndexSCacheIndex < 0 {
		panic("Snapshot Called, But Not found correspond Log")
	} else if inputIndexSCacheIndex >= len(rf.commandLog.Msgs) {
		// rf.Dolog(-1, "Snapshot called but not found correspond Log , inputIndexSCacheIndex >= len(rf.commandLog.Msgs)", "index", index, "inputIndexSCacheIndex", inputIndexSCacheIndex, "len(rf.commandLog.Msgs)", len(rf.commandLog.Msgs))
		if InputSnapShotTerm == -1 {
			return
		} else {
			newMagsHead = &ApplyMsg{CommandValid: false, Command: nil, CommandIndex: 0, CommandTerm: -1, SnapshotValid: true, Snapshot: snapshot, SnapshotTerm: InputSnapShotTerm, SnapshotIndex: index}
		}
	} else {
		term := rf.commandLog.Msgs[inputIndexSCacheIndex].CommandTerm
		if InputSnapShotTerm != -1 && InputSnapShotTerm > term {
			term = InputSnapShotTerm
		}
		newMagsHead = &ApplyMsg{CommandValid: false, Command: nil, CommandIndex: 0, CommandTerm: -1, SnapshotValid: true, Snapshot: snapshot, SnapshotTerm: term, SnapshotIndex: index}
	}
	oldMsgs := rf.commandLog.Msgs
	rf.commandLog.Msgs = make([]*ApplyMsg, 0)
	if inputIndexSCacheIndex >= len(oldMsgs) {
		rf.commandLog.Msgs = append(rf.commandLog.Msgs, newMagsHead)
	} else {
		if len(oldMsgs) > 1 {
			rf.commandLog.Msgs = append(rf.commandLog.Msgs, newMagsHead)
			rf.commandLog.Msgs = append(rf.commandLog.Msgs, oldMsgs[inputIndexSCacheIndex+1:]...)
		} else {
			rf.commandLog.Msgs = append(rf.commandLog.Msgs, newMagsHead)
		}
	}
	rf.persistUnsafe(rf.getSnapshotUnsafe())
	rf.Dolog(-1, fmt.Sprintf("Saved snapshot Index:%v  data :%v", index, snapshot))
}

func (r *RequestArgs) string() string {
	if r.Msg == nil || len(r.Msg) == 0 {
		return fmt.Sprintf("%+v ", *r)
	} else {
		str := ""
		for i := 0; i < len(r.Msg); i++ {
			str += fmt.Sprintf("Msg:(%s)", r.Msg[i].string())
		}
		str += fmt.Sprintf("\n\t%+v", *r)
		return str
	}
}
func (r *RequestReply) string() string {
	if r.LogDataMsg == nil {
		return fmt.Sprintf("%+v ", *r)
	} else {
		return fmt.Sprintf("Msg:(%s) %+v", r.LogDataMsg.string(), *r)
	}
}

func (rf *Raft) RequestPreVote(args *RequestArgs, reply *RequestReply) {
	if rf.killed() {
		return
	}
	defer rf.checkFuncDone("RequestPreVote")()
	reply.PeerLastLogIndex, reply.PeerLastLogTerm = rf.getLastLogData()
	reply.PeerSelfTerm = rf.getTerm()
	reply.ReturnTime = time.Now()
	selfCommitINdex := rf.getCommitIndex()
	reply.PeerCommitIndex = selfCommitINdex
	reply.IsAgree = args.CommitIndex >= selfCommitINdex && (atomic.LoadInt32(&rf.isLeaderAlive) == 0 && args.LastLogTerm >= reply.PeerLastLogTerm && (args.LastLogTerm > reply.PeerLastLogTerm || args.LastLogIndex >= reply.PeerLastLogIndex) && reply.PeerSelfTerm < args.SelfTerm)
}

func (rf *Raft) RequestVote(args *RequestArgs, reply *RequestReply) {
	if rf.killed() {
		return
	}
	defer rf.checkFuncDone("RequestVote")()
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.termLock.Lock()
	defer rf.termLock.Unlock()
	reply.PeerSelfTerm = rf.term
	reply.PeerLastLogIndex, reply.PeerLastLogTerm = rf.getLastLogData()
	//	选举投票与否的标准是 各个节点的commit程度 各个节点的日志的新旧程度 当新旧程度一样时 再比较投票的任期
	reply.IsAgree = true
	selfCommitINdex := rf.getCommitIndex()
	if args.CommitIndex > selfCommitINdex {
		reply.IsAgree = true
	} else if args.CommitIndex < selfCommitINdex {
		reply.IsAgree = false
	} else {
		if args.LastLogTerm < reply.PeerLastLogTerm {
			reply.IsAgree = false
			rf.Dolog(int(args.SelfIndex), "Disagree Vote Because of "+"args.LastLogTerm < reply.PeerLastLogTerm")
		} else if args.LastLogTerm == reply.PeerLastLogTerm {
			if args.LastLogIndex < reply.PeerLastLogIndex {
				reply.IsAgree = false
				rf.Dolog(int(args.SelfIndex), "Disagree Vote Because of "+"args.LastLogIndex < reply.PeerLastLogIndex")
			} else if args.LastLogIndex == reply.PeerLastLogIndex {
				if args.SelfTerm <= reply.PeerSelfTerm {
					reply.IsAgree = false
					rf.Dolog(int(args.SelfIndex), "Disagree Vote Because of "+"args.SelfTerm <= reply.PeerSelfTerm")
				}
			}
		}
	}
	if args.SelfTerm <= reply.PeerSelfTerm {
		reply.IsAgree = false
		rf.Dolog(int(args.SelfIndex), "Disagree Vote Because of "+"args.SelfTerm <= reply.PeerSelfTerm")
	}
	if reply.IsAgree {
		if rf.getLevel() == LevelLeader {
			rf.changeToFollower(nil)
		}
		rf.setTermUnsafe(args.SelfTerm)
		atomic.StoreInt32(&rf.isLeaderAlive, 1)
	}
	reply.ReturnTime = time.Now()
	reply.PeerCommitIndex = selfCommitINdex
	rf.Dolog(int(args.SelfIndex), "answer RequestVote", args.string(), reply.string())
}

func (rf *Raft) checkMsg(data *LogData) int {
	if data == nil {
		return -1
	}
	// check过程中进行拿锁

	if len(rf.commandLog.Msgs) == 0 {
		panic("len(rf.commandLog.Msgs) == 0")
	}

	rf.pMsgStore.mu.Lock()
	defer rf.pMsgStore.mu.Unlock()
	if rf.pMsgStore.term < int32(data.SelfTermNow) {
		rf.pMsgStore = &MsgStore{msgs: make([]*LogData, 0), owner: data.SelfIndex, mu: sync.Mutex{}, term: int32(data.SelfTermNow)}
	}
	if data.Msg.SnapshotValid {
		return LogCheckSnap
	}

	// 将 snapindex 记录成 commandindex 去进行Check
	lastRfLog := *rf.commandLog.Msgs[len(rf.commandLog.Msgs)-1]
	if lastRfLog.SnapshotValid {
		lastRfLog.CommandIndex, lastRfLog.CommandTerm = lastRfLog.SnapshotIndex, lastRfLog.SnapshotTerm
	}
	//rf.Dolog(-1, "Will check ", data.string())

	switch {
	// 如果已经提交过的 忽略
	case data.Msg.CommandIndex <= int(rf.getCommitIndexUnsafe()):
		return LogCheckIgnore

	// 没有快照之前的 第一项log
	case data.LastTimeIndex == 0 && data.LastTimeTerm == -1:
		if len(rf.commandLog.Msgs) == 1 || rf.commandLog.Msgs[1].CommandTerm != data.Msg.CommandTerm {
			return LogCheckBeginOrReset
		} else {
			return LogCheckIgnore
		}

	case data.LastTimeIndex == lastRfLog.CommandIndex:
		if data.LastTimeTerm == lastRfLog.CommandTerm {
			// prelog和现有log最后一项 完全相同 append
			return LogCheckAppend
		} else {
			if lastRfLog.SnapshotValid {
				panic("\nsnapshot Dis-agreement \n" + lastRfLog.string())
			}
			// 否则 store
			return LogCheckStore
		}
	// 传入数据 索引元高于本地
	case data.LastTimeIndex > lastRfLog.CommandIndex:
		// 进行(同步)缓存
		return LogCheckStore

	// store
	case data.LastTimeIndex < lastRfLog.CommandIndex:
		if data.LastTimeIndex <= 0 {
			panic("requeste update command index is out of range [<=0]")
		} else if i := rf.GetTargetCacheIndex(data.Msg.CommandIndex); i <= 0 {
			if rf.getSnapshotUnsafe().SnapshotValid && rf.getSnapshotUnsafe().SnapshotIndex >= data.Msg.CommandIndex {
				return LogCheckIgnore
			} else {
				panic("requeste update command index is out of range[<=0]\n" + data.string() + "\n" + rf.getSnapshotUnsafe().string())
			}
		} else if rf.commandLog.Msgs[rf.GetTargetCacheIndex(data.Msg.CommandIndex)].CommandTerm == data.Msg.CommandTerm && rf.commandLog.Msgs[rf.GetTargetCacheIndex(data.LastTimeIndex)].CommandTerm == data.LastTimeTerm {
			// [S][C][C][C][C][C][][][][]
			//    -------^-
			//   (check)[C] --> same
			return LogCheckIgnore
		}
		return LogCheckStore
	default:
	}
	return -1
}
func (rf *Raft) appendMsg(msg *LogData) {
	if msg == nil || rf.getLevel() == LevelLeader {
		return
	}
	rf.commandLog.Msgs = append(rf.commandLog.Msgs, msg.Msg)
	rf.raftPeers[rf.me].logIndexTermLock.Lock()
	defer rf.raftPeers[rf.me].logIndexTermLock.Unlock()
	rf.raftPeers[rf.me].logIndex = int32(msg.Msg.CommandIndex)
	rf.raftPeers[rf.me].lastLogTerm = int32(msg.Msg.CommandTerm)
	rf.Dolog(-1, "LogAppend", msg.Msg.string())
}
func (rf *Raft) getLastLogData() (int32, int32) {

	rf.commandLog.MsgRwMu.RLock()
	defer rf.commandLog.MsgRwMu.RUnlock()
	if rf.commandLog.Msgs[len(rf.commandLog.Msgs)-1].SnapshotValid {
		return int32(rf.commandLog.Msgs[len(rf.commandLog.Msgs)-1].SnapshotIndex), int32(rf.commandLog.Msgs[len(rf.commandLog.Msgs)-1].SnapshotTerm)
	}
	return int32(rf.commandLog.Msgs[len(rf.commandLog.Msgs)-1].CommandIndex), int32(rf.commandLog.Msgs[len(rf.commandLog.Msgs)-1].CommandTerm)
}

func (m *MsgStore) insert(target *LogData) {
	if len(m.msgs) == 0 {
		m.msgs = []*LogData{target}
		return
	}
	if target.Msg.CommandIndex < m.msgs[0].Msg.CommandIndex {
		m.msgs = append([]*LogData{target}, m.msgs...)
		return
	}
	if target.Msg.CommandIndex > m.msgs[len(m.msgs)-1].Msg.CommandIndex {
		m.msgs = append(m.msgs, target)
		return
	}
	index := 0
	right := len(m.msgs)
	for index < right {
		mid := index + (right-index)/2
		if m.msgs[mid].Msg.CommandIndex < target.Msg.CommandIndex {
			index = mid + 1
		} else {
			right = mid
		}
	}
	if m.msgs[index].Msg.CommandIndex == target.Msg.CommandIndex {
		if m.msgs[index].Msg.CommandTerm != target.Msg.CommandTerm {
			m.msgs[index] = target
		}
	} else {
		m.msgs = append(m.msgs, nil)
		copy(m.msgs[index+1:], m.msgs[index:])
		m.msgs[index] = target
	}
	// log.Printf("m: %v\n", m)
}

func (rf *Raft) saveMsg() (*LogData, bool) {

	if func() *MsgStore {
		rf.pMsgStoreCreateLock.Lock()
		defer rf.pMsgStoreCreateLock.Unlock()
		if rf.pMsgStore == nil {
			return nil
		} else {
			rf.pMsgStore.mu.Lock()
			defer rf.pMsgStore.mu.Unlock()
			if len(rf.pMsgStore.msgs) == 0 {
				return nil
			}
			return rf.pMsgStore
		}
	}() == nil {
		return nil, false
	}
	IsChangeMsg := false

	rf.pMsgStore.mu.Lock()
	defer rf.pMsgStore.mu.Unlock()
	// store的 更新到头了
	if rf.pMsgStore.msgs == nil || len(rf.pMsgStore.msgs) == 0 {
		panic("rf.pMsgStore.msgs is not initialized , May race state result")

		// 有快照之前的追加
	} else if !rf.getSnapshotUnsafe().SnapshotValid && rf.pMsgStore.msgs[0].LastTimeTerm == -1 {
		rf.commandLog.Msgs = append(make([]*ApplyMsg, 0), rf.commandLog.Msgs[:1]...)
		rf.commandLog.Msgs = append(rf.commandLog.Msgs, rf.pMsgStore.msgs[0].Msg)
		rf.pMsgStore.msgs = rf.pMsgStore.msgs[1:]
		// rf.pMsgStore.msgs = make([]*LogData, 0)
		// return nil, true
		IsChangeMsg = true
	}

	for {
		if len(rf.pMsgStore.msgs) == 0 {
			break
		}
		rf.Dolog(-1, "Try Save "+rf.pMsgStore.msgs[0].string())
		index := rf.GetTargetCacheIndex(rf.pMsgStore.msgs[0].LastTimeIndex)
		if index >= len(rf.commandLog.Msgs) {
			break
		} else if index <= 0 {
			if rf.getSnapshotUnsafe().SnapshotValid && rf.pMsgStore.msgs[0].LastTimeIndex == rf.getSnapshotUnsafe().SnapshotIndex && rf.pMsgStore.msgs[0].LastTimeTerm == rf.getSnapshotUnsafe().SnapshotTerm {
				if len(rf.commandLog.Msgs) > 2 && (rf.pMsgStore.msgs[0].Msg.CommandIndex == rf.commandLog.Msgs[1].CommandIndex && rf.pMsgStore.msgs[0].Msg.CommandTerm == rf.commandLog.Msgs[1].CommandTerm) {
				} else {
					rf.commandLog.Msgs = append(make([]*ApplyMsg, 0), rf.commandLog.Msgs[:1]...)
					rf.commandLog.Msgs = append(rf.commandLog.Msgs, rf.pMsgStore.msgs[0].Msg)
					IsChangeMsg = true
				}
			} else if !rf.getSnapshotUnsafe().SnapshotValid || rf.pMsgStore.msgs[0].Msg.CommandIndex > rf.getSnapshotUnsafe().SnapshotIndex {
				log.Panic("require snapshot ? Access out of bounds")
			} else {
			}
		} else {
			if rf.pMsgStore.msgs[0].LastTimeTerm == rf.commandLog.Msgs[index].CommandTerm {
				if index+1 == len(rf.commandLog.Msgs) {
					rf.commandLog.Msgs = append(rf.commandLog.Msgs, rf.pMsgStore.msgs[0].Msg)
				} else {
					rf.commandLog.Msgs[index+1] = rf.pMsgStore.msgs[0].Msg
					if len(rf.commandLog.Msgs) >= index+1 {
						rf.commandLog.Msgs = rf.commandLog.Msgs[:index+2]
					}
				}
				IsChangeMsg = true
			} else {
				break
			}
		}
		rf.Dolog(-1, "Saved "+rf.pMsgStore.msgs[0].string())
		rf.pMsgStore.msgs = rf.pMsgStore.msgs[1:]
	}
	if len(rf.pMsgStore.msgs) != 0 {
		return rf.pMsgStore.msgs[0], IsChangeMsg
	} else {
		rf.pMsgStore.msgs = make([]*LogData, 0)
		return nil, IsChangeMsg
	}
}
func (rf *Raft) storeMsg(msg *LogData) {
	defer rf.checkFuncDone("storeMsg")()
	rf.pMsgStoreCreateLock.Lock()
	defer rf.pMsgStoreCreateLock.Unlock()
	if msg == nil {
		return
	}
	if rf.pMsgStore == nil || rf.pMsgStore.owner != msg.SelfIndex || rf.pMsgStore.term < int32(msg.Msg.CommandTerm) {
		if rf.pMsgStore != nil {
			rf.Dolog(-1, "'rf.pMsgStore' has been overwritten", "brfore Leader:", rf.pMsgStore.owner, "Now:", msg.SelfIndex)
		} else {
			rf.Dolog(-1, "'rf.pMsgStore' has been overwritten", "brfore Leader:", nil, "Now:", msg.SelfIndex)
		}
		if rf.pMsgStore != nil {
			rf.pMsgStore.mu.Lock()
			defer rf.pMsgStore.mu.Unlock()
		}
		rf.pMsgStore = &MsgStore{msgs: make([]*LogData, 0), owner: msg.SelfIndex, mu: sync.Mutex{}, term: int32(msg.Msg.CommandTerm)}
	}
	rf.pMsgStore.insert(msg)
}
func (rf *Raft) logBeginOrResetMsg(log *LogData) {

	if len(rf.commandLog.Msgs) > 1 {
		rf.commandLog.Msgs = rf.commandLog.Msgs[:1]
	}

	rf.commandLog.Msgs = append(rf.commandLog.Msgs, log.Msg)
	rf.raftPeers[rf.me].logIndexTermLock.Lock()
	rf.raftPeers[rf.me].logIndex = int32(log.Msg.CommandIndex)
	rf.raftPeers[rf.me].lastLogTerm = int32(log.Msg.CommandTerm)
	rf.raftPeers[rf.me].logIndexTermLock.Unlock()

	rf.Dolog(-1, "LogAppend", log.Msg.string())
	rf.Dolog(-1, "Log", log.Msg.string())
}
func (rf *Raft) LoadSnap(data *LogData) {
	if data.Msg.SnapshotIndex < rf.getSnapshotUnsafe().SnapshotIndex || (data.Msg.CommandIndex == rf.getSnapshotUnsafe().SnapshotIndex && data.Msg.CommandTerm == rf.getSnapshotUnsafe().SnapshotTerm) {
		return
	} else {
		// rf.SnapshotUnsafe(data.Msg.SnapshotIndex, data.Msg.SnapshotTerm, data.Msg.Snapshot)
		oldmsg := rf.commandLog.Msgs
		cacheIndex := rf.GetTargetCacheIndex(data.Msg.SnapshotIndex)
		if cacheIndex < 0 {
			return
		} else if cacheIndex >= len(rf.commandLog.Msgs) {
			rf.commandLog.Msgs = append(make([]*ApplyMsg, 0), data.Msg)
		} else {
			rf.commandLog.Msgs = append(make([]*ApplyMsg, 0), data.Msg)
			rf.commandLog.Msgs = append(rf.commandLog.Msgs, oldmsg[cacheIndex+1:]...)
		}
		rf.persistUnsafe(rf.getSnapshotUnsafe())
		// os.Stdout.WriteString(fmt.Sprintf("\t F[%d] LoadSnapShot:%#v\n", rf.me, *rf.getSnapshotUnsafe()))
	}
}
func (rf *Raft) updateMsgs(msg []*LogData) *LogData {
	defer rf.checkFuncDone("updateMsgs")()

	var IsStoreMsg bool = false
	var IsChangeMsg bool = false
	rf.commandLog.MsgRwMu.Lock()
	defer rf.commandLog.MsgRwMu.Unlock()

	for i := 0; i < len(msg); i++ {
		result := rf.checkMsg(msg[i])
		switch result {
		case LogCheckSnap:
			rf.Dolog(-1, "LogCheckSnap", msg[i].string())
			rf.LoadSnap(msg[i])
		case LogCheckBeginOrReset:
			rf.Dolog(-1, "LogCheckBeginOrReset", msg[i].string())
			rf.logBeginOrResetMsg(msg[i])
			IsChangeMsg = true
		case LogCheckAppend:
			rf.Dolog(-1, "LogCheckAppend", msg[i].string())
			rf.appendMsg(msg[i])
			IsChangeMsg = true
		case LogCheckIgnore:
			rf.Dolog(-1, "LogCheckIgnore", msg[i].string())
		case LogCheckStore:
			IsStoreMsg = true
			rf.Dolog(-1, "LogCheckStore", msg[i].string())
			rf.storeMsg(msg[i])
			// IsChangeMsg = true
		default:
			rf.pMsgStore.mu.Lock()
			rf.Dolog(-1, "The requested data is out of bounds ", rf.pMsgStore.string(), "RequestIndex:>", rf.pMsgStore.msgs[0].LastTimeIndex, "process will be killed")
			log.Panic(-1, "The requested data is out of bounds ", rf.pMsgStore.string(), "RequestIndex:>", rf.pMsgStore.msgs[0].LastTimeIndex, "process will be killed")
		}
	}
	i, IsSave := rf.saveMsg()
	if IsStoreMsg {
		//rf.DebugLoger.Printf("Pmsg:> \n%s", rf.pMsgStore.string())
	}
	if IsChangeMsg || IsSave {
		// rf.registPersist()
	}

	return i
}

func (rf *Raft) IsLeader() bool {
	if rf.getLevel() == LevelLeader && rf.checkOutLeaderOnline() {
		return true
	}
	return false
}
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	defer rf.checkFuncDone("Start")()
	//rf.Dolog(-1, "Start Called ")

	TermNow := rf.getTerm()
	LevelNow := rf.getLevel()
	lastLogIndex := rf.getLogIndex()

	if i, m, checkOutLeaderOnline := int(lastLogIndex), int(TermNow), rf.checkOutLeaderOnline(); LevelNow != LevelLeader || (!checkOutLeaderOnline) {
		rf.Dolog(-1, "Start return ", "LogIndex", i, "Term", m, "Level", LevelNow)
		return i, m, false
	}
	if command == nil {
		return int(lastLogIndex), int(TermNow), true
	}

	rf.commandLog.MsgRwMu.Lock()
	defer rf.commandLog.MsgRwMu.Unlock()

	var arg *RequestArgs
	newMessage := &ApplyMsg{
		CommandTerm:  int(TermNow),
		CommandValid: true,
		Command:      command,
		CommandIndex: int(rf.getLogIndexUnsafe()) + 1}

	rf.commandLog.Msgs = append(rf.commandLog.Msgs, newMessage)
	rf.raftPeers[rf.me].logIndexTermLock.Lock()
	rf.raftPeers[rf.me].logIndex = int32(newMessage.CommandIndex)
	rf.raftPeers[rf.me].lastLogTerm = int32(newMessage.CommandTerm)
	rf.raftPeers[rf.me].logIndexTermLock.Unlock()

	rf.Dolog(-1, "Start TO LogAppend", newMessage.string())
	// rf.registPersist()

	arg = &RequestArgs{
		SelfTerm:  TermNow,
		Time:      time.Now(),
		SelfIndex: int32(rf.me),
		Msg: append(make([]*LogData, 0),
			&LogData{
				Msg:         newMessage,
				SelfIndex:   rf.me,
				SelfTermNow: int(TermNow)}),
	}

	if rf.commandLog.Msgs[len(rf.commandLog.Msgs)-1].SnapshotValid {
		arg.LastLogIndex, arg.LastLogTerm = int32(rf.commandLog.Msgs[len(rf.commandLog.Msgs)-1].SnapshotIndex), int32(rf.commandLog.Msgs[len(rf.commandLog.Msgs)-1].SnapshotTerm)
	} else {
		arg.LastLogIndex, arg.LastLogTerm = int32(rf.commandLog.Msgs[len(rf.commandLog.Msgs)-1].CommandIndex), int32(rf.commandLog.Msgs[len(rf.commandLog.Msgs)-1].CommandTerm)
	}

	if len(rf.commandLog.Msgs) == 2 && rf.commandLog.Msgs[len(rf.commandLog.Msgs)-2].SnapshotValid {
		arg.Msg[0].LastTimeIndex = rf.commandLog.Msgs[0].SnapshotIndex
		arg.Msg[0].LastTimeTerm = rf.commandLog.Msgs[0].SnapshotTerm
	} else {
		arg.Msg[0].LastTimeIndex = rf.commandLog.Msgs[len(rf.commandLog.Msgs)-2].CommandIndex
		arg.Msg[0].LastTimeTerm = rf.commandLog.Msgs[len(rf.commandLog.Msgs)-2].CommandTerm
	}

	for i := range rf.raftPeers {
		if i != rf.me {
			rf.wg.Add(1)
			go func(index int) {
				defer rf.wg.Done()
				rpl := &RequestReply{}
				select {
				case rf.raftPeers[index].JumpHeartBeat <- struct{}{}:
				default:
				}
				rf.Dolog(index, "RaftServer.Heartbeat[LoadMsgBegin]", arg.string())
				ok := rf.call(index, "RaftServer.HeartBeat", arg, rpl)
				switch {
				case !ok:
					rf.Dolog(index, "RaftServer.HeartBeat(sendMsg)", "Timeout")
				case !rpl.IsAgree:
					rf.Dolog(index, "RaftServer.HeartBeat(sendMsg)", "Peer DisAgree", rpl.string())
				case rpl.LogDataMsg != nil:
					rf.tryleaderUpdatePeer(index, rpl.LogDataMsg)
				default:
					rf.raftPeers[index].logIndexTermLock.Lock()
					rf.raftPeers[index].lastLogTerm = rpl.PeerLastLogTerm
					rf.raftPeers[index].logIndex = rpl.PeerLastLogIndex
					rf.raftPeers[index].commitIndex = rpl.PeerCommitIndex
					rf.raftPeers[index].logIndexTermLock.Unlock()
				}
			}(i)
		} else {
			continue
		}
	}
	i, m, l := int(rf.getLogIndexUnsafe()), int(TermNow), LevelNow == LevelLeader
	rf.Dolog(-1, "Start return ", "LogIndex", i, "Term", m, "Level", l)
	return i, m, l
}

func (rf *Raft) getLogs(index int, Len int) []*LogData {
	defer rf.checkFuncDone("getLogs")()
	termNow := rf.getTerm()
	rf.commandLog.MsgRwMu.RLock()
	defer rf.commandLog.MsgRwMu.RUnlock()
	targetLogIndexEnd := rf.GetTargetCacheIndex(index)
	if targetLogIndexEnd < 0 {
		// panic("Request target index < 0 ")
		targetLogIndexEnd = 0
	} else if targetLogIndexEnd >= len(rf.commandLog.Msgs) {
		panic("Request target index out of range ")
	}
	targetLogIndexBegin := targetLogIndexEnd - Len
	result := make([]*LogData, 0)
	if targetLogIndexBegin <= 0 {
		if rf.commandLog.Msgs[0].SnapshotValid {
			result = append(result, &LogData{SelfIndex: rf.me, SelfTermNow: int(termNow)})
			i := *rf.commandLog.Msgs[0]
			result[0].Msg = &i
		}
		targetLogIndexBegin = 1
	}
	for targetLogIndexBegin <= targetLogIndexEnd {
		result = append(result, &LogData{
			SelfTermNow: int(termNow),
			Msg:         rf.commandLog.Msgs[targetLogIndexBegin],
			SelfIndex:   rf.me})
		if rf.commandLog.Msgs[targetLogIndexBegin-1].SnapshotValid {
			result[len(result)-1].LastTimeIndex, result[len(result)-1].LastTimeTerm = rf.commandLog.Msgs[targetLogIndexBegin-1].SnapshotIndex, rf.commandLog.Msgs[targetLogIndexBegin-1].SnapshotTerm
		} else if rf.commandLog.Msgs[targetLogIndexBegin-1].CommandValid {
			result[len(result)-1].LastTimeIndex, result[len(result)-1].LastTimeTerm = rf.commandLog.Msgs[targetLogIndexBegin-1].CommandIndex, rf.commandLog.Msgs[targetLogIndexBegin-1].CommandTerm
		}
		targetLogIndexBegin++
	}
	return result

}
func (rf *Raft) tryleaderUpdatePeer(index int, msg *LogData) {
	if rf.raftPeers[index].modeLock.TryLock() {
		rf.wg.Add(1)
		go rf.leaderUpdatePeer(index, msg)
	}
}
func (rf *Raft) isInLog(index int, Term int) bool {
	rf.commandLog.MsgRwMu.RLock()
	defer rf.commandLog.MsgRwMu.RUnlock()
	i := rf.GetTargetCacheIndex(index)
	if i < 0 || i >= len(rf.commandLog.Msgs) {
		return false
	} else if (rf.commandLog.Msgs[i].SnapshotValid && rf.commandLog.Msgs[i].SnapshotTerm == Term) || (rf.commandLog.Msgs[i].CommandValid && rf.commandLog.Msgs[i].CommandTerm == Term) {
		return true
	}
	return false
}
func (rf *Raft) leaderUpdatePeer(peerIndex int, msg *LogData) {
	defer rf.wg.Done()
	defer rf.checkFuncDone("leaderUpdatePeer")()
	defer rf.raftPeers[peerIndex].modeLock.Unlock()
	rf.Dolog(peerIndex, "leaderUpdate: leaderUpdatePeer Get RQ")
	arg := RequestArgs{
		SelfTerm:  rf.getTerm(),
		SelfIndex: int32(rf.me),
	}
	for {
		arg.LastLogIndex, arg.LastLogTerm = rf.getLastLogData()
		arg.Time = time.Now()
		if rf.getLevel() != LevelLeader {
			break
		}
		rf.raftPeers[peerIndex].logIndexTermLock.Lock()
		peerLastLogIndex, peerLastLogTerm := rf.raftPeers[peerIndex].logIndex, rf.raftPeers[peerIndex].lastLogTerm
		rf.raftPeers[peerIndex].logIndexTermLock.Unlock()
		if ok := rf.isInLog(int(peerLastLogIndex), int(peerLastLogTerm)); ok {
			getLen := msg.LastTimeIndex - int(peerLastLogIndex)
			arg.Msg = rf.getLogs(msg.LastTimeIndex, getLen)
		} else {
			arg.Msg = rf.getLogs(msg.LastTimeIndex, UpdateLogLines)
		}
		arg.CommitIndex = rf.getCommitIndex()
		rpl := RequestReply{}
		rf.Dolog(peerIndex, "leaderUpdate: HeartBeat(Update) Go ", arg.string())

		select {
		case rf.raftPeers[peerIndex].JumpHeartBeat <- struct{}{}:
		default:
		}

		// 没有超时机制
		ok := rf.raftPeers[peerIndex].C.Call("RaftServer.HeartBeat", &arg, &rpl)
		if ok {
			rf.raftPeers[peerIndex].updateLastTalkTime()
			rf.raftPeers[peerIndex].logIndexTermLock.Lock()
			rf.raftPeers[peerIndex].commitIndex = rpl.PeerCommitIndex
			rf.raftPeers[peerIndex].lastLogTerm = rpl.PeerLastLogTerm
			rf.raftPeers[peerIndex].logIndex = rpl.PeerLastLogIndex
			rf.raftPeers[peerIndex].logIndexTermLock.Unlock()
		}

		rf.Dolog(peerIndex, "leaderUpdate: HeartBeat(Update) return ", rpl.string())

		if !ok {
			rf.Dolog(peerIndex, "leaderUpdate: HeartBeat(Update) Timeout CallFalse", arg.string())
			break
		} else if !rpl.IsAgree {
			rf.Dolog(peerIndex, "leaderUpdate: HeartBeat(Update) DisAgree", rpl.string())
			break
		} else if rpl.LogDataMsg != nil {
			rf.Dolog(peerIndex, "leaderUpdate: HeartBeat(Update) ReWriteCacheToGetNextOne", rpl.string())
			msg = rpl.LogDataMsg
		} else {
			rf.Dolog(peerIndex, "leaderUpdate: HeartBeat(Update) UpdateDone", rpl.string())
			break
		}
	}
	rf.Dolog(peerIndex, "leaderUpdate: Update Done")
}

func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	rf.Dolog(-1, "killdead")
	for i := 0; i < 100; i++ {
		rf.KilledChan <- true
	}
	close(rf.KilledChan)
	rf.wg.Wait()
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) getLevel() int32 {
	return atomic.LoadInt32(&rf.level)
}

func (rf *Raft) setLevel(i int32) {
	atomic.StoreInt32(&rf.level, i)
	rf.Dolog(-1, "Level set to ", i)
}

func (rf *Raft) ticker() {
	defer rf.wg.Done()
	for !rf.killed() {
		switch rf.getLevel() {
		case LevelFollower:
			select {
			case <-rf.levelChangeChan:
			case <-rf.KilledChan:
				return
			case <-rf.timeOutChan:
				atomic.StoreInt32(&rf.isLeaderAlive, 1)
			case <-time.NewTimer(time.Duration((int64(voteTimeOut) + rand.Int63()%150) * time.Hour.Milliseconds())).C:
				rf.Dolog(-1, "TimeOut")
				atomic.StoreInt32(&rf.isLeaderAlive, 0)
				rf.wg.Add(1)
				go TryToBecomeLeader(rf)
			}
		case LevelCandidate:
			select {
			case <-rf.levelChangeChan:
			case <-rf.KilledChan:
				return
			case <-rf.timeOutChan:
			}
		case LevelLeader:
			select {
			case <-rf.KilledChan:
				return
			case <-rf.levelChangeChan:
			case <-rf.timeOutChan:
			}
		}
	}
}

func TryToBecomeLeader(rf *Raft) {
	defer rf.wg.Done()
	defer rf.checkFuncDone("TryToBecomeLeader")()
	rf.changeToCandidate()
	arg := RequestArgs{SelfTerm: rf.getTerm() + 1, Time: time.Now(), SelfIndex: int32(rf.me), CommitIndex: rf.getCommitIndex()}

	// 这里要拿日志锁
	arg.LastLogIndex, arg.LastLogTerm = rf.getLastLogData()

	rpl := make([]RequestReply, len(rf.peers))
	wg := &sync.WaitGroup{}
	for i := range rf.raftPeers {
		if i != rf.me {
			wg.Add(1)
			go func(index int) {
				defer wg.Done()
				rf.Dolog(index, "RaftServer.RequestPreVote  GO ", index, arg.string())
				ok := rf.call(index, "RaftServer.RequestPreVote", &arg, &rpl[index])
				rf.Dolog(index, "RaftServer.RequestPreVote RETURN ", ok, rpl[index].IsAgree, rpl[index].string())
			}(i)
		}
	}
	wg.Wait()
	count := 1
	for i := range rpl {
		if i != rf.me {
			if rpl[i].PeerLastLogTerm > arg.LastLogTerm || (rpl[i].PeerLastLogTerm == arg.LastLogTerm && rpl[i].PeerLastLogIndex > rf.getLogIndex()) {
				// timeout case
				if rpl[i].PeerLastLogTerm == 0 && rpl[i].PeerLastLogIndex == 0 {
					continue
				}
				rf.changeToFollower(nil)
				return
			}
			if rpl[i].IsAgree {
				count++
			}
		}
	}
	if count > len(rf.peers)/2 {
		// 在这之前对其他的投票了
		// arg.SelfTerm = rf.getTerm()
		rf.setTerm(arg.SelfTerm)
	} else {
		rf.changeToFollower(nil)
		return
	}
	rpl = make([]RequestReply, len(rf.peers))

	for i := range rf.raftPeers {
		if i != rf.me {
			wg.Add(1)
			go func(index int) {
				rf.Dolog(index, "RaftServer.RequestVote GO ", arg.string())
				ok := rf.call(index, "RaftServer.RequestVote", &arg, &rpl[index])
				rf.Dolog(index, "RaftServer.RequestVote RETUEN ", ok, rpl[index].IsAgree, rpl[index].string())
				wg.Done()
			}(i)
		}
	}
	wg.Wait()
	count = 1
	for i := range rpl {
		if i != rf.me {
			if rpl[i].PeerLastLogTerm > arg.LastLogTerm || (rpl[i].PeerLastLogTerm == arg.LastLogTerm && rpl[i].PeerLastLogIndex > rf.getLogIndex()) {
				// timeout case
				if rpl[i].PeerLastLogTerm == 0 && rpl[i].PeerLastLogIndex == 0 {
					continue
				}
				rf.changeToFollower(nil)
				return
			}
			if rpl[i].IsAgree {
				count++
			}
		}
	}
	if count > len(rf.peers)/2 {
		for i := range rpl {
			if i != rf.me && rpl[i].IsAgree {
				rf.raftPeers[i].logIndexTermLock.Lock()
				rf.raftPeers[i].lastLogTerm, rf.raftPeers[i].logIndex = rpl[i].PeerLastLogIndex, rpl[i].PeerLastLogTerm
				rf.raftPeers[i].logIndexTermLock.Unlock()
			}
		}
		rf.changeToLeader()
	} else {
		rf.changeToFollower(nil)
		return
	}
}

func (rf *Raft) call(index int, FuncName string, arg *RequestArgs, rpl *RequestReply) bool {
	asdf := time.Now().UnixNano()
	ctx, cancel := context.WithTimeout(context.Background(), HeartbeatTimeout)
	defer cancel()
	i := false
	rf.wg.Add(1)
	go func() {
		defer rf.wg.Done()
		if arg.Msg != nil && len(arg.Msg) > 1 {
			rf.Dolog(-1, asdf, "UPdate rpcGo", arg.string())
		}
		i = rf.peers[index].Call(FuncName, arg, rpl)
		if arg.Msg != nil && len(arg.Msg) > 1 {
			rf.Dolog(-1, asdf, "UPdate rpcReturn", rpl.string())
		}
		cancel()
	}()
	select {
	case <-ctx.Done():
		rf.raftPeers[index].updateLastTalkTime()
	case <-time.After(HeartbeatTimeout):
		rf.Dolog(index, "Rpc Timeout ", FuncName, arg.string(), rpl.string())
	}
	return i
}

func Make(peers []*ClientEnd, me int, persister *Persister.Persister, applyCh chan ApplyMsg) *Raft {
	// os.Stderr.WriteString("RaftServer Make \n")
	rf := &Raft{}
	rf.pMsgStore = &MsgStore{msgs: make([]*LogData, 0), owner: -1, term: -1, mu: sync.Mutex{}}
	rf.commitChan = make(chan int32, commitChanSize)
	rf.applyChan = applyCh
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.level = LevelFollower
	rf.isLeaderAlive = 0
	rf.term = 0
	rf.timeOutChan = make(chan struct{}, 1)
	rf.levelChangeChan = make(chan struct{}, 1)
	rf.raftPeers = make([]RaftPeer, len(rf.peers))
	rf.commandLog.Msgs = append(make([]*ApplyMsg, 0), &ApplyMsg{CommandValid: true, Command: nil, CommandIndex: 0, CommandTerm: -1, SnapshotValid: false, Snapshot: nil, SnapshotTerm: 0, SnapshotIndex: 0})
	rf.commandLog.MsgRwMu = sync.RWMutex{}
	rf.pMsgStoreCreateLock = sync.Mutex{}
	rf.KilledChan = make(chan bool, 100)
	rf.termLock = sync.Mutex{}

	for i := range rf.peers {
		rf.raftPeers[i] = RaftPeer{C: rf.peers[i], SendHeartBeat: make(chan struct{}), JumpHeartBeat: make(chan struct{}, 1), BeginHeartBeat: make(chan struct{}), StopHeartBeat: make(chan struct{})}
		rf.raftPeers[i].modeLock = sync.Mutex{}
		if i != rf.me {
			rf.wg.Add(1)
			go rf.registeHeartBeat(i)
		}
	}

	rf.wg.Add(1)
	go func() {
		defer rf.wg.Done()
		rf.readPersist(persister.ReadRaftState())
		// start ticker goroutine to start elections
		rf.commandLog.MsgRwMu.RLock()

		// may bug

		//		rf.wg.Add(1)
		//		go func() {
		//			defer rf.wg.Done()

		// may bug
		defer rf.commandLog.MsgRwMu.RUnlock()
		if rf.commandLog.Msgs[0].SnapshotValid {
			rf.applyChan <- *rf.commandLog.Msgs[0]
		}
		asdf := rf.GetTargetCacheIndex(int(rf.getCommitIndexUnsafe()))
		for i := 1; i <= asdf && i < len(rf.commandLog.Msgs); i++ {
			rf.applyChan <- *rf.commandLog.Msgs[i]
		}
		rf.wg.Add(1)
		go rf.ticker()
		rf.wg.Add(1)
		go rf.committer(applyCh)
		//}()
	}()
	return rf
}

type peersLogSlice []peersLog
type peersLog struct {
	term        int32
	index       int32
	commitIndex int32
}

func (s peersLogSlice) Len() int {
	return len(s)
}

func (s peersLogSlice) Less(i, j int) bool {
	if s[i].term == s[j].term {
		return s[i].index < s[j].index
	} else {
		return s[i].term < s[j].term
	}
}

func (s peersLogSlice) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}
func (rf *Raft) sendHeartBeat() {
	for i := range rf.raftPeers {
		if i != rf.me {
			select {
			case rf.raftPeers[i].SendHeartBeat <- struct{}{}:
			default:
			}
		}
	}
}
func (rf *Raft) committer(applyCh chan ApplyMsg) {
	defer rf.wg.Done()
	rf.Dolog(-1, "Committer Create \n")
	var ToLogIndex int32
	var LogedIndex int32
	var ok bool
	// init If restart
	LogedIndex = rf.getCommitIndex()
	ToLogIndex = LogedIndex

	for {
		peerLogedIndexs := make([]peersLog, len(rf.raftPeers))
		select {
		// leader scan followers to deside New TologIndex
		case <-rf.KilledChan:
			return
		case <-time.After(5 * time.Millisecond):
			if rf.getLevel() == LevelLeader {
				//halfLenPeersCommitted := 0
				for i := range rf.raftPeers {
					if i == rf.me {
						continue
					}
					rf.raftPeers[i].logIndexTermLock.Lock()
					peerLogedIndexs[i].index = rf.raftPeers[i].logIndex
					peerLogedIndexs[i].term = rf.raftPeers[i].lastLogTerm
					peerLogedIndexs[i].commitIndex = rf.raftPeers[i].commitIndex
					rf.raftPeers[i].logIndexTermLock.Unlock()
				}
				sort.Sort(peersLogSlice(peerLogedIndexs))
				ToLogHalfIndex := peerLogedIndexs[(len(peerLogedIndexs))/2+1]
				_, SelfNowLastLogTerm := rf.getLastLogData()
				if SelfNowLastLogTerm > ToLogHalfIndex.term {
					continue
				}
				if ToLogHalfIndex.index > ToLogIndex {
					ToLogIndex = int32(ToLogHalfIndex.index)
					rf.Dolog(-1, "Committer: Update CommitIndex", ToLogIndex)
					rf.justSetCommitIndex(ToLogHalfIndex.index)
					rf.persist(rf.GetSnapshot())
					rf.sendHeartBeat()
				}
			}

			// follower get new TologIndex from Leader heartbeat
		case ToLogIndex, ok = <-rf.commitChan:
			if rf.getLevel() == LevelLeader {
				rf.Dolog(-1, "Committer: FALAT err: leader Get CommitChan returned")
				os.Exit(1)
			}
			if !ok {
				return
			} else {
				rf.Dolog(-1, "Committer: Get TologIndex ", ToLogIndex)
			}
		}
		// check
		if ToLogIndex > LogedIndex {
			rf.Dolog(-1, "Committer: ", "ToLogIndex ", ToLogIndex, "> LogedIndex", LogedIndex)
		}
		if ToLogIndex <= LogedIndex {
			ToLogIndex = LogedIndex
			continue
		} else {
			findLogSuccess := false
			expectedLogMsgCacheIndex, exceptLogMsgIndex := int32(0), int32(0)
			for {
				rf.commandLog.MsgRwMu.RLock()
				findLogSuccess, expectedLogMsgCacheIndex, exceptLogMsgIndex =
					func() (bool, int32, int32) {
						defer rf.commandLog.MsgRwMu.RUnlock()
						for exceptLogMsgIndex := LogedIndex + 1; exceptLogMsgIndex <= ToLogIndex; exceptLogMsgIndex++ {
							//rf.DebugLoger.Println("1")

							// get cache index
							expectedLogMsgCacheIndex := int32(rf.GetTargetCacheIndex(int(exceptLogMsgIndex)))
							//rf.DebugLoger.Println("2")
							if expectedLogMsgCacheIndex <= 0 {
								i := rf.getSnapshotUnsafe()
								if i.SnapshotValid && i.SnapshotIndex >= int(exceptLogMsgIndex) {
									exceptLogMsgIndex = int32(i.SnapshotIndex)
									expectedLogMsgCacheIndex = 0
								}
							}
							// out of range
							if expectedLogMsgCacheIndex >= int32(len(rf.commandLog.Msgs)) {
								return false, expectedLogMsgCacheIndex, exceptLogMsgIndex
							} else {
								//rf.DebugLoger.Println("3")
								// commit operation
								select {
								case applyCh <- *rf.commandLog.Msgs[expectedLogMsgCacheIndex]:
									MyLogger.INFO("submitCommand", rf.commandLog.Msgs[expectedLogMsgCacheIndex].Command)
								// 阻塞占有锁 不呢超过5ms
								case <-time.After(5 * time.Millisecond):
									goto done
								}
								rf.Dolog(-1, fmt.Sprintf("Committer: Commit log message CacheIndex:[%d] Index[%d] %s", expectedLogMsgCacheIndex, exceptLogMsgIndex, rf.commandLog.Msgs[expectedLogMsgCacheIndex].string()))
								//rf.DebugLoger.Println("4")
								LogedIndex = exceptLogMsgIndex
								//rf.DebugLoger.Println("5")
							}
						}
					done:
						return true, -1, -1
					}()
				if rf.getLevel() != LevelLeader {
					rf.setCommitIndex(LogedIndex)
				}
				if !findLogSuccess || LogedIndex == ToLogIndex {
					rf.persist(rf.GetSnapshot())
					break
				}
			}
			if !findLogSuccess {
				//log
				rf.Dolog(-1, fmt.Sprintf("Committer:  Trying to Log Message[%d] But failed(OutOfRange[%d])", exceptLogMsgIndex, expectedLogMsgCacheIndex))
			}
		}
	}
}
