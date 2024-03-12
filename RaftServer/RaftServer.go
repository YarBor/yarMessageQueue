package RaftServer

import (
	"MqServer/Err"
	mqLog "MqServer/Log"
	"MqServer/RaftServer/Pack"
	"MqServer/RaftServer/Persister"
	pb "MqServer/rpc"
	"bytes"
	"context"
	"errors"
	"google.golang.org/grpc"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

const (
	ErrNotLeader       = "not leader"
	ErrCommitTimeout   = "commit timeout"
	ErrNodeDidNotStart = "node did not start"
)

var commitTimeout time.Duration = time.Millisecond * 500
var RaftLogSize = 1024 * 1024 * 2

const (
	RfNodeNotFound        = "RfNodeNotFound"
	UnKnownTopicPartition = "UnKnownTopicPartition"
	TopicAlreadyExist     = "TopicAlreadyExist"
)

var (
// for option set
// RaftServerID        = ""
// RaftServerUrl       = ""
)

type syncIdMap struct {
	mu  sync.Mutex
	Map map[uint32]struct {
		fn func(err error, data interface{})
	}
}

func (s *syncIdMap) Add(i uint32, fn func(err error, data interface{})) {
	s.mu.Lock()
	s.Map[i] = struct {
		fn func(err error, data interface{})
	}{fn: fn}
	s.mu.Unlock()
}

func (s *syncIdMap) GetCallDelete(i uint32, err error, data interface{}) {
	s.mu.Lock()
	f, ok := s.Map[i]
	if ok {
		defer f.fn(err, data)
		delete(s.Map, i)
	}
	s.mu.Unlock()
}

func (s *syncIdMap) Delete(i uint32) {
	s.mu.Lock()
	delete(s.Map, i)
	s.mu.Unlock()
}

type CommandHandler interface {
	Handle(interface{}) (error, interface{})
}
type SnapshotHandler interface {
	MakeSnapshot() []byte
	LoadSnapshot([]byte)
}

type RaftNode struct {
	rf         *Raft
	T          string
	P          string
	Peers      []*ClientEnd
	me         int
	ch         chan ApplyMsg
	Persistent *Persister.Persister

	idMap           syncIdMap
	wg              sync.WaitGroup
	commandIdOffset uint32

	CommandHandler  CommandHandler
	SnapshotHandler SnapshotHandler
}

func (rn *RaftNode) IsLeader() bool {
	if rn.rf == nil || rn.Peers == nil || len(rn.Peers) <= 1 {
		return true
	}
	return rn.rf.IsLeader()
}

func (rn *RaftNode) CloseAllConn() {
	for _, r := range rn.Peers {
		if r != nil && r.Conn != nil {
			r.Conn.Close()
		}
	}
}

func (rn *RaftNode) LinkPeerRpcServer(addr, id string) (*ClientEnd, error) {
	conn, err := grpc.Dial(addr, grpc.WithInsecure())
	if err != nil {
		panic(err.Error())
	}
	res := &ClientEnd{
		RaftCallClient: pb.NewRaftCallClient(conn),
		ID:             id,
		Rfn:            rn,
		Conn:           conn,
	}
	return res, nil
}

func (rn *RaftNode) Stop() {
	rn.rf.Kill()
	rn.CloseAllConn()
	rn.wg.Wait()
}

type Entry struct {
	Id      uint32
	command interface{}
}

func (rn *RaftNode) GetNewCommandId() uint32 {
	return atomic.AddUint32(&rn.commandIdOffset, 1)
}

func (rn *RaftNode) Commit(command interface{}) (error, interface{}) {
	entry := Entry{
		Id:      rn.GetNewCommandId(),
		command: command,
	}
	ch := make(chan struct {
		err  error
		data interface{}
	}, 1)

	f := func(err error, data interface{}) {
		ch <- struct {
			err  error
			data interface{}
		}{err: err, data: data}
	}

	if len(rn.Peers) == 1 {
		rn.idMap.Add(entry.Id, f)
		rn.ch <- ApplyMsg{
			CommandValid: true,
			Command:      command,
		}
	} else {
		if rn.rf == nil || rn.rf.killed() {
			return errors.New(ErrNodeDidNotStart), nil
		}
		if !rn.rf.IsLeader() {
			return errors.New(ErrNotLeader), nil
		}
		rn.idMap.Add(entry.Id, f)
		_, _, ok := rn.rf.Start(entry)
		if !ok {
			rn.idMap.Delete(entry.Id)
			return errors.New(ErrNotLeader), nil
		}
	}
	select {
	case <-time.After(commitTimeout):
		rn.idMap.Delete(entry.Id)
		return errors.New(ErrCommitTimeout), nil
	case p, ok := <-ch:
		// success
		if !ok {
			panic("Ub")
		} else {
			return p.err, p.data
		}
	}
}

func (rn *RaftNode) CommandHandleFunc() {
	defer rn.wg.Done()
	for {
		select {
		case <-time.After(10 * time.Millisecond):
			if rn.rf.killed() {
				return
			}
		case applyMsg := <-rn.ch:
			if applyMsg.CommandValid {
				command, ok := applyMsg.Command.(Entry)
				if !ok {
					panic("not reflect command")
				}
				err, data := rn.CommandHandler.Handle(command)
				rn.idMap.GetCallDelete(command.Id, err, data)
				if rn.rf.persister.RaftStateSize() > RaftLogSize/3 {
					bt := rn.SnapshotHandler.MakeSnapshot()
					rn.rf.Snapshot(applyMsg.CommandIndex, bt)
				}
			} else if applyMsg.SnapshotValid {
				rn.SnapshotHandler.LoadSnapshot(applyMsg.Snapshot)
			}
		}
	}
}

func (rn *RaftNode) Start() {
	if len(rn.Peers) == 1 {
		return
	}
	rn.rf = Make(rn.Peers, rn.me, Persister.MakePersister(), rn.ch)
	rn.wg.Add(1)
	go rn.CommandHandleFunc()
}

type RaftServer struct {
	pb.UnimplementedRaftCallServer
	mu            sync.RWMutex
	server        *grpc.Server
	listener      net.Listener
	isRaftAddrSet int32
	ID            string
	Url           string
	metadataRaft  *RaftNode
	rfs           map[string]map[string]*RaftNode
}

func (rs *RaftServer) Serve() error {
	if atomic.LoadInt32(&rs.isRaftAddrSet) == 0 {
		return Err.ErrSourceNotExist
	}
	lis, err := net.Listen("tcp", rs.Url)
	if err != nil {
		return err
	}
	rs.listener = lis
	s := grpc.NewServer()
	rs.server = s
	pb.RegisterRaftCallServer(s, rs)

	return rs.server.Serve(rs.listener)
}
func (rs *RaftServer) HeartBeat(_ context.Context, arg *pb.HeartBeatRequest) (rpl *pb.HeartBeatResponse, err error) {
	rs.mu.RLock()
	defer rs.mu.RUnlock()
	var rf *Raft
	if tp, par := arg.GetTopic(), arg.GetPartition(); tp == "" || par == "" {
		//panic("invalid topic argument and MessageMem argument")
		rf = rs.metadataRaft.rf
	} else {
		rfnode, ok := rs.rfs[tp][par]
		if !ok {
			return nil, errors.New(RfNodeNotFound)
		}
		rf = rfnode.rf
	}
	rfNodeArgs := RequestArgs{}
	err = Pack.NewDecoder(bytes.NewBuffer(arg.Arg)).Decode(&rfNodeArgs)
	if err != nil {
		mqLog.FATAL(err.Error())
	}
	rfNodeReply := RequestReply{}
	rf.HeartBeat(&rfNodeArgs, &rfNodeReply)

	bff := bytes.Buffer{}
	err = Pack.NewEncoder(&bff).Encode(rfNodeReply)
	if err != nil {
		mqLog.FATAL(err.Error())
	}
	if rpl == nil {
		rpl = &pb.HeartBeatResponse{}
	}
	rpl.Topic = arg.Topic
	rpl.Partition = arg.Partition
	rpl.Result = bff.Bytes()
	return rpl, nil
}

func (rs *RaftServer) RequestPreVote(_ context.Context, arg *pb.RequestPreVoteRequest) (rpl *pb.RequestPreVoteResponse, err error) {
	rs.mu.RLock()
	defer rs.mu.RUnlock()
	var rf *Raft

	if tp, par := arg.GetTopic(), arg.GetPartition(); tp == "" || par == "" {
		//panic("invalid topic argument and MessageMem argument")
		rf = rs.metadataRaft.rf
	} else {
		x, ok := rs.rfs[tp]
		if !ok {
			return nil, errors.New(RfNodeNotFound)
		}
		rfnode, ok := x[par]
		if !ok {
			return nil, errors.New(RfNodeNotFound)
		}
		rf = rfnode.rf
	}

	rfNodeArgs := RequestArgs{}
	err = Pack.NewDecoder(bytes.NewBuffer(arg.Arg)).Decode(&rfNodeArgs)
	if err != nil {
		mqLog.FATAL(err.Error())
	}
	rfNodeReply := RequestReply{}
	rf.RequestPreVote(&rfNodeArgs, &rfNodeReply)

	bff := bytes.Buffer{}
	err = Pack.NewEncoder(&bff).Encode(rfNodeReply)
	if err != nil {
		mqLog.FATAL(err.Error())
	}
	if rpl == nil {
		rpl = &pb.RequestPreVoteResponse{}
	}
	rpl.Topic = arg.Topic
	rpl.Partition = arg.Partition
	rpl.Result = bff.Bytes()
	return rpl, nil
}

func (rs *RaftServer) RequestVote(_ context.Context, arg *pb.RequestVoteRequest) (rpl *pb.RequestVoteResponse, err error) {
	rs.mu.RLock()
	defer rs.mu.RUnlock()
	var rf *Raft
	if tp, par := arg.GetTopic(), arg.GetPartition(); tp == "" || par == "" {
		//panic("invalid topic argument and MessageMem argument")
		rf = rs.metadataRaft.rf
	} else {
		x, ok := rs.rfs[tp]
		if !ok {
			return nil, errors.New(RfNodeNotFound)
		}
		rfnode, ok := x[par]
		if !ok {
			return nil, errors.New(RfNodeNotFound)
		}
		rf = rfnode.rf
	}
	rfNodeArgs := RequestArgs{}
	err = Pack.NewDecoder(bytes.NewBuffer(arg.Arg)).Decode(&rfNodeArgs)
	if err != nil {
		mqLog.FATAL(err.Error())
	}
	rfNodeReply := RequestReply{}
	rf.RequestVote(&rfNodeArgs, &rfNodeReply)

	bff := bytes.Buffer{}
	err = Pack.NewEncoder(&bff).Encode(rfNodeReply)
	if err != nil {
		mqLog.FATAL(err.Error())
	}
	if rpl == nil {
		rpl = &pb.RequestVoteResponse{}
	}
	rpl.Topic = arg.Topic
	rpl.Partition = arg.Partition
	rpl.Result = bff.Bytes()
	return rpl, nil
}

// url包含自己
func (rs *RaftServer) RegisterRfNode(T, P string, ch CommandHandler, sh SnapshotHandler, peers ...struct{ ID, Url string }) (*RaftNode, error) {
	if atomic.LoadInt32(&rs.isRaftAddrSet) == 0 {
		return nil, (Err.ErrSourceNotExist)
	}
	if T == "" || P == "" {
		return nil, errors.New(UnKnownTopicPartition)
	}
	rn := RaftNode{
		rf:              nil,
		T:               T,
		P:               P,
		Peers:           make([]*ClientEnd, len(peers)),
		me:              0,
		ch:              make(chan ApplyMsg),
		Persistent:      Persister.MakePersister(),
		commandIdOffset: 0,
		CommandHandler:  ch,
		SnapshotHandler: sh,
	}
	for i, n := range peers {
		if n.Url == rs.Url {
			rn.me = i
		} else {
			peer, _ := rn.LinkPeerRpcServer(n.Url, n.ID)
			rn.Peers[i] = peer
		}
	}
	if rn.me == -1 {
		rn.CloseAllConn()
		return nil, Err.ErrRequestIllegal
	}
	rs.mu.Lock()
	_, ok := rs.rfs[T]
	if !ok {
		rs.rfs[T] = make(map[string]*RaftNode)
	}
	rs.rfs[T][P] = &rn
	rs.mu.Unlock()

	return &rn, nil
}

func (rs *RaftServer) SetRaftServerInfo(ID, Url string) bool {
	atomic.StoreInt32(&rs.isRaftAddrSet, 1)
	rs.ID = ID
	rs.Url = Url
	return true
}

func MakeRaftServer() (*RaftServer, error) {
	res := &RaftServer{
		UnimplementedRaftCallServer: pb.UnimplementedRaftCallServer{},
		mu:                          sync.RWMutex{},
		metadataRaft:                nil,
		rfs:                         make(map[string]map[string]*RaftNode),
	}
	return res, nil
}

func (rs *RaftServer) RegisterMetadataRaft(url_IDs []struct{ Url, ID string }, ch CommandHandler, sh SnapshotHandler) (*RaftNode, error) {
	if atomic.LoadInt32(&rs.isRaftAddrSet) == 0 {
		return nil, (Err.ErrSourceNotExist)
	}
	T, P := "", ""
	rn := RaftNode{
		T:               T,
		P:               P,
		Peers:           make([]*ClientEnd, len(url_IDs)),
		me:              -1,
		ch:              make(chan ApplyMsg),
		Persistent:      Persister.MakePersister(),
		idMap:           syncIdMap{},
		wg:              sync.WaitGroup{},
		commandIdOffset: 0,
		CommandHandler:  ch,
		SnapshotHandler: sh,
	}
	for i, n := range url_IDs {
		if n.Url == rs.Url {
			rn.me = i
			continue
		} else {
			peer, _ := rn.LinkPeerRpcServer(n.Url, n.ID)
			rn.Peers[i] = peer
			rn.Peers[i].Rfn = &rn
		}
	}
	if rn.me == -1 {
		rn.CloseAllConn()
		return nil, (Err.ErrRequestIllegal)
	}
	rs.mu.Lock()
	_, ok := rs.rfs[T]
	if !ok {
		rs.rfs[T] = make(map[string]*RaftNode)
	}
	rs.rfs[T][P] = &rn
	rs.mu.Unlock()
	if ok {
		return nil, (Err.ErrSourceAlreadyExist)
	}
	rs.metadataRaft = &rn
	return &rn, nil
}

func (rs *RaftServer) Stop() {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	rfnode := make([]*RaftNode, 0)
	for _, i := range rs.rfs {
		for _, j := range i {
			rfnode = append(rfnode, j)
		}
	}
	for _, node := range rfnode {
		node.Stop()
	}
	rs.server.Stop()
	rs.metadataRaft = nil
	rs.rfs = make(map[string]map[string]*RaftNode)
}
