package Raft

import (
	"MqServer/Raft/Net"
	"MqServer/Raft/Persister"
	pb "MqServer/rpc"
	"errors"
	"google.golang.org/grpc"
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

type syncIdMap struct {
	mu  sync.Mutex
	Map map[uint32]struct {
		fn func()
	}
}

func (s *syncIdMap) Add(i uint32, fn func()) {
	s.mu.Lock()
	s.Map[i] = struct{ fn func() }{fn: fn}
	s.mu.Unlock()
}

func (s *syncIdMap) GetCallDelete(i uint32) {
	s.mu.Lock()
	f, ok := s.Map[i]
	if ok {
		defer f.fn()
		delete(s.Map, i)
	}
	s.mu.Unlock()
}

func (s *syncIdMap) Delete(i uint32) {
	s.mu.Lock()
	delete(s.Map, i)
	s.mu.Unlock()
}

type CammandHandler interface {
	Handle(interface{})
}
type SnapshotHandler interface {
	MakeSnapshot() []byte
	LoadSnapshot([]byte)
}

type RaftNode struct {
	rf         *Raft
	T          string
	P          string
	Peers      []*Net.ClientEnd
	me         int
	ch         chan ApplyMsg
	Persistent *Persister.Persister

	idMap         syncIdMap
	wg            sync.WaitGroup
	commandOffset uint32

	CommandHandler  CammandHandler
	SnapshotHandler SnapshotHandler
}

func (rn *RaftNode) IsLeader() bool {
	return rn.rf.IsLeader()
}

func (rn *RaftNode) CloseAllConn() {
	for _, r := range rn.Peers {
		if r != nil && r.Conn != nil {
			r.Conn.Close()
		}
	}
}

func (rn *RaftNode) LinkPeerRpcServer(addr string) (*Net.ClientEnd, error) {
	conn, err := grpc.Dial(addr, grpc.WithInsecure())
	if err != nil {
		panic(err.Error())
	}
	res := &Net.ClientEnd{
		RaftCallClient: pb.NewRaftCallClient(conn),
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
	return atomic.AddUint32(&rn.commandOffset, 1)
}

func (rn *RaftNode) Commit(command interface{}) error {
	if rn.rf == nil {
		return errors.New(ErrNodeDidNotStart)
	}
	if !rn.rf.IsLeader() {
		return errors.New(ErrNotLeader)
	}
	entry := Entry{
		Id:      rn.GetNewCommandId(),
		command: command,
	}
	ch := make(chan struct{}, 1)
	rn.idMap.Add(entry.Id, func() {
		ch <- struct{}{}
	})
	select {
	case <-time.After(commitTimeout):
		rn.idMap.Delete(entry.Id)
		return errors.New(ErrCommitTimeout)
	case <-ch:
		// success
		return nil
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
				if ok {
					rn.idMap.GetCallDelete(command.Id)
				} else {
					panic("not reflect command")
				}
				rn.CommandHandler.Handle(command)
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
	rn.rf = Make(rn.Peers, rn.me, Persister.MakePersister(), rn.ch)
}
