package RaftServer

import (
	"MqServer/RaftServer/Persister"
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
	return rn.rf.IsLeader()
}

func (rn *RaftNode) CloseAllConn() {
	for _, r := range rn.Peers {
		if r != nil && r.Conn != nil {
			r.Conn.Close()
		}
	}
}

func (rn *RaftNode) LinkPeerRpcServer(addr string) (*ClientEnd, error) {
	conn, err := grpc.Dial(addr, grpc.WithInsecure())
	if err != nil {
		panic(err.Error())
	}
	res := &ClientEnd{
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
