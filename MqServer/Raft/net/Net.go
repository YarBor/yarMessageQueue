package net

import (
	"MqServer/Log"
	"MqServer/Raft"
	"MqServer/Raft/Gob"
	pb "MqServer/rpc"
	"bytes"
	"context"
	"errors"
	"google.golang.org/grpc"
	"net"
	"sync"
)

var (
	RaftListenAddr string = ""
)

const (
	RfNodeNotFound = "RfNodeNotFound"
)

type ClientEnd struct {
	pb.RaftCallClient
	frn  *raftNode
	conn *grpc.ClientConn
}

type raftNode struct {
	rf    *Raft.Raft
	T     string
	P     string
	Peers []*ClientEnd
	me    int
}

// *RequestArgs *RequestReply
func (c *ClientEnd) Call(fName string, args, reply interface{}) bool {
	var err error
	arg, ok := args.(*Raft.RequestArgs)
	if !ok {
		panic("args translate error")
	}
	rpl, ok := reply.(*Raft.RequestReply)
	if !ok {
		panic("reply translate error")
	}
	buff := bytes.Buffer{}
	if err = Gob.NewEncoder(&buff).Encode(*arg); err != nil {
		panic("encode error")
	}

	switch fName {
	case "Raft.RequestVote":
		i, err := c.RequestVote(context.Background(), &pb.RequestVoteRequest{
			Topic:     c.frn.T,
			Partition: c.frn.P,
			Arg:       buff.Bytes(),
		})
		if errors.Is(err, grpc.ErrServerStopped) {
			Log.ERROR(err.Error())
			return false
		} else if err != nil {
			Log.ERROR(err.Error())
			return false
		}
		if err = Gob.NewDecoder(bytes.NewBuffer(i.Result)).Decode(rpl); err != nil {
			panic(err.Error())
		}
		break
	case "Raft.RequestPreVote":
		i, err := c.RequestPreVote(context.Background(), &pb.RequestPreVoteRequest{
			Topic:     c.frn.T,
			Partition: c.frn.P,
			Arg:       buff.Bytes(),
		})
		if errors.Is(err, grpc.ErrServerStopped) {
			Log.ERROR(err.Error())
			return false
		} else if err != nil {
			Log.ERROR(err.Error())
			return false
		}
		if err = Gob.NewDecoder(bytes.NewBuffer(i.Result)).Decode(rpl); err != nil {
			panic(err.Error())
		}
		break
	case "Raft.HeartBeat":
		i, err := c.HeartBeat(context.Background(), &pb.HeartBeatRequest{
			Topic:     c.frn.T,
			Partition: c.frn.P,
			Arg:       buff.Bytes(),
		})
		if errors.Is(err, grpc.ErrServerStopped) {
			Log.ERROR(err.Error())
			return false
		} else if err != nil {
			Log.ERROR(err.Error())
			return false
		}
		if err = Gob.NewDecoder(bytes.NewBuffer(i.Result)).Decode(rpl); err != nil {
			panic(err.Error())
		}
		break
	default:
		panic("unknown RPC request")
	}
	return true
}

type RaftServer struct {
	pb.UnimplementedRaftCallServer
	mu       sync.RWMutex
	server   *grpc.Server
	listener net.Listener
	Addr     string
	rfs      map[string]map[string]*raftNode
}

func (rs *RaftServer) Serve() error {
	return rs.server.Serve(rs.listener)
}

func (rs *RaftServer) HeartBeat(context context.Context, arg *pb.HeartBeatRequest) (rpl *pb.HeartBeatResponse, err error) {
	rs.mu.RLock()
	defer rs.mu.RUnlock()
	if tp, par := arg.GetTopic(), arg.GetPartition(); tp == "" || par == "" {
		panic("invalid topic argument and partition argument")
	} else {
		rfnode, ok := rs.rfs[tp][par]
		if !ok {
			return nil, errors.New(RfNodeNotFound)
		} else {
			rfNodeArgs := Raft.RequestArgs{}
			err = Gob.NewDecoder(bytes.NewBuffer(arg.Arg)).Decode(&rfNodeArgs)
			if err != nil {
				Log.FATAL(err.Error())
			}
			rfNodeReply := Raft.RequestReply{}
			rfnode.rf.HeartBeat(&rfNodeArgs, &rfNodeReply)

			bff := bytes.Buffer{}
			err = Gob.NewEncoder(&bff).Encode(rfNodeReply)
			if err != nil {
				Log.FATAL(err.Error())
			}
			if rpl == nil {
				rpl = &pb.HeartBeatResponse{}
			}
			rpl.Topic = arg.Topic
			rpl.Partition = arg.Partition
			rpl.Result = bff.Bytes()
			return rpl, nil
		}
	}
}
func (rs *RaftServer) RequestPreVote(context context.Context, arg *pb.RequestPreVoteRequest) (rpl *pb.RequestPreVoteResponse, err error) {
	rs.mu.RLock()
	defer rs.mu.RUnlock()
	if tp, par := arg.GetTopic(), arg.GetPartition(); tp == "" || par == "" {
		panic("invalid topic argument and partition argument")
	} else {
		x, ok := rs.rfs[tp]
		if !ok {
			return nil, errors.New(RfNodeNotFound)
		}
		rfnode, ok := x[par]
		if !ok {
			return nil, errors.New(RfNodeNotFound)
		} else {
			rfNodeArgs := Raft.RequestArgs{}
			err = Gob.NewDecoder(bytes.NewBuffer(arg.Arg)).Decode(&rfNodeArgs)
			if err != nil {
				Log.FATAL(err.Error())
			}
			rfNodeReply := Raft.RequestReply{}
			rfnode.rf.RequestPreVote(&rfNodeArgs, &rfNodeReply)

			bff := bytes.Buffer{}
			err = Gob.NewEncoder(&bff).Encode(rfNodeReply)
			if err != nil {
				Log.FATAL(err.Error())
			}
			if rpl == nil {
				rpl = &pb.RequestPreVoteResponse{}
			}
			rpl.Topic = arg.Topic
			rpl.Partition = arg.Partition
			rpl.Result = bff.Bytes()
			return rpl, nil
		}

	}
}
func (rs *RaftServer) RequestVote(context context.Context, arg *pb.RequestVoteRequest) (rpl *pb.RequestVoteResponse, err error) {
	rs.mu.RLock()
	defer rs.mu.RUnlock()
	if tp, par := arg.GetTopic(), arg.GetPartition(); tp == "" || par == "" {
		panic("invalid topic argument and partition argument")
	} else {
		x, ok := rs.rfs[tp]
		if !ok {
			return nil, errors.New(RfNodeNotFound)
		}
		rfnode, ok := x[par]
		if !ok {
			return nil, errors.New(RfNodeNotFound)
		} else {
			rfNodeArgs := Raft.RequestArgs{}
			err = Gob.NewDecoder(bytes.NewBuffer(arg.Arg)).Decode(&rfNodeArgs)
			if err != nil {
				Log.FATAL(err.Error())
			}
			rfNodeReply := Raft.RequestReply{}
			rfnode.rf.RequestVote(&rfNodeArgs, &rfNodeReply)

			bff := bytes.Buffer{}
			err = Gob.NewEncoder(&bff).Encode(rfNodeReply)
			if err != nil {
				Log.FATAL(err.Error())
			}
			if rpl == nil {
				rpl = &pb.RequestVoteResponse{}
			}
			rpl.Topic = arg.Topic
			rpl.Partition = arg.Partition
			rpl.Result = bff.Bytes()
			return rpl, nil
		}

	}
}

func (rn *raftNode) LinkPeerRpcServer(addr string) (*ClientEnd, error) {
	conn, err := grpc.Dial(addr, grpc.WithInsecure())
	if err != nil {
		panic(err.Error())
	}
	res := &ClientEnd{
		RaftCallClient: pb.NewRaftCallClient(conn),
		frn:            rn,
		conn:           conn,
	}
	return res, nil
}

// url包含自己
func (rf *RaftServer) RegisterRfNode(T, P string, NodesUrl []string, ch chan Raft.ApplyMsg) error {
	rn := raftNode{
		T:     T,
		P:     P,
		Peers: make([]*ClientEnd, len(NodesUrl)),
	}
	selfIndex := -1
	for i, n := range NodesUrl {
		if n == RaftListenAddr {
			selfIndex = i
			continue
		} else {
			peer, _ := rn.LinkPeerRpcServer(n)
			rn.Peers[i] = peer
		}
	}
	if selfIndex == -1 {
		panic("register node failed")
	}
	rf.mu.Lock()
	_, ok := rf.rfs[T]
	if !ok {
		rf.rfs[T] = make(map[string]*raftNode)
	}
	rf.rfs[T][P] = &rn
	rf.mu.Unlock()
	rn.rf = Raft.Make(rn.Peers, selfIndex, Raft.MakePersister(), ch)
	return nil
}

func MakeRaftServer() (*RaftServer, error) {
	if RaftListenAddr == "" {
		panic("RaftListenAddr must be set")
	}
	lis, err := net.Listen("tcp", RaftListenAddr)
	if err != nil {
		Log.FATAL(err)
	}
	s := grpc.NewServer()
	res := &RaftServer{
		UnimplementedRaftCallServer: pb.UnimplementedRaftCallServer{},
		mu:                          sync.RWMutex{},
		server:                      s,
		listener:                    lis,
		Addr:                        RaftListenAddr,
		rfs:                         make(map[string]map[string]*raftNode),
	}
	pb.RegisterRaftCallServer(s, res)
	return res, nil
}
