package MqServer

import (
	"MqServer/Raft"
	"MqServer/Random"
	pb "MqServer/rpc"
	"errors"
	"google.golang.org/grpc"
	"net"
	"sync"
)

type Producer struct {
	Account pb.Account
	Topic   string
}

type Consumer struct {
	Account   pb.Account
	Topic     string
	Partition string
}

type Broker struct {
	Id         string
	ServiceUrl string
	RaftUrl    string
}

type MqServer struct {
	lis net.Listener
	mu  sync.RWMutex
	MqServerInfo
	raftServer      *Raft.RaftServer
	server          *grpc.Server
	metadataHandler *MetadataHandler
	pb.UnimplementedMqServerCallServer
}

func (s *MqServer) IsMultipleClusters() bool {
	return len(s.clustersInfo) == 0 && s.RaftUrl != ""
}

func (s *MqServer) Serve() error {
	if s.IsMultipleClusters() {
		err := s.raftServer.Serve()
		if err != nil {
			return err
		}
		urls := make([]string, len(s.clustersInfo)+1)
		for i := range s.clustersInfo {
			urls[i] = s.clustersInfo[i].RaftUrl
		}
		urls[len(urls)-1] = s.RaftUrl
		err = s.raftServer.RegisterMetadataRaft(urls, s.metadataHandler.RaftApplyChan)
		if err != nil {
			return err
		}
	}
	err := s.server.Serve(s.lis)
	if err != nil {
		s.raftServer.Stop()
		return err
	}
	s.stat = ServerStatWorking
	return nil
}

func (s *MqServer) Stop() {
	s.stat = ServerStatStop
	s.metadataHandler.Stop()
	s.server.Stop()
	s.raftServer.Stop()
}

type Server interface {
	Serve() error
	Stop()
}

const (
	NotSetServerListenAddr = "NotSetServerListenAddr"
)

func MakeMqServer(options ...Option) (Server, error) {
	s := &MqServer{
		MqServerInfo: MqServerInfo{
			stat:         ServerStatIniting,
			Name:         "",
			SelfUrl:      "",
			RaftUrl:      "",
			clustersInfo: make([]MqServerInfo, 0),
		},
		raftServer:                      nil,
		server:                          nil,
		metadataHandler:                 MakeMetadataHandler(),
		UnimplementedMqServerCallServer: pb.UnimplementedMqServerCallServer{},
	}

	for _, option := range options {
		option(&s.MqServerInfo)
	}

	if s.SelfUrl == "" {
		return nil, errors.New(NotSetServerListenAddr)
	}
	if s.Name == "" {
		s.Name = Random.RandStringBytes(16)
	}
	if s.IsMultipleClusters() {
		Raft.SetRaftListenAddr(s.MqServerInfo.RaftUrl)
		if server, err := Raft.MakeRaftServer(); err != nil {
			return nil, err
		} else {
			s.raftServer = server
		}
	}
	if lis, err := net.Listen("tcp", s.MqServerInfo.SelfUrl); err != nil {
		return nil, err
	} else {
		s.lis = lis
		s.server = grpc.NewServer()
		pb.RegisterMqServerCallServer(s.server, s)
	}
	return Server(s), nil
}
