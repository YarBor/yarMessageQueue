package MqServer

import (
	"MqServer/MessageMem"
	"MqServer/Raft"
	pb "MqServer/rpc"
	"context"
	"google.golang.org/grpc"
	"sync"
	"time"
)

type Server interface {
	Serve() error
	Stop() error
}
type ServerClient struct {
	pb.RaftCallClient
	Conn *grpc.ClientConn
}

type broker struct {
	pb.UnimplementedMqServerCallServer
	RaftServer           Raft.RaftServer
	Url                  string
	ID                   string
	Key                  string
	Conns                map[string]*ServerClient
	MetadataLeader       *ServerClient
	MetaDataController   MetaDataController
	PartitionsController PartitionsController
}

type Consumer struct {
	ConsumerMD
	HeartBeat time.Time
}

// 客户端和server之间的心跳

// 注册消费者
func (s *broker) RegisterConsumer(_ context.Context, req *pb.RegisterConsumerRequest) (*pb.RegisterConsumerResponse, error) {
	res := s.MetaDataController.RegisterConsumer(req)
	if res == nil {
		panic("Ub")
	}
	return res, nil
}

// 注册生产者
func (s *broker) RegisterProducer(_ context.Context, req *pb.RegisterProducerRequest) (*pb.RegisterProducerResponse, error) {
	res := s.MetaDataController.RegisterProducer(req)
	if res.Response.Mode == pb.Response_Success {
		res.Credential.Key = s.Key
	}
	return res, nil
}

// 创建话题
func (s *broker) CreateTopic(_ context.Context, req *pb.CreateTopicRequest) (*pb.CreateTopicResponse, error) {

	res := s.MetaDataController.CreateTopic(req)
	if res == nil {
		panic("Ub")
	}
	return res, nil
}
func (s *broker) QueryTopic(_ context.Context, req *pb.QueryTopicRequest) (*pb.QueryTopicResponse, error) {
	res := s.MetaDataController.QueryTopic(req)
	if res == nil {
		panic("Ub")
	}
	return res, nil
}
func (s *broker) DestroyTopic(_ context.Context, req *pb.DestroyTopicRequest) (*pb.DestroyTopicResponse, error) {
	res := s.MetaDataController.DestroyTopic(req)
	if res == nil {
		panic("Ub")
	}
	return res, nil
}
func (s *broker) ManagePartition(_ context.Context, req *pb.ManagePartitionRequest) (*pb.ManagePartitionResponse, error) {

}

// 注销
func (s *broker) UnRegisterConsumer(_ context.Context, req *pb.UnRegisterConsumerRequest) (*pb.UnRegisterConsumerResponse, error) {
	res := s.MetaDataController.UnRegisterConsumer(req)
	if res == nil {
		panic("Ub")
	}
	return res, nil
}

func (s *broker) UnRegisterProducer(_ context.Context, req *pb.UnRegisterProducerRequest) (*pb.UnRegisterProducerResponse, error) {
	res := s.MetaDataController.UnRegisterProducer(req)
	if res == nil {
		panic("Ub")
	}
	return res, nil
}

// 拉取消息
func (s *broker) PullMessage(_ context.Context, req *pb.PullMessageRequest) (*pb.PullMessageResponse, error) {
}

// 推送消息
func (s *broker) PushMessage(_ context.Context, req *pb.PushMessageRequest) (*pb.PushMessageResponse, error) {
}

func (s *broker) Heartbeat(_ context.Context, req *pb.Ack) (*pb.Response, error) {
}

func ResponseFailure() *pb.Response {
	return &pb.Response{Mode: pb.Response_Failure}
}
func ResponseErrTimeout() *pb.Response {
	return &pb.Response{Mode: pb.Response_ErrTimeout}
}
func ResponseErrNotLeader() *pb.Response {
	return &pb.Response{Mode: pb.Response_ErrNotLeader}
}
func ResponseErrSourceNotExist() *pb.Response {
	return &pb.Response{Mode: pb.Response_ErrSourceNotExist}
}
func ResponseErrSourceAlreadyExist() *pb.Response {
	return &pb.Response{Mode: pb.Response_ErrSourceAlreadyExist}
}
func ResponseErrPartitionChanged() *pb.Response {
	return &pb.Response{Mode: pb.Response_ErrPartitionChanged}
}
func ResponseErrRequestIllegal() *pb.Response {
	return &pb.Response{Mode: pb.Response_ErrRequestIllegal}
}
func ResponseErrSourceNotEnough() *pb.Response {
	return &pb.Response{Mode: pb.Response_ErrSourceNotEnough}
}

func ResponseSuccess() *pb.Response {
	return &pb.Response{Mode: pb.Response_Success}
}
func ResponseNotServer() *pb.Response {
	return &pb.Response{Mode: pb.Response_NotServe}
}

type Partition struct {
	P                        string
	T                        string
	ConsumerHeartBeatManager *ConsumerHeartBeatManager
	Part                     *MessageMem.MessageEntry
}

func newPartition(t, p string, MaxEntries, MaxSize uint64) *Partition {
	return &Partition{
		P:                        p,
		T:                        t,
		ConsumerHeartBeatManager: newConsumerHeartBeatManager(),
		Part:                     MessageMem.NewMessageEntry(MaxEntries, MaxSize),
	}
}

func (ptc *PartitionsController) RegisterPart(t, p string, MaxEntries, MaxSize uint64) {
	ptc.mu.Lock()
	defer ptc.mu.Unlock()
	ptc.P[t+"/"+p] = newPartition(t, p, MaxEntries, MaxSize)
}

type PartitionsController struct {
	mu sync.RWMutex
	P  map[string]*Partition // key: "Topic/Partition"
}

func (c *PartitionsController) RegisterConsumer() error {

}

type ConsumerHeartBeatManager struct {
	mu            sync.RWMutex
	wg            sync.WaitGroup
	ConsumeOffset uint64
	IdHash        map[uint32]int // CredId -- Index
	Consumers     []Consumer
}

func newConsumerHeartBeatManager() *ConsumerHeartBeatManager {
	return &ConsumerHeartBeatManager{
		ConsumeOffset: 0,
		IdHash:        make(map[uint32]int),
		Consumers:     make([]Consumer, 0),
	}
}
