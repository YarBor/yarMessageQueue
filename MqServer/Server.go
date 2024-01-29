package MqServer

import (
	"MqServer/Raft"
	pb "MqServer/rpc"
	"context"
	"google.golang.org/grpc"
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

type ServerImpl struct {
	pb.UnimplementedMqServerCallServer
	RaftServer               Raft.RaftServer
	Url                      string
	ID                       string
	Key                      string
	Conns                    map[string]*ServerClient
	MetadataLeader           *ServerClient
	MetaDataController       MetaDataController
	PartitionsController     PartitionsController
	ConsumerHeartBeatManager ConsumerHeartBeatManager
}

type Consumer struct {
	ConsumerMD
	HeartBeat time.Time
}

// 客户端和server之间的心跳

// 注册消费者
func (s *ServerImpl) RegisterConsumer(_ context.Context, req *pb.RegisterConsumerRequest) (*pb.RegisterConsumerResponse, error) {
	res := s.MetaDataController.RegisterConsumer(req)
	if res == nil {
		panic("Ub")
	}
	return res, nil
}

// 注册生产者
func (s *ServerImpl) RegisterProducer(_ context.Context, req *pb.RegisterProducerRequest) (*pb.RegisterProducerResponse, error) {
	res := s.MetaDataController.RegisterProducer(req)
	if res.Response.Mode == pb.Response_Success {
		res.Credential.Key = s.Key
	}
	return res, nil
}

// 创建话题
func (s *ServerImpl) CreateTopic(_ context.Context, req *pb.CreateTopicRequest) (*pb.CreateTopicResponse, error) {

	res := s.MetaDataController.CreateTopic(req)
	if res == nil {
		panic("Ub")
	}
	return res, nil
}
func (s *ServerImpl) QueryTopic(_ context.Context, req *pb.QueryTopicRequest) (*pb.QueryTopicResponse, error) {
	res := s.MetaDataController.QueryTopic(req)
	if res == nil {
		panic("Ub")
	}
	return res, nil
}
func (s *ServerImpl) DestroyTopic(_ context.Context, req *pb.DestroyTopicRequest) (*pb.DestroyTopicResponse, error) {
	res := s.MetaDataController.DestroyTopic(req)
	if res == nil {
		panic("Ub")
	}
	return res, nil
}
func (s *ServerImpl) ManagePartition(_ context.Context, req *pb.ManagePartitionRequest) (*pb.ManagePartitionResponse, error) {

}

// 注销
func (s *ServerImpl) UnRegisterConsumer(_ context.Context, req *pb.UnRegisterConsumerRequest) (*pb.UnRegisterConsumerResponse, error) {
	res := s.MetaDataController.UnRegisterConsumer(req)
	if res == nil {
		panic("Ub")
	}
	return res, nil
}

func (s *ServerImpl) UnRegisterProducer(_ context.Context, req *pb.UnRegisterProducerRequest) (*pb.UnRegisterProducerResponse, error) {
	res := s.MetaDataController.UnRegisterProducer(req)
	if res == nil {
		panic("Ub")
	}
	return res, nil
}

// 拉取消息
func (s *ServerImpl) PullMessage(_ context.Context, req *pb.PullMessageRequest) (*pb.PullMessageResponse, error) {
}

// 推送消息
func (s *ServerImpl) PushMessage(_ context.Context, req *pb.PushMessageRequest) (*pb.PushMessageResponse, error) {
}

func (s *ServerImpl) Heartbeat(_ context.Context, req *pb.Ack) (*pb.Response, error) {
}

func ErrResponse_Failure() *pb.Response {
	return &pb.Response{Mode: pb.Response_Failure}
}
func ErrResponse_ErrTimeout() *pb.Response {
	return &pb.Response{Mode: pb.Response_ErrTimeout}
}
func ErrResponse_ErrNotLeader() *pb.Response {
	return &pb.Response{Mode: pb.Response_ErrNotLeader}
}
func ErrResponse_ErrSourceNotExist() *pb.Response {
	return &pb.Response{Mode: pb.Response_ErrSourceNotExist}
}
func ErrResponse_ErrSourceAlreadyExist() *pb.Response {
	return &pb.Response{Mode: pb.Response_ErrSourceAlreadyExist}
}
func ErrResponse_ErrPartitionChanged() *pb.Response {
	return &pb.Response{Mode: pb.Response_ErrPartitionChanged}
}
func ErrResponse_ErrRequestIllegal() *pb.Response {
	return &pb.Response{Mode: pb.Response_ErrRequestIllegal}
}
func ErrResponse_ErrSourceNotEnough() *pb.Response {
	return &pb.Response{Mode: pb.Response_ErrSourceNotEnough}
}

func ResponseSuccess() *pb.Response {
	return &pb.Response{Mode: pb.Response_Success}
}
func ErrResponse_NotServer() *pb.Response {
	return &pb.Response{Mode: pb.Response_NotServe}
}
