package MqServer

import (
	"MqServer/Raft"
	pb "MqServer/rpc"
	"context"
)

type Server interface {
	Serve() error
	Stop() error
}

type ServerImpl struct {
	pb.UnimplementedMqServerCallServer
	RaftServer         Raft.RaftServer
	MetaDataController MetaDataController
}

// 客户端和server之间的心跳

// 注册消费者
func (s *ServerImpl) RegisterConsumer(context.Context, *pb.RegisterConsumerRequest) (*pb.RegisterConsumerResponse, error) {
}

// 注册生产者
func (s *ServerImpl) RegisterProducer(ctx context.Context, req *pb.RegisterProducerRequest) (*pb.RegisterProducerResponse, error) {
	if err := s.MetaDataController.RegisterProducer(req); err != nil {
		if err.Error() == Raft.ErrNotLeader {
			return nil, err
		}
	}
}

// 创建话题
func (s *ServerImpl) CreateTopic(context.Context, *pb.CreateTopicRequest) (*pb.CreateTopicResponse, error) {
}
func (s *ServerImpl) QueryTopic(context.Context, *pb.QueryTopicRequest) (*pb.QueryTopicResponse, error) {
}
func (s *ServerImpl) DestroyTopic(context.Context, *pb.DestroyTopicRequest) (*pb.DestroyTopicResponse, error) {
}
func (s *ServerImpl) ManagePartition(context.Context, *pb.ManagePartitionRequest) (*pb.ManagePartitionResponse, error) {
}

// 注销
func (s *ServerImpl) UnRegisterConsumer(context.Context, *pb.UnRegisterConsumerRequest) (*pb.UnRegisterConsumerResponse, error) {
}

func (s *ServerImpl) UnRegisterProducer(context.Context, *pb.UnRegisterProducerRequest) (*pb.UnRegisterProducerResponse, error) {
}

// 拉取消息
func (s *ServerImpl) PullMessage(context.Context, *pb.PullMessageRequest) (*pb.PullMessageResponse, error) {
}

// 推送消息
func (s *ServerImpl) PushMessage(context.Context, *pb.PushMessageRequest) (*pb.PushMessageResponse, error) {
}

func (s *ServerImpl) Heartbeat(context.Context, *pb.Ack) (*pb.Response, error) {
}

 
func ErrResponse_Failure() *pb.Response {
	return &pb.Response{ Mode : pb.Response_Failure}
}
func ErrResponse_ErrTimeout() *pb.Response {
	return &pb.Response{ Mode : pb.Response_ErrTimeout}
}
func ErrResponse_ErrNotLeader() *pb.Response {
	return &pb.Response{ Mode : pb.Response_ErrNotLeader}
}
func ErrResponse_ErrSourceNotExist() *pb.Response {
	return &pb.Response{ Mode : pb.Response_ErrSourceNotExist}
}
func ErrResponse_ErrSourceAlreadyExist() *pb.Response {
	return &pb.Response{ Mode : pb.Response_ErrSourceAlreadyExist}
}
func ErrResponse_ErrPartitionChanged() *pb.Response {
	return &pb.Response{ Mode : pb.Response_ErrPartitionChanged}
}
func ErrResponse_ErrRequestIllegal() *pb.Response {
	return &pb.Response{ Mode : pb.Response_ErrRequestIllegal}
}
func ErrResponse_ErrSourceNotEnough() *pb.Response {
	return &pb.Response{ Mode : pb.Response_ErrSourceNotEnough}
}

func ResponseSuccess() *pb.Response {
	return &pb.Response{ Mode : pb.Response_Success }
}
