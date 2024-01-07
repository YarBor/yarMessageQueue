package MqServer

import (
	pb "MqServer/rpc"
	"context"
)

func (s *MqServer) Heartbeat(_ context.Context, req *pb.Ack) (res *pb.Response, err error) {
	return res, err
}
func (s *MqServer) RegisterConsumer(_ context.Context, req *pb.RegisterConsumerRequest) (res *pb.RegisterConsumerResponse, err error) {
	return res, err
}
func (s *MqServer) RegisterProducer(_ context.Context, req *pb.RegisterProducerRequest) (res *pb.RegisterProducerResponse, err error) {
	return res, err
}
func (s *MqServer) PullMessage(_ context.Context, req *pb.PullMessageRequest) (res *pb.PullMessageResponse, err error) {
	return res, err
}
func (s *MqServer) PushMessage(_ context.Context, req *pb.PushMessageRequest) (res *pb.PushMessageResponse, err error) {
	return res, err
}
func (s *MqServer) UpdateMetadata(_ context.Context, req *pb.UpdateMetadataRequest) (res *pb.UpdateMetadataResponse, err error) {
	return res, err
}
