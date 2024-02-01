// Code generated by protoc-gen-go-grpc. DO NOT EDIT.
// versions:
// - protoc-gen-go-grpc v1.2.0
// - protoc             v4.23.1
// source: MqServerRpc.proto

package rpc

import (
	context "context"
	grpc "google.golang.org/grpc"
	codes "google.golang.org/grpc/codes"
	status "google.golang.org/grpc/status"
)

// This is a compile-time assertion to ensure that this generated file
// is compatible with the grpc package it is being compiled against.
// Requires gRPC-Go v1.32.0 or later.
const _ = grpc.SupportPackageIsVersion7

// MqServerCallClient is the client API for MqServerCall service.
//
// For semantics around ctx use and closing/ending streaming RPCs, please refer to https://pkg.go.dev/google.golang.org/grpc/?tab=doc#ClientConn.NewStream.
type MqServerCallClient interface {
	JoinConsumerGroup(ctx context.Context, in *JoinConsumerGroupRequest, opts ...grpc.CallOption) (*JoinConsumerGroupResponse, error)
	LeaveConsumerGroup(ctx context.Context, in *LeaveConsumerGroupRequest, opts ...grpc.CallOption) (*LeaveConsumerGroupResponse, error)
	CheckSourceTerm(ctx context.Context, in *CheckSourceTermRequest, opts ...grpc.CallOption) (*CheckSourceTermResponse, error)
	GetCorrespondPartition(ctx context.Context, in *GetCorrespondPartitionRequest, opts ...grpc.CallOption) (*GetCorrespondPartitionResponse, error)
	// 注册消费者
	RegisterConsumerGroup(ctx context.Context, in *RegisterConsumerGroupRequest, opts ...grpc.CallOption) (*RegisterConsumerGroupResponse, error)
	UnRegisterConsumerGroup(ctx context.Context, in *UnRegisterConsumerGroupRequest, opts ...grpc.CallOption) (*UnRegisterConsumerGroupResponse, error)
	RegisterConsumer(ctx context.Context, in *RegisterConsumerRequest, opts ...grpc.CallOption) (*RegisterConsumerResponse, error)
	// 注册生产者
	RegisterProducer(ctx context.Context, in *RegisterProducerRequest, opts ...grpc.CallOption) (*RegisterProducerResponse, error)
	// 创建话题
	CreateTopic(ctx context.Context, in *CreateTopicRequest, opts ...grpc.CallOption) (*CreateTopicResponse, error)
	QueryTopic(ctx context.Context, in *QueryTopicRequest, opts ...grpc.CallOption) (*QueryTopicResponse, error)
	DestroyTopic(ctx context.Context, in *DestroyTopicRequest, opts ...grpc.CallOption) (*DestroyTopicResponse, error)
	ManagePartition(ctx context.Context, in *ManagePartitionRequest, opts ...grpc.CallOption) (*ManagePartitionResponse, error)
	// 注销
	UnRegisterConsumer(ctx context.Context, in *UnRegisterConsumerRequest, opts ...grpc.CallOption) (*UnRegisterConsumerResponse, error)
	UnRegisterProducer(ctx context.Context, in *UnRegisterProducerRequest, opts ...grpc.CallOption) (*UnRegisterProducerResponse, error)
	// 客户端和server之间的心跳
	Heartbeat(ctx context.Context, in *Ack, opts ...grpc.CallOption) (*Response, error)
	// 拉取消息
	PullMessage(ctx context.Context, in *PullMessageRequest, opts ...grpc.CallOption) (*PullMessageResponse, error)
	// 推送消息
	PushMessage(ctx context.Context, in *PushMessageRequest, opts ...grpc.CallOption) (*PushMessageResponse, error)
}

type mqServerCallClient struct {
	cc grpc.ClientConnInterface
}

func NewMqServerCallClient(cc grpc.ClientConnInterface) MqServerCallClient {
	return &mqServerCallClient{cc}
}

func (c *mqServerCallClient) JoinConsumerGroup(ctx context.Context, in *JoinConsumerGroupRequest, opts ...grpc.CallOption) (*JoinConsumerGroupResponse, error) {
	out := new(JoinConsumerGroupResponse)
	err := c.cc.Invoke(ctx, "/MqServer.MqServerCall/JoinConsumerGroup", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *mqServerCallClient) LeaveConsumerGroup(ctx context.Context, in *LeaveConsumerGroupRequest, opts ...grpc.CallOption) (*LeaveConsumerGroupResponse, error) {
	out := new(LeaveConsumerGroupResponse)
	err := c.cc.Invoke(ctx, "/MqServer.MqServerCall/LeaveConsumerGroup", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *mqServerCallClient) CheckSourceTerm(ctx context.Context, in *CheckSourceTermRequest, opts ...grpc.CallOption) (*CheckSourceTermResponse, error) {
	out := new(CheckSourceTermResponse)
	err := c.cc.Invoke(ctx, "/MqServer.MqServerCall/CheckSourceTerm", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *mqServerCallClient) GetCorrespondPartition(ctx context.Context, in *GetCorrespondPartitionRequest, opts ...grpc.CallOption) (*GetCorrespondPartitionResponse, error) {
	out := new(GetCorrespondPartitionResponse)
	err := c.cc.Invoke(ctx, "/MqServer.MqServerCall/GetCorrespondPartition", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *mqServerCallClient) RegisterConsumerGroup(ctx context.Context, in *RegisterConsumerGroupRequest, opts ...grpc.CallOption) (*RegisterConsumerGroupResponse, error) {
	out := new(RegisterConsumerGroupResponse)
	err := c.cc.Invoke(ctx, "/MqServer.MqServerCall/RegisterConsumerGroup", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *mqServerCallClient) UnRegisterConsumerGroup(ctx context.Context, in *UnRegisterConsumerGroupRequest, opts ...grpc.CallOption) (*UnRegisterConsumerGroupResponse, error) {
	out := new(UnRegisterConsumerGroupResponse)
	err := c.cc.Invoke(ctx, "/MqServer.MqServerCall/UnRegisterConsumerGroup", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *mqServerCallClient) RegisterConsumer(ctx context.Context, in *RegisterConsumerRequest, opts ...grpc.CallOption) (*RegisterConsumerResponse, error) {
	out := new(RegisterConsumerResponse)
	err := c.cc.Invoke(ctx, "/MqServer.MqServerCall/RegisterConsumer", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *mqServerCallClient) RegisterProducer(ctx context.Context, in *RegisterProducerRequest, opts ...grpc.CallOption) (*RegisterProducerResponse, error) {
	out := new(RegisterProducerResponse)
	err := c.cc.Invoke(ctx, "/MqServer.MqServerCall/RegisterProducer", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *mqServerCallClient) CreateTopic(ctx context.Context, in *CreateTopicRequest, opts ...grpc.CallOption) (*CreateTopicResponse, error) {
	out := new(CreateTopicResponse)
	err := c.cc.Invoke(ctx, "/MqServer.MqServerCall/CreateTopic", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *mqServerCallClient) QueryTopic(ctx context.Context, in *QueryTopicRequest, opts ...grpc.CallOption) (*QueryTopicResponse, error) {
	out := new(QueryTopicResponse)
	err := c.cc.Invoke(ctx, "/MqServer.MqServerCall/QueryTopic", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *mqServerCallClient) DestroyTopic(ctx context.Context, in *DestroyTopicRequest, opts ...grpc.CallOption) (*DestroyTopicResponse, error) {
	out := new(DestroyTopicResponse)
	err := c.cc.Invoke(ctx, "/MqServer.MqServerCall/DestroyTopic", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *mqServerCallClient) ManagePartition(ctx context.Context, in *ManagePartitionRequest, opts ...grpc.CallOption) (*ManagePartitionResponse, error) {
	out := new(ManagePartitionResponse)
	err := c.cc.Invoke(ctx, "/MqServer.MqServerCall/ManagePartition", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *mqServerCallClient) UnRegisterConsumer(ctx context.Context, in *UnRegisterConsumerRequest, opts ...grpc.CallOption) (*UnRegisterConsumerResponse, error) {
	out := new(UnRegisterConsumerResponse)
	err := c.cc.Invoke(ctx, "/MqServer.MqServerCall/UnRegisterConsumer", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *mqServerCallClient) UnRegisterProducer(ctx context.Context, in *UnRegisterProducerRequest, opts ...grpc.CallOption) (*UnRegisterProducerResponse, error) {
	out := new(UnRegisterProducerResponse)
	err := c.cc.Invoke(ctx, "/MqServer.MqServerCall/UnRegisterProducer", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *mqServerCallClient) Heartbeat(ctx context.Context, in *Ack, opts ...grpc.CallOption) (*Response, error) {
	out := new(Response)
	err := c.cc.Invoke(ctx, "/MqServer.MqServerCall/Heartbeat", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *mqServerCallClient) PullMessage(ctx context.Context, in *PullMessageRequest, opts ...grpc.CallOption) (*PullMessageResponse, error) {
	out := new(PullMessageResponse)
	err := c.cc.Invoke(ctx, "/MqServer.MqServerCall/PullMessage", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *mqServerCallClient) PushMessage(ctx context.Context, in *PushMessageRequest, opts ...grpc.CallOption) (*PushMessageResponse, error) {
	out := new(PushMessageResponse)
	err := c.cc.Invoke(ctx, "/MqServer.MqServerCall/PushMessage", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// MqServerCallServer is the server API for MqServerCall service.
// All implementations must embed UnimplementedMqServerCallServer
// for forward compatibility
type MqServerCallServer interface {
	JoinConsumerGroup(context.Context, *JoinConsumerGroupRequest) (*JoinConsumerGroupResponse, error)
	LeaveConsumerGroup(context.Context, *LeaveConsumerGroupRequest) (*LeaveConsumerGroupResponse, error)
	CheckSourceTerm(context.Context, *CheckSourceTermRequest) (*CheckSourceTermResponse, error)
	GetCorrespondPartition(context.Context, *GetCorrespondPartitionRequest) (*GetCorrespondPartitionResponse, error)
	// 注册消费者
	RegisterConsumerGroup(context.Context, *RegisterConsumerGroupRequest) (*RegisterConsumerGroupResponse, error)
	UnRegisterConsumerGroup(context.Context, *UnRegisterConsumerGroupRequest) (*UnRegisterConsumerGroupResponse, error)
	RegisterConsumer(context.Context, *RegisterConsumerRequest) (*RegisterConsumerResponse, error)
	// 注册生产者
	RegisterProducer(context.Context, *RegisterProducerRequest) (*RegisterProducerResponse, error)
	// 创建话题
	CreateTopic(context.Context, *CreateTopicRequest) (*CreateTopicResponse, error)
	QueryTopic(context.Context, *QueryTopicRequest) (*QueryTopicResponse, error)
	DestroyTopic(context.Context, *DestroyTopicRequest) (*DestroyTopicResponse, error)
	ManagePartition(context.Context, *ManagePartitionRequest) (*ManagePartitionResponse, error)
	// 注销
	UnRegisterConsumer(context.Context, *UnRegisterConsumerRequest) (*UnRegisterConsumerResponse, error)
	UnRegisterProducer(context.Context, *UnRegisterProducerRequest) (*UnRegisterProducerResponse, error)
	// 客户端和server之间的心跳
	Heartbeat(context.Context, *Ack) (*Response, error)
	// 拉取消息
	PullMessage(context.Context, *PullMessageRequest) (*PullMessageResponse, error)
	// 推送消息
	PushMessage(context.Context, *PushMessageRequest) (*PushMessageResponse, error)
	mustEmbedUnimplementedMqServerCallServer()
}

// UnimplementedMqServerCallServer must be embedded to have forward compatible implementations.
type UnimplementedMqServerCallServer struct {
}

func (UnimplementedMqServerCallServer) JoinConsumerGroup(context.Context, *JoinConsumerGroupRequest) (*JoinConsumerGroupResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method JoinConsumerGroup not implemented")
}
func (UnimplementedMqServerCallServer) LeaveConsumerGroup(context.Context, *LeaveConsumerGroupRequest) (*LeaveConsumerGroupResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method LeaveConsumerGroup not implemented")
}
func (UnimplementedMqServerCallServer) CheckSourceTerm(context.Context, *CheckSourceTermRequest) (*CheckSourceTermResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method CheckSourceTerm not implemented")
}
func (UnimplementedMqServerCallServer) GetCorrespondPartition(context.Context, *GetCorrespondPartitionRequest) (*GetCorrespondPartitionResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method GetCorrespondPartition not implemented")
}
func (UnimplementedMqServerCallServer) RegisterConsumerGroup(context.Context, *RegisterConsumerGroupRequest) (*RegisterConsumerGroupResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method RegisterConsumerGroup not implemented")
}
func (UnimplementedMqServerCallServer) UnRegisterConsumerGroup(context.Context, *UnRegisterConsumerGroupRequest) (*UnRegisterConsumerGroupResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method UnRegisterConsumerGroup not implemented")
}
func (UnimplementedMqServerCallServer) RegisterConsumer(context.Context, *RegisterConsumerRequest) (*RegisterConsumerResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method RegisterConsumer not implemented")
}
func (UnimplementedMqServerCallServer) RegisterProducer(context.Context, *RegisterProducerRequest) (*RegisterProducerResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method RegisterProducer not implemented")
}
func (UnimplementedMqServerCallServer) CreateTopic(context.Context, *CreateTopicRequest) (*CreateTopicResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method CreateTopic not implemented")
}
func (UnimplementedMqServerCallServer) QueryTopic(context.Context, *QueryTopicRequest) (*QueryTopicResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method QueryTopic not implemented")
}
func (UnimplementedMqServerCallServer) DestroyTopic(context.Context, *DestroyTopicRequest) (*DestroyTopicResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method DestroyTopic not implemented")
}
func (UnimplementedMqServerCallServer) ManagePartition(context.Context, *ManagePartitionRequest) (*ManagePartitionResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method ManagePartition not implemented")
}
func (UnimplementedMqServerCallServer) UnRegisterConsumer(context.Context, *UnRegisterConsumerRequest) (*UnRegisterConsumerResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method UnRegisterConsumer not implemented")
}
func (UnimplementedMqServerCallServer) UnRegisterProducer(context.Context, *UnRegisterProducerRequest) (*UnRegisterProducerResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method UnRegisterProducer not implemented")
}
func (UnimplementedMqServerCallServer) Heartbeat(context.Context, *Ack) (*Response, error) {
	return nil, status.Errorf(codes.Unimplemented, "method Heartbeat not implemented")
}
func (UnimplementedMqServerCallServer) PullMessage(context.Context, *PullMessageRequest) (*PullMessageResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method PullMessage not implemented")
}
func (UnimplementedMqServerCallServer) PushMessage(context.Context, *PushMessageRequest) (*PushMessageResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method PushMessage not implemented")
}
func (UnimplementedMqServerCallServer) mustEmbedUnimplementedMqServerCallServer() {}

// UnsafeMqServerCallServer may be embedded to opt out of forward compatibility for this service.
// Use of this interface is not recommended, as added methods to MqServerCallServer will
// result in compilation errors.
type UnsafeMqServerCallServer interface {
	mustEmbedUnimplementedMqServerCallServer()
}

func RegisterMqServerCallServer(s grpc.ServiceRegistrar, srv MqServerCallServer) {
	s.RegisterService(&MqServerCall_ServiceDesc, srv)
}

func _MqServerCall_JoinConsumerGroup_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(JoinConsumerGroupRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(MqServerCallServer).JoinConsumerGroup(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/MqServer.MqServerCall/JoinConsumerGroup",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(MqServerCallServer).JoinConsumerGroup(ctx, req.(*JoinConsumerGroupRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _MqServerCall_LeaveConsumerGroup_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(LeaveConsumerGroupRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(MqServerCallServer).LeaveConsumerGroup(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/MqServer.MqServerCall/LeaveConsumerGroup",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(MqServerCallServer).LeaveConsumerGroup(ctx, req.(*LeaveConsumerGroupRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _MqServerCall_CheckSourceTerm_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(CheckSourceTermRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(MqServerCallServer).CheckSourceTerm(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/MqServer.MqServerCall/CheckSourceTerm",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(MqServerCallServer).CheckSourceTerm(ctx, req.(*CheckSourceTermRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _MqServerCall_GetCorrespondPartition_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(GetCorrespondPartitionRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(MqServerCallServer).GetCorrespondPartition(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/MqServer.MqServerCall/GetCorrespondPartition",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(MqServerCallServer).GetCorrespondPartition(ctx, req.(*GetCorrespondPartitionRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _MqServerCall_RegisterConsumerGroup_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(RegisterConsumerGroupRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(MqServerCallServer).RegisterConsumerGroup(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/MqServer.MqServerCall/RegisterConsumerGroup",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(MqServerCallServer).RegisterConsumerGroup(ctx, req.(*RegisterConsumerGroupRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _MqServerCall_UnRegisterConsumerGroup_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(UnRegisterConsumerGroupRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(MqServerCallServer).UnRegisterConsumerGroup(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/MqServer.MqServerCall/UnRegisterConsumerGroup",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(MqServerCallServer).UnRegisterConsumerGroup(ctx, req.(*UnRegisterConsumerGroupRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _MqServerCall_RegisterConsumer_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(RegisterConsumerRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(MqServerCallServer).RegisterConsumer(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/MqServer.MqServerCall/RegisterConsumer",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(MqServerCallServer).RegisterConsumer(ctx, req.(*RegisterConsumerRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _MqServerCall_RegisterProducer_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(RegisterProducerRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(MqServerCallServer).RegisterProducer(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/MqServer.MqServerCall/RegisterProducer",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(MqServerCallServer).RegisterProducer(ctx, req.(*RegisterProducerRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _MqServerCall_CreateTopic_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(CreateTopicRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(MqServerCallServer).CreateTopic(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/MqServer.MqServerCall/CreateTopic",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(MqServerCallServer).CreateTopic(ctx, req.(*CreateTopicRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _MqServerCall_QueryTopic_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(QueryTopicRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(MqServerCallServer).QueryTopic(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/MqServer.MqServerCall/QueryTopic",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(MqServerCallServer).QueryTopic(ctx, req.(*QueryTopicRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _MqServerCall_DestroyTopic_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(DestroyTopicRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(MqServerCallServer).DestroyTopic(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/MqServer.MqServerCall/DestroyTopic",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(MqServerCallServer).DestroyTopic(ctx, req.(*DestroyTopicRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _MqServerCall_ManagePartition_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(ManagePartitionRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(MqServerCallServer).ManagePartition(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/MqServer.MqServerCall/ManagePartition",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(MqServerCallServer).ManagePartition(ctx, req.(*ManagePartitionRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _MqServerCall_UnRegisterConsumer_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(UnRegisterConsumerRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(MqServerCallServer).UnRegisterConsumer(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/MqServer.MqServerCall/UnRegisterConsumer",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(MqServerCallServer).UnRegisterConsumer(ctx, req.(*UnRegisterConsumerRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _MqServerCall_UnRegisterProducer_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(UnRegisterProducerRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(MqServerCallServer).UnRegisterProducer(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/MqServer.MqServerCall/UnRegisterProducer",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(MqServerCallServer).UnRegisterProducer(ctx, req.(*UnRegisterProducerRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _MqServerCall_Heartbeat_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(Ack)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(MqServerCallServer).Heartbeat(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/MqServer.MqServerCall/Heartbeat",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(MqServerCallServer).Heartbeat(ctx, req.(*Ack))
	}
	return interceptor(ctx, in, info, handler)
}

func _MqServerCall_PullMessage_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(PullMessageRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(MqServerCallServer).PullMessage(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/MqServer.MqServerCall/PullMessage",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(MqServerCallServer).PullMessage(ctx, req.(*PullMessageRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _MqServerCall_PushMessage_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(PushMessageRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(MqServerCallServer).PushMessage(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/MqServer.MqServerCall/PushMessage",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(MqServerCallServer).PushMessage(ctx, req.(*PushMessageRequest))
	}
	return interceptor(ctx, in, info, handler)
}

// MqServerCall_ServiceDesc is the grpc.ServiceDesc for MqServerCall service.
// It's only intended for direct use with grpc.RegisterService,
// and not to be introspected or modified (even as a copy)
var MqServerCall_ServiceDesc = grpc.ServiceDesc{
	ServiceName: "MqServer.MqServerCall",
	HandlerType: (*MqServerCallServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "JoinConsumerGroup",
			Handler:    _MqServerCall_JoinConsumerGroup_Handler,
		},
		{
			MethodName: "LeaveConsumerGroup",
			Handler:    _MqServerCall_LeaveConsumerGroup_Handler,
		},
		{
			MethodName: "CheckSourceTerm",
			Handler:    _MqServerCall_CheckSourceTerm_Handler,
		},
		{
			MethodName: "GetCorrespondPartition",
			Handler:    _MqServerCall_GetCorrespondPartition_Handler,
		},
		{
			MethodName: "RegisterConsumerGroup",
			Handler:    _MqServerCall_RegisterConsumerGroup_Handler,
		},
		{
			MethodName: "UnRegisterConsumerGroup",
			Handler:    _MqServerCall_UnRegisterConsumerGroup_Handler,
		},
		{
			MethodName: "RegisterConsumer",
			Handler:    _MqServerCall_RegisterConsumer_Handler,
		},
		{
			MethodName: "RegisterProducer",
			Handler:    _MqServerCall_RegisterProducer_Handler,
		},
		{
			MethodName: "CreateTopic",
			Handler:    _MqServerCall_CreateTopic_Handler,
		},
		{
			MethodName: "QueryTopic",
			Handler:    _MqServerCall_QueryTopic_Handler,
		},
		{
			MethodName: "DestroyTopic",
			Handler:    _MqServerCall_DestroyTopic_Handler,
		},
		{
			MethodName: "ManagePartition",
			Handler:    _MqServerCall_ManagePartition_Handler,
		},
		{
			MethodName: "UnRegisterConsumer",
			Handler:    _MqServerCall_UnRegisterConsumer_Handler,
		},
		{
			MethodName: "UnRegisterProducer",
			Handler:    _MqServerCall_UnRegisterProducer_Handler,
		},
		{
			MethodName: "Heartbeat",
			Handler:    _MqServerCall_Heartbeat_Handler,
		},
		{
			MethodName: "PullMessage",
			Handler:    _MqServerCall_PullMessage_Handler,
		},
		{
			MethodName: "PushMessage",
			Handler:    _MqServerCall_PushMessage_Handler,
		},
	},
	Streams:  []grpc.StreamDesc{},
	Metadata: "MqServerRpc.proto",
}
