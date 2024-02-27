package MqServer

import (
	"MqServer/ConsumerGroup"
	"MqServer/Err"
	"MqServer/MessageMem"
	"MqServer/RaftServer"
	pb "MqServer/rpc"
	"context"
	"errors"
	"google.golang.org/grpc"
	"math"
	"sync"
)

type Server interface {
	Serve() error
	Stop() error
}
type ServerClient struct {
	pb.RaftCallClient
	Conn *grpc.ClientConn
}

type connections struct {
	mu             sync.RWMutex
	Conn           map[string]*ServerClient
	MetadataLeader *ServerClient
}

type broker struct {
	pb.UnimplementedMqServerCallServer
	RaftServer RaftServer.RaftServer
	Url        string
	ID         string
	Key        string
	Peers      map[string]*struct {
		Client pb.MqServerCallClient
		Conn   *grpc.ClientConn
	}
	MetaDataController   *MetaDataController
	PartitionsController PartitionsController
}

var (
	defaultMaxEntries = uint64(math.MaxUint64)
	defaultMaxSize    = uint64(math.MaxUint64)
)

type PartitionsController struct {
	mu            sync.RWMutex
	P             map[string]*Partition // key: "Topic/Partition"
	handleTimeout ConsumerGroup.SessionLogoutNotifier
}

// 客户端和server之间的心跳
// 注册消费者
func (s *broker) RegisterConsumer(_ context.Context, req *pb.RegisterConsumerRequest) (*pb.RegisterConsumerResponse, error) {
	if s.MetaDataController == nil {
		return &pb.RegisterConsumerResponse{
			Response: ResponseErrNotLeader(),
		}, nil
	}
	res := s.MetaDataController.RegisterConsumer(req)
	if res.Response.Mode == pb.Response_Success {
		res.Credential.Key = s.Key
	}
	return res, nil
}

//func test() {
//	b := broker{
//		UnimplementedMqServerCallServer: pb.UnimplementedMqServerCallServer{},
//		RaftServer:                      RaftServer.RaftServer{},
//		Url:                             "",
//		ID:                              "",
//		Key:                             "",
//		Peers:                           nil,
//		MetaDataController:              nil,
//		PartitionsController:            PartitionsController{},
//	}
//	newServer := grpc.NewServer()
//	pb.RegisterMqServerCallServer(newServer, &b)
//}

func (s *broker) SubscribeTopic(_ context.Context, req *pb.SubscribeTopicRequest) (*pb.SubscribeTopicResponse, error) {
	if s.MetaDataController == nil {
		return &pb.SubscribeTopicResponse{
			Response: ResponseErrNotLeader(),
		}, nil
	}
	return s.MetaDataController.AddTopicRegisterConsumerGroup(req), nil
}
func (s *broker) UnSubscribeTopic(_ context.Context, req *pb.UnSubscribeTopicRequest) (*pb.UnSubscribeTopicResponse, error) {
	if s.MetaDataController == nil {
		return &pb.UnSubscribeTopicResponse{
			Response: ResponseErrNotLeader(),
		}, nil
	}
	return s.MetaDataController.DelTopicRegisterConsumerGroup(req), nil
}
func (s *broker) AddPart(_ context.Context, req *pb.AddPartRequest) (*pb.AddPartResponse, error) {
	if s.MetaDataController == nil {
		return &pb.AddPartResponse{
			Response: ResponseErrNotLeader(),
		}, nil
	}
	return s.MetaDataController.AddPart(req), nil
}
func (s *broker) RemovePart(_ context.Context, req *pb.RemovePartRequest) (*pb.RemovePartResponse, error) {
	if s.MetaDataController == nil {
		return &pb.RemovePartResponse{
			Response: ResponseErrNotLeader(),
		}, nil
	}
	return s.MetaDataController.RemovePart(req), nil
}

func (s *broker) ConsumerDisConnect(_ context.Context, req *pb.DisConnectInfo) (*pb.Response, error) {
	if s.MetaDataController == nil {
		return ResponseErrNotLeader(), nil
	}
	return s.MetaDataController.ConsumerDisConnect(req), nil
}

func (s *broker) ProducerDisConnect(_ context.Context, req *pb.DisConnectInfo) (*pb.Response, error) {
	if s.MetaDataController == nil {
		return ResponseErrNotLeader(), nil
	}
	return s.MetaDataController.ProducerDisConnect(req), nil
}

// 注册生产者
func (s *broker) RegisterProducer(_ context.Context, req *pb.RegisterProducerRequest) (*pb.RegisterProducerResponse, error) {
	if s.MetaDataController == nil {
		return &pb.RegisterProducerResponse{
			Response: ResponseErrNotLeader(),
		}, nil
	}
	res := s.MetaDataController.RegisterProducer(req)
	if res.Response.Mode == pb.Response_Success {
		res.Credential.Key = s.Key
	}
	return res, nil
}

// 创建话题
func (s *broker) CreateTopic(_ context.Context, req *pb.CreateTopicRequest) (*pb.CreateTopicResponse, error) {
	if s.MetaDataController == nil {
		return &pb.CreateTopicResponse{
			Response: ResponseErrNotLeader(),
		}, nil
	}
	res := s.MetaDataController.CreateTopic(req)
	if res == nil {
		panic("Ub")
	}
	return res, nil
}
func (s *broker) QueryTopic(_ context.Context, req *pb.QueryTopicRequest) (*pb.QueryTopicResponse, error) {
	if s.MetaDataController == nil {
		return &pb.QueryTopicResponse{
			Response: ResponseErrNotLeader(),
		}, nil
	}
	res := s.MetaDataController.QueryTopic(req)
	if res == nil {
		panic("Ub")
	}
	return res, nil
}

// 注销
func (s *broker) UnRegisterConsumer(_ context.Context, req *pb.UnRegisterConsumerRequest) (*pb.UnRegisterConsumerResponse, error) {
	if s.MetaDataController == nil {
		return &pb.UnRegisterConsumerResponse{
			Response: ResponseErrNotLeader(),
		}, nil
	}
	res := s.MetaDataController.UnRegisterConsumer(req)
	if res == nil {
		panic("Ub")
	}
	return res, nil
}

func (s *broker) UnRegisterProducer(_ context.Context, req *pb.UnRegisterProducerRequest) (*pb.UnRegisterProducerResponse, error) {
	if s.MetaDataController == nil {
		return &pb.UnRegisterProducerResponse{
			Response: ResponseErrNotLeader(),
		}, nil
	}
	res := s.MetaDataController.UnRegisterProducer(req)
	if res == nil {
		panic("Ub")
	}
	return res, nil
}

func (s *broker) JoinConsumerGroup(_ context.Context, req *pb.JoinConsumerGroupRequest) (*pb.JoinConsumerGroupResponse, error) {
	if s.MetaDataController == nil {
		return &pb.JoinConsumerGroupResponse{
			Response: ResponseErrNotLeader(),
		}, nil
	}
	if req.Cred.Key != s.Key {
		return nil, errors.New(Err.ErrRequestIllegal)
	}
	res := s.MetaDataController.JoinRegisterConsumerGroup(req)
	return res, nil
}

func (s *broker) LeaveConsumerGroup(_ context.Context, req *pb.LeaveConsumerGroupRequest) (*pb.LeaveConsumerGroupResponse, error) {
	if s.MetaDataController == nil {
		return &pb.LeaveConsumerGroupResponse{
			Response: ResponseErrNotLeader(),
		}, nil
	}
	if req.GroupCred.Key != s.Key || req.ConsumerCred.Key != s.Key {
		return nil, errors.New(Err.ErrRequestIllegal)
	}
	res := s.MetaDataController.LeaveRegisterConsumerGroup(req)
	return res, nil
}

func (s *broker) CheckSourceTerm(_ context.Context, req *pb.CheckSourceTermRequest) (*pb.CheckSourceTermResponse, error) {
	if s.MetaDataController == nil {
		return &pb.CheckSourceTermResponse{
			Response: ResponseErrNotLeader(),
		}, nil
	}
	if req.Self.Key != s.Key {
		return nil, errors.New(Err.ErrRequestIllegal)
	}
	return s.MetaDataController.CheckSourceTerm(req), nil
}

//func (s *broker) GetCorrespondPartition(_ context.Context, req *pb.GetCorrespondPartitionRequest) (*pb.GetCorrespondPartitionResponse, error) {
//	return nil, nil
//}

func (s *broker) RegisterConsumerGroup(_ context.Context, req *pb.RegisterConsumerGroupRequest) (*pb.RegisterConsumerGroupResponse, error) {
	if s.MetaDataController == nil {
		return &pb.RegisterConsumerGroupResponse{
			Response: ResponseErrNotLeader(),
		}, nil
	} //TODO:
	res := s.MetaDataController.RegisterConsumerGroup(req)
	if res == nil {
		panic("Ub")
	}
	return res, nil
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

// TODO: Need Part To Confirm

func (s *broker) ConfirmIdentity(_ context.Context, req *pb.ConfirmIdentityRequest) (*pb.ConfirmIdentityResponse, error) {
	if s.MetaDataController == nil {
		return &pb.ConfirmIdentityResponse{
			Response: ResponseErrNotLeader(),
		}, nil
	}

}

// TODO complete PULL PUSH HEARTBEAT

// 拉取消息
func (s *broker) PullMessage(_ context.Context, req *pb.PullMessageRequest) (*pb.PullMessageResponse, error) {

	return nil, nil
}

// 推送消息
func (s *broker) PushMessage(_ context.Context, req *pb.PushMessageRequest) (*pb.PushMessageResponse, error) {
	// TODO:
	return nil, nil
}

func (s *broker) Heartbeat(_ context.Context, req *pb.MQHeartBeatData) (*pb.Response, error) {
	// TODO:
	switch req.Self.Identity {
	case pb.Credentials_Consumer:
		if req.GroupCred == nil || req.Offset == nil {
			return ResponseErrRequestIllegal(), nil
		} else {

		}
	case pb.Credentials_Broker:

	case pb.Credentials_Producer:
	}
	return nil, nil
}

type Partition struct {
	wg                   sync.WaitGroup
	T                    string
	P                    string
	ConsumerGroupManager *ConsumerGroup.GroupsManager
	MessageEntry         *MessageMem.MessageEntry
}

func newPartition(t, p string, MaxEntries, MaxSize uint64, handleTimeout ConsumerGroup.SessionLogoutNotifier) *Partition {
	res := ConsumerGroup.NewGroupsManager(handleTimeout)
	part := &Partition{
		T:                    t,
		P:                    p,
		ConsumerGroupManager: res,
		MessageEntry:         MessageMem.NewMessageEntry(MaxEntries, MaxSize),
	}
	part.wg.Add(1)
	go part.ConsumerGroupManager.HeartbeatCheck()
	return part

}

func (p *Partition) registerConsumerGroup(groupId string, maxReturnMessageSize int32) error {
	return p.ConsumerGroupManager.RegisterConsumerGroup(ConsumerGroup.NewConsumerGroup(groupId, maxReturnMessageSize))
}

func NewPartitionsController(handleTimeout ConsumerGroup.SessionLogoutNotifier) *PartitionsController {
	return &PartitionsController{
		P:             make(map[string]*Partition),
		handleTimeout: handleTimeout,
	}
}

func (pc *PartitionsController) getPartition(t, p string) *Partition {
	pc.mu.RLock()
	part, ok := pc.P[t+"/"+p]
	pc.mu.RUnlock()
	if ok {
		return part
	}
	return nil
}

func (ptc *PartitionsController) RegisterPart(t, p string, MaxEntries, MaxSize uint64) {
	ptc.mu.Lock()
	defer ptc.mu.Unlock()
	if MaxSize == -1 {
		MaxSize = defaultMaxSize
	}
	if MaxEntries == -1 {
		MaxEntries = defaultMaxEntries
	}
	ptc.P[t+"/"+p] = newPartition(t, p, MaxEntries, MaxSize, ptc.handleTimeout)
}
