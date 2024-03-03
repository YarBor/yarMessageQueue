package MqServer

import (
	"MqServer/ConsumerGroup"
	"MqServer/Err"
	Log "MqServer/Log"
	"MqServer/MessageMem"
	"MqServer/RaftServer"
	pb "MqServer/rpc"
	"context"
	"errors"
	"google.golang.org/grpc"
	"math"
	"sync"
	"sync/atomic"
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

type connections struct {
	mu             sync.RWMutex
	Conn           map[string]*ServerClient
	MetadataLeader *ServerClient
}

type brokerPeer struct {
	Client pb.MqServerCallClient
	Conn   *grpc.ClientConn
	ID     string
	Url    string
}

type broker struct {
	pb.UnimplementedMqServerCallServer
	RaftServer               *RaftServer.RaftServer
	Url                      string
	ID                       string
	Key                      string
	ProducerIDCache          sync.Map
	CacheStayTimeMs          int32
	MetadataLeaderID         atomic.Value // *string
	MetadataPeers            map[string]*brokerPeer
	MetaDataController       *MetaDataController
	PartitionsController     *PartitionsController
	IsStop                   bool
	wg                       sync.WaitGroup
	RequestTimeoutSessionsMs int32
}

func (bk *broker) CheckProducerTimeout() {
	for !bk.IsStop {
		now := time.Now().UnixMilli()
		keysToDelete := make([]interface{}, 0)

		bk.ProducerIDCache.Range(func(key, value interface{}) bool {
			t := value.(int64)
			if t <= now {
				keysToDelete = append(keysToDelete, key)
			}
			return true
		})

		for _, key := range keysToDelete {
			bk.ProducerIDCache.Delete(key)
		}
	}
}
func (bk *broker) Serve() error {
	err := bk.feasibilityTest()
	if err != nil {
		return err
	}
	err = bk.RaftServer.Serve()
	if err != nil {
		return err
	}
	if bk.MetaDataController != nil {
		bk.MetaDataController.MetaDataRaft.Start()
	}
	bk.wg.Add(1)
	return nil
}

func (b *broker) CancelReg2Cluster(consumer *ConsumerGroup.Consumer) {
}

type BrokerOption func(*broker) error

var MqCredentials *pb.Credentials

// for option call
func (b *broker) registerRaftNode() (err error) {
	if b.MetaDataController == nil {
		return errors.New(Err.ErrRequestIllegal)
	}
	ID_Url := []struct{ Url, ID string }{}
	for ID, peer := range b.MetadataPeers {
		ID_Url = append(ID_Url, struct{ Url, ID string }{Url: peer.Url, ID: ID})
	}
	b.MetaDataController.MetaDataRaft, err = b.RaftServer.RegisterMetadataRaft(ID_Url, b.MetaDataController, b.MetaDataController)
	return err
}

func newBroker(option ...BrokerOption) (*broker, error) {
	var err error
	bk := &broker{
		UnimplementedMqServerCallServer: pb.UnimplementedMqServerCallServer{},
		MetadataLeaderID:                atomic.Value{},
		MetadataPeers:                   nil,
		MetaDataController:              nil,
	}
	bk.MetadataLeaderID.Store(new(string))
	if err != nil {
		return nil, err
	}
	for _, brokerOption := range option {
		err = brokerOption(bk)
		if err != nil {
			return nil, err
		}
	}
	bk.RaftServer, err = RaftServer.MakeRaftServer()
	bk.PartitionsController = NewPartitionsController(bk)
	if err = bk.feasibilityTest(); err != nil {
		return nil, err
	}
	MqCredentials = &pb.Credentials{
		Identity: pb.Credentials_Broker,
		Id:       bk.ID,
		Key:      bk.Key,
	}
	return bk, nil
}
func (bk *broker) feasibilityTest() error {
	if bk.ID == "" || bk.Url == "" || bk.Key == "" || bk.MetadataPeers == nil {
		return errors.New(Err.ErrRequestIllegal)
	}
	return nil
}

var (
	defaultMaxEntries = uint64(math.MaxUint64)
	defaultMaxSize    = uint64(math.MaxUint64)
)

type PartitionsController struct {
	ttMu              sync.RWMutex
	TopicTerm         map[string]*int32
	cgMu              sync.RWMutex
	ConsumerGroupTerm map[string]*int32
	partsMu           sync.RWMutex
	P                 map[string]*map[string]*Partition // key: "Topic/Partition"
	handleTimeout     ConsumerGroup.SessionLogoutNotifier
}

func (p *PartitionsController) GetTopicTerm(id string) (int32, error) {
	p.ttMu.RLock()
	defer p.ttMu.RUnlock()
	i, ok := p.TopicTerm[id]
	if ok {
		return atomic.LoadInt32(i), nil
	} else {
		return -1, errors.New(Err.ErrSourceNotExist)
	}
}
func (p *PartitionsController) GetConsumerGroupTerm(id string) (int32, error) {
	p.cgMu.RLock()
	defer p.cgMu.RUnlock()
	i, ok := p.ConsumerGroupTerm[id]
	if ok {
		return atomic.LoadInt32(i), nil
	} else {
		return -1, errors.New(Err.ErrSourceNotExist)
	}
}
func (s *broker) GetMetadataServers() (func() *brokerPeer, func()) {
	i := -2
	peers := []*brokerPeer{}
	for _, peer := range s.MetadataPeers {
		peers = append(peers, peer)
	}
	return func() *brokerPeer {
			i++
			if i == -1 {
				if ID := s.MetadataLeaderID.Load().(*string); *ID != "" {
					if p, OK := s.MetadataPeers[*ID]; OK && p != nil {
						return p
					} else {
						i++
					}
				}
			}
			return peers[i%len(peers)]
		}, func() {
			if i != -1 {
				s.MetadataLeaderID.Store(&peers[i%len(peers)].ID)
			}
		}

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
	} else {
		err := s.MetaDataController.ConfirmIdentity(req.CheckIdentity)
		return &pb.ConfirmIdentityResponse{Response: ErrToResponse(err)}, nil
	}
}

// TODO complete PULL PUSH HEARTBEAT

// 拉取消息
func (s *broker) PullMessage(_ context.Context, req *pb.PullMessageRequest) (*pb.PullMessageResponse, error) {

	return nil, nil
}

func (s *broker) checkProducer(cxt context.Context, Credential *pb.Credentials) error {
	if Credential == nil {
		return errors.New(Err.ErrRequestIllegal)
	}
	if _, ok := s.ProducerIDCache.Load(Credential.Id); !ok {
		f, set := s.GetMetadataServers()
		for {
			select {
			case <-cxt.Done():
				return errors.New(Err.ErrRequestTimeout)
			default:
			}
			cc := f()
			res, err := cc.Client.ConfirmIdentity(cxt, &pb.ConfirmIdentityRequest{
				Self:          MqCredentials,
				CheckIdentity: Credential,
			})
			if err != nil {
				Log.ERROR("Call False:", err.Error())
			} else if res.Response.Mode != pb.Response_Success {
				set()
				break
			} else {
				Log.WARN(err.Error())
			}
		}
		s.ProducerIDCache.Store(Credential.Id, s.CacheStayTimeMs)
	}
	return nil
}

func (s *broker) CheckSourceTermCall(ctx context.Context, req *pb.CheckSourceTermRequest) (res *pb.CheckSourceTermResponse, err error) {
	for {
		select {
		case <-ctx.Done():
			return nil, errors.New(Err.ErrRequestTimeout)
		default:
		}
		f, set := s.GetMetadataServers()
		for {
			res, err = f().Client.CheckSourceTerm(context.Background(), req)
			if err != nil {
				Log.ERROR("Call False:", err.Error())
			} else {
				if res.Response.Mode == pb.Response_Success {
					set()
					return res, nil
				} else {
					Log.WARN("Call False:", res.Response)
				}
			}
		}
	}
}

func (bk *broker) UpdateTp(t string, req *pb.CheckSourceTermResponse) error {
	if req.Response.Mode != pb.Response_Success || req.TopicData == nil {
		return errors.New(Err.ErrRequestIllegal)
	}
	bk.PartitionsController.ttMu.RLock()
	i, ok := bk.PartitionsController.TopicTerm[t]
	bk.PartitionsController.ttMu.RUnlock()
	termNow := atomic.LoadInt32(i)
	if !ok {
		//return errors.New(Err.ErrSourceNotExist)
		if req.TopicData.FcParts[0].Topic != t {
			return errors.New(Err.ErrRequestIllegal)
		}
		i = new(int32)
		*i = -1
		bk.PartitionsController.ttMu.Lock()
		bk.PartitionsController.TopicTerm[t] = i
		bk.PartitionsController.ttMu.Unlock()

	} else if termNow >= req.TopicTerm {
		return nil
	}
	bk.PartitionsController.partsMu.Lock()
	if atomic.LoadInt32(i) >= req.TopicTerm {
		bk.PartitionsController.partsMu.Unlock()
		return nil
	} else {
		atomic.StoreInt32(i, req.TopicTerm)
	}
	if _, ok = bk.PartitionsController.P[t]; !ok {
		bk.PartitionsController.P[t] = new(map[string]*Partition)
	}
	NewP := map[string]*Partition{}
	var OldP *map[string]*Partition
	for _, part := range req.TopicData.FcParts {
		for _, brokerData := range part.Brokers {
			if brokerData.Id == MqCredentials.Id {
				goto Found
			}
		}
		continue
	Found:
		if OldP, ok = bk.PartitionsController.P[part.Topic]; !ok {
			panic("Ub")
		} else {
			if p, ok := (*OldP)[part.Part]; ok {
				delete(*OldP, part.Part)
				NewP[part.Part] = p
			} else {
				NewP[part.Part] = &Partition{
					T:                    part.Topic,
					P:                    part.Part,
					ConsumerGroupManager: ConsumerGroup.NewGroupsManager(bk),
					MessageEntry:         MessageMem.NewMessageEntry(defaultMaxEntries, defaultMaxSize),
				}
			}
		}
	}
	for _, partition := range *OldP {
		atomic.StoreInt32(&partition.Mode, Partition_Mode_ToDel)
	}
	bk.PartitionsController.P[t] = &NewP
	bk.PartitionsController.partsMu.Unlock()
	return nil
}

// 推送消息
func (s *broker) PushMessage(ctx context.Context, req *pb.PushMessageRequest) (*pb.PushMessageResponse, error) {
	// TODO:
	if req.Credential.Key != s.Key {
		return &pb.PushMessageResponse{Response: ResponseErrRequestIllegal()}, nil
	}
	err := s.checkProducer(ctx, req.Credential)
	if err != nil {
		return &pb.PushMessageResponse{Response: ErrToResponse(err)}, nil
	}
	term, err1 := s.PartitionsController.GetTopicTerm(req.Topic + "/" + req.Part)
	if err1 != nil || req.TopicTerm != term {
		res, err := s.CheckSourceTermCall(ctx, &pb.CheckSourceTermRequest{
			Self: MqCredentials,
			TopicData: &pb.CheckSourceTermRequest_TopicCheck{
				Topic:     req.Topic,
				TopicTerm: term,
			},
			ConsumerData: nil,
		})
		if err != nil {
			return nil, err
		}
		if res.TopicTerm == term {
			return &pb.PushMessageResponse{Response: ResponseErrRequestIllegal()}, nil
		} else {
			err = s.UpdateTp(req.Topic, res)
			if err != nil {
				return nil, err
			}
			NowTime := time.Now().UnixMilli()
			for _, id := range res.TopicData.FollowerProducerIDs.ID {
				s.ProducerIDCache.Store(id, NowTime)
			}
			if _, ok := s.ProducerIDCache.Load(req.Credential.Id); ok {
				return &pb.PushMessageResponse{Response: ResponseErrRequestIllegal()}, nil
			}
		}
	}
	p, err2 := s.PartitionsController.getPartition(req.Topic, req.Part)
	if err2 != nil {
		Log.ERROR(`Partition not found`)
		return &pb.PushMessageResponse{Response: ResponseErrSourceNotExist()}, nil
	}
	for _, message := range req.Msgs {
		p.MessageEntry.Write(message.Message)
	}
	return &pb.PushMessageResponse{Response: ResponseSuccess()}, nil
}

func (s *broker) Heartbeat(_ context.Context, req *pb.MQHeartBeatData) (*pb.HeartBeatResponseData, error) {
	// TODO:
	switch req.BrokerData.Identity {
	case pb.Credentials_Broker:
		if s.MetaDataController == nil {
			return &pb.HeartBeatResponseData{
				Response: ResponseErrSourceNotExist(),
			}, nil
		} else {
			ret := &pb.HeartBeatResponseData{
				Response:             ResponseSuccess(),
				ChangedTopic:         nil,
				ChangedConsumerGroup: nil,
			}
			var err error
			if req.CheckTopic != nil {
				ret.ChangedTopic = &pb.HeartBeatResponseDataTpKv{}
				ret.ChangedTopic.TopicTerm, err = s.MetaDataController.GetTopicTermDiff(req.CheckTopic.TopicTerm)
				if err != nil {
					return &pb.HeartBeatResponseData{Response: ErrToResponse(err)}, nil
				}
			}
			if req.CheckConsumerGroup != nil {
				ret.ChangedConsumerGroup = &pb.HeartBeatResponseDataCgKv{}
				ret.ChangedConsumerGroup.ChangedGroup, err = s.MetaDataController.GetConsumerGroupTermDiff(req.CheckConsumerGroup.ConsumerGroup)
				if err != nil {
					return &pb.HeartBeatResponseData{Response: ErrToResponse(err)}, nil
				}
			}
			return ret, nil
		}
	default:
		return &pb.HeartBeatResponseData{
			Response: ResponseErrRequestIllegal(),
		}, nil
	}
}

var (
	Partition_Mode_ToDel  = int32(1)
	Partition_Mode_Normal = int32(0)
)

type Partition struct {
	wg                   sync.WaitGroup
	Mode                 int32
	T                    string
	P                    string
	ConsumerGroupManager *ConsumerGroup.GroupsManager
	MessageEntry         *MessageMem.MessageEntry
}

func newPartition(t, p string, MaxEntries, MaxSize uint64, handleTimeout ConsumerGroup.SessionLogoutNotifier) *Partition {
	res := ConsumerGroup.NewGroupsManager(handleTimeout)
	part := &Partition{
		Mode:                 Partition_Mode_Normal,
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
		P:             make(map[string]*map[string]*Partition),
		handleTimeout: handleTimeout,
	}
}

func (pc *PartitionsController) getPartition(t, p string) (*Partition, error) {
	pc.partsMu.RLock()
	parts, ok := pc.P[t]
	pc.partsMu.RUnlock()
	if ok {
		part, ok := (*parts)[p]
		if ok {
			return part, nil
		}
	}
	return nil, errors.New(Err.ErrSourceNotExist)
}

func (ptc *PartitionsController) RegisterPart(t, p string, MaxEntries, MaxSize uint64) {
	ptc.partsMu.Lock()
	defer ptc.partsMu.Unlock()
	if MaxSize == -1 {
		MaxSize = defaultMaxSize
	}
	if MaxEntries == -1 {
		MaxEntries = defaultMaxEntries
	}
	if _, ok := ptc.P[t]; ok {
	} else {
		ptc.P[t] = new(map[string]*Partition)
	}
	_, ok := (*ptc.P[t])[p]
	if !ok {
		(*ptc.P[t])[p] = newPartition(t, p, MaxEntries, MaxSize, ptc.handleTimeout)
	}
}
