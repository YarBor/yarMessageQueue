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
	HeartBeatSessionsMs      int32
}

func (bk *broker) CheckProducerTimeout() {
	defer bk.wg.Done()
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

func (bk *broker) goSendHeartbeat(ch chan *pb.CheckSourceTermRequest) {
	defer bk.wg.Done()
	for !bk.IsStop {
		for ID, peer := range bk.MetadataPeers {
			for {
				if !bk.IsStop {
					return
				}
				req := pb.MQHeartBeatData{
					BrokerData:         MqCredentials,
					CheckTopic:         &pb.MQHeartBeatDataTpKv{TopicTerm: make(map[string]int32)},
					CheckConsumerGroup: &pb.MQHeartBeatDataCgKv{ConsumerGroup: make(map[string]int32)},
				}
				bk.PartitionsController.cgMu.RLock()
				for s, i := range bk.PartitionsController.ConsumerGroupTerm {
					req.CheckConsumerGroup.ConsumerGroup[s] = atomic.LoadInt32(i)
				}
				bk.PartitionsController.cgMu.RUnlock()
				bk.PartitionsController.ttMu.RLock()
				for s, i := range bk.PartitionsController.TopicTerm {
					req.CheckTopic.TopicTerm[s] = atomic.LoadInt32(i)
				}
				bk.PartitionsController.ttMu.RUnlock()
				res, err := peer.Client.Heartbeat(context.Background(), &req)
				if err != nil {
					goto next
				}
				if res.Response.Mode != pb.Response_Success {
					if res.ChangedTopic != nil {
						bk.wg.Add(1)
						go func() {
							defer bk.wg.Done()
							for Tp, _ := range res.ChangedTopic.TopicTerm {
								ch <- &pb.CheckSourceTermRequest{
									Self: MqCredentials,
									TopicData: &pb.CheckSourceTermRequest_TopicCheck{
										Topic:     Tp,
										TopicTerm: req.CheckTopic.TopicTerm[Tp],
									},
									ConsumerData: nil,
								}
							}
						}()
					}
				}
				time.Sleep(time.Duration(bk.RequestTimeoutSessionsMs) * time.Millisecond)
				bk.MetadataLeaderID.Store(&ID)
			}

		next:
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
	ch := make(chan *pb.CheckSourceTermRequest)
	bk.wg.Add(3)
	go bk.CheckProducerTimeout()
	go bk.goSendHeartbeat(ch)
	go bk.keepUpdates(ch)
	return nil
}

func (b *broker) CancelReg2Cluster(consumer *ConsumerGroup.Consumer) {
	f, s := b.GetMetadataServers()
	for {
		cc := f()
		res, err := cc.Client.ConsumerDisConnect(context.Background(), &pb.DisConnectInfo{
			BrokerInfo: MqCredentials,
			TargetInfo: &pb.Credentials{
				Identity: pb.Credentials_Consumer,
				Id:       consumer.SelfId,
				Key:      b.Key,
			},
		})
		if err != nil || res.Mode != pb.Response_Success {
			Log.WARN("CancelReg2Cluster Call False")
			continue
		} else {
			s()
			return
		}
	}
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
func (bk *broker) UpdateCg(CId, CgId string, req *pb.CheckSourceTermResponse) error {
	if req.Response.Mode != pb.Response_Success || req.ConsumersData == nil || req.ConsumerTimeoutSession == nil || req.ConsumerMaxReturnMessageSize == nil {
		return errors.New(Err.ErrRequestIllegal)
	}
	bk.PartitionsController.cgMu.RLock()
	term, ok := bk.PartitionsController.ConsumerGroupTerm[CgId]
	bk.PartitionsController.cgMu.RUnlock()

	if !ok {
		for _, part := range req.ConsumersData.FcParts {
			for _, brokerData := range part.Brokers {
				if brokerData.Id == bk.ID {
					goto success
				}
			}
		}
		return errors.New(Err.ErrSourceNotExist)
	success:
		term = new(int32)
		*term = -1
		bk.PartitionsController.cgMu.Lock()
		if _, ok = bk.PartitionsController.ConsumerGroupTerm[CgId]; ok {
			bk.PartitionsController.cgMu.Unlock()
			return errors.New(Err.ErrSourceAlreadyExist)
		} else {
			bk.PartitionsController.ConsumerGroupTerm[CgId] = term
		}
		bk.PartitionsController.cgMu.Unlock()
	} else if atomic.LoadInt32(term) >= req.GroupTerm {
		return nil
	}

	bk.PartitionsController.partsMu.Lock()
	defer bk.PartitionsController.partsMu.Unlock()
	if atomic.LoadInt32(term) >= req.GroupTerm {
		return nil
	} else {
		// 更新 term 值
		atomic.StoreInt32(term, req.TopicTerm)
	}
	for _, part := range req.ConsumersData.FcParts {
		for _, brokerData := range part.Brokers {
			if brokerData.Id == bk.ID {
				p := bk.PartitionsController.GetPart(part.Topic, part.Part)
				if p == nil {
					p, _ = bk.PartitionsController.RegisterPart(part.Topic, part.Part, defaultMaxEntries, defaultMaxSize)
				}
				g, err := p.ConsumerGroupManager.GetConsumerGroup(CgId)
				if err != nil {
					g, err = p.ConsumerGroupManager.RegisterConsumerGroup(&ConsumerGroup.ConsumerGroup{
						GroupId:         CgId,
						GroupTerm:       req.GroupTerm,
						Consumers:       ConsumerGroup.NewConsumer(CId, CgId, *req.ConsumerTimeoutSession, *req.ConsumerMaxReturnMessageSize),
						ConsumeOffset:   0,
						LastConsumeData: nil,
					})
				} else {
					g.Consumers = ConsumerGroup.NewConsumer(CId, CgId, *req.ConsumerTimeoutSession, *req.ConsumerMaxReturnMessageSize)
				}
			}
		}
	}
	return nil
}
func (bk *broker) UpdateTp(t string, UpdateReq *pb.CheckSourceTermResponse) error {
	// 检查响应模式是否为成功，以及 TopicData 是否为 nil
	if UpdateReq.Response.Mode != pb.Response_Success || UpdateReq.TopicData == nil || UpdateReq.TopicData.FcParts == nil {
		return errors.New(Err.ErrRequestIllegal)
	}

	// 获取 TopicTerm 锁，读取当前的 term 值
	bk.PartitionsController.ttMu.RLock()
	term, ok := bk.PartitionsController.TopicTerm[t]
	bk.PartitionsController.ttMu.RUnlock()

	//termNow :=
	if !ok {
		for _, part := range UpdateReq.TopicData.FcParts {
			for _, bkData := range part.Brokers {
				if bkData.Id == bk.ID {
					goto Success
				}
			}
		}
		return errors.New(Err.ErrRequestIllegal)
	Success:
		// 创建一个新的 int32 指针，并将其值设置为 -1
		term = new(int32)
		*term = -1

		// 获取 TopicTerm 锁，将新的 int32 指针添加到 TopicTerm 中
		bk.PartitionsController.ttMu.Lock()
		if _, ok := bk.PartitionsController.TopicTerm[t]; ok {
			bk.PartitionsController.ttMu.Unlock()
			return errors.New(Err.ErrSourceAlreadyExist)
		} else {
			bk.PartitionsController.TopicTerm[t] = term
		}
		bk.PartitionsController.ttMu.Unlock()
	} else if atomic.LoadInt32(term) >= UpdateReq.TopicTerm {
		// 如果当前的 term 值大于等于请求的 TopicTerm，则不进行更新
		return nil
	}

	// 获取 partitions 锁
	bk.PartitionsController.partsMu.Lock()
	defer bk.PartitionsController.partsMu.Unlock()

	if atomic.LoadInt32(term) >= UpdateReq.TopicTerm {
		// 如果当前的 term 值大于等于请求的 TopicTerm，则不进行更新
		return nil
	} else {
		// 更新 term 值
		atomic.StoreInt32(term, UpdateReq.TopicTerm)
	}

	// 检查 P 中是否存在 t，如果不存在，则添加一个新的 map[string]*Partition
	if _, ok = bk.PartitionsController.P[t]; !ok {
		bk.PartitionsController.P[t] = new(map[string]*Partition)
	}

	NewP := map[string]*Partition{}
	var OldP *map[string]*Partition

	// 遍历请求的 TopicData 中的分区和 broker 数据
	for _, part := range UpdateReq.TopicData.FcParts {
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
				// 如果旧的 Partition 存在，则将其从 OldP 中删除，并添加到 NewP 中
				delete(*OldP, part.Part)
				NewP[part.Part] = p
			} else {
				// 如果旧的 Partition 不存在，则创建一个新的 Partition 并添加到 NewP 中
				NewP[part.Part] = &Partition{
					T:                    part.Topic,
					P:                    part.Part,
					ConsumerGroupManager: ConsumerGroup.NewGroupsManager(bk),
					MessageEntry:         MessageMem.NewMessageEntry(defaultMaxEntries, defaultMaxSize),
				}
			}
		}
	}

	// 将 OldP 中剩余的 Partition 的 Mode 设置为 Partition_Mode_ToDel
	for k, partition := range *OldP {
		atomic.StoreInt32(&partition.Mode, Partition_Mode_ToDel)
		NewP[k] = partition
	}

	// 将 NewP 赋值给 P[t]
	bk.PartitionsController.P[t] = &NewP

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
	termSelf, GetTopicTermErr := s.PartitionsController.GetTopicTerm(req.Topic)
	res, CheckSrcTermErr := s.CheckSourceTermCall(ctx, &pb.CheckSourceTermRequest{Self: MqCredentials, TopicData: &pb.CheckSourceTermRequest_TopicCheck{Topic: req.Topic, TopicTerm: termSelf}, ConsumerData: nil})
	if CheckSrcTermErr != nil {
		return &pb.PushMessageResponse{Response: ErrToResponse(CheckSrcTermErr)}, nil
	}
	if res.TopicTerm != termSelf {
		err = s.UpdateTp(req.Topic, res)
		if err != nil {
			return &pb.PushMessageResponse{Response: ErrToResponse(err)}, nil
		} else {
			NowTime := time.Now().UnixMilli() + int64(2*s.CacheStayTimeMs)
			for _, id := range res.TopicData.FollowerProducerIDs.ID {
				s.ProducerIDCache.Store(id, NowTime)
			}
		}
		termSelf, GetTopicTermErr = s.PartitionsController.GetTopicTerm(req.Topic)
	}
	if GetTopicTermErr != nil {
		return &pb.PushMessageResponse{Response: ErrToResponse(err)}, nil
	} else if req.TopicTerm < termSelf {
		return &pb.PushMessageResponse{Response: ResponseErrPartitionChanged()}, nil
	} else if req.TopicTerm > termSelf {
		return &pb.PushMessageResponse{Response: ResponseErrRequestIllegal()}, nil //
	} else {
		s.ProducerIDCache.Store(req.Credential.Id, time.Now().UnixMilli()+int64(2*s.CacheStayTimeMs))
	}
	p, GetPartErr := s.PartitionsController.getPartition(req.Topic, req.Part)
	if GetPartErr != nil {
		Log.ERROR(`Partition not found`)
		return &pb.PushMessageResponse{Response: ErrToResponse(err)}, nil
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
			err = s.MetaDataController.KeepBrokersAlive(req.BrokerData.Id)
			if err != nil {
				return &pb.HeartBeatResponseData{Response: ErrToResponse(err)}, nil
			}
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

func (bk *broker) keepUpdates(ch chan *pb.CheckSourceTermRequest) {
	defer bk.wg.Done()
	for {
		select {
		case <-ch:
		default:
			if bk.IsStop {
				return
			}
			time.Sleep(time.Millisecond * 50)
		}
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

func (p *Partition) registerConsumerGroup(groupId string, maxReturnMessageSize int32) (*ConsumerGroup.ConsumerGroup, error) {
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

func (ptc *PartitionsController) RegisterPart(t, p string, MaxEntries, MaxSize uint64) (part *Partition, err error) {
	ptc.partsMu.Lock()
	defer ptc.partsMu.Unlock()
	if MaxSize == -1 {
		MaxSize = defaultMaxSize
	}
	if MaxEntries == -1 {
		MaxEntries = defaultMaxEntries
	}
	ok := false
	if _, ok = ptc.P[t]; ok {
	} else {
		ptc.P[t] = new(map[string]*Partition)
	}
	part, ok = (*ptc.P[t])[p]
	if !ok {
		part = newPartition(t, p, MaxEntries, MaxSize, ptc.handleTimeout)
		(*ptc.P[t])[p] = part
	}
	return part, nil
}

func (ptc *PartitionsController) GetPart(t, p string) (part *Partition) {
	ptc.partsMu.Lock()
	defer ptc.partsMu.Unlock()
	ok := false
	if _, ok = ptc.P[t]; ok {
	} else {
		return nil
	}
	part, ok = (*ptc.P[t])[p]
	if !ok {
		return nil
	}
	return part
}
