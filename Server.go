package MqServer

import (
	"MqServer/ConsumerGroup"
	"MqServer/Err"
	Log "MqServer/Log"
	"MqServer/RaftServer"
	pb "MqServer/api"
	"context"
	"google.golang.org/grpc"
	"math"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

type Server interface {
	Serve() error
	Stop() error
}

//type ServerClient struct {
//	pb.RaftCallClient
//	Conn *grpc.ClientConn
//}

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

func (s *broker) CheckProducerTimeout() {
	defer s.wg.Done()
	for !s.IsStop {
		now := time.Now().UnixMilli()
		keysToDelete := make([]interface{}, 0)

		s.ProducerIDCache.Range(func(key, value interface{}) bool {
			t := value.(int64)
			if t <= now {
				keysToDelete = append(keysToDelete, key)
			}
			return true
		})

		for _, key := range keysToDelete {
			s.ProducerIDCache.Delete(key)
		}
	}
}

func MakeMqServer(opt ...BrokerOption) (Server, error) {
	i, err := newBroker(opt...)
	s := Server(i)
	return s, err
}

func (s *broker) goSendHeartbeat() {
	defer s.wg.Done()
	for !s.IsStop {
		for ID, peer := range s.MetadataPeers {
			for {
				if !s.IsStop {
					return
				}
				req := pb.MQHeartBeatData{
					BrokerData:         MqCredentials,
					CheckTopic:         &pb.MQHeartBeatDataTpKv{TopicTerm: make(map[string]int32)},
					CheckConsumerGroup: &pb.MQHeartBeatDataCgKv{ConsumerGroup: make(map[string]int32)},
				}
				//bk.PartitionsController.cgtMu.RLock()
				//for s, i := range bk.PartitionsController.ConsumerGroupTerm {
				//	req.CheckConsumerGroup.ConsumerGroup[s] = atomic.LoadInt32(i)
				//}
				//bk.PartitionsController.cgtMu.RUnlock()
				//bk.PartitionsController.ttMu.RLock()
				//for s, i := range bk.PartitionsController.TopicTerm {
				//	req.CheckTopic.TopicTerm[s] = atomic.LoadInt32(i)
				//}
				data := s.PartitionsController.GetAllPart()
				for _, partition := range data {
					//bk.PartitionsController.GetPart()
					if partition.IsLeader() {
						if _, ok := req.CheckTopic.TopicTerm[partition.T]; !ok {
							t, err := s.PartitionsController.GetTopicTerm(partition.T)
							if err != nil {
								Log.DEBUG("Get Partiton Term False ", partition.T)
								continue
							}
							req.CheckTopic.TopicTerm[partition.T] = t
						}
						cgs := partition.GetAllConsumerGroup()
						for _, cg := range cgs {
							t, err := s.PartitionsController.GetConsumerGroupTerm(cg.GroupId)
							if err != nil {
								Log.DEBUG("Get Partiton Term False ", partition.T)
								continue
							}
							req.CheckConsumerGroup.ConsumerGroup[cg.GroupId] = t
						}
					}
				}
				s.PartitionsController.ttMu.RUnlock()
				res, err := peer.Client.Heartbeat(context.Background(), &req)
				if err != nil {
					goto next
				} else {
					s.MetadataLeaderID.Store(&ID)
				}
				if res.Response.Mode != pb.Response_Success {
					if res.ChangedTopic != nil {
						for Tp, _ := range res.ChangedTopic.TopicTerm {
							tp := Tp
							s.wg.Add(1)
							go func() {
								defer s.wg.Done()
								res, err := s.CheckSourceTermCall(context.Background(), &pb.CheckSourceTermRequest{Self: MqCredentials, TopicData: &pb.CheckSourceTermRequest_TopicCheck{Topic: tp, TopicTerm: -1}, ConsumerData: nil})
								if err == nil {
									err = s.PartitionsController.UpdateTp(tp, res)
									if err != nil {
										Log.ERROR("Heart Beat Get Tp Diff Success Check Success UpdateFalse ", res)
									}
								} else {
									Log.ERROR("Heart beat Get Diff But Check Err ", err)
								}
							}()
						}
					} else if res.ChangedConsumerGroup != nil {
						for _groupID, _ := range res.ChangedConsumerGroup.ChangedGroup {
							groupID := _groupID
							s.wg.Add(1)
							go func() {
								defer s.wg.Done()
								res, err := s.CheckSourceTermCall(context.Background(), &pb.CheckSourceTermRequest{
									Self:      MqCredentials,
									TopicData: nil,
									ConsumerData: &pb.CheckSourceTermRequest_ConsumerCheck{
										ConsumerId: nil,
										GroupID:    groupID,
										GroupTerm:  -1,
									},
								})
								if err == nil {
									err = s.PartitionsController.UpdateCg(groupID, res)
									if err != nil {
										Log.ERROR("Heart Beat Get Cg Diff Success Check Success UpdateFalse ", res)
									}
								} else {
									Log.ERROR("Heart beat Get Cg Diff But Check Err ", err)
								}
							}()
						}
					}
				}
				time.Sleep(time.Duration(s.RequestTimeoutSessionsMs) * time.Millisecond)
			}

		next:
		}
	}
}

func (s *broker) Serve() error {
	err := s.feasibilityTest()
	if err != nil {
		return err
	}
	err = s.RaftServer.Serve()
	if err != nil {
		return err
	}
	if s.MetaDataController != nil {
		s.MetaDataController.MetaDataRaft.Start()
	}
	s.wg.Add(3)
	go s.CheckProducerTimeout()
	go s.goSendHeartbeat()
	//go bk.keepUpdates(ch)
	return nil
}

func (s *broker) Stop() error {
	s.IsStop = true
	if s.MetaDataController != nil {
		s.MetaDataController.Stop()
	}
	s.PartitionsController.Stop()
	s.RaftServer.Stop()
	s.wg.Wait()
	return nil
}

func (s *broker) CancelReg2Cluster(consumer *ConsumerGroup.Consumer) {
	f, set := s.GetMetadataServers()
	for {
		cc := f()
		res, err := cc.Client.ConsumerDisConnect(context.Background(), &pb.DisConnectInfo{
			BrokerInfo: MqCredentials,
			TargetInfo: &pb.Credentials{
				Identity: pb.Credentials_Consumer,
				Id:       consumer.SelfId,
				Key:      s.Key,
			},
		})
		if err != nil || res.Mode != pb.Response_Success {
			Log.WARN("CancelReg2Cluster Call False")
			continue
		} else {
			set()
			return
		}
	}
}

var MqCredentials *pb.Credentials

// for option call
func (s *broker) registerRaftNode() (err error) {
	if s.MetaDataController == nil {
		return (Err.ErrRequestIllegal)
	}
	ID_Url := []struct{ ID, Url string }{}
	for ID, peer := range s.MetadataPeers {
		ID_Url = append(ID_Url, struct{ ID, Url string }{Url: peer.Url, ID: ID})
	}
	s.MetaDataController.MetaDataRaft, err = s.RaftServer.RegisterMetadataRaft(ID_Url, s.MetaDataController, s.MetaDataController)
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
	bk.PartitionsController = NewPartitionsController(bk.RaftServer, bk)
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
func (s *broker) feasibilityTest() error {
	if s.ID == "" || s.Url == "" || s.Key == "" || s.MetadataPeers == nil {
		return (Err.ErrRequestIllegal)
	}
	return nil
}

var (
	defaultMaxEntries = uint64(math.MaxUint64)
	defaultMaxSize    = uint64(math.MaxUint64)
)

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
		return nil, (Err.ErrRequestIllegal)
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
		return nil, (Err.ErrRequestIllegal)
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
		return nil, (Err.ErrRequestIllegal)
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

func ResponseErrNeedToWait() *pb.Response {
	return &pb.Response{Mode: pb.Response_ErrNeedToWait}
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
func (s *broker) PullMessage(ctx context.Context, req *pb.PullMessageRequest) (*pb.PullMessageResponse, error) {
	if req.Group == nil || req.Self == nil || req.Group.Key != s.Key || req.Self.Key != s.Key {
		return &pb.PullMessageResponse{Response: ResponseErrRequestIllegal(), Msgs: nil}, nil
	}
	s.PartitionsController.cgtMu.RLock()
	gt, ok := s.PartitionsController.ConsumerGroupTerm[req.Group.Id]
	s.PartitionsController.cgtMu.RUnlock()
	var term int32
	if ok {
		term = atomic.LoadInt32(gt)
	} else {
		term = -1
	}
	// term check
	if term < req.GroupTerm {
		res, err := s.CheckSourceTermCall(ctx, &pb.CheckSourceTermRequest{
			Self:      MqCredentials,
			TopicData: nil,
			ConsumerData: &pb.CheckSourceTermRequest_ConsumerCheck{
				ConsumerId: &req.Self.Id,
				GroupID:    req.Group.Id,
				GroupTerm:  term,
			},
		})
		if err != nil {
			return &pb.PullMessageResponse{Response: ErrToResponse(err)}, nil
		} else {
			err = s.PartitionsController.UpdateCg(req.Group.Id, res)
			if err != nil {
				return &pb.PullMessageResponse{Response: ErrToResponse(err)}, nil
			}
			// Go Recheck
			s.PartitionsController.cgtMu.RLock()
			gt, ok = s.PartitionsController.ConsumerGroupTerm[req.Group.Id]
			s.PartitionsController.cgtMu.RUnlock()
			if !ok {
				return &pb.PullMessageResponse{Response: ResponseErrSourceNotExist()}, nil
			} else if atomic.LoadInt32(gt) > req.GroupTerm {
				return &pb.PullMessageResponse{Response: ResponseErrPartitionChanged()}, nil
			}
		}
	} else if term > req.GroupTerm {
		return &pb.PullMessageResponse{Response: ResponseErrPartitionChanged()}, nil
	}

	// part commit And read
	p, err := s.PartitionsController.GetPart(req.Topic, req.Part)
	if err != nil {
		return &pb.PullMessageResponse{Response: ResponseErrSourceNotExist()}, nil
	}
	//s.RaftServer. // TODO Raft
	data, ReadBeginOffset, _, IsAllow2Del, errRead := p.Read(req.Self.Id, req.Group.Id, req.LastTimeOffset, req.ReadEntryNum)
	if errRead != nil {
		return &pb.PullMessageResponse{
			Response: ErrToResponse(errRead),
		}, nil
	} else {
		return &pb.PullMessageResponse{
			Response:      ResponseSuccess(),
			MessageOffset: ReadBeginOffset,
			Msgs:          &pb.Message{Message: data},
			IsCouldToDel:  IsAllow2Del,
		}, nil
	}

}
func (s *broker) checkProducer(cxt context.Context, Credential *pb.Credentials) error {
	if Credential == nil {
		return Err.ErrRequestIllegal
	}
	if _, ok := s.ProducerIDCache.Load(Credential.Id); !ok {
		f, set := s.GetMetadataServers()
		for {
			select {
			case <-cxt.Done():
				return Err.ErrRequestTimeout
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
		s.ProducerIDCache.Store(Credential.Id, time.Now().UnixMilli()+int64(s.CacheStayTimeMs))
	}
	return nil
}

func (s *broker) CheckSourceTermCall(ctx context.Context, req *pb.CheckSourceTermRequest) (res *pb.CheckSourceTermResponse, err error) {
	for {
		select {
		case <-ctx.Done():
			return nil, Err.ErrRequestTimeout
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

func (p *PartitionsController) UpdateCg(CgId string, req *pb.CheckSourceTermResponse) error {
	if req.Response.Mode != pb.Response_Success || req.ConsumersData == nil {
		return Err.ErrRequestIllegal
	}
	p.cgtMu.RLock()
	term, ok := p.ConsumerGroupTerm[CgId]
	p.cgtMu.RUnlock()

	if !ok {
		for _, part := range req.ConsumersData.FcParts {
			for _, brokerData := range part.Part.Brokers {
				if brokerData.Id == MqCredentials.Id {
					goto success
				}
			}
		}
		return Err.ErrSourceNotExist
	success:
		term = new(int32)
		*term = -1
		p.cgtMu.Lock()
		if _, ok = p.ConsumerGroupTerm[CgId]; ok {
			p.cgtMu.Unlock()
			return Err.ErrSourceAlreadyExist
		} else {
			p.ConsumerGroupTerm[CgId] = term
		}
		p.cgtMu.Unlock()
	} else if atomic.LoadInt32(term) >= req.GroupTerm {
		return nil
	}
	isSetTerm := true
	p.partsMu.Lock()
	defer p.partsMu.Unlock()
	if atomic.LoadInt32(term) >= req.GroupTerm {
		return nil
	} else {
		// 更新 term 值
		defer func(Is *bool) {
			if isSetTerm {
				atomic.StoreInt32(term, req.TopicTerm)
			}
		}(&isSetTerm)
	}
	for _, part := range req.ConsumersData.FcParts {
		for _, brokerData := range part.Part.Brokers {
			if brokerData.Id == MqCredentials.Id {
				getPart, _ := p.GetPart(part.Part.Topic, part.Part.PartName)
				if getPart == nil {
					peers := []struct{ ID, Url string }{}
					for _, data := range part.Part.Brokers {
						peers = append(peers, struct{ ID, Url string }{data.Id, data.Url})
					}
					getPart, _ = p.RegisterPart(part.Part.Topic, part.Part.PartName, defaultMaxEntries, defaultMaxSize, peers...)
				}
				g, err := getPart.ConsumerGroupManager.GetConsumerGroup(CgId)
				if err != nil {
					var Off int64
					switch *req.ConsumerGroupOption {
					case pb.RegisterConsumerGroupRequest_Earliest:
						Off = getPart.MessageEntry.GetBeginOffset()
					case pb.RegisterConsumerGroupRequest_Latest:
						Off = getPart.MessageEntry.GetEndOffset()
					}
					g, err = getPart.registerConsumerGroup(CgId, ConsumerGroup.NewConsumer(*part.ConsumerID,
						CgId, *part.ConsumerTimeoutSession, *part.ConsumerMaxReturnMessageSize, *part.ConsumerMaxReturnMessageEntries), Off)
					if err != nil {
						isSetTerm = false
					}
				} else {
					if !g.CheckConsumer(*part.ConsumerID) {
						err = getPart.UpdateGroupConsumer(g.GroupId, ConsumerGroup.NewConsumer(*part.ConsumerID, CgId, *part.ConsumerTimeoutSession, *part.ConsumerMaxReturnMessageSize, *part.ConsumerMaxReturnMessageEntries))
						if err != nil {
							Log.ERROR("updateGroupConsumer False", err.Error())
						}
					}
				}
				break
			}
		}
	}
	return nil
}

func (p *PartitionsController) UpdateTp(t string, UpdateReq *pb.CheckSourceTermResponse) error {
	// 检查响应模式是否为成功，以及 TopicData 是否为 nil
	if UpdateReq.Response.Mode != pb.Response_Success || UpdateReq.TopicData == nil || UpdateReq.TopicData.FcParts == nil {
		return Err.ErrRequestIllegal
	}

	// 获取 TopicTerm 锁，读取当前的 term 值
	p.ttMu.RLock()
	term, ok := p.TopicTerm[t]
	p.ttMu.RUnlock()

	//termNow :=
	if !ok {
		for _, part := range UpdateReq.TopicData.FcParts {
			for _, bkData := range part.Part.Brokers {
				if bkData.Id == MqCredentials.Id {
					goto Success
				}
			}
		}
		return Err.ErrRequestIllegal
	Success:
		// 创建一个新的 int32 指针，并将其值设置为 -1
		term = new(int32)
		*term = -1

		// 获取 TopicTerm 锁，将新的 int32 指针添加到 TopicTerm 中
		p.ttMu.Lock()
		if _, ok := p.TopicTerm[t]; ok {
			p.ttMu.Unlock()
			return Err.ErrSourceAlreadyExist
		} else {
			p.TopicTerm[t] = term
		}
		p.ttMu.Unlock()
	} else if atomic.LoadInt32(term) >= UpdateReq.TopicTerm {
		// 如果当前的 term 值大于等于请求的 TopicTerm，则不进行更新
		return nil
	}

	// 获取 partitions 锁
	p.partsMu.Lock()
	defer p.partsMu.Unlock()

	IsSetNewTerm := true
	if atomic.LoadInt32(term) >= UpdateReq.TopicTerm {
		// 如果当前的 term 值大于等于请求的 TopicTerm，则不进行更新
		return nil
	} else {
		// 更新 term 值
		defer func(bool2 *bool) {
			if *bool2 {
				atomic.StoreInt32(term, UpdateReq.TopicTerm)
			}
		}(&IsSetNewTerm)
	}

	// 检查 P 中是否存在 t，如果不存在，则添加一个新的 map[string]*Partition
	if _, ok = p.P[t]; !ok {
		p.P[t] = new(map[string]*Partition)
	}

	NewP := map[string]*Partition{}
	var OldP *map[string]*Partition

	// 遍历请求的 TopicData 中的分区和 broker 数据
	for _, part := range UpdateReq.TopicData.FcParts {
		for _, brokerData := range part.Part.Brokers {
			if brokerData.Id == MqCredentials.Id {
				goto Found
			}
		}
		continue
	Found:
		if OldP, ok = p.P[part.Part.Topic]; !ok {
			panic("Ub")
		} else {
			if GetP, ok := (*OldP)[part.Part.PartName]; ok {
				// 如果旧的 Partition 存在，则将其从 OldP 中删除，并添加到 NewP 中
				NewP[part.Part.PartName] = GetP
				delete(*OldP, part.Part.PartName)
			} else {
				peers := []struct{ ID, Url string }{}
				for _, data := range part.Part.Brokers {
					peers = append(peers, struct{ ID, Url string }{ID: data.Id, Url: data.Url})
				}
				// 如果旧的 Partition 不存在，则创建一个新的 Partition 并添加到 NewP 中
				var err error
				NewP[part.Part.PartName], err = newPartition(part.Part.Topic, part.Part.PartName, defaultMaxEntries, defaultMaxSize, p.handleTimeout, p.rfServer, peers...)
				if err != nil {
					Log.ERROR("createPartition failed during updatePartition")
				}
			}
		}
	}

	// 将 OldP 中剩余的 Partition 的 Mode 设置为 Partition_Mode_ToDel
	for k, partition := range *OldP {
		if partition.Node.IsLeader() {
			err := partition.PartToDel()
			Log.ERROR(err.Error())
		}
		NewP[k] = partition
	}

	sort.Strings(UpdateReq.TopicData.FollowerConsumerGroupIDs.ID)
	toDelPart := []*Partition{}
	for _, partition := range NewP {
		groups := partition.GetAllConsumerGroup()
		toDelGroup := []*ConsumerGroup.ConsumerGroup{}
		for _, group := range groups {
			index := sort.SearchStrings(UpdateReq.TopicData.FollowerConsumerGroupIDs.ID, group.GroupId)
			if UpdateReq.TopicData.FollowerConsumerGroupIDs.ID[index] != group.GroupId {
				toDelGroup = append(toDelGroup, group)
			}
		}
		for _, todel_group := range toDelGroup {
			_ = partition.ConsumerGroupManager.UnregisterConsumerGroup(todel_group.GroupId)
		}
		if partition.CheckToDel() {
			toDelPart = append(toDelPart, partition)
		}
	}
	for _, ToDelPartition := range toDelPart {
		delete(NewP, ToDelPartition.P)
	}
	// 将 NewP 赋值给 P[t]
	p.P[t] = &NewP
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
		err = s.PartitionsController.UpdateTp(req.Topic, res)
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
	p, GetPartErr := s.PartitionsController.GetPart(req.Topic, req.Part)
	if GetPartErr != nil {
		Log.ERROR(`Partition not found`)
		return &pb.PushMessageResponse{Response: ErrToResponse(err)}, nil
	}
	err = p.Write(req.Msgs.Message)
	if err != nil {
		return &pb.PushMessageResponse{Response: ErrToResponse(err)}, nil
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
