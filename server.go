package MqServer

import (
	"context"
	"errors"
	"net"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

import (
	"google.golang.org/grpc"
)

import (
	"github.com/YarBor/BorsMqServer/api"
	"github.com/YarBor/BorsMqServer/common"
	"github.com/YarBor/BorsMqServer/consumer_group"
	err_ "github.com/YarBor/BorsMqServer/error"
	Log "github.com/YarBor/BorsMqServer/logger"
	"github.com/YarBor/BorsMqServer/raft_server"
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
	Client api.MqServerCallClient
	Conn   *grpc.ClientConn
	ID     string
	Url    string
}

type broker struct {
	api.UnimplementedMqServerCallServer
	mqCredentials        *api.Credentials
	Config               *BrokerOptions
	RaftServer           *raft_server.RaftServer
	Url                  string
	ID                   string
	Key                  string
	Listener             net.Listener
	Server               *grpc.Server
	ProducerIDCache      sync.Map
	CacheStayTimeMs      int32
	MetadataLeaderID     atomic.Value // *string
	MetadataPeers        map[string]*brokerPeer
	MetaDataController   *MetaDataController
	PartitionsController *PartitionsController
	IsStop               bool
	wg                   sync.WaitGroup
	/*for stream call, check and close */
	StreamRequestTimeoutSessionsMs int32
	RaftHeartBeatSessionsMs        int32
	/*Periodically register with the registration center */
	BrokerToRegisterCenterHeartBeatSession int32
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
		time.Sleep(time.Duration(s.CacheStayTimeMs) * time.Millisecond)
	}
}

//func MakeMqServer(opt ...BuildOptions) (Server, error) {
//	i, err_ := newBroker(opt...)
//	s := Server(i)
//	return s, err_
//}

func (s *broker) goSendHeartbeat() {
	defer s.wg.Done()
	for !s.IsStop {
		for ID, MDpeer := range s.MetadataPeers {
			for {
				if s.IsStop {
					return
				}
				req := api.MQHeartBeatData{
					BrokerData:         s.mqCredentials,
					CheckTopic:         &api.MQHeartBeatDataTpKv{TopicTerm: make(map[string]int32)},
					CheckConsumerGroup: &api.MQHeartBeatDataCgKv{ConsumerGroup: make(map[string]int32)},
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
				//Log.DEBUG(s.ID, " SendHeartbeat To", MDpeer.ID)
				//s.PartitionsController.ttMu.RUnlock()
				res, err := MDpeer.Client.Heartbeat(context.Background(), &req)
				if err != nil || res.Response.Mode != api.Response_Success {
					Log.DEBUG(s.ID, " SendHeartbeat To", MDpeer.ID, " Err")
					goto next
				} else {
					Log.DEBUG(s.ID, " SendHeartbeat To", MDpeer.ID, " Return ", res)
					s.MetadataLeaderID.Store(&ID)
				}
				if res.Response.Mode != api.Response_Success {
					if res.ChangedTopic != nil {
						for Tp, _ := range res.ChangedTopic.TopicTerm {
							tp := Tp
							s.wg.Add(1)
							go func() {
								defer s.wg.Done()
								res, err := s.CheckSourceTermCall(context.Background(), &api.CheckSourceTermRequest{Self: s.mqCredentials, TopicData: &api.CheckSourceTermRequest_TopicCheck{Topic: tp, TopicTerm: -1}, ConsumerData: nil})
								if err == nil {
									err = s.PartitionsController.UpdateTp(s.ID, tp, res)
									if err != nil {
										Log.ERROR("Heart Beat Get Tp Diff Success Check Success UpdateFalse ", res)
									}
								} else {
									Log.ERROR("Heart beat Get Diff But Check Err ", err)
								}
							}()
						}
					}
					if res.ChangedConsumerGroup != nil {
						for _groupID, _ := range res.ChangedConsumerGroup.ChangedGroup {
							groupID := _groupID
							s.wg.Add(1)
							go func() {
								defer s.wg.Done()
								res, err := s.CheckSourceTermCall(context.Background(), &api.CheckSourceTermRequest{
									Self:      s.mqCredentials,
									TopicData: nil,
									ConsumerData: &api.CheckSourceTermRequest_ConsumerCheck{
										ConsumerId: nil,
										GroupID:    groupID,
										GroupTerm:  -1,
									},
								})
								if err == nil {
									err = s.PartitionsController.UpdateCg(s.ID, groupID, res)
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
				time.Sleep(time.Duration(s.BrokerToRegisterCenterHeartBeatSession) * time.Millisecond)
			}
		next:
			time.Sleep(50 * time.Millisecond)
		}
	}
}

func (s *broker) Serve() error {
	err := s.feasibilityTest()
	if err != nil {
		return err
	}
	//mqCredentials
	go func() {
		err := s.RaftServer.Serve()
		if err != nil {
			panic(err)
		}
	}()
	go func() {
		lis, err := net.Listen("tcp", s.Url)
		if err != nil {
			panic(err)
		}
		s.Listener = lis
		ns := grpc.NewServer()
		s.Server = ns
		api.RegisterMqServerCallServer(ns, s)
		Log.DEBUG("MQServer Serve ", s.ID, s.Url)
		err = s.Server.Serve(lis)
		if err != nil {
			panic(err)
		}
	}()
	if s.MetaDataController != nil {
		s.MetaDataController.MetaDataRaft.Start()
		_ = s.MetaDataController.Start()
	}
	if s.Config.data["IsMetaDataServer"].(bool) == false {
		for _, peer := range s.MetadataPeers {
			res, err := peer.Client.RegisterBroker(context.Background(),
				&api.RegisterBrokerRequest{IsMetadataNode: false, ID: s.ID, MqServerUrl: s.Url,
					HeartBeatSession: int64(s.BrokerToRegisterCenterHeartBeatSession)})
			if err != nil && res.Response.Mode == api.Response_Success {
				goto success
			}
		}
		return errors.New("not Find RegisterServer Register failed")
	}
success:
	s.wg.Add(2)
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

func (s *broker) CancelReg2Cluster(consumer *consumer_group.Consumer) {
	f, set := s.GetMetadataServers()
	for {
		cc := f()
		res, err := cc.Client.ConsumerDisConnect(context.Background(), &api.DisConnectInfo{
			BrokerInfo: s.mqCredentials,
			TargetInfo: &api.Credentials{
				Identity: api.Credentials_Consumer,
				Id:       consumer.SelfId,
				Key:      s.Key,
			},
		})
		if err != nil || res.Mode != api.Response_Success {
			Log.WARN("CancelReg2Cluster Call False")
			continue
		} else {
			set()
			return
		}
	}
}

//// for option call
//func (s *broker) registerRaftNode() (err_ error) {
//	if s.MetaDataController == nil {
//		return (Err.ErrRequestIllegal)
//	}
//	ID_Url := []struct{ ID, Url string }{}
//	for ID, peer := range s.MetadataPeers {
//		ID_Url = append(ID_Url, struct{ ID, Url string }{Url: peer.Url, ID: ID})
//	}
//	s.MetaDataController.MetaDataRaft, err_ = s.RaftServer.RegisterMetadataRaft(ID_Url, s.MetaDataController, s.MetaDataController)
//	return err_
//}

func (s *broker) RegisterBroker(ctx context.Context, req *api.RegisterBrokerRequest) (response *api.RegisterBrokerResponse, err error) {
	if req.Cred.Key != s.Key {
		return nil, errors.New("NOT Same Key")
	}
	if s.MetaDataController == nil || s.MetaDataController.IsLeader() {
		f, set := s.GetMetadataServers()
		for {
			select {
			case <-ctx.Done():
				return nil, err_.ErrRequestTimeout
			default:
			}
			l := f()
			res, err := l.Client.RegisterBroker(ctx, req)
			if err != nil || res.Response.Mode != api.Response_Success {
				continue
			} else {
				set()
				return res, nil
			}
		}
	}
	return s.MetaDataController.RegisterBroker(req), nil
}

func newBroker(option *BrokerOptions) (*broker, error) {
	//var err_ error
	bk := &broker{
		UnimplementedMqServerCallServer:        api.UnimplementedMqServerCallServer{},
		mqCredentials:                          &api.Credentials{Identity: api.Credentials_Broker, Id: option.data["BrokerID"].(string), Key: option.data["BrokerKey"].(string)},
		Config:                                 option,
		RaftServer:                             nil,
		Url:                                    option.data["BrokerAddr"].(string),
		ID:                                     option.data["BrokerID"].(string),
		Key:                                    option.data["BrokerKey"].(string),
		Listener:                               nil,
		Server:                                 nil,
		ProducerIDCache:                        sync.Map{},
		CacheStayTimeMs:                        int32(common.CacheStayTime_Ms),
		MetadataLeaderID:                       atomic.Value{},
		MetadataPeers:                          make(map[string]*brokerPeer),
		MetaDataController:                     nil,
		PartitionsController:                   nil, // TODO:
		IsStop:                                 false,
		wg:                                     sync.WaitGroup{},
		StreamRequestTimeoutSessionsMs:         int32(common.MQStreamRequestTimeoutSessions_Ms),
		RaftHeartBeatSessionsMs:                int32(common.RaftHeartbeatTimeout),
		BrokerToRegisterCenterHeartBeatSession: option.data["BrokerToRegisterCenterHeartBeatSession"].(int32),
	}

	i, ok := option.data["IsMetaDataServer"]

	// metadata server
	peers, ok1 := option.data["MetadataServerInfo"].(map[string]interface{})
	if !ok1 {
		return nil, err_.ErrRequestIllegal
	}
	MDpeers := []*BrokerMD{}
	for ID, Data := range peers {
		bk.MetadataPeers[ID] = &brokerPeer{
			Client: nil,
			Conn:   nil,
			ID:     ID,
			Url:    Data.(map[string]interface{})["Url"].(string),
		}
		conn, err := grpc.Dial(bk.MetadataPeers[ID].Url, grpc.WithInsecure())
		if err != nil {
			return nil, err
		}
		bk.MetadataPeers[ID].Conn = conn
		bk.MetadataPeers[ID].Client = api.NewMqServerCallClient(conn)
		if ok && i.(bool) {
			MDpeers = append(MDpeers, NewBrokerMD(true, ID, Data.(map[string]interface{})["RaftUrl"].(string), Data.(map[string]interface{})["HeartBeatSession"].(int64)))
		}
	}
	bk.RaftServer, _ = raft_server.MakeRaftServer()
	// raft server
	if !bk.RaftServer.SetRaftServerInfo(bk.ID, option.data["RaftServerAddr"].(map[string]interface{})["Url"].(string)) {
		return nil, err_.ErrRequestIllegal
	}

	if ok {
		bk.MetaDataController = NewMetaDataController(MDpeers...)
		d := make([]struct {
			ID, Url string
		}, 0)
		for _, dpeer := range MDpeers {
			d = append(d, struct{ ID, Url string }{ID: dpeer.ID, Url: dpeer.Url})
		}
		node, err := bk.RaftServer.RegisterMetadataRaft(d, bk.MetaDataController, bk.MetaDataController)
		if err != nil {
			return nil, err
		}
		bk.MetaDataController.MetaDataRaft = node
	}

	bk.PartitionsController = NewPartitionsController(bk.RaftServer, bk)
	return bk, nil
}

func (s *broker) feasibilityTest() error {
	if s.ID == "" || s.Url == "" || s.Key == "" || s.MetadataPeers == nil || len(s.MetadataPeers) == 0 {
		return err_.ErrRequestIllegal
	}
	return nil
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
func (s *broker) RegisterConsumer(ctx context.Context, req *api.RegisterConsumerRequest) (*api.RegisterConsumerResponse, error) {
	if s.MetaDataController == nil || s.MetaDataController.IsLeader() {
		f, set := s.GetMetadataServers()
		for {
			select {
			case <-ctx.Done():
				return nil, err_.ErrRequestTimeout
			default:
			}
			l := f()
			res, err := l.Client.RegisterConsumer(ctx, req)
			if err != nil || res.Response.Mode != api.Response_Success {
				continue
			} else {
				set()
				return res, nil
			}
		}
	}
	res := s.MetaDataController.RegisterConsumer(req)
	if res.Response.Mode == api.Response_Success {
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

func (s *broker) SubscribeTopic(ctx context.Context, req *api.SubscribeTopicRequest) (*api.SubscribeTopicResponse, error) {
	if s.MetaDataController == nil || s.MetaDataController.IsLeader() {
		f, set := s.GetMetadataServers()
		for {
			select {
			case <-ctx.Done():
				return nil, err_.ErrRequestTimeout
			default:
			}
			l := f()
			res, err := l.Client.SubscribeTopic(ctx, req)
			if err != nil || res.Response.Mode != api.Response_Success {
				continue
			} else {
				set()
				return res, nil
			}
		}
	}
	return s.MetaDataController.AddTopicRegisterConsumerGroup(req), nil
}
func (s *broker) UnSubscribeTopic(ctx context.Context, req *api.UnSubscribeTopicRequest) (*api.UnSubscribeTopicResponse, error) {
	if s.MetaDataController == nil || s.MetaDataController.IsLeader() {
		f, set := s.GetMetadataServers()
		for {
			select {
			case <-ctx.Done():
				return nil, err_.ErrRequestTimeout
			default:
			}
			l := f()
			res, err := l.Client.UnSubscribeTopic(ctx, req)
			if err != nil || res.Response.Mode != api.Response_Success {
				continue
			} else {
				set()
				return res, nil
			}
		}
	}
	return s.MetaDataController.DelTopicRegisterConsumerGroup(req), nil
}
func (s *broker) AddPart(ctx context.Context, req *api.AddPartRequest) (*api.AddPartResponse, error) {
	if s.MetaDataController == nil || s.MetaDataController.IsLeader() {
		f, set := s.GetMetadataServers()
		for {
			select {
			case <-ctx.Done():
				return nil, err_.ErrRequestTimeout
			default:
			}
			l := f()
			res, err := l.Client.AddPart(ctx, req)
			if err != nil || res.Response.Mode != api.Response_Success {
				continue
			} else {
				set()
				return res, nil
			}
		}
	}
	return s.MetaDataController.AddPart(req), nil
}
func (s *broker) RemovePart(ctx context.Context, req *api.RemovePartRequest) (*api.RemovePartResponse, error) {
	if s.MetaDataController == nil || s.MetaDataController.IsLeader() {
		f, set := s.GetMetadataServers()
		for {
			select {
			case <-ctx.Done():
				return nil, err_.ErrRequestTimeout
			default:
			}
			l := f()
			res, err := l.Client.RemovePart(ctx, req)
			if err != nil || res.Response.Mode != api.Response_Success {
				continue
			} else {
				set()
				return res, nil
			}
		}
	}
	return s.MetaDataController.RemovePart(req), nil
}

func (s *broker) ConsumerDisConnect(ctx context.Context, req *api.DisConnectInfo) (*api.Response, error) {
	if s.MetaDataController == nil || s.MetaDataController.IsLeader() {
		f, set := s.GetMetadataServers()
		for {
			select {
			case <-ctx.Done():
				return nil, err_.ErrRequestTimeout
			default:
			}
			l := f()
			res, err := l.Client.ConsumerDisConnect(ctx, req)
			if err != nil || res.Mode != api.Response_Success {
				continue
			} else {
				set()
				return res, nil
			}
		}
	}
	return s.MetaDataController.ConsumerDisConnect(req), nil
}

func (s *broker) ProducerDisConnect(ctx context.Context, req *api.DisConnectInfo) (*api.Response, error) {
	if s.MetaDataController == nil || s.MetaDataController.IsLeader() {
		f, set := s.GetMetadataServers()
		for {
			select {
			case <-ctx.Done():
				return nil, err_.ErrRequestTimeout
			default:
			}
			l := f()
			res, err := l.Client.ProducerDisConnect(ctx, req)
			if err != nil || res.Mode != api.Response_Success {
				continue
			} else {
				set()
				return res, nil
			}
		}
	}
	return s.MetaDataController.ProducerDisConnect(req), nil
}

// 注册生产者
func (s *broker) RegisterProducer(ctx context.Context, req *api.RegisterProducerRequest) (*api.RegisterProducerResponse, error) {
	if s.MetaDataController == nil || s.MetaDataController.IsLeader() {
		f, set := s.GetMetadataServers()
		for {
			select {
			case <-ctx.Done():
				return nil, err_.ErrRequestTimeout
			default:
			}
			l := f()
			res, err := l.Client.RegisterProducer(ctx, req)
			if err != nil || res.Response.Mode != api.Response_Success {
				continue
			} else {
				set()
				return res, nil
			}
		}
	}
	res := s.MetaDataController.RegisterProducer(req)
	if res.Response.Mode == api.Response_Success {
		res.Credential.Key = s.Key
	}
	return res, nil
}

// 创建话题
func (s *broker) CreateTopic(ctx context.Context, req *api.CreateTopicRequest) (*api.CreateTopicResponse, error) {
	if s.MetaDataController == nil || s.MetaDataController.IsLeader() {
		f, set := s.GetMetadataServers()
		for {
			select {
			case <-ctx.Done():
				return nil, err_.ErrRequestTimeout
			default:
			}
			l := f()
			res, err := l.Client.CreateTopic(ctx, req)
			if err != nil || res.Response.Mode != api.Response_Success {
				continue
			} else {
				set()
				return res, nil
			}
		}
	}
	res := s.MetaDataController.CreateTopic(req)
	if res == nil {
		panic("Ub")
	}
	return res, nil
}
func (s *broker) QueryTopic(ctx context.Context, req *api.QueryTopicRequest) (*api.QueryTopicResponse, error) {
	if s.MetaDataController == nil || s.MetaDataController.IsLeader() {
		f, set := s.GetMetadataServers()
		for {
			select {
			case <-ctx.Done():
				return nil, err_.ErrRequestTimeout
			default:
			}
			l := f()
			res, err := l.Client.QueryTopic(ctx, req)
			if err != nil || res.Response.Mode != api.Response_Success {
				continue
			} else {
				set()
				return res, nil
			}
		}
	}
	res := s.MetaDataController.QueryTopic(req)
	if res == nil {
		panic("Ub")
	}
	return res, nil
}

// 注销
func (s *broker) UnRegisterConsumer(ctx context.Context, req *api.UnRegisterConsumerRequest) (*api.UnRegisterConsumerResponse, error) {
	if s.MetaDataController == nil || s.MetaDataController.IsLeader() {
		f, set := s.GetMetadataServers()
		for {
			select {
			case <-ctx.Done():
				return nil, err_.ErrRequestTimeout
			default:
			}
			l := f()
			res, err := l.Client.UnRegisterConsumer(ctx, req)
			if err != nil || res.Response.Mode != api.Response_Success {
				continue
			} else {
				set()
				return res, nil
			}
		}
	}
	res := s.MetaDataController.UnRegisterConsumer(req)
	if res == nil {
		panic("Ub")
	}
	return res, nil
}

func (s *broker) UnRegisterProducer(ctx context.Context, req *api.UnRegisterProducerRequest) (*api.UnRegisterProducerResponse, error) {
	if s.MetaDataController == nil || s.MetaDataController.IsLeader() {
		f, set := s.GetMetadataServers()
		for {
			select {
			case <-ctx.Done():
				return nil, err_.ErrRequestTimeout
			default:
			}
			l := f()
			res, err := l.Client.UnRegisterProducer(ctx, req)
			if err != nil || res.Response.Mode != api.Response_Success {
				continue
			} else {
				set()
				return res, nil
			}
		}
	}
	res := s.MetaDataController.UnRegisterProducer(req)
	if res == nil {
		panic("Ub")
	}
	return res, nil
}

func (s *broker) JoinConsumerGroup(ctx context.Context, req *api.JoinConsumerGroupRequest) (*api.JoinConsumerGroupResponse, error) {
	if s.MetaDataController == nil || s.MetaDataController.IsLeader() {
		f, set := s.GetMetadataServers()
		for {
			select {
			case <-ctx.Done():
				return nil, err_.ErrRequestTimeout
			default:
			}
			l := f()
			res, err := l.Client.JoinConsumerGroup(ctx, req)
			if err != nil || res.Response.Mode != api.Response_Success {
				continue
			} else {
				set()
				return res, nil
			}
		}
	}
	if req.Cred.Key != s.Key {
		return nil, (err_.ErrRequestIllegal)
	}
	res := s.MetaDataController.JoinRegisterConsumerGroup(req)
	return res, nil
}

func (s *broker) LeaveConsumerGroup(ctx context.Context, req *api.LeaveConsumerGroupRequest) (*api.LeaveConsumerGroupResponse, error) {
	if s.MetaDataController == nil || s.MetaDataController.IsLeader() {
		f, set := s.GetMetadataServers()
		for {
			select {
			case <-ctx.Done():
				return nil, err_.ErrRequestTimeout
			default:
			}
			l := f()
			res, err := l.Client.LeaveConsumerGroup(ctx, req)
			if err != nil || res.Response.Mode != api.Response_Success {
				continue
			} else {
				set()
				return res, nil
			}
		}
	}
	if req.GroupCred.Key != s.Key || req.ConsumerCred.Key != s.Key {
		return nil, (err_.ErrRequestIllegal)
	}
	res := s.MetaDataController.LeaveRegisterConsumerGroup(req)
	return res, nil
}

func (s *broker) CheckSourceTerm(ctx context.Context, req *api.CheckSourceTermRequest) (*api.CheckSourceTermResponse, error) {
	if req.Self.Key != s.Key {
		return nil, (err_.ErrRequestIllegal)
	}
	if s.MetaDataController == nil || !s.MetaDataController.IsLeader() {
		return s.CheckSourceTermCall(ctx, req)
	}
	return s.MetaDataController.CheckSourceTerm(req), nil
}

//func (s *broker) GetCorrespondPartition(_ context.Context, req *pb.GetCorrespondPartitionRequest) (*pb.GetCorrespondPartitionResponse, error) {
//	return nil, nil
//}

func (s *broker) RegisterConsumerGroup(ctx context.Context, req *api.RegisterConsumerGroupRequest) (*api.RegisterConsumerGroupResponse, error) {
	if s.MetaDataController == nil || s.MetaDataController.IsLeader() {
		f, set := s.GetMetadataServers()
		for {
			select {
			case <-ctx.Done():
				return nil, err_.ErrRequestTimeout
			default:
			}
			l := f()
			res, err := l.Client.RegisterConsumerGroup(ctx, req)
			if err != nil || res.Response.Mode != api.Response_Success {
				continue
			} else {
				set()
				return res, nil
			}
		}
	}

	res := s.MetaDataController.RegisterConsumerGroup(req)
	if res == nil {
		panic("Ub")
	}
	res.Cred.Key = s.Key
	return res, nil
}

func ResponseErrNeedToWait() *api.Response {
	return &api.Response{Mode: api.Response_ErrNeedToWait}
}
func ResponseFailure() *api.Response {
	return &api.Response{Mode: api.Response_Failure}
}
func ResponseErrTimeout() *api.Response {
	return &api.Response{Mode: api.Response_ErrTimeout}
}
func ResponseErrNotLeader() *api.Response {
	return &api.Response{Mode: api.Response_ErrNotLeader}
}
func ResponseErrSourceNotExist() *api.Response {
	return &api.Response{Mode: api.Response_ErrSourceNotExist}
}
func ResponseErrSourceAlreadyExist() *api.Response {
	return &api.Response{Mode: api.Response_ErrSourceAlreadyExist}
}
func ResponseErrPartitionChanged() *api.Response {
	return &api.Response{Mode: api.Response_ErrPartitionChanged}
}
func ResponseErrRequestIllegal() *api.Response {
	return &api.Response{Mode: api.Response_ErrRequestIllegal}
}
func ResponseErrSourceNotEnough() *api.Response {
	return &api.Response{Mode: api.Response_ErrSourceNotEnough}
}
func ResponseSuccess() *api.Response {
	return &api.Response{Mode: api.Response_Success}
}
func ResponseNotServer() *api.Response {
	return &api.Response{Mode: api.Response_NotServe}
}

// TODO: Need Part To Confirm

func (s *broker) ConfirmIdentity(ctx context.Context, req *api.ConfirmIdentityRequest) (*api.ConfirmIdentityResponse, error) {
	if s.MetaDataController == nil || s.MetaDataController.IsLeader() {
		f, set := s.GetMetadataServers()
		for {
			select {
			case <-ctx.Done():
				return nil, err_.ErrRequestTimeout
			default:
			}
			l := f()
			res, err := l.Client.ConfirmIdentity(ctx, req)
			if err != nil || res.Response.Mode != api.Response_Success {
				continue
			} else {
				set()
				return res, nil
			}
		}
	} else {
		err := s.MetaDataController.ConfirmIdentity(req.CheckIdentity)
		return &api.ConfirmIdentityResponse{Response: ErrToResponse(err)}, nil
	}
}

// TODO complete PULL PUSH HEARTBEAT

// 拉取消息
func (s *broker) PullMessage(ctx context.Context, req *api.PullMessageRequest) (*api.PullMessageResponse, error) {
	if req.Group == nil || req.Self == nil || req.Group.Key != s.Key || req.Self.Key != s.Key {
		return &api.PullMessageResponse{Response: ResponseErrRequestIllegal(), Msgs: nil}, nil
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
		res, err := s.CheckSourceTermCall(ctx, &api.CheckSourceTermRequest{
			Self:      s.mqCredentials,
			TopicData: nil,
			ConsumerData: &api.CheckSourceTermRequest_ConsumerCheck{
				ConsumerId: &req.Self.Id,
				GroupID:    req.Group.Id,
				GroupTerm:  term,
			},
		})
		if err != nil {
			return &api.PullMessageResponse{Response: ErrToResponse(err)}, nil
		} else {
			err = s.PartitionsController.UpdateCg(s.ID, req.Group.Id, res)
			if err != nil {
				return &api.PullMessageResponse{Response: ErrToResponse(err)}, nil
			}
			// Go Recheck
			s.PartitionsController.cgtMu.RLock()
			gt, ok = s.PartitionsController.ConsumerGroupTerm[req.Group.Id]
			s.PartitionsController.cgtMu.RUnlock()
			if !ok {
				return &api.PullMessageResponse{Response: ResponseErrSourceNotExist()}, nil
			} else if atomic.LoadInt32(gt) > req.GroupTerm {
				return &api.PullMessageResponse{Response: ResponseErrPartitionChanged()}, nil
			}
		}
	} else if term > req.GroupTerm {
		return &api.PullMessageResponse{Response: ResponseErrPartitionChanged()}, nil
	}

	// part commit And read
	p, err := s.PartitionsController.GetPart(req.Topic, req.Part)
	if err != nil {
		return &api.PullMessageResponse{Response: ResponseErrSourceNotExist()}, nil
	}
	//s.RaftServer. // TODO Raft
	data, ReadBeginOffset, _, IsAllow2Del, errRead := p.Read(req.Self.Id, req.Group.Id, req.LastTimeOffset, req.ReadEntryNum)
	if errRead != nil {
		return &api.PullMessageResponse{
			Response: ErrToResponse(errRead),
		}, nil
	} else {
		return &api.PullMessageResponse{
			Response:      ResponseSuccess(),
			MessageOffset: ReadBeginOffset,
			Msgs:          &api.Message{Message: data},
			IsCouldToDel:  IsAllow2Del,
		}, nil
	}

}
func (s *broker) checkProducer(cxt context.Context, Credential *api.Credentials) error {
	if Credential == nil {
		return err_.ErrRequestIllegal
	}
	if _, ok := s.ProducerIDCache.Load(Credential.Id); !ok {
		f, set := s.GetMetadataServers()
		l := len(s.MetadataPeers)
		for i := 0; i < l; i++ {
			select {
			case <-cxt.Done():
				return err_.ErrRequestTimeout
			default:
			}
			cc := f()
			res, err := cc.Client.ConfirmIdentity(cxt, &api.ConfirmIdentityRequest{
				Self:          s.mqCredentials,
				CheckIdentity: Credential,
			})
			if err != nil {
				Log.ERROR("Call False:", err.Error())
			} else if res.Response.Mode == api.Response_Success {
				set()
				s.ProducerIDCache.Store(Credential.Id, time.Now().UnixMilli()+int64(s.CacheStayTimeMs))
				return nil
			} else {
				Log.WARN("CheckProducer Failed:", res)
			}
		}
	}
	return err_.ErrSourceNotExist
}

func (s *broker) CheckSourceTermCall(ctx context.Context, req *api.CheckSourceTermRequest) (res *api.CheckSourceTermResponse, err error) {
	f, set := s.GetMetadataServers()
	for {
		select {
		case <-ctx.Done():
			return nil, err_.ErrRequestTimeout
		default:
		}
		res, err = f().Client.CheckSourceTerm(context.Background(), req)
		if err != nil {
			Log.ERROR("Call False:", err.Error())
		} else {
			if res.Response.Mode == api.Response_Success {
				set()
				return res, nil
			} else {
				Log.WARN("Call False:", res.Response)
			}
		}
	}
}

func (p *PartitionsController) UpdateCg(bkID, CgID string, req *api.CheckSourceTermResponse) error {
	if req.Response.Mode != api.Response_Success || req.ConsumersData == nil {
		return err_.ErrRequestIllegal
	}
	p.cgtMu.RLock()
	term, ok := p.ConsumerGroupTerm[CgID]
	p.cgtMu.RUnlock()

	if !ok {
		for _, part := range req.ConsumersData.FcParts {
			for _, brokerData := range part.Part.Brokers {
				if brokerData.Id == bkID {
					goto success
				}
			}
		}
		return err_.ErrSourceNotExist
	success:
		term = new(int32)
		*term = -1
		p.cgtMu.Lock()
		if _, ok = p.ConsumerGroupTerm[CgID]; ok {
			p.cgtMu.Unlock()
			return err_.ErrSourceAlreadyExist
		} else {
			p.ConsumerGroupTerm[CgID] = term
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
			if brokerData.Id == bkID {
				getPart, _ := p.GetPart(part.Part.Topic, part.Part.PartName)
				if getPart == nil {
					peers := []struct{ ID, Url string }{}
					for _, data := range part.Part.Brokers {
						peers = append(peers, struct{ ID, Url string }{data.Id, data.Url})
					}
					getPart, _ = p.RegisterPart(part.Part.Topic, part.Part.PartName, uint64(common.PartDefaultMaxEntries), uint64(common.PartDefaultMaxSize), peers...)
				}
				g, err := getPart.ConsumerGroupManager.GetConsumerGroup(CgID)
				if err != nil {
					var Off int64
					switch *req.ConsumerGroupOption {
					case api.RegisterConsumerGroupRequest_Earliest:
						Off = getPart.MessageEntry.GetBeginOffset()
					case api.RegisterConsumerGroupRequest_Latest:
						Off = getPart.MessageEntry.GetEndOffset()
					}
					g, err = getPart.registerConsumerGroup(CgID, consumer_group.NewConsumer(*part.ConsumerID,
						CgID, *part.ConsumerTimeoutSession, *part.ConsumerMaxReturnMessageSize, *part.ConsumerMaxReturnMessageEntries), Off)
					if err != nil {
						isSetTerm = false
					}
				} else {
					if !g.CheckConsumer(*part.ConsumerID) {
						err = getPart.UpdateGroupConsumer(g.GroupId, consumer_group.NewConsumer(*part.ConsumerID, CgID, *part.ConsumerTimeoutSession, *part.ConsumerMaxReturnMessageSize, *part.ConsumerMaxReturnMessageEntries))
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

func (p *PartitionsController) UpdateTp(bkID, t string, UpdateReq *api.CheckSourceTermResponse) error {
	// 检查响应模式是否为成功，以及 TopicData 是否为 nil
	if UpdateReq.Response.Mode != api.Response_Success || UpdateReq.TopicData == nil || UpdateReq.TopicData.FcParts == nil {
		return err_.ErrRequestIllegal
	}

	// 获取 TopicTerm 锁，读取当前的 term 值
	p.ttMu.RLock()
	term, ok := p.TopicTerm[t]
	p.ttMu.RUnlock()

	//termNow :=
	if !ok {
		for _, part := range UpdateReq.TopicData.FcParts {
			for _, bkData := range part.Part.Brokers {
				if bkData.Id == bkID {
					goto Success
				}
			}
		}
		return err_.ErrRequestIllegal
	Success:
		// 创建一个新的 int32 指针，并将其值设置为 -1
		term = new(int32)
		*term = -1

		// 获取 TopicTerm 锁，将新的 int32 指针添加到 TopicTerm 中
		p.ttMu.Lock()
		if _, ok := p.TopicTerm[t]; ok {
			p.ttMu.Unlock()
			return err_.ErrSourceAlreadyExist
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
			if brokerData.Id == bkID {
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
				NewP[part.Part.PartName], err = newPartition(part.Part.Topic, part.Part.PartName, uint64(common.PartDefaultMaxEntries), uint64(common.PartDefaultMaxSize), p.handleTimeout, p.rfServer, peers...)
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
		toDelGroup := []*consumer_group.ConsumerGroup{}
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
func (s *broker) PushMessage(ctx context.Context, req *api.PushMessageRequest) (*api.PushMessageResponse, error) {
	// TODO:
	if req.Credential.Key != s.Key {
		return &api.PushMessageResponse{Response: ResponseErrRequestIllegal()}, nil
	}
	err := s.checkProducer(ctx, req.Credential)
	if err != nil {
		return &api.PushMessageResponse{Response: ErrToResponse(err)}, nil
	}
	termSelf, GetTopicTermErr := s.PartitionsController.GetTopicTerm(req.Topic)
	res, CheckSrcTermErr := s.CheckSourceTermCall(ctx, &api.CheckSourceTermRequest{Self: s.mqCredentials, TopicData: &api.CheckSourceTermRequest_TopicCheck{Topic: req.Topic, TopicTerm: termSelf}, ConsumerData: nil})
	if CheckSrcTermErr != nil {
		return &api.PushMessageResponse{Response: ErrToResponse(CheckSrcTermErr)}, nil
	}
	if res.TopicTerm != termSelf {
		err = s.PartitionsController.UpdateTp(s.ID, req.Topic, res)
		if err != nil {
			return &api.PushMessageResponse{Response: ErrToResponse(err)}, nil
		} else {
			NowTime := time.Now().UnixMilli() + int64(2*s.CacheStayTimeMs)
			for _, id := range res.TopicData.FollowerProducerIDs.ID {
				s.ProducerIDCache.Store(id, NowTime)
			}
		}
		termSelf, GetTopicTermErr = s.PartitionsController.GetTopicTerm(req.Topic)
	}
	if GetTopicTermErr != nil {
		return &api.PushMessageResponse{Response: ErrToResponse(err)}, nil
	} else if req.TopicTerm < termSelf {
		return &api.PushMessageResponse{Response: ResponseErrPartitionChanged()}, nil
	} else if req.TopicTerm > termSelf {
		return &api.PushMessageResponse{Response: ResponseErrRequestIllegal()}, nil //
	} else {
		s.ProducerIDCache.Store(req.Credential.Id, time.Now().UnixMilli()+int64(2*s.CacheStayTimeMs))
	}
	p, GetPartErr := s.PartitionsController.GetPart(req.Topic, req.Part)
	if GetPartErr != nil {
		Log.ERROR(`Partition not found`)
		return &api.PushMessageResponse{Response: ErrToResponse(err)}, nil
	}
	err = p.Write(req.Msgs.Message)
	if err != nil {
		return &api.PushMessageResponse{Response: ErrToResponse(err)}, nil
	}
	return &api.PushMessageResponse{Response: ResponseSuccess()}, nil
}

func (s *broker) Heartbeat(_ context.Context, req *api.MQHeartBeatData) (*api.HeartBeatResponseData, error) {
	// TODO:
	switch req.BrokerData.Identity {
	case api.Credentials_Broker:
		if s.MetaDataController == nil {
			return &api.HeartBeatResponseData{
				Response: ResponseErrNotLeader(),
			}, nil
		} else {
			ret := &api.HeartBeatResponseData{
				Response:             ResponseSuccess(),
				ChangedTopic:         nil,
				ChangedConsumerGroup: nil,
			}
			var err error
			err = s.MetaDataController.KeepBrokersAlive(req.BrokerData.Id)
			if err != nil {
				return &api.HeartBeatResponseData{Response: ErrToResponse(err)}, nil
			}
			if req.CheckTopic != nil {
				ret.ChangedTopic = &api.HeartBeatResponseDataTpKv{}
				ret.ChangedTopic.TopicTerm, err = s.MetaDataController.GetTopicTermDiff(req.CheckTopic.TopicTerm)
				if err != nil {
					return &api.HeartBeatResponseData{Response: ErrToResponse(err)}, nil
				}
			}
			if req.CheckConsumerGroup != nil {
				ret.ChangedConsumerGroup = &api.HeartBeatResponseDataCgKv{}
				ret.ChangedConsumerGroup.ChangedGroup, err = s.MetaDataController.GetConsumerGroupTermDiff(req.CheckConsumerGroup.ConsumerGroup)
				if err != nil {
					return &api.HeartBeatResponseData{Response: ErrToResponse(err)}, nil
				}
			}
			return ret, nil
		}
	default:
		return &api.HeartBeatResponseData{
			Response: ResponseErrRequestIllegal(),
		}, nil
	}
}

func (s *broker) PullMessages(stream api.MqServerCall_PullMessagesServer) error {
	timer := time.NewTimer(0)             // 创建一个初始时间为0的定时器，用于设置超时时间
	defer timer.Stop()                    // 在函数结束时停止定时器
	messageChan := make(chan interface{}) // 创建一个通道，用于接收消息或错误信息
	s.wg.Add(1)                           // 增加等待组计数器，表示有一个goroutine正在运行
	go func() {
		// 接收消息
		defer s.wg.Done() // 减少等待组计数器，表示goroutine已经完成
		for {
			request, err := stream.Recv() // 从流中接收消息
			if err != nil {
				// 发送错误到消息通道
				messageChan <- err
				return
			} else {
				// 发送消息到消息通道
				messageChan <- request
			}
		}
	}()

	for {
		timer.Reset(time.Millisecond *
			time.Duration(s.StreamRequestTimeoutSessionsMs)) // 重设定时器的超时时间
		select {
		case <-timer.C:
			// 超时关闭连接
			return err_.ErrRequestTimeout
		case request := <-messageChan:
			if data, ok := request.(error); ok {
				return data // 如果接收到的是错误信息，则返回错误
			}
			if data, ok := request.(*api.PullMessageRequest); ok {
				res, err := s.PullMessage(context.Background(), data) // 调用PullMessage处理请求
				if err != nil {
					return err
				} else {
					err = stream.Send(res) // 将处理结果发送回流
					if err != nil {
						return err
					}
				}
				continue
			}
			// 接收到消息，重设超时时间
			panic("Ub") // 如果接收到的是其他类型的消息，抛出panic
		}
	}
	return nil
}

func (s *broker) PushMessages(stream api.MqServerCall_PushMessagesServer) error {
	timer := time.NewTimer(0)             // 创建一个初始时间为0的定时器，用于设置超时时间
	defer timer.Stop()                    // 在函数结束时停止定时器
	messageChan := make(chan interface{}) // 创建一个通道，用于接收消息或错误信息
	s.wg.Add(1)                           // 增加等待组计数器，表示有一个goroutine正在运行
	go func() {
		// 接收消息
		defer s.wg.Done() // 减少等待组计数器，表示goroutine已经完成
		for {
			request, err := stream.Recv() // 从流中接收消息
			if err != nil {
				// 发送错误到消息通道
				messageChan <- err
				return
			} else {
				// 发送消息到消息通道
				messageChan <- request
			}
		}
	}()

	for {
		timer.Reset(time.Millisecond *
			time.Duration(s.StreamRequestTimeoutSessionsMs)) // 重设定时器的超时时间
		select {
		case <-timer.C:
			// 超时关闭连接
			return err_.ErrRequestTimeout
		case request := <-messageChan:
			if data, ok := request.(error); ok {
				return data // 如果接收到的是错误信息，则返回错误
			}
			if data, ok := request.(*api.PushMessageRequest); ok {
				res, err := s.PushMessage(context.Background(), data) // 调用PullMessage处理请求
				if err != nil {
					return err
				} else {
					err = stream.Send(res) // 将处理结果发送回流
					if err != nil {
						return err
					}
				}
				continue
			}
			// 接收到消息，重设超时时间
			panic("Ub") // 如果接收到的是其他类型的消息，抛出panic
		}
	}
	return nil
}
