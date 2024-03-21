package MqServer

import (
	Log "BorsMqServer/Log"
	"BorsMqServer/RaftServer"
	"BorsMqServer/Random"
	"BorsMqServer/api"
	"github.com/stretchr/testify/assert"
	"sort"
	"strconv"
	"sync"
	"testing"
	"time"
)

type TestPartition struct {
}

func (t *TestPartition) MakeSnapshot() []byte {
	Log.DEBUG("Call TestPartition MakeSnapshot")
	return []byte(Random.RandStringBytes(16))
}

func (t *TestPartition) LoadSnapshot(bytes []byte) {
	Log.DEBUG("Call TestPartition LoadSnapshot", string(bytes))
}

func (t *TestPartition) Handle(i interface{}) (error, interface{}) {
	Log.DEBUG("Call TestPartition Handle")
	return nil, i
}

func MakeRaftServer(NodeINfO struct {
	ID, Url string
}) *RaftServer.RaftServer {
	ser, err := RaftServer.MakeRaftServer()
	if err != nil {
		panic(err)
	}
	ser.SetRaftServerInfo(NodeINfO.ID, NodeINfO.Url)
	return ser
}

func pullUpTestMetadataClusters(NodeINfO ...struct {
	ID, Url string
}) []*MetaDataController {
	res := make([]*MetaDataController, 0)
	for _, info := range NodeINfO {
		bks := []*BrokerMD{}
		for _, s := range NodeINfO {
			bks = append(bks, NewBrokerMD(true, s.ID, s.Url, 500))
		}
		ser := MakeRaftServer(info)
		go ser.Serve()
		//tp := TestPartition{}
		//bk := NewBrokerMD(true, info.ID, info.Url, 50)
		//bk.IsDisconnect = BrokerMode_BrokerConnected
		controller := NewMetaDataController(bks...)
		node, err1 := ser.RegisterMetadataRaft(append(make([]struct {
			ID, Url string
		}, 0), NodeINfO...), controller, controller)
		if err1 != nil {
			panic(err1)
		} else {
			controller.SetRaftNode(node)
			node.Start()
			_ = controller.Start()
			go func() {
				for controller.isAlive {
					for _, s := range bks {
						_ = controller.KeepBrokersAlive(s.ID)
					}
					time.Sleep(200 * time.Millisecond)
				}
			}()
		}
		res = append(res, controller)
	}
	return res
}

var (
	OneNodeINfO = struct {
		ID, Url string
	}{"wang", "127.0.0.1:20000"}
	ThreeNodeInfo = []struct {
		ID, Url string
	}{{"wang1", "127.0.0.1:20001"}, {"wang2", "127.0.0.1:20002"}, {"wang3", "127.0.0.1:20003"}}
)

func TestNewMetaDataController(t *testing.T) {
	Sers := pullUpTestMetadataClusters(OneNodeINfO)
	controller := Sers[0]
	Log.DEBUG(controller.ToGetJson())
	time.Sleep(time.Second)
	res := controller.CreateTopic(&api.CreateTopicRequest{
		Topic: "testTopic1",
		Partition: append(append(make([]*api.CreateTopicRequest_PartitionCreateDetails, 0), &api.CreateTopicRequest_PartitionCreateDetails{
			PartitionName:     "part1",
			ReplicationNumber: 1,
		}), &api.CreateTopicRequest_PartitionCreateDetails{
			PartitionName:     "part2",
			ReplicationNumber: 1,
		}),
	})
	assert.Equal(t, api.Response_Success, res.Response.Mode)
	res = controller.CreateTopic(&api.CreateTopicRequest{
		Topic: "testTopic1",
		Partition: append(append(make([]*api.CreateTopicRequest_PartitionCreateDetails, 0), &api.CreateTopicRequest_PartitionCreateDetails{
			PartitionName:     "part11",
			ReplicationNumber: 1,
		}), &api.CreateTopicRequest_PartitionCreateDetails{
			PartitionName:     "part22",
			ReplicationNumber: 1,
		}),
	})
	assert.Equal(t, api.Response_ErrSourceAlreadyExist, res.Response.Mode)
	res = controller.CreateTopic(&api.CreateTopicRequest{
		Topic: "testTopic2",
		Partition: append(append(make([]*api.CreateTopicRequest_PartitionCreateDetails, 0), &api.CreateTopicRequest_PartitionCreateDetails{
			PartitionName:     "part1",
			ReplicationNumber: 2,
		}), &api.CreateTopicRequest_PartitionCreateDetails{
			PartitionName:     "part2",
			ReplicationNumber: 1,
		}),
	})
	assert.Equal(t, api.Response_ErrSourceNotEnough, res.Response.Mode)
	// test recycle
	time.Sleep(1 * time.Second)

	i := controller.CheckSourceTerm(
		&api.CheckSourceTermRequest{
			Self: &api.Credentials{
				Identity: api.Credentials_Broker, Id: "wang", Key: ""}, TopicData: &api.CheckSourceTermRequest_TopicCheck{Topic: "testTopic1", TopicTerm: -1}, ConsumerData: nil})
	assert.Equal(t, api.Response_ErrPartitionChanged, i.Response.Mode)

	time.Sleep(1 * time.Second)

	res = controller.CreateTopic(&api.CreateTopicRequest{Topic: "testTopic2", Partition: append(append(make([]*api.CreateTopicRequest_PartitionCreateDetails, 0), &api.CreateTopicRequest_PartitionCreateDetails{PartitionName: "part1", ReplicationNumber: 1}), &api.CreateTopicRequest_PartitionCreateDetails{PartitionName: "part2", ReplicationNumber: 1})})
	assert.Equal(t, api.Response_Success, res.Response.Mode)

	time.Sleep(1 * time.Second)

	i = controller.CheckSourceTerm(&api.CheckSourceTermRequest{Self: &api.Credentials{Identity: api.Credentials_Broker, Id: "wang", Key: ""},
		TopicData: &api.CheckSourceTermRequest_TopicCheck{Topic: "testTopic1", TopicTerm: -1}, ConsumerData: nil})
	assert.Equal(t, api.Response_ErrPartitionChanged, i.Response.Mode)
	Log.DEBUG(i)

	i = controller.CheckSourceTerm(&api.CheckSourceTermRequest{Self: &api.Credentials{Identity: api.Credentials_Broker, Id: "wang", Key: ""}, TopicData: &api.CheckSourceTermRequest_TopicCheck{
		Topic: "testTopic2", TopicTerm: -1}, ConsumerData: nil})
	assert.Equal(t, api.Response_ErrPartitionChanged, i.Response.Mode)
}

func TestMetaData_s_RegisterTopicConsumers(t *testing.T) {
	_ = Log.SetLogLevel(Log.LogLevel_DEBUG)
	clusters := pullUpTestMetadataClusters(ThreeNodeInfo...)
	time.Sleep(1 * time.Second)
	for _, cluster := range clusters {
		if cluster.IsLeader() {
			goto success
		}
	}
	t.Fatalf("pullUpTestMetadataClusters Failed: %#v", clusters)
success:
	for _, cluster := range clusters {
		res := cluster.CreateTopic(&api.CreateTopicRequest{
			Topic:     "12321",
			Partition: NewPartInfo([]int{1, 2, 3, 2, 1}),
		})
		if res.Response.Mode != api.Response_Success {
			continue
		}
		res = cluster.CreateTopic(&api.CreateTopicRequest{
			Topic:     "12",
			Partition: NewPartInfo([]int{1, 2}),
		})
		res = cluster.CreateTopic(&api.CreateTopicRequest{
			Topic:     "1221",
			Partition: NewPartInfo([]int{1, 2, 2, 1}),
		})
		res = cluster.CreateTopic(&api.CreateTopicRequest{
			Topic:     "1111",
			Partition: NewPartInfo([]int{1, 1, 1, 1}),
		})
		res = cluster.CreateTopic(&api.CreateTopicRequest{
			Topic:     "1111",
			Partition: NewPartInfo([]int{1, 1, 1, 1}),
		})
		res = cluster.CreateTopic(&api.CreateTopicRequest{
			Topic:     "2222",
			Partition: NewPartInfo([]int{2, 2, 2, 2}),
		})
	}
	PrintClusters(clusters)
}

func TestMetaDataController_RegisterProducer(t *testing.T) {
	_ = Log.SetLogLevel(Log.LogLevel_DEBUG)
	clusters := pullUpTestMetadataClusters(ThreeNodeInfo...)
	time.Sleep(1 * time.Second)
	for _, cluster := range clusters {
		if cluster.IsLeader() {
			goto success
		}
	}
	t.Fatalf("pullUpTestMetadataClusters Failed: %#v", clusters)
success:
	for _, cluster := range clusters {
		resCreateTopic := cluster.CreateTopic(&api.CreateTopicRequest{
			Topic:     "1111",
			Partition: NewPartInfo([]int{1, 2, 3, 2, 1}),
		})
		if resCreateTopic.Response.Mode != api.Response_Success {
			continue
		}
		PrintClusters(clusters)
		num := 10
		wg := sync.WaitGroup{}
		wg.Add(10)
		for ; num != 0; num-- {
			go func() {
				defer wg.Done()
				resRegisterProducer := cluster.RegisterProducer(&api.RegisterProducerRequest{
					FocalTopic:         "1111",
					MaxPushMessageSize: 1e4,
				})
				Log.DEBUG("resRegisterProducer", resRegisterProducer)
				assert.Equal(t, api.Response_Success, resRegisterProducer.Response.Mode)
				assert.Equal(t, 5, len(resRegisterProducer.TpData.Parts))
				data := []int{}
				check := []int{1, 1, 2, 2, 3}
				for _, part := range resRegisterProducer.TpData.Parts {
					data = append(data, len(part.Brokers))
				}
				sort.Ints(data)
				assert.Equal(t, check, data)
			}()
		}
		wg.Wait()
		resRegisterProducer := cluster.RegisterProducer(&api.RegisterProducerRequest{
			FocalTopic:         "1111",
			MaxPushMessageSize: 1e4,
		})
		PrintClusters(clusters)
		UnRegisterProducerRes := cluster.UnRegisterProducer(&api.UnRegisterProducerRequest{Credential: resRegisterProducer.Credential})
		Log.DEBUG("UnRegisterProducerRes", UnRegisterProducerRes)
		PrintClusters(clusters)
	}

}

func NewPartInfo(num []int) []*api.CreateTopicRequest_PartitionCreateDetails {
	res := []*api.CreateTopicRequest_PartitionCreateDetails{}
	for _, i := range num {
		res = append(res, &api.CreateTopicRequest_PartitionCreateDetails{
			PartitionName:     Random.RandStringBytes(len(num)),
			ReplicationNumber: int32(i),
		})
	}
	return res
}

func PrintClusters(clusters []*MetaDataController) {
	for i, cluster := range clusters {
		// 打印 cluster 到日志
		Log.DEBUG(`-----------------------------------------------------------------------------`+"\n"+strconv.Itoa(i)+"\n", cluster.ToGetJson())
	}
}

func TestMetaDataController_RegisterConsumer_Group_Add_Leave_Reblance(t *testing.T) {
	var (
	//int1 = 1
	)
	_ = Log.SetLogLevel(Log.LogLevel_INFO)
	clusters := pullUpTestMetadataClusters(ThreeNodeInfo...)
	time.Sleep(1 * time.Second)
	for _, cluster := range clusters {
		if cluster.IsLeader() {
			goto success
		}
	}
	t.Fatalf("pullUpTestMetadataClusters Failed: %#v", clusters)
success:
	var cluster *MetaDataController
	for _, clusterT := range clusters {
		res := clusterT.CreateTopic(&api.CreateTopicRequest{Topic: "testTopic1", Partition: NewPartInfo([]int{1, 1, 2, 2})})
		if res.Response.Mode == api.Response_Success {
			cluster = clusterT
			goto success1
		}
	}
	t.Fatalf("CreateTopic Fail %v , %v , %v\n", clusters[0].ToGetJson(), clusters[1].ToGetJson(), clusters[2].ToGetJson())
success1:
	//time.Sleep(time.Second)
	//if clusters[0] == cluster {
	//	clusters[1].Stop()
	//} else {
	//	clusters[0].Stop()
	//}

	groupID := "GroupID_test"
	consumer1 := cluster.RegisterConsumer(&api.RegisterConsumerRequest{MaxReturnMessageSize: 1024 * 1024, MaxReturnMessageEntries: 10, TimeoutSessionMsec: 5000})
	consumer2 := cluster.RegisterConsumer(&api.RegisterConsumerRequest{MaxReturnMessageSize: 1024 * 1024, MaxReturnMessageEntries: 10, TimeoutSessionMsec: 5000})
	consumer3 := cluster.RegisterConsumer(&api.RegisterConsumerRequest{MaxReturnMessageSize: 1024 * 1024, MaxReturnMessageEntries: 10, TimeoutSessionMsec: 5000})
	consumer4 := cluster.RegisterConsumer(&api.RegisterConsumerRequest{MaxReturnMessageSize: 1024 * 1024, MaxReturnMessageEntries: 10, TimeoutSessionMsec: 5000})
	consumer5 := cluster.RegisterConsumer(&api.RegisterConsumerRequest{MaxReturnMessageSize: 1024 * 1024, MaxReturnMessageEntries: 10, TimeoutSessionMsec: 5000})

	if !(assert.Equal(t, api.Response_Success, consumer1.Response.Mode) &&
		assert.Equal(t, api.Response_Success, consumer2.Response.Mode) &&
		assert.Equal(t, api.Response_Success, consumer3.Response.Mode) &&
		assert.Equal(t, api.Response_Success, consumer4.Response.Mode) &&
		assert.Equal(t, api.Response_Success, consumer5.Response.Mode)) {
		Log.DEBUG("after RegisterConsumer", consumer1)
		Log.DEBUG("after RegisterConsumer", consumer2)
		Log.DEBUG("after RegisterConsumer", consumer3)
		Log.DEBUG("after RegisterConsumer", consumer4)
		Log.DEBUG("after RegisterConsumer", consumer5)
		t.Fatal()
	}

	group := cluster.RegisterConsumerGroup(&api.RegisterConsumerGroupRequest{PullOption: api.RegisterConsumerGroupRequest_Latest, GroupId: &groupID})
	Log.DEBUG("after RegisterConsumerGroup", group)
	assert.Equal(t, api.Response_Success, group.Response.Mode)

	//time.Sleep(5 * time.Second)
	assert.Equal(t, true, cluster.IsLeader())

	resss := cluster.AddTopicRegisterConsumerGroup(&api.SubscribeTopicRequest{CGCred: group.Cred, Tp: "testTopic1"})
	Log.DEBUG("after AddTopicRegisterConsumerGroup", resss)
	assert.Equal(t, api.Response_Success, resss.Response.Mode)
	PrintClusters(clusters)

	assert.Equal(t, true, cluster.IsLeader())
	ress := cluster.JoinRegisterConsumerGroup(&api.JoinConsumerGroupRequest{Cred: consumer1.Credential, ConsumerGroupId: groupID})
	Log.DEBUG("after JoinRegisterConsumerGroup", ress)
	assert.Equal(t, api.Response_Success, ress.Response.Mode)

	Log.INFO(cluster.MD.ConsGroup)

	assert.Equal(t, true, cluster.IsLeader())
	ress = cluster.JoinRegisterConsumerGroup(&api.JoinConsumerGroupRequest{Cred: consumer2.Credential, ConsumerGroupId: groupID})
	Log.DEBUG("after JoinRegisterConsumerGroup", ress)
	assert.Equal(t, api.Response_Success, ress.Response.Mode)

	Log.INFO(cluster.MD.ConsGroup)

	assert.Equal(t, true, cluster.IsLeader())
	ress = cluster.JoinRegisterConsumerGroup(&api.JoinConsumerGroupRequest{Cred: consumer3.Credential, ConsumerGroupId: groupID})
	Log.DEBUG("after JoinRegisterConsumerGroup", ress)
	assert.Equal(t, api.Response_Success, ress.Response.Mode)

	Log.INFO(cluster.MD.ConsGroup)

	assert.Equal(t, true, cluster.IsLeader())
	ress = cluster.JoinRegisterConsumerGroup(&api.JoinConsumerGroupRequest{Cred: consumer4.Credential, ConsumerGroupId: groupID})
	Log.DEBUG("after JoinRegisterConsumerGroup", ress)
	assert.Equal(t, api.Response_Success, ress.Response.Mode)

	Log.INFO(cluster.MD.ConsGroup)

	assert.Equal(t, true, cluster.IsLeader())
	ress = cluster.JoinRegisterConsumerGroup(&api.JoinConsumerGroupRequest{Cred: consumer5.Credential, ConsumerGroupId: groupID})
	Log.DEBUG("after JoinRegisterConsumerGroup", ress)
	assert.Equal(t, api.Response_Success, ress.Response.Mode)
	assert.Equal(t, 0, len(ress.FcParts))

	Log.INFO(cluster.MD.ConsGroup)

	assert.Equal(t, true, cluster.IsLeader())
	ressss := cluster.CheckSourceTerm(&api.CheckSourceTermRequest{Self: group.Cred, TopicData: nil, ConsumerData: &api.CheckSourceTermRequest_ConsumerCheck{ConsumerId: &consumer1.Credential.Id, GroupID: groupID, GroupTerm: -1}})
	assert.Equal(t, api.Response_ErrPartitionChanged, ressss.Response.Mode)

	Log.DEBUG("after CheckSourceTerm", ressss)
	Log.INFO(cluster.MD.ConsGroup)

	resssss := cluster.UnRegisterConsumer(&api.UnRegisterConsumerRequest{Credential: consumer1.Credential})
	assert.Equal(t, api.Response_Success, resssss.Response.Mode)
	assert.NotEqual(t, 5, cluster.MD.ConsGroup[groupID].ConsumersFcPart)
	Log.INFO(cluster.MD.ConsGroup)

	ressss = cluster.CheckSourceTerm(&api.CheckSourceTermRequest{Self: group.Cred, TopicData: nil, ConsumerData: &api.CheckSourceTermRequest_ConsumerCheck{ConsumerId: &consumer5.Credential.Id, GroupID: groupID, GroupTerm: ress.GetGroupTerm()}})
	assert.Equal(t, api.Response_ErrPartitionChanged, ressss.Response.Mode)
	assert.NotEqual(t, 0, len(ressss.ConsumersData.FcParts))
	Log.DEBUG("after CheckSourceTerm", ressss)
	Log.INFO(cluster.MD.ConsGroup)

	PrintClusters(clusters)

	res := cluster.AddPart(&api.AddPartRequest{Cred: consumer2.Credential, Part: &api.Partition{Topic: "testTopic1", PartName: "ADDPART", Brokers: []*api.BrokerData{{Id: ThreeNodeInfo[0].ID, Url: ThreeNodeInfo[0].Url}, nil, nil}}})
	Log.DEBUG("after AddPart", res)

	PrintClusters(clusters)
	checkNum := []int{1, 1, 1, 2}
	srcNum := []int{}
	for _, i := range cluster.MD.ConsGroup[groupID].ConsumersFcPart {
		srcNum = append(srcNum, len(*i))
	}
	sort.Ints(srcNum)
	assert.Equal(t, checkNum, srcNum)
	rres := cluster.DelTopicRegisterConsumerGroup(&api.UnSubscribeTopicRequest{
		CGCred: group.Cred,
		Tp:     "testTopic1",
	})
	assert.Equal(t, api.Response_Success, rres.Response.Mode)
	PrintClusters(clusters)
}

func TestMetaDataController_Part_Add_Remove(t *testing.T) {
	i := pullUpTestMetadataClusters(ThreeNodeInfo...)
	time.Sleep(time.Second)
	var leader *MetaDataController
	for _, controller := range i {
		res := controller.CreateTopic(&api.CreateTopicRequest{
			Topic:     "currentTopic",
			Partition: NewPartInfo([]int{1, 1, 2, 2}),
		})
		if res.Response.Mode == api.Response_Success {
			leader = controller
			goto done
		}
	}
	t.Fatalf("Not Leader Comming")
done:
	cre := api.Credentials{
		Identity: api.Credentials_Broker,
		Id:       ThreeNodeInfo[0].ID,
		Key:      "",
	}
	addPartRes := leader.AddPart(&api.AddPartRequest{
		Cred: &cre,
		Part: &api.Partition{
			Topic:    "WrongTopic",
			PartName: "ADDPART",
			Brokers:  []*api.BrokerData{{Id: ThreeNodeInfo[0].ID, Url: ThreeNodeInfo[0].Url}}}},
	)
	assert.Equal(t, api.Response_ErrSourceNotExist, addPartRes.Response.Mode)
	addPartRes = leader.AddPart(&api.AddPartRequest{
		Cred: &cre,
		Part: &api.Partition{
			Topic:    "currentTopic",
			PartName: "ADDPART",
			Brokers:  []*api.BrokerData{{Id: ThreeNodeInfo[0].ID, Url: ThreeNodeInfo[0].Url}}}},
	)
	assert.Equal(t, api.Response_Success, addPartRes.Response.Mode)
	addPartRes = leader.AddPart(&api.AddPartRequest{
		Cred: &cre,
		Part: &api.Partition{
			Topic:    "currentTopic",
			PartName: "ADDPART",
			Brokers:  []*api.BrokerData{{Id: ThreeNodeInfo[0].ID, Url: ThreeNodeInfo[0].Url}}}},
	)
	assert.Equal(t, api.Response_ErrSourceAlreadyExist, addPartRes.Response.Mode)
	removePartRes := leader.RemovePart(&api.RemovePartRequest{
		Cred:  &cre,
		Topic: "currentTopic",
		Part:  "ADDPART-Wrong",
	})
	assert.Equal(t, api.Response_ErrSourceNotExist, removePartRes.Response.Mode)
	removePartRes = leader.RemovePart(&api.RemovePartRequest{
		Cred:  &cre,
		Topic: "currentTopic",
		Part:  "ADDPART",
	})
	assert.Equal(t, api.Response_Success, removePartRes.Response.Mode)
}
