package MqServer

import (
	Log "MqServer/Log"
	"MqServer/RaftServer"
	"MqServer/Random"
	"MqServer/api"
	"github.com/stretchr/testify/assert"
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
	Url, ID string
}) *RaftServer.RaftServer {
	ser, err := RaftServer.MakeRaftServer()
	if err != nil {
		panic(err)
	}
	ser.SetRaftServerInfo(NodeINfO.ID, NodeINfO.Url)
	return ser
}

func pullUpTestMetadataClusters(NodeINfO ...struct {
	Url, ID string
}) []*MetaDataController {
	bks := []*BrokerMD{}
	for _, s := range NodeINfO {
		bks = append(bks, NewBrokerMD(true, s.ID, s.Url, 50))
	}
	res := make([]*MetaDataController, 0)
	for _, info := range NodeINfO {
		ser := MakeRaftServer(info)
		go ser.Serve()
		//tp := TestPartition{}
		//bk := NewBrokerMD(true, info.ID, info.Url, 50)
		//bk.IsDisconnect = BrokerMode_BrokerConnected
		controller := NewMetaDataController(bks...)
		node, err1 := ser.RegisterMetadataRaft(append(make([]struct{ Url, ID string }, 0), NodeINfO...), controller, controller)
		if err1 != nil {
			panic(err1)
		} else {
			controller.SetRaftNode(node)
			node.Start()
			_ = controller.Start()
			go func() {
				for {
					for _, s := range NodeINfO {
						_ = controller.KeepBrokersAlive(s.ID)
					}
					time.Sleep(25 * time.Millisecond)
				}
			}()
		}
		res = append(res, controller)
	}
	return res
}

var (
	OneNodeINfO = struct {
		Url, ID string
	}{"127.0.0.1:20000", "wang"}
	ThreeNodeInfo = []struct {
		Url, ID string
	}{{"127.0.0.1:20001", "wang1"}, {"127.0.0.1:20002", "wang2"}, {"127.0.0.1:20003", "wang3"}}
)

func TestNewMetaDataController(t *testing.T) {
	Sers := pullUpTestMetadataClusters(OneNodeINfO)
	controller := Sers[0]
	Log.DEBUG(controller.ToGetJson())
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
			PartitionName:     "part1",
			ReplicationNumber: 1,
		}), &api.CreateTopicRequest_PartitionCreateDetails{
			PartitionName:     "part2",
			ReplicationNumber: 1,
		}),
	})
	assert.NotEqual(t, api.Response_Success, res.Response.Mode)
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
	assert.NotEqual(t, api.Response_Success, res.Response.Mode)
	// test recycle
	time.Sleep(1 * time.Second)

	i := controller.CheckSourceTerm(&api.CheckSourceTermRequest{Self: &api.Credentials{Identity: api.Credentials_Broker, Id: "wang", Key: ""}, TopicData: &api.CheckSourceTermRequest_TopicCheck{Topic: "testTopic1", TopicTerm: -1}, ConsumerData: nil})
	assert.Equal(t, api.Response_ErrPartitionChanged, i.Response.Mode)

	time.Sleep(1 * time.Second)

	res = controller.CreateTopic(&api.CreateTopicRequest{Topic: "testTopic2", Partition: append(append(make([]*api.CreateTopicRequest_PartitionCreateDetails, 0), &api.CreateTopicRequest_PartitionCreateDetails{PartitionName: "part1", ReplicationNumber: 1}), &api.CreateTopicRequest_PartitionCreateDetails{PartitionName: "part2", ReplicationNumber: 1})})
	assert.Equal(t, api.Response_Success, res.Response.Mode)

	time.Sleep(1 * time.Second)

	i = controller.CheckSourceTerm(&api.CheckSourceTermRequest{Self: &api.Credentials{Identity: api.Credentials_Broker, Id: "wang", Key: ""}, TopicData: &api.CheckSourceTermRequest_TopicCheck{Topic: "testTopic1", TopicTerm: -1}, ConsumerData: nil})
	assert.Equal(t, api.Response_ErrSourceNotExist, i.Response.Mode)

	i = controller.CheckSourceTerm(&api.CheckSourceTermRequest{Self: &api.Credentials{Identity: api.Credentials_Broker, Id: "wang", Key: ""}, TopicData: &api.CheckSourceTermRequest_TopicCheck{Topic: "testTopic2", TopicTerm: -1}, ConsumerData: nil})
	assert.Equal(t, api.Response_ErrPartitionChanged, i.Response.Mode)
}

func TestMetaData_s_CheckTopic(t *testing.T) {
	var (
	//int1 = 1
	)
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
			Topic: "testTopic1",
			Partition: append(append(make([]*api.CreateTopicRequest_PartitionCreateDetails, 0), &api.CreateTopicRequest_PartitionCreateDetails{
				PartitionName:     "part1",
				ReplicationNumber: 1,
			}), &api.CreateTopicRequest_PartitionCreateDetails{
				PartitionName:     "part2",
				ReplicationNumber: 1,
			}),
		})
		if res.Response.Mode == api.Response_Success {
			//int1 = i
			goto success1
		}
	}
	t.Fatalf("CreateTopic Fail %v , %v , %v\n", clusters[0].ToGetJson(), clusters[1].ToGetJson(), clusters[2].ToGetJson())
success1:
	time.Sleep(time.Second)
	t.Logf("\n\n\n\nCreateTopic Success %v , %v , %v\n\n\n\n\n", clusters[0].ToGetJson(), clusters[1].ToGetJson(), clusters[2].ToGetJson())

	groupID := "GroupID_test"
	for _, cluster := range clusters {
		consumer := cluster.RegisterConsumer(&api.RegisterConsumerRequest{
			MaxReturnMessageSize:    1024 * 1024,
			MaxReturnMessageEntries: 10,
			TimeoutSessionMsec:      5000,
		})
		Log.DEBUG("after RegisterConsumer", consumer.String())
		if consumer.Response.Mode == api.Response_Success {
			t.Logf("\n\n\n\nCreateTopic consumer %v , %v , %v\n\n\n\n\n", clusters[0].ToGetJson(), clusters[1].ToGetJson(), clusters[2].ToGetJson())

			group := cluster.RegisterConsumerGroup(&api.RegisterConsumerGroupRequest{
				PullOption: api.RegisterConsumerGroupRequest_Latest,
				GroupId:    &groupID,
			})
			Log.DEBUG("after RegisterConsumerGroup", group.String())
			assert.Equal(t, api.Response_Success, group.Response.Mode)
			t.Logf("\n\n\n\nCreateTopic consumerGroup %v , %v , %v\n\n\n\n\n", clusters[0].ToGetJson(), clusters[1].ToGetJson(), clusters[2].ToGetJson())
			ress := cluster.JoinRegisterConsumerGroup(&api.JoinConsumerGroupRequest{
				Cred:            consumer.Credential,
				ConsumerGroupId: groupID,
			})
			Log.DEBUG("after JoinRegisterConsumerGroup", ress.String())
			assert.Equal(t, api.Response_Success, ress.Response.Mode)
			t.Logf("\n\n\n\nJoinConsumerGroup %v , %v , %v\n\n\n\n\n", clusters[0].ToGetJson(), clusters[1].ToGetJson(), clusters[2].ToGetJson())
			resss := cluster.AddTopicRegisterConsumerGroup(&api.SubscribeTopicRequest{
				CGCred: &api.Credentials{
					Identity: api.Credentials_ConsumerGroup,
					Id:       groupID,
					Key:      "",
				},
				Tp: "testTopic1",
			})
			Log.DEBUG("after AddTopicRegisterConsumerGroup", resss.String())
			assert.Equal(t, api.Response_Success, resss.Response.Mode)
			t.Logf("\n\n\n\nAddTopicRegisterConsumerGroup %v , %v , %v\n\n\n\n\n", clusters[0].ToGetJson(), clusters[1].ToGetJson(), clusters[2].ToGetJson())
			ressss := cluster.CheckSourceTerm(&api.CheckSourceTermRequest{
				Self:      group.Cred,
				TopicData: nil,
				ConsumerData: &api.CheckSourceTermRequest_ConsumerCheck{
					ConsumerId: &consumer.Credential.Id,
					GroupID:    groupID,
					GroupTerm:  -1,
				},
			})
			assert.Equal(t, api.Response_ErrPartitionChanged, ressss.Response.Mode)
			Log.DEBUG("after CheckSourceTerm", ressss.String())
			goto success2
		}
	}
	t.Fatalf("CreateTopic Fail %v , %v , %v\n", clusters[0].ToGetJson(), clusters[1].ToGetJson(), clusters[2].ToGetJson())
success2:
	time.Sleep(time.Second)
	for _, cluster := range clusters {
		Log.DEBUG(cluster.ToGetJson())
	}

}

func TestMetaDataController_RegisterConsumer(t *testing.T) {

}
