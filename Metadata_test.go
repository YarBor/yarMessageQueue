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
	res := make([]*MetaDataController, 0)
	for _, info := range NodeINfO {
		ser := MakeRaftServer(info)
		go ser.Serve()
		//tp := TestPartition{}
		bk := NewBrokerMD(true, info.ID, info.Url, 3e17)
		//bk.IsDisconnect = BrokerMode_BrokerConnected
		controller := NewMetaDataController(bk)
		node, err1 := ser.RegisterMetadataRaft(append(make([]struct{ Url, ID string }, 0), NodeINfO...), controller, controller)
		if err1 != nil {
			panic(err1)
		} else {
			node.Start()
			controller.SetRaftNode(node)
			_ = controller.Start()
			go func() {
				for {
					_ = controller.KeepBrokersAlive("wang")
					time.Sleep(25 * time.Millisecond)
				}
			}()
		}
		res = append(res, controller)
	}
	return res
}

func TestNewMetaDataController(t *testing.T) {
	NodeINfO := struct {
		Url, ID string
	}{"wang", "127.0.0.1:20000"}
	Sers := pullUpTestMetadataClusters(NodeINfO)
	controller := Sers[0]
	println(controller.ToGetJson())
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
