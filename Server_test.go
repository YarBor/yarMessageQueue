package MqServer

import (
	"MqServer/ConsumerGroup"
	Log "MqServer/Log"
	"MqServer/api"
	"MqServer/common"
	"context"
	"sync"
	"testing"
	"time"
)

func pullUp3BrokersWithMetadata() []*broker {
	bks := []*broker{nil, nil, nil}
	Log.SetLogLevel(Log.LogLevel_TRACE)
	wg := &sync.WaitGroup{}
	wg.Add(3)
	go func() {
		defer wg.Done()
		opt, err := NewBrokerOptions().With(
			common.BrokerAddr("127.0.0.1", "10001"),
			common.IsMetaDataServer(true),
			common.RaftServerAddr("RaftServer-1", "127.0.0.1", "20001"),
			common.MetadataServerInfo("MetadataServer-1", "127.0.0.1", "20001", -1),
			common.MetadataServerInfo("MetadataServer-2", "127.0.0.1", "20002", -1),
			common.MetadataServerInfo("MetadataServer-3", "127.0.0.1", "20003", -1),
		).Build()
		if err != nil {
			panic(err)
		}
		bk, err1 := newBroker(opt)
		if err1 != nil {
			panic(err1)
		}
		err = bk.Serve()
		if err != nil {
			panic(err)
		}
		bks[0] = bk
	}()
	go func() {
		defer wg.Done()
		opt, err := NewBrokerOptions().With(
			common.BrokerAddr("127.0.0.1", "10002"),
			common.IsMetaDataServer(true),
			common.RaftServerAddr("RaftServer-2", "127.0.0.1", "20002"),
			common.MetadataServerInfo("MetadataServer-1", "127.0.0.1", "20001", -1),
			common.MetadataServerInfo("MetadataServer-2", "127.0.0.1", "20002", -1),
			common.MetadataServerInfo("MetadataServer-3", "127.0.0.1", "20003", -1),
		).Build()
		if err != nil {
			panic(err)
		}
		bk, err1 := newBroker(opt)
		if err1 != nil {
			panic(err1)
		}
		err = bk.Serve()
		if err != nil {
			panic(err)
		}
		bks[1] = bk
	}()
	go func() {
		defer wg.Done()
		opt, err := NewBrokerOptions().With(
			common.BrokerAddr("127.0.0.1", "10003"),
			common.IsMetaDataServer(true),
			common.RaftServerAddr("RaftServer-3", "127.0.0.1", "20003"),
			common.MetadataServerInfo("MetadataServer-1", "127.0.0.1", "20001", -1),
			common.MetadataServerInfo("MetadataServer-2", "127.0.0.1", "20002", -1),
			common.MetadataServerInfo("MetadataServer-3", "127.0.0.1", "20003", -1),
		).Build()
		if err != nil {
			panic(err)
		}
		bk, err1 := newBroker(opt)
		if err1 != nil {
			panic(err1)
		}
		err = bk.Serve()
		if err != nil {
			panic(err)
		}
		bks[2] = bk
	}()
	wg.Wait()
	time.Sleep(time.Second)
	for _, bk := range bks {
		res, err := bk.CreateTopic(context.Background(), &api.CreateTopicRequest{
			Topic:     "TestTopic",
			Partition: NewPartInfo([]int{1, 2, 3, 1, 1}),
		})
		if err != nil {
			continue
		}
		Log.DEBUG(res)
		goto success
	}
	panic("no leader")

	//select {}
success:
	return bks
}

func TestBroker_NewBroker(t *testing.T) {
	bks := pullUp3BrokersWithMetadata()
	for _, bk := range bks {
		res, err := bk.CreateTopic(context.Background(), &api.CreateTopicRequest{
			Topic:     "test",
			Partition: NewPartInfo([]int{1, 2, 3, 1}),
		})
		if err != nil {
			continue
		}
		Log.DEBUG(res)
		goto success
	}
	t.Failed()
success:
}
func TestBroker_CheckProducerTimeout(t *testing.T) {
	//bks := pullUp3BrokersWithMetadata()
	bks := pullUp3BrokersWithMetadata()

	for _, bk := range bks {
		bk.CheckProducerTimeout()
	}
}
func TestBroker_goSendHeartbeat(t *testing.T) {
	bks := pullUp3BrokersWithMetadata()
	for _, bk := range bks {
		bk.goSendHeartbeat()
	}
}
func TestBroker_Serve(t *testing.T) {
	bks := pullUp3BrokersWithMetadata()
	for _, bk := range bks {
		bk.Serve()
	}
}
func TestBroker_Stop(t *testing.T) {
	bks := pullUp3BrokersWithMetadata()
	for _, bk := range bks {
		bk.Stop()
	}
}
func TestBroker_CancelReg2Cluster(t *testing.T) {
	bks := pullUp3BrokersWithMetadata()
	for _, bk := range bks {
		bk.CancelReg2Cluster(&ConsumerGroup.Consumer{})
	}
}
func TestBroker_registerRaftNode(t *testing.T) {
	bks := pullUp3BrokersWithMetadata()
	for _, bk := range bks {
		bk.registerRaftNode()
	}
}
func TestBroker_feasibilityTest(t *testing.T) {
	bks := pullUp3BrokersWithMetadata()
	for _, bk := range bks {
		bk.feasibilityTest()
	}
}
func TestBroker_GetMetadataServers(t *testing.T) {
	bks := pullUp3BrokersWithMetadata()
	for _, bk := range bks {
		bk.GetMetadataServers()
	}
}
func TestBroker_RegisterConsumer(t *testing.T) {
	bks := pullUp3BrokersWithMetadata()
	for _, bk := range bks {
		bk.RegisterConsumer(context.Background(), &api.RegisterConsumerRequest{})
	}
}
func TestBroker_SubscribeTopic(t *testing.T) {
	bks := pullUp3BrokersWithMetadata()
	for _, bk := range bks {
		bk.SubscribeTopic(context.Background(), &api.SubscribeTopicRequest{})
	}
}
func TestBroker_UnSubscribeTopic(t *testing.T) {
	bks := pullUp3BrokersWithMetadata()
	for _, bk := range bks {
		bk.UnSubscribeTopic(context.Background(), &api.UnSubscribeTopicRequest{})
	}
}
func TestBroker_AddPart(t *testing.T) {
	bks := pullUp3BrokersWithMetadata()
	for _, bk := range bks {
		bk.AddPart(context.Background(), &api.AddPartRequest{})
	}
}
func TestBroker_RemovePart(t *testing.T) {
	bks := pullUp3BrokersWithMetadata()
	for _, bk := range bks {
		bk.RemovePart(context.Background(), &api.RemovePartRequest{})
	}
}
func TestBroker_ConsumerDisConnect(t *testing.T) {
	bks := pullUp3BrokersWithMetadata()
	for _, bk := range bks {
		bk.ConsumerDisConnect(context.Background(), &api.DisConnectInfo{})
	}
}
func TestBroker_ProducerDisConnect(t *testing.T) {
	bks := pullUp3BrokersWithMetadata()
	for _, bk := range bks {
		bk.ProducerDisConnect(context.Background(), &api.DisConnectInfo{})
	}
}
func TestBroker_RegisterProducer(t *testing.T) {
	bks := pullUp3BrokersWithMetadata()
	for _, bk := range bks {
		bk.RegisterProducer(context.Background(), &api.RegisterProducerRequest{})
	}
}
func TestBroker_CreateTopic(t *testing.T) {
	bks := pullUp3BrokersWithMetadata()
	for _, bk := range bks {
		bk.CreateTopic(context.Background(), &api.CreateTopicRequest{})
	}
}
func TestBroker_QueryTopic(t *testing.T) {
	bks := pullUp3BrokersWithMetadata()
	for _, bk := range bks {
		bk.QueryTopic(context.Background(), &api.QueryTopicRequest{})
	}
}
func TestBroker_UnRegisterConsumer(t *testing.T) {
	bks := pullUp3BrokersWithMetadata()
	for _, bk := range bks {
		bk.UnRegisterConsumer(context.Background(), &api.UnRegisterConsumerRequest{})
	}
}
func TestBroker_UnRegisterProducer(t *testing.T) {
	bks := pullUp3BrokersWithMetadata()
	for _, bk := range bks {
		bk.UnRegisterProducer(context.Background(), &api.UnRegisterProducerRequest{})
	}
}
func TestBroker_JoinConsumerGroup(t *testing.T) {
	bks := pullUp3BrokersWithMetadata()
	for _, bk := range bks {
		bk.JoinConsumerGroup(context.Background(), &api.JoinConsumerGroupRequest{})
	}
}
func TestBroker_LeaveConsumerGroup(t *testing.T) {
	bks := pullUp3BrokersWithMetadata()
	for _, bk := range bks {
		bk.LeaveConsumerGroup(context.Background(), &api.LeaveConsumerGroupRequest{})
	}
}
func TestBroker_CheckSourceTerm(t *testing.T) {
	bks := pullUp3BrokersWithMetadata()
	for _, bk := range bks {
		bk.CheckSourceTerm(context.Background(), &api.CheckSourceTermRequest{})
	}
}
func TestBroker_RegisterConsumerGroup(t *testing.T) {
	bks := pullUp3BrokersWithMetadata()
	for _, bk := range bks {
		bk.RegisterConsumerGroup(context.Background(), &api.RegisterConsumerGroupRequest{})
	}
}
func TestBroker_ConfirmIdentity(t *testing.T) {
	bks := pullUp3BrokersWithMetadata()
	for _, bk := range bks {
		bk.ConfirmIdentity(context.Background(), &api.ConfirmIdentityRequest{})
	}
}
func TestBroker_PullMessage(t *testing.T) {
	bks := pullUp3BrokersWithMetadata()
	for _, bk := range bks {
		bk.PullMessage(context.Background(), &api.PullMessageRequest{})
	}
}
func TestBroker_checkProducer(t *testing.T) {
	bks := pullUp3BrokersWithMetadata()
	for _, bk := range bks {
		bk.checkProducer(context.Background(), &api.Credentials{})
	}
}
func TestBroker_CheckSourceTermCall(t *testing.T) {
	bks := pullUp3BrokersWithMetadata()
	for _, bk := range bks {
		bk.CheckSourceTermCall(context.Background(), &api.CheckSourceTermRequest{})
	}
}
func TestBroker_PushMessage(t *testing.T) {
	bks := pullUp3BrokersWithMetadata()
	for _, bk := range bks {
		bk.PushMessage(context.Background(), &api.PushMessageRequest{})
	}
}
func TestBroker_Heartbeat(t *testing.T) {
	bks := pullUp3BrokersWithMetadata()
	for _, bk := range bks {
		bk.Heartbeat(context.Background(), &api.MQHeartBeatData{})
	}
}
