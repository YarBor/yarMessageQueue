package MqServer

import (
	"MqServer/ConsumerGroup"
	Log "MqServer/Log"
	"MqServer/Random"
	"MqServer/api"
	"MqServer/common"
	"context"
	"sync"
	"testing"
	"time"
)

func pullUp3BrokersWithMetadata() []*broker {
	bks := []*broker{nil, nil, nil}
	//Log.SetLogLevel(Log.LogLevel_TRACE)
	wg := &sync.WaitGroup{}
	wg.Add(3)
	key := Random.RandStringBytes(15)
	go func() {
		defer wg.Done()
		opt, err := NewBrokerOptions().With(
			common.BrokerAddr("127.0.0.1", "10001"),
			common.RaftServerAddr("127.0.0.1", "20001"),
			common.IsMetaDataServer(true),
			common.BrokerID("Bk1"),
			common.MetadataServerInfo("Bk1", "127.0.0.1", "20001", "127.0.0.1", "10001", -1),
			common.MetadataServerInfo("Bk2", "127.0.0.1", "20002", "127.0.0.1", "10002", -1),
			common.MetadataServerInfo("Bk3", "127.0.0.1", "20003", "127.0.0.1", "10003", -1),
			common.BrokerKey(key),
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
			common.BrokerID("Bk2"),
			common.IsMetaDataServer(true),
			common.RaftServerAddr("127.0.0.1", "20002"),
			common.MetadataServerInfo("Bk1", "127.0.0.1", "20001", "127.0.0.1", "10001", -1),
			common.MetadataServerInfo("Bk2", "127.0.0.1", "20002", "127.0.0.1", "10002", -1),
			common.MetadataServerInfo("Bk3", "127.0.0.1", "20003", "127.0.0.1", "10003", -1),
			common.BrokerKey(key),
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
			common.RaftServerAddr("127.0.0.1", "20003"),
			common.MetadataServerInfo("Bk1", "127.0.0.1", "20001", "127.0.0.1", "10001", -1),
			common.MetadataServerInfo("Bk2", "127.0.0.1", "20002", "127.0.0.1", "10002", -1),
			common.MetadataServerInfo("Bk3", "127.0.0.1", "20003", "127.0.0.1", "10003", -1),
			common.BrokerID("Bk3"),
			common.BrokerKey(key),
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
	time.Sleep(3 * time.Second)
	//select {}
	//success:
	return bks
}

func TestBroker_NewBroker(t *testing.T) { //bks := pullUp3BrokersWithMetadata()
	Log.SetLogLevel(Log.LogLevel_TRACE)
	bks := pullUp3BrokersWithMetadata()
	for _, bk := range bks {
		Log.DEBUG("To Call CreateTopic", bk.MetaDataController.ToGetJson())
		res, err := bk.CreateTopic(context.Background(), &api.CreateTopicRequest{
			Topic:     "testTopic",
			Partition: NewPartInfo([]int{1, 2, 3, 1}),
		})
		Log.DEBUG("CreateTopic , Res :", res)
		if err != nil || res.Response.Mode != api.Response_Success {
			continue
		}
		Log.DEBUG(res)
		goto success
	}
	t.Fail()
success:
}
func TestBroker_CheckProducerTimeout(t *testing.T) {
	//bks := pullUp3BrokersWithMetadata()
	Log.SetLogLevel(Log.LogLevel_TRACE)
	bks := pullUp3BrokersWithMetadata()
	for _, bk := range bks {
		res, err := bk.CreateTopic(context.Background(), &api.CreateTopicRequest{
			Topic:     "testTopic",
			Partition: NewPartInfo([]int{1, 2, 3, 1}),
		})
		Log.ERROR("CreateTopic , Res :", res)
		if err != nil || res.Response.Mode != api.Response_Success {
			continue
		}
		p, err1 := bk.RegisterProducer(context.Background(), &api.RegisterProducerRequest{
			FocalTopic:         "testTopic",
			MaxPushMessageSize: 1e5,
		})
		if err1 != nil {
			panic(err1)
		}
		// NO Rpc call
		err = bk.checkProducer(context.Background(), p.Credential)
		if err != nil {
			panic(err)
		}
		time.Sleep(time.Duration(bk.CacheStayTimeMs*2) * time.Millisecond)
		err = bk.checkProducer(context.Background(), p.Credential)
		if err != nil {
			panic(err)
		}
		//bk.CheckProducerTimeout()
	}
	t.Fail()
}
func TestBroker_goSendHeartbeat(t *testing.T) {
	bks := pullUp3BrokersWithMetadata()
	for _, bk := range bks {
		res, err := bk.CreateTopic(context.Background(), &api.CreateTopicRequest{
			Topic:     "testTopic",
			Partition: NewPartInfo([]int{1, 2}),
		})
		if err != nil || res.Response.Mode != api.Response_Success {
			continue
		}
		Log.DEBUG(res)
		bk.goSendHeartbeat()
	}
}
func TestBroker_Stop(t *testing.T) {
	bks := pullUp3BrokersWithMetadata()
	for _, bk := range bks {
		res, err := bk.CreateTopic(context.Background(), &api.CreateTopicRequest{
			Topic:     "testTopic",
			Partition: NewPartInfo([]int{1, 2}),
		})
		if err != nil || res.Response.Mode != api.Response_Success {
			continue
		}
		Log.DEBUG(res)
		bk.Stop()
	}
}
func TestBroker_CancelReg2Cluster(t *testing.T) {
	bks := pullUp3BrokersWithMetadata()
	for _, bk := range bks {
		res, err := bk.CreateTopic(context.Background(), &api.CreateTopicRequest{
			Topic:     "testTopic",
			Partition: NewPartInfo([]int{1, 2}),
		})
		if err != nil || res.Response.Mode != api.Response_Success {
			continue
		}
		Log.DEBUG(res)
		bk.CancelReg2Cluster(&ConsumerGroup.Consumer{})
	}
}
func TestBroker_registerRaftNode(t *testing.T) {
	bks := pullUp3BrokersWithMetadata()
	for _, bk := range bks {
		res, err := bk.CreateTopic(context.Background(), &api.CreateTopicRequest{
			Topic:     "testTopic",
			Partition: NewPartInfo([]int{1, 2}),
		})
		if err != nil || res.Response.Mode != api.Response_Success {
			continue
		}
		Log.DEBUG(res)
		bk.registerRaftNode()
	}
}
func TestBroker_feasibilityTest(t *testing.T) {
	bks := pullUp3BrokersWithMetadata()
	for _, bk := range bks {
		res, err := bk.CreateTopic(context.Background(), &api.CreateTopicRequest{
			Topic:     "testTopic",
			Partition: NewPartInfo([]int{1, 2}),
		})
		if err != nil || res.Response.Mode != api.Response_Success {
			continue
		}
		Log.DEBUG(res)
		bk.feasibilityTest()
	}
}
func TestBroker_GetMetadataServers(t *testing.T) {
	bks := pullUp3BrokersWithMetadata()
	for _, bk := range bks {
		res, err := bk.CreateTopic(context.Background(), &api.CreateTopicRequest{
			Topic:     "testTopic",
			Partition: NewPartInfo([]int{1, 2}),
		})
		if err != nil || res.Response.Mode != api.Response_Success {
			continue
		}
		Log.DEBUG(res)
		bk.GetMetadataServers()
	}
}
func TestBroker_RegisterConsumer(t *testing.T) {
	bks := pullUp3BrokersWithMetadata()
	for _, bk := range bks {
		res, err := bk.CreateTopic(context.Background(), &api.CreateTopicRequest{
			Topic:     "testTopic",
			Partition: NewPartInfo([]int{1, 2}),
		})
		if err != nil || res.Response.Mode != api.Response_Success {
			continue
		}
		Log.DEBUG(res)
		bk.RegisterConsumer(context.Background(), &api.RegisterConsumerRequest{})
	}
}
func TestBroker_SubscribeTopic(t *testing.T) {
	bks := pullUp3BrokersWithMetadata()
	for _, bk := range bks {
		res, err := bk.CreateTopic(context.Background(), &api.CreateTopicRequest{
			Topic:     "testTopic",
			Partition: NewPartInfo([]int{1, 2}),
		})
		if err != nil || res.Response.Mode != api.Response_Success {
			continue
		}
		Log.DEBUG(res)
		bk.SubscribeTopic(context.Background(), &api.SubscribeTopicRequest{})
	}
}
func TestBroker_UnSubscribeTopic(t *testing.T) {
	bks := pullUp3BrokersWithMetadata()
	for _, bk := range bks {
		res, err := bk.CreateTopic(context.Background(), &api.CreateTopicRequest{
			Topic:     "testTopic",
			Partition: NewPartInfo([]int{1, 2}),
		})
		if err != nil || res.Response.Mode != api.Response_Success {
			continue
		}
		Log.DEBUG(res)
		bk.UnSubscribeTopic(context.Background(), &api.UnSubscribeTopicRequest{})
	}
}
func TestBroker_AddPart(t *testing.T) {
	bks := pullUp3BrokersWithMetadata()
	for _, bk := range bks {
		res, err := bk.CreateTopic(context.Background(), &api.CreateTopicRequest{
			Topic:     "testTopic",
			Partition: NewPartInfo([]int{1, 2}),
		})
		if err != nil || res.Response.Mode != api.Response_Success {
			continue
		}
		Log.DEBUG(res)
		bk.AddPart(context.Background(), &api.AddPartRequest{})
	}
}
func TestBroker_RemovePart(t *testing.T) {
	bks := pullUp3BrokersWithMetadata()
	for _, bk := range bks {
		res, err := bk.CreateTopic(context.Background(), &api.CreateTopicRequest{
			Topic:     "testTopic",
			Partition: NewPartInfo([]int{1, 2}),
		})
		if err != nil || res.Response.Mode != api.Response_Success {
			continue
		}
		Log.DEBUG(res)
		bk.RemovePart(context.Background(), &api.RemovePartRequest{})
	}
}
func TestBroker_ConsumerDisConnect(t *testing.T) {
	bks := pullUp3BrokersWithMetadata()
	for _, bk := range bks {
		res, err := bk.CreateTopic(context.Background(), &api.CreateTopicRequest{
			Topic:     "testTopic",
			Partition: NewPartInfo([]int{1, 2}),
		})
		if err != nil || res.Response.Mode != api.Response_Success {
			continue
		}
		Log.DEBUG(res)
		bk.ConsumerDisConnect(context.Background(), &api.DisConnectInfo{})
	}
}
func TestBroker_ProducerDisConnect(t *testing.T) {
	bks := pullUp3BrokersWithMetadata()
	for _, bk := range bks {
		res, err := bk.CreateTopic(context.Background(), &api.CreateTopicRequest{
			Topic:     "testTopic",
			Partition: NewPartInfo([]int{1, 2}),
		})
		if err != nil || res.Response.Mode != api.Response_Success {
			continue
		}
		Log.DEBUG(res)
		bk.ProducerDisConnect(context.Background(), &api.DisConnectInfo{})
	}
}
func TestBroker_RegisterProducer(t *testing.T) {
	bks := pullUp3BrokersWithMetadata()
	for _, bk := range bks {
		res, err := bk.CreateTopic(context.Background(), &api.CreateTopicRequest{
			Topic:     "testTopic",
			Partition: NewPartInfo([]int{1, 2}),
		})
		if err != nil || res.Response.Mode != api.Response_Success {
			continue
		}
		Log.DEBUG(res)
		bk.RegisterProducer(context.Background(), &api.RegisterProducerRequest{})
	}
}
func TestBroker_CreateTopic(t *testing.T) {
	bks := pullUp3BrokersWithMetadata()
	for _, bk := range bks {
		res, err := bk.CreateTopic(context.Background(), &api.CreateTopicRequest{
			Topic:     "testTopic",
			Partition: NewPartInfo([]int{1, 2}),
		})
		if err != nil || res.Response.Mode != api.Response_Success {
			continue
		}
		Log.DEBUG(res)
		bk.CreateTopic(context.Background(), &api.CreateTopicRequest{})
	}
}
func TestBroker_QueryTopic(t *testing.T) {
	bks := pullUp3BrokersWithMetadata()
	for _, bk := range bks {
		res, err := bk.CreateTopic(context.Background(), &api.CreateTopicRequest{
			Topic:     "testTopic",
			Partition: NewPartInfo([]int{1, 2}),
		})
		if err != nil || res.Response.Mode != api.Response_Success {
			continue
		}
		Log.DEBUG(res)
		bk.QueryTopic(context.Background(), &api.QueryTopicRequest{})
	}
}
func TestBroker_UnRegisterConsumer(t *testing.T) {
	bks := pullUp3BrokersWithMetadata()
	for _, bk := range bks {
		res, err := bk.CreateTopic(context.Background(), &api.CreateTopicRequest{
			Topic:     "testTopic",
			Partition: NewPartInfo([]int{1, 2}),
		})
		if err != nil || res.Response.Mode != api.Response_Success {
			continue
		}
		Log.DEBUG(res)
		bk.UnRegisterConsumer(context.Background(), &api.UnRegisterConsumerRequest{})
	}
}
func TestBroker_UnRegisterProducer(t *testing.T) {
	bks := pullUp3BrokersWithMetadata()
	for _, bk := range bks {
		res, err := bk.CreateTopic(context.Background(), &api.CreateTopicRequest{
			Topic:     "testTopic",
			Partition: NewPartInfo([]int{1, 2}),
		})
		if err != nil || res.Response.Mode != api.Response_Success {
			continue
		}
		Log.DEBUG(res)
		bk.UnRegisterProducer(context.Background(), &api.UnRegisterProducerRequest{})
	}
}
func TestBroker_JoinConsumerGroup(t *testing.T) {
	bks := pullUp3BrokersWithMetadata()
	for _, bk := range bks {
		res, err := bk.CreateTopic(context.Background(), &api.CreateTopicRequest{
			Topic:     "testTopic",
			Partition: NewPartInfo([]int{1, 2}),
		})
		if err != nil || res.Response.Mode != api.Response_Success {
			continue
		}
		Log.DEBUG(res)
		bk.JoinConsumerGroup(context.Background(), &api.JoinConsumerGroupRequest{})
	}
}
func TestBroker_LeaveConsumerGroup(t *testing.T) {
	bks := pullUp3BrokersWithMetadata()
	for _, bk := range bks {
		res, err := bk.CreateTopic(context.Background(), &api.CreateTopicRequest{
			Topic:     "testTopic",
			Partition: NewPartInfo([]int{1, 2}),
		})
		if err != nil || res.Response.Mode != api.Response_Success {
			continue
		}
		Log.DEBUG(res)
		bk.LeaveConsumerGroup(context.Background(), &api.LeaveConsumerGroupRequest{})
	}
}
func TestBroker_CheckSourceTerm(t *testing.T) {
	bks := pullUp3BrokersWithMetadata()
	for _, bk := range bks {
		res, err := bk.CreateTopic(context.Background(), &api.CreateTopicRequest{
			Topic:     "testTopic",
			Partition: NewPartInfo([]int{1, 2}),
		})
		if err != nil || res.Response.Mode != api.Response_Success {
			continue
		}
		Log.DEBUG(res)
		bk.CheckSourceTerm(context.Background(), &api.CheckSourceTermRequest{})
	}
}
func TestBroker_RegisterConsumerGroup(t *testing.T) {
	bks := pullUp3BrokersWithMetadata()
	for _, bk := range bks {
		res, err := bk.CreateTopic(context.Background(), &api.CreateTopicRequest{
			Topic:     "testTopic",
			Partition: NewPartInfo([]int{1, 2}),
		})
		if err != nil || res.Response.Mode != api.Response_Success {
			continue
		}
		Log.DEBUG(res)
		bk.RegisterConsumerGroup(context.Background(), &api.RegisterConsumerGroupRequest{})
	}
}
func TestBroker_ConfirmIdentity(t *testing.T) {
	bks := pullUp3BrokersWithMetadata()
	for _, bk := range bks {
		res, err := bk.CreateTopic(context.Background(), &api.CreateTopicRequest{
			Topic:     "testTopic",
			Partition: NewPartInfo([]int{1, 2}),
		})
		if err != nil || res.Response.Mode != api.Response_Success {
			continue
		}
		Log.DEBUG(res)
		bk.ConfirmIdentity(context.Background(), &api.ConfirmIdentityRequest{})
	}
}
func TestBroker_PullMessage(t *testing.T) {
	bks := pullUp3BrokersWithMetadata()
	for _, bk := range bks {
		res, err := bk.CreateTopic(context.Background(), &api.CreateTopicRequest{
			Topic:     "testTopic",
			Partition: NewPartInfo([]int{1, 2}),
		})
		if err != nil || res.Response.Mode != api.Response_Success {
			continue
		}
		Log.DEBUG(res)
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
		res, err := bk.CreateTopic(context.Background(), &api.CreateTopicRequest{
			Topic:     "testTopic",
			Partition: NewPartInfo([]int{1, 2}),
		})
		if err != nil || res.Response.Mode != api.Response_Success {
			continue
		}
		Log.DEBUG(res)
		bk.CheckSourceTermCall(context.Background(), &api.CheckSourceTermRequest{})
	}
}
func TestBroker_PushMessage(t *testing.T) {
	bks := pullUp3BrokersWithMetadata()
	for _, bk := range bks {
		res, err := bk.CreateTopic(context.Background(), &api.CreateTopicRequest{
			Topic:     "testTopic",
			Partition: NewPartInfo([]int{1, 2}),
		})
		if err != nil || res.Response.Mode != api.Response_Success {
			continue
		}
		Log.DEBUG(res)
		bk.PushMessage(context.Background(), &api.PushMessageRequest{})
	}
}
func TestBroker_Heartbeat(t *testing.T) {
	bks := pullUp3BrokersWithMetadata()
	for _, bk := range bks {
		res, err := bk.CreateTopic(context.Background(), &api.CreateTopicRequest{
			Topic:     "testTopic",
			Partition: NewPartInfo([]int{1, 2}),
		})
		if err != nil || res.Response.Mode != api.Response_Success {
			continue
		}
		Log.DEBUG(res)
		bk.Heartbeat(context.Background(), &api.MQHeartBeatData{})
	}
}
