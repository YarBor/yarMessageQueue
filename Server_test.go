package MqServer

import (
	"MqServer/ConsumerGroup"
	Log "MqServer/Log"
	"MqServer/Random"
	"MqServer/api"
	"MqServer/common"
	"context"
	"github.com/stretchr/testify/assert"
	"sync"
	"sync/atomic"
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
	time.Sleep(2 * time.Second)
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
	panic(nil)
success:
}
func TestBroker_CheckProducerTimeout(t *testing.T) {
	//bks := pullUp3BrokersWithMetadata()
	Log.SetLogLevel(Log.LogLevel_DEBUG)
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
		Log.DEBUG("Sleep wait for cache clear ")
		time.Sleep(time.Duration(bk.CacheStayTimeMs*2) * time.Millisecond)
		err = bk.checkProducer(context.Background(), p.Credential)
		if err != nil {
			panic(err)
		}
		return
		//bk.CheckProducerTimeout()
	}
	t.Fail()
	panic(nil)
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
		//bk.goSendHeartbeat()
		bk.MetaDataController.mu.RLock()
		for _, md := range bk.MetaDataController.MD.Brokers {
			assert.Equal(t, BrokerMode_BrokerConnected, atomic.LoadInt32(&md.IsConnect))
		}
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
	}
	wg := sync.WaitGroup{}
	wg.Add(3)
	for _, bk := range bks {
		bk := bk
		go func() {
			defer wg.Done()
			err := bk.Stop()
			if err != nil {
				panic(err)
			}
		}()
	}
	wg.Wait()
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

//func TestBroker_registerRaftNode(t *testing.T) {
//	bks := pullUp3BrokersWithMetadata()
//	for _, bk := range bks {
//		res, err := bk.CreateTopic(context.Background(), &api.CreateTopicRequest{
//			Topic:     "testTopic",
//			Partition: NewPartInfo([]int{1, 2}),
//		})
//		if err != nil || res.Response.Mode != api.Response_Success {
//			continue
//		}
//		Log.DEBUG(res)
//		bk.registerRaftNode()
//	}
////}
//func TestBroker_feasibilityTest(t *testing.T) {
//	bks := pullUp3BrokersWithMetadata()
//	for _, bk := range bks {
//		res, err := bk.CreateTopic(context.Background(), &api.CreateTopicRequest{
//			Topic:     "testTopic",
//			Partition: NewPartInfo([]int{1, 2}),
//		})
//		if err != nil || res.Response.Mode != api.Response_Success {
//			continue
//		}
//		Log.DEBUG(res)
//		bk.feasibilityTest()
//	}
//}

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
		rc, err1 := bk.RegisterConsumer(context.Background(), &api.RegisterConsumerRequest{
			MaxReturnMessageSize:    1e5,
			MaxReturnMessageEntries: 1e1,
			TimeoutSessionMsec:      1e3,
		})
		if err1 != nil || rc.Response.Mode != api.Response_Success {
			t.Fail()
			panic(nil)
		}
		return
	}
	t.Fail()
	panic(nil)
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
		pro, err9 := bk.RegisterProducer(context.Background(), &api.RegisterProducerRequest{
			FocalTopic:         "testTopic",
			MaxPushMessageSize: 1e6,
		})
		if err9 != nil || pro.Response.Mode != api.Response_Success {
			t.Fail()
			panic(nil)
		}
		con, err1 := bk.RegisterConsumer(context.Background(), &api.RegisterConsumerRequest{
			MaxReturnMessageSize:    1e5,
			MaxReturnMessageEntries: 1e1,
			TimeoutSessionMsec:      1e3,
		})
		if err1 != nil || con.Response.Mode != api.Response_Success {
			t.Fail()
			panic(nil)
		}
		conG, err2 := bk.RegisterConsumerGroup(context.Background(), &api.RegisterConsumerGroupRequest{
			PullOption: api.RegisterConsumerGroupRequest_Latest,
			GroupId:    nil,
		})
		if err2 != nil || conG.Response.Mode != api.Response_Success {
			t.Fail()
			panic(nil)
		}
		j, err3 := bk.JoinConsumerGroup(context.Background(), &api.JoinConsumerGroupRequest{
			Cred:            con.Credential,
			ConsumerGroupId: conG.Cred.Id,
		})
		if err3 != nil || j.Response.Mode != api.Response_Success {
			t.Fail()
			panic(nil)
		}
		data, err4 := bk.SubscribeTopic(context.Background(), &api.SubscribeTopicRequest{
			CGCred: conG.Cred,
			Tp:     "testTopic",
		})
		if err4 != nil || data.Response.Mode != api.Response_Success {
			t.Fail()
			panic(nil)
		}
		Log.DEBUG(data)
		checkdata, err5 := bk.CheckSourceTerm(context.Background(), &api.CheckSourceTermRequest{
			Self:      conG.Cred,
			TopicData: nil,
			ConsumerData: &api.CheckSourceTermRequest_ConsumerCheck{
				ConsumerId: &con.Credential.Id,
				GroupID:    conG.Cred.Id,
				GroupTerm:  conG.GroupTerm,
			},
		})
		if err5 != nil || checkdata.Response.Mode != api.Response_ErrPartitionChanged {
			t.Fail()
			panic(nil)
		}
		assert.Equal(t, 2, len(checkdata.ConsumersData.FcParts))
		unsb, err6 := bk.UnSubscribeTopic(context.Background(), &api.UnSubscribeTopicRequest{
			CGCred: conG.Cred,
			Tp:     "testTopic",
		})
		if err6 != nil || unsb.Response.Mode != api.Response_Success {
			t.Fail()
			panic(nil)
		}
		checkdata, err5 = bk.CheckSourceTerm(context.Background(), &api.CheckSourceTermRequest{
			Self:      conG.Cred,
			TopicData: nil,
			ConsumerData: &api.CheckSourceTermRequest_ConsumerCheck{
				ConsumerId: &con.Credential.Id,
				GroupID:    conG.Cred.Id,
				GroupTerm:  conG.GroupTerm,
			},
		})
		if err5 != nil || checkdata.Response.Mode != api.Response_ErrPartitionChanged {
			t.Fail()
			panic(nil)
		}
		data, err4 = bk.SubscribeTopic(context.Background(), &api.SubscribeTopicRequest{
			CGCred: conG.Cred,
			Tp:     "testTopic",
		})
		if err4 != nil || data.Response.Mode != api.Response_Success {
			t.Fail()
			panic(nil)
		}
		uc, err8 := bk.UnRegisterConsumer(context.Background(), &api.UnRegisterConsumerRequest{Credential: con.Credential})
		if err8 != nil || uc.Response.Mode != api.Response_Success {
			t.Fail()
			panic(nil)
		}
		lcg, err7 := bk.LeaveConsumerGroup(context.Background(), &api.LeaveConsumerGroupRequest{
			GroupCred:    conG.Cred,
			ConsumerCred: con.Credential,
		})
		if err7 != nil || lcg.Response.Mode != api.Response_ErrSourceNotExist {
			t.Fail()
			panic(nil)
		}
		checkdata, err5 = bk.CheckSourceTerm(context.Background(), &api.CheckSourceTermRequest{
			Self:      conG.Cred,
			TopicData: nil,
			ConsumerData: &api.CheckSourceTermRequest_ConsumerCheck{
				ConsumerId: &con.Credential.Id,
				GroupID:    conG.Cred.Id,
				GroupTerm:  conG.GroupTerm,
			},
		})
		up, err10 := bk.UnRegisterProducer(context.Background(), &api.UnRegisterProducerRequest{Credential: pro.Credential})
		if err10 != nil || up.Response.Mode != api.Response_Success {
			t.Fail()
			panic(nil)
		}
		Log.DEBUG(checkdata, err5)
		return
	}
}

func TestBroker_AddPart(t *testing.T) {
	bks := pullUp3BrokersWithMetadata()
	creatReq := &api.CreateTopicRequest{
		Topic:     "testTopic",
		Partition: NewPartInfo([]int{1, 2}),
	}
	for _, bk := range bks {
		res, err := bk.CreateTopic(context.Background(), creatReq)
		if err != nil || res.Response.Mode != api.Response_Success {
			continue
		}
		Log.DEBUG(res)
		ress, err1 := bk.AddPart(context.Background(), &api.AddPartRequest{
			Cred: bk.mqCredentials,
			Part: &api.Partition{
				Topic:    "testTopic",
				PartName: "AddPart",
				Brokers: append(make([]*api.BrokerData, 0), &api.BrokerData{
					Id:  "Bk2",
					Url: "",
				}),
			},
		})
		if err1 != nil || ress.Response.Mode != api.Response_Success {
			t.Fail()
		}
		return
	}
}
func TestBroker_RemovePart(t *testing.T) {
	bks := pullUp3BrokersWithMetadata()
	creatReq := &api.CreateTopicRequest{
		Topic:     "testTopic",
		Partition: NewPartInfo([]int{1, 2}),
	}
	for _, bk := range bks {
		res, err := bk.CreateTopic(context.Background(), creatReq)
		if err != nil || res.Response.Mode != api.Response_Success {
			continue
		}
		Log.DEBUG(res)
		ress, err1 := bk.RemovePart(context.Background(), &api.RemovePartRequest{
			Cred:  bk.mqCredentials,
			Topic: "testTopic",
			Part:  creatReq.Partition[0].PartitionName,
		})
		if err1 != nil || ress.Response.Mode != api.Response_Success {
			t.Fail()
		}
		return
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
