package MqServer

import (
	"testing"
	"time"
)

import (
	"github.com/stretchr/testify/assert"
)

import (
	consumerGroup "github.com/YarBor/BorsMqServer/consumer_group"
	Log "github.com/YarBor/BorsMqServer/logger"
	"github.com/YarBor/BorsMqServer/raft_server"
)

func pullUpRaftServers(NodeINfO ...struct {
	ID, Url string
}) []*raft_server.RaftServer {
	res := []*raft_server.RaftServer{}
	for _, in := range NodeINfO {
		ser := MakeRaftServer(in)
		res = append(res, ser)
		go ser.Serve()
	}
	return res
}

type Notifier struct {
}

func (n *Notifier) CancelReg2Cluster(consumer *consumerGroup.Consumer) {
	Log.DEBUG("CancelReg2Cluster Get Call", consumer)
}

func buildMQPartitionsController(ser ...*raft_server.RaftServer) []*PartitionsController {
	n, res := Notifier{}, []*PartitionsController{}
	for _, server := range ser {
		resT := NewPartitionsController(server, &n)
		res = append(res, resT)
	}
	return res
}

func TestPartitions_new(t *testing.T) {
	Log.SetLogLevel(Log.LogLevel_DEBUG)
	ThreeRFserver := pullUpRaftServers(ThreeNodeInfo...)
	PCs := buildMQPartitionsController(ThreeRFserver...)
	MaxEntries, MaxSize := 10, 100000
	parts := make([]*Partition, 0, 3)
	for _, c := range PCs {
		part, err := c.RegisterPart("Topic", "Partitions", uint64(MaxEntries), uint64(MaxSize), ThreeNodeInfo...)
		if err != nil {
			panic(err)
		} else {
			_ = part.Start()
		}
		parts = append(parts, part)
	}
	time.Sleep(time.Second)
	leaderIndex := -1
	for i := range parts {
		err := parts[i].Write([][]byte{[]byte("topic"), []byte("Partitions")})
		if err == nil {
			leaderIndex = i
		}
	}
	assert.NotEqual(t, int(-1), leaderIndex)
	cons := consumerGroup.NewConsumer("ConsumerTest", "GroupTest", 1e6, int32(MaxSize), int32(MaxEntries))
	_, err := parts[leaderIndex].registerConsumerGroup("GroupTest", cons, 0)
	if err != nil {
		panic(err)
	}
	time.Sleep(time.Second)
	Log.DEBUG(parts[0])
	Log.DEBUG(parts[1])
	Log.DEBUG(parts[2])
	Data, ReadBeginOffset, ReadEntriesNum, IsAllow2Del, err1 := parts[leaderIndex].Read(cons.SelfId, cons.GroupId, -1, 1)
	Log.DEBUG(Data, ReadBeginOffset, ReadEntriesNum, IsAllow2Del, err1)
	Data, ReadBeginOffset, ReadEntriesNum, IsAllow2Del, err1 = parts[leaderIndex].Read(cons.SelfId, cons.GroupId, ReadBeginOffset, 1)
	Log.DEBUG(Data, ReadBeginOffset, ReadEntriesNum, IsAllow2Del, err1)
	Data, ReadBeginOffset, ReadEntriesNum, IsAllow2Del, err1 = parts[leaderIndex].Read(cons.SelfId, cons.GroupId, ReadBeginOffset, 100)
	Log.DEBUG(Data, ReadBeginOffset, ReadEntriesNum, IsAllow2Del, err1)

}
