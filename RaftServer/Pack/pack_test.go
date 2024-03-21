package Pack

import (
	pb "BorsMqServer/api"
	"encoding/json"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestMarshal(t *testing.T) {
	data := pb.CheckSourceTermResponse{
		Response:  &pb.Response{Mode: pb.Response_Success},
		TopicTerm: 1,
		GroupTerm: 12,
		ConsumersData: &pb.CheckSourceTermResponse_PartsData{
			FcParts:                  make([]*pb.CheckSourceTermResponse_PartsData_Parts, 0),
			FollowerProducerIDs:      &pb.CheckSourceTermResponse_IDs{ID: append(make([]string, 0), "producer_id")},
			FollowerConsumerGroupIDs: &pb.CheckSourceTermResponse_IDs{ID: append(make([]string, 0), "consumer1")},
		},
		ConsumerGroupOption: new(pb.RegisterConsumerGroupRequest_PullOptionMode),
		TopicData: &pb.CheckSourceTermResponse_PartsData{
			FcParts:                  make([]*pb.CheckSourceTermResponse_PartsData_Parts, 0),
			FollowerProducerIDs:      &pb.CheckSourceTermResponse_IDs{ID: append(make([]string, 0), "producer_id")},
			FollowerConsumerGroupIDs: &pb.CheckSourceTermResponse_IDs{ID: append(make([]string, 0), "consumer2")},
		},
	}
	data.ConsumersData.FcParts = append(data.ConsumersData.FcParts, &pb.CheckSourceTermResponse_PartsData_Parts{
		Part: &pb.Partition{
			Topic:    "lskdfaklsdf",
			PartName: "alskdjfalksfdalksdf",
			Brokers: append(append(make([]*pb.BrokerData, 0), &pb.BrokerData{
				Id:  "1",
				Url: "123",
			}), &pb.BrokerData{
				Id:  "2",
				Url: "234",
			}),
		},
		ConsumerID:                      new(string),
		ConsumerTimeoutSession:          new(int32),
		ConsumerMaxReturnMessageSize:    new(int32),
		ConsumerMaxReturnMessageEntries: new(int32),
	})
	i, err := Marshal(&data)
	if err != nil {
		panic(err)
	}
	println(string(i), "\n", len(i))
	check := &pb.CheckSourceTermResponse{}
	err = Unmarshal(i, check)
	if err != nil {
		panic(err)
	}
	str, err := json.Marshal(check)
	if err != nil {
		panic(err)
	}
	println(string(str))
	assert.Equal(t, &data, check)
}
