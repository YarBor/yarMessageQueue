package MqServer

import (
	"MqServer/Raft"
	"MqServer/Raft/Gob"
	pb "MqServer/rpc"
	"bytes"
	"errors"
	"sort"
	"sync"
	"sync/atomic"
)

type MetaData struct {
	SelfIncrementId uint32
	Brokers         []*BrokerMD
	tpTree          map[string]*TopicMD
	Consumers       map[uint32]*ConsumerMD
	Producers       map[uint32]*ProducerMD
}

type ConsumerMD struct {
	Cred   pb.Credentials
	Config pb.RegisterConsumerRequest
}

type ProducerMD struct {
	Cred   pb.Credentials
	Config pb.RegisterProducerRequest
}

type BrokerMD struct {
	Name         string
	Url          string
	PartitionNum uint32
}

type BrokersGroupMD struct {
	Leader  BrokerMD
	Members []BrokerMD
}

type ConsumersGroupMD struct {
	Consumers []ConsumerMD
}

type PartitionMD struct {
	Name      string
	AddrUrl   string
	Brokers   BrokersGroupMD
	Consumers ConsumersGroupMD
}
type MetaDataController struct {
	MetaDataRaft *Raft.RaftNode
	mu           sync.RWMutex
	MD           MetaData
}

func (md *MetaData) GetIncreaseID() uint32 {
	return atomic.AddUint32(&md.SelfIncrementId, 1)
}

func (c *ConsumerMD) Copy() ConsumerMD {
	part := make([]string, len(c.Config.FocalPartitions))
	copy(part, c.Config.FocalPartitions)
	return ConsumerMD{
		Cred: pb.Credentials{
			Identity: c.Cred.Identity,
			Id:       c.Cred.Id,
			Hash:     c.Cred.Hash,
		},
		Config: pb.RegisterConsumerRequest{
			FocalTopic:           c.Config.FocalTopic,
			FocalPartitions:      part,
			MaxReturnMessageSize: c.Config.MaxReturnMessageSize,
			TimeoutSessionMsec:   c.Config.TimeoutSessionMsec,
			OffsetResetMode:      c.Config.OffsetResetMode,
		},
	}
}

func (p *ProducerMD) Copy() ProducerMD {
	return ProducerMD{
		Cred: pb.Credentials{
			Identity: p.Cred.Identity,
			Id:       p.Cred.Id,
			Hash:     p.Cred.Hash,
		},
		Config: pb.RegisterProducerRequest{
			FocalTopic:         p.Config.FocalTopic,
			MaxPushMessageSize: p.Config.MaxPushMessageSize,
		},
	}
}

func (b *BrokerMD) Copy() BrokerMD {
	tb := *b
	return tb
}

func (bg *BrokersGroupMD) Copy() BrokersGroupMD {
	tbg := BrokersGroupMD{
		Leader:  bg.Leader.Copy(),
		Members: make([]BrokerMD, len(bg.Members)),
	}
	copy(tbg.Members, bg.Members)
	return tbg
}

func (cg *ConsumersGroupMD) Copy() ConsumersGroupMD {
	tcg := ConsumersGroupMD{Consumers: make([]ConsumerMD, len(cg.Consumers))}
	copy(tcg.Consumers, cg.Consumers)
	return tcg
}

func (p *PartitionMD) Copy() PartitionMD {
	return PartitionMD{
		Name:      p.Name,
		AddrUrl:   p.AddrUrl,
		Brokers:   p.Brokers.Copy(),
		Consumers: p.Consumers.Copy(),
	}
}

type TopicMD struct {
	Name       string
	Partitions []*PartitionMD
}

func (t *TopicMD) Copy() TopicMD {
	tt := TopicMD{
		Name:       t.Name,
		Partitions: make([]*PartitionMD, len(t.Partitions)),
	}
	for i := range t.Partitions {
		p := t.Partitions[i].Copy()
		tt.Partitions[i] = &p
	}
	return tt
}

func (md *MetaData) SnapShot() []byte {
	buffer := bytes.Buffer{}
	encoder := Gob.NewEncoder(&buffer)
	err := encoder.Encode(*md)
	if err != nil {
		panic(err)
	}
	return buffer.Bytes()
}

const (
	MetadataCommandRegisterProducer   = "rp"
	MetadataCommandRegisterConsumer   = "rc"
	MetadataCommandUnRegisterProducer = "urp"
	MetadataCommandUnRegisterConsumer = "urc"
	//MetadataCommandRegisterProducer = "rp"
	//MetadataCommandRegisterProducer = "rp"
)

type MetadataCommand struct {
	Mode string
	data interface{}
}

func (mdc *MetaDataController) IsLeader() bool {
	return mdc.MetaDataRaft.IsLeader()
}

const ErrSourceNotExist = "ErrSourceNotExist"
const ErrSourceAlreadyExist = "ErrSourceAlreadyExist"
const ErrSourceNotEnough = "ErrSourceNotEnough"
const ErrRequestIllegal = "ErrRequestIllegal"

func (md *MetaData) CheckTopic(t string) bool {
	//mdc.mu.RLock()
	//defer mdc.mu.RUnlock()
	_, ok := md.tpTree[t]
	return ok
}

func (md *MetaData) QueryTopic(t string) (*TopicMD, error) {
	//mdc.mu.RLock()
	//defer mdc.mu.RUnlock()
	if i, ok := md.tpTree[t]; ok {
		p := i.Copy()
		return &p, nil
	} else {
		return nil, errors.New(ErrSourceNotExist)
	}
}

func (md *MetaData) CheckConsumer(id uint32) bool {
	//mdc.mu.RLock()
	//defer mdc.mu.RUnlock()
	_, ok := md.Consumers[id]
	return ok
}

func (md *MetaData) QueryConsumer(id uint32) (*ConsumerMD, error) {
	//mdc.mu.RLock()
	//defer mdc.mu.RUnlock()
	if i, ok := md.Consumers[id]; ok {
		p := i.Copy()
		return &p, nil
	} else {
		return nil, errors.New(ErrSourceNotExist)
	}
}

func (md *MetaData) CheckProducer(id uint32) bool {
	_, ok := md.Producers[id]
	return ok
}

func (md *MetaData) QueryProducer(id uint32) (*ProducerMD, error) {
	//mdc.mu.RLock()
	//defer mdc.mu.RUnlock()
	if i, ok := md.Producers[id]; ok {
		p := i.Copy()
		return &p, nil
	} else {
		return nil, errors.New(ErrSourceNotExist)
	}
}

func (md *MetaData) GetFreeBrokers(brokersNum int32) ([]*BrokerMD, error) {
	if brokersNum == 0 {
		return nil, errors.New(ErrRequestIllegal)
	} else if brokersNum > int32(len(md.Brokers)) {
		return nil, errors.New(ErrSourceNotEnough)
	} else {
		tmp := make([]*BrokerMD, len(md.Brokers))
		copy(tmp, md.Brokers)
		sort.Slice(tmp, func(i, j int) bool {
			return tmp[i].PartitionNum < tmp[j].PartitionNum
		})
		return tmp[:brokersNum], nil
	}
}

func (mdc *MetaDataController) RegisterProducer(request *pb.RegisterProducerRequest) *pb.RegisterProducerResponse {
	if !mdc.IsLeader() {
		goto NotLeader
	}
	mdc.mu.RLock()
	if !mdc.MD.CheckTopic(request.FocalTopic) {
		goto NotExist
	} else {
		// 在这里做好分配
		// 在handle中去做挂树？
		p := ProducerMD{
			Cred: pb.Credentials{
				Identity: pb.Credentials_Producer,
				Id:       mdc.MD.GetIncreaseID(),
				Hash:     0,
			},
			Config: pb.RegisterProducerRequest{
				FocalTopic:         request.GetFocalTopic(),
				MaxPushMessageSize: request.GetMaxPushMessageSize(),
			},
		}
		mdc.mu.RUnlock()
		err := mdc.MetaDataRaft.Commit(MetadataCommand{Mode: MetadataCommandRegisterProducer, data: p})
		mdc.mu.RLock()
		if err != nil {
			switch err.Error() {
			case Raft.ErrNotLeader:
				goto NotLeader
			case Raft.ErrCommitTimeout:
				goto TimeOut
			}
		}
		tp, err := mdc.MD.QueryTopic(request.FocalTopic)
		if err.Error() == ErrSourceNotExist {
			goto NotExist
		}
		res := pb.RegisterProducerResponse{
			Response:        ResponseSuccess(),
			Credential:      &p.Cred,
			FocalPartitions: make([]*pb.Partition, len(tp.Partitions)),
		}
		for _, partition := range tp.Partitions {
			res.FocalPartitions = append(res.FocalPartitions, &pb.Partition{
				Topic: request.FocalTopic,
				Name:  partition.Name,
				Url:   partition.AddrUrl,
			})
		}
		mdc.mu.RUnlock()
		return &res
	}
NotLeader:
	mdc.mu.RUnlock()
	return &pb.RegisterProducerResponse{
		Response: ErrResponse_ErrNotLeader(),
	}
TimeOut:
	mdc.mu.RLock()
	return &pb.RegisterProducerResponse{
		Response: ErrResponse_ErrTimeout(),
	}
NotExist:
	mdc.mu.RLock()
	return &pb.RegisterProducerResponse{
		Response: ErrResponse_ErrSourceNotExist(),
	}
}

func (mdc *MetaDataController) Handle(command interface{}) {
	Cmd, ok := command.(MetadataCommand)
	if !ok {
		panic("Not Reflect Right Variant")
	}
	switch Cmd.Mode {
	case MetadataCommandRegisterProducer:
		data, ok := Cmd.data.(ProducerMD)
		if !ok {
			panic("Not Reflect Right Variant")
		}
		mdc.mu.Lock()
		defer mdc.mu.Unlock()
		mdc.MD.Producers[data.Cred.Id] = &data

	default:
		panic(Cmd)
	}
}

func (mdc *MetaDataController) MakeSnapshot() []byte {
	mdc.mu.RLock()
	defer mdc.mu.RUnlock()
	return mdc.MD.SnapShot()
}
func (mdc *MetaDataController) LoadSnapshot(bt []byte) {
	i := MetaData{}
	buffer := bytes.NewBuffer(bt)
	err := Gob.NewDecoder(buffer).Decode(&i)
	if err != nil {
		panic(err)
	}
	mdc.mu.Lock()
	defer mdc.mu.Unlock()
	mdc.MD = i
}
