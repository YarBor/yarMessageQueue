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
	Members []*BrokerMD
}

type ConsumersGroupMD struct {
	Consumers []ConsumerMD
}

type PartitionMD struct {
	Name      string
	Brokers   BrokersGroupMD
	Consumers ConsumersGroupMD
}
type MetaDataController struct {
	MetaDataRaft *Raft.RaftNode
	idMap        syncIdMap
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
			Key:      c.Cred.Key,
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
			Key:      p.Cred.Key,
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
		Members: make([]*BrokerMD, len(bg.Members)),
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
	MetadataCommandCreateTopic        = "ct"
	//MetadataCommandRegisterProducer = "rp"
	ErrSourceNotExist        = "ErrSourceNotExist"
	ErrSourceAlreadyExist    = "ErrSourceAlreadyExist"
	ErrSourceNotEnough       = "ErrSourceNotEnough"
	ErrRequestIllegal        = "ErrRequestIllegal"
	ErrRequestTimeout        = "ErrRequestTimeout"
	ErrRequestServerNotServe = "ErrRequestServerNotServe"
	ErrRequestNotLeader      = "ErrRequestNotLeader"
)

func ErrToResponse(err error) *pb.Response {
	if err == nil {
		return ResponseSuccess()
	}
	switch err.Error() {
	case ErrSourceNotExist:
		return ErrResponse_ErrSourceNotExist()
	case ErrSourceAlreadyExist:
		return ErrResponse_ErrSourceAlreadyExist()
	case ErrSourceNotEnough:
		return ErrResponse_ErrSourceNotEnough()
	case ErrRequestIllegal:
		return ErrResponse_ErrRequestIllegal()
	case ErrRequestTimeout:
		return ErrResponse_ErrTimeout()
	case ErrRequestServerNotServe:
		return ErrResponse_NotServer()
	case ErrRequestNotLeader:
		return ErrResponse_ErrNotLeader()
	default:
		panic(err)
	}
}

type MetadataCommand struct {
	Id   uint32
	Mode string
	data interface{}
}

func (mdc *MetaDataController) IsLeader() bool {
	return mdc.MetaDataRaft.IsLeader()
}

func (md *MetaData) CheckTopic(t string) error {
	//mdc.mu.RLock()
	//defer mdc.mu.RUnlock()
	_, ok := md.tpTree[t]
	if ok {
		return errors.New(ErrSourceAlreadyExist)
	}
	return nil
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

func (md *MetaData) CheckConsumer(id uint32) error {
	//mdc.mu.RLock()
	//defer mdc.mu.RUnlock()
	//_, ok := md.Consumers[id]
	//return ok
	_, ok := md.Consumers[id]
	if ok {
		return errors.New(ErrSourceAlreadyExist)
	}
	return nil
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

func (md *MetaData) CheckProducer(id uint32) error {
	//_, ok := md.Producers[id]
	//return ok
	_, ok := md.Producers[id]
	if ok {
		return errors.New(ErrSourceAlreadyExist)
	}
	return nil
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

// 线程安全 对数据之读 原子改
func (md *MetaData) GetFreeBrokers(brokersNum int32) ([]*BrokerMD, error) {
	if brokersNum == 0 {
		return nil, errors.New(ErrRequestIllegal)
	} else if brokersNum > int32(len(md.Brokers)) {
		tmp := make([]*BrokerMD, len(md.Brokers))
		copy(tmp, md.Brokers)
		return tmp, nil
	} else {
		tmp := make([]*BrokerMD, len(md.Brokers))
		copy(tmp, md.Brokers)
		sort.Slice(tmp, func(i, j int) bool {
			return atomic.LoadUint32(&tmp[i].PartitionNum) < atomic.LoadUint32(&tmp[j].PartitionNum)
		})
		return tmp[:brokersNum], nil
	}
}

func (mdc *MetaDataController) RegisterProducer(request *pb.RegisterProducerRequest) *pb.RegisterProducerResponse {
	if !mdc.IsLeader() {
		return &pb.RegisterProducerResponse{Response: ErrResponse_ErrNotLeader()}
	}
	mdc.mu.RLock()
	if mdc.MD.CheckTopic(request.FocalTopic) != nil {
		mdc.mu.RUnlock()
		return &pb.RegisterProducerResponse{Response: ErrResponse_ErrSourceNotExist()}
	} else {
		// 在这里做好分配
		// 在handle中去做挂树？
		p := ProducerMD{
			Cred: pb.Credentials{
				Identity: pb.Credentials_Producer,
				Id:       mdc.MD.GetIncreaseID(),
			},
			Config: pb.RegisterProducerRequest{
				FocalTopic:         request.GetFocalTopic(),
				MaxPushMessageSize: request.GetMaxPushMessageSize(),
			},
		}
		mdc.mu.RUnlock()
		err := mdc.commit(MetadataCommandRegisterProducer, p)
		if err != nil {
			return &pb.RegisterProducerResponse{Response: ErrToResponse(err)}
		}
		mdc.mu.RLock()
		tp, err := mdc.MD.QueryTopic(request.FocalTopic)
		if err.Error() == ErrSourceNotExist {
			mdc.mu.RUnlock()
			return &pb.RegisterProducerResponse{Response: ErrToResponse(err)}
		}
		res := pb.RegisterProducerResponse{
			Response:        ResponseSuccess(),
			Credential:      &p.Cred,
			FocalPartitions: make([]*pb.Partition, len(tp.Partitions)),
		}
		for _, partition := range tp.Partitions {
			p := &pb.Partition{
				Topic: request.FocalTopic,
				Name:  partition.Name,
				Urls:  make([]string, 0, len(partition.Brokers.Members)),
			}
			for _, broker := range partition.Brokers.Members {
				p.Urls = append(p.Urls, broker.Url)
			}
			res.FocalPartitions = append(res.FocalPartitions, p)
		}
		mdc.mu.RUnlock()
		return &res
	}

}

func (mdc *MetaDataController) Handle(command interface{}) {
	Cmd, ok := command.(MetadataCommand)
	var errrrr error = nil
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
	case MetadataCommandCreateTopic:
		data, ok := Cmd.data.(TopicMD)
		if !ok {
			panic("Not Reflect Right Variant")
		}
	default:
		panic(Cmd)
	}
	mdc.idMap.GetCallDelete(Cmd.Id, errrrr)
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

func (mdc *MetaDataController) CreateTopic(req *pb.CreateTopicRequest) *pb.CreateTopicResponse {
	mdc.mu.RLock()
	err := mdc.MD.CheckTopic(req.Topic)
	if err != nil {
		mdc.mu.RUnlock()
		return &pb.CreateTopicResponse{Response: ErrResponse_ErrSourceAlreadyExist()}
	} else {
		// 查数据合法
		// 构建TopicMD
		// 期望是 for { GetFreeBrokers（） }
		// 交给Handle去构建
		for _, p := range req.Partition {
			if int(p.ReplicationNumber) > len(mdc.MD.Brokers) {
				return &pb.CreateTopicResponse{Response: ErrResponse_ErrSourceNotEnough()}
			}
		}
		tp := TopicMD{
			Name:       req.Topic,
			Partitions: make([]*PartitionMD, 0, len(req.Partition)),
		}
		for _, p := range req.Partition {
			i, err := mdc.MD.GetFreeBrokers(p.ReplicationNumber)
			if err != nil {
				return &pb.CreateTopicResponse{Response: ErrToResponse(err)}
			} else {
				for _, q := range i {
					atomic.AddUint32(&q.PartitionNum, 1)
				}
			}
			tp.Partitions = append(tp.Partitions, &PartitionMD{
				Name: p.PartitionName,
				Brokers: BrokersGroupMD{
					Members: i,
				},
				Consumers: ConsumersGroupMD{Consumers: make([]ConsumerMD, 0)},
			})
		}
		mdc.mu.RUnlock()
		err = mdc.commit(MetadataCommandCreateTopic, tp)

		// 构建response
		if err != nil {
			return &pb.CreateTopicResponse{Response: ErrToResponse(err)}
		} else {
			res := &pb.CreateTopicResponse{Response: ResponseSuccess(), PartitionDetails: make([]*pb.Partition, len(tp.Partitions))}
			for i := range tp.Partitions {
				res.PartitionDetails[i] = &pb.Partition{
					Topic: req.Topic,
					Name:  tp.Partitions[i].Name,
					Urls:  make([]string, len(tp.Partitions[i].Brokers.Members)),
				}
				for m := range tp.Partitions[i].Brokers.Members {
					res.PartitionDetails[m].Urls[i] = tp.Partitions[i].Brokers.Members[m].Url
				}
			}
		}
	}
}

type syncIdMap struct {
	id  uint32
	mu  sync.Mutex
	Map map[uint32]struct {
		fn func(error)
	}
}

func (s *syncIdMap) GetID() uint32 {
	return atomic.AddUint32(&s.id, 1)
}

func (s *syncIdMap) Add(i uint32, fn func(error)) {
	s.mu.Lock()
	s.Map[i] = struct{ fn func(error) }{fn: fn}
	s.mu.Unlock()
}
func (s *syncIdMap) GetCallDelete(i uint32, err error) {
	s.mu.Lock()
	f, ok := s.Map[i]
	if ok {
		defer f.fn(err)
		delete(s.Map, i)
	}
	s.mu.Unlock()
}
func (s *syncIdMap) Delete(i uint32) {
	s.mu.Lock()
	delete(s.Map, i)
	s.mu.Unlock()
}

func (mdc *MetaDataController) commit(mode string, data interface{}) error {
	id := mdc.idMap.GetID()
	ch := make(chan error, 1)
	mdc.idMap.Add(id, func(i error) {
		ch <- i
	})
	err := mdc.MetaDataRaft.Commit(MetadataCommand{
		Id:   id,
		Mode: mode,
		data: data,
	})
	if err != nil {
		goto ERR
	}
	return <-ch
ERR:
	switch err.Error() {
	case Raft.ErrCommitTimeout:
		return errors.New(ErrRequestTimeout)
	case Raft.ErrNodeDidNotStart:
		return errors.New(ErrRequestServerNotServe)
	case Raft.ErrNotLeader:
		return errors.New(ErrRequestNotLeader)
	default:
		panic(err)
	}
}
