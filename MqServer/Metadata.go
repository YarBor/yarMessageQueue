package MqServer

import (
	"MqServer/Raft"
	"MqServer/Raft/Pack"
	pb "MqServer/rpc"
	"bytes"
	"errors"
	"sort"
	"sync"
	"sync/atomic"
)

type MetaData struct {
	SelfIncrementId uint32
	Brokers         map[string]*BrokerMD
	tpTree          map[string]*TopicMD
	Consumers       map[uint32]*ConsumerMD
	Producers       map[uint32]*ProducerMD
}
type CredentialsMD struct {
	Identity pb.Credentials_CredentialsIdentity
	Id       uint32
}
type ConsumerMD struct {
	Cred                 CredentialsMD
	FocalTopic           string
	FocalPartitions      []string
	MaxReturnMessageSize int32
	TimeoutSessionMsec   int32
	OffsetResetMode      pb.RegisterConsumerRequest_OffsetReset
}

type ProducerMD struct {
	Cred               CredentialsMD
	FocalTopic         string
	MaxPushMessageSize int32
}

type BrokerMD struct {
	Name         string
	Url          string
	PartitionNum uint32
}

type BrokersGroupMD struct {
	Members []*struct {
		Name string
		Url  string
	}
}

type ConsumersGroupMD struct {
	Consumers []*ConsumerMD
}

type PartitionMD struct {
	Name          string
	BrokerGroup   BrokersGroupMD
	ConsumerGroup ConsumersGroupMD
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
	a := ConsumerMD{
		Cred:                 c.Cred,
		FocalTopic:           c.FocalTopic,
		FocalPartitions:      make([]string, len(c.FocalPartitions)),
		MaxReturnMessageSize: c.MaxReturnMessageSize,
		TimeoutSessionMsec:   c.TimeoutSessionMsec,
		OffsetResetMode:      c.OffsetResetMode,
	}
	a.FocalPartitions = append(a.FocalPartitions, c.FocalPartitions...)
	return a
}

func (p *ProducerMD) Copy() ProducerMD {
	a := ProducerMD{
		Cred:               p.Cred,
		FocalTopic:         p.FocalTopic,
		MaxPushMessageSize: p.MaxPushMessageSize,
	}
	return a
}

func (b *BrokerMD) Copy() BrokerMD {
	tb := *b
	return tb
}

func (bg *BrokersGroupMD) Copy() BrokersGroupMD {
	tbg := BrokersGroupMD{
		Members: make([]*struct {
			Name string
			Url  string
		}, len(bg.Members)),
	}
	copy(tbg.Members, bg.Members)
	return tbg
}

func (cg *ConsumersGroupMD) Copy() ConsumersGroupMD {
	tcg := ConsumersGroupMD{Consumers: make([]*ConsumerMD, len(cg.Consumers))}
	copy(tcg.Consumers, cg.Consumers)
	return tcg
}

func (p *PartitionMD) Copy() PartitionMD {
	return PartitionMD{
		Name:          p.Name,
		BrokerGroup:   p.BrokerGroup.Copy(),
		ConsumerGroup: p.ConsumerGroup.Copy(),
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
	encoder := Pack.NewEncoder(&buffer)
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
	MetadataCommandDestroyTopic       = "dt"
	ErrSourceNotExist                 = "ErrSourceNotExist"
	ErrSourceAlreadyExist             = "ErrSourceAlreadyExist"
	ErrSourceNotEnough                = "ErrSourceNotEnough"
	ErrRequestIllegal                 = "ErrRequestIllegal"
	ErrRequestTimeout                 = "ErrRequestTimeout"
	ErrRequestServerNotServe          = "ErrRequestServerNotServe"
	ErrRequestNotLeader               = "ErrRequestNotLeader"
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

func (md *MetaData) GetFreeBrokers(brokersNum int32) ([]*BrokerMD, error) {
	if brokersNum == 0 {
		return nil, errors.New(ErrRequestIllegal)
	} else if brokersNum > int32(len(md.Brokers)) {
		tmp := make([]*BrokerMD, len(md.Brokers))
		//copy(tmp, md.Brokers)
		for _, v := range md.Brokers {
			tmp = append(tmp, v)
		}
		return tmp, nil
	} else {
		tmp := make([]*BrokerMD, len(md.Brokers))
		for _, v := range md.Brokers {
			tmp = append(tmp, v)
		}
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
	tp, err := mdc.MD.QueryTopic(request.FocalTopic)
	mdc.mu.RUnlock()

	if err != nil {
		return &pb.RegisterProducerResponse{Response: ErrResponse_ErrSourceNotExist()}
	} else {
		// 在这里做好分配
		// 在handle中去做挂树？
		p := ProducerMD{
			Cred: CredentialsMD{
				Identity: pb.Credentials_Producer,
				Id:       mdc.MD.GetIncreaseID(),
			},

			FocalTopic:         request.GetFocalTopic(),
			MaxPushMessageSize: request.GetMaxPushMessageSize(),
		}
		res := pb.RegisterProducerResponse{
			Response: ResponseSuccess(),
			Credential: &pb.Credentials{
				Identity: p.Cred.Identity,
				Id:       p.Cred.Id,
			},
			FocalPartitions: make([]*pb.Partition, len(tp.Partitions)),
		}
		for _, partition := range tp.Partitions {
			p := &pb.Partition{
				Topic: request.FocalTopic,
				Name:  partition.Name,
				Urls:  make([]string, 0, len(partition.BrokerGroup.Members)),
			}
			for _, broker := range partition.BrokerGroup.Members {
				p.Urls = append(p.Urls, broker.Url)
			}
			res.FocalPartitions = append(res.FocalPartitions, p)
		}
		err = mdc.commit(MetadataCommandRegisterProducer, p)
		if err != nil {
			return &pb.RegisterProducerResponse{Response: ErrToResponse(err)}
		}
		return &res
	}

}

func (mdc *MetaDataController) Handle(command interface{}) {
	Cmd, ok := command.(MetadataCommand)
	var ReturnErr error = nil
	defer func(err *error) {
		mdc.idMap.GetCallDelete(Cmd.Id, *err)
	}(&ReturnErr)
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
		if ReturnErr = mdc.MD.CheckProducer(data.Cred.Id); ReturnErr != nil {
			mdc.MD.Producers[data.Cred.Id] = &data
		}
		mdc.mu.Unlock()
	case MetadataCommandCreateTopic:
		data, ok := Cmd.data.(TopicMD)
		if !ok {
			panic("Not Reflect Right Variant")
		} else {
			mdc.mu.Lock()
			if ReturnErr = mdc.MD.CheckTopic(data.Name); ReturnErr == nil {
				for _, q := range data.Partitions {
					for _, p := range q.BrokerGroup.Members {
						atomic.AddUint32(&mdc.MD.Brokers[p.Name].PartitionNum, 1)
					}
				}
				mdc.MD.tpTree[data.Name] = &data
			}
			mdc.mu.Unlock()
		}
	case MetadataCommandRegisterConsumer:
		data, ok := Cmd.data.(ConsumerMD)
		if !ok {
			panic("Not Reflect Right Variant")
		}
		mdc.mu.Lock()
		if tp, ok := mdc.MD.tpTree[data.FocalTopic]; ok {
			tmpPar := make([]*PartitionMD, 0, len(data.FocalPartitions))
			for _, partition := range data.FocalPartitions {
				for _, tpp := range tp.Partitions {
					if tpp.Name == partition {
						tmpPar = append(tmpPar, tpp)
						goto next1
					}
				}
				ReturnErr = errors.New(ErrSourceNotExist)
				break
			next1:
			}
			if ReturnErr != nil {
				for i := range tmpPar {
					tmpPar[i].ConsumerGroup.Consumers = append(tmpPar[i].ConsumerGroup.Consumers, &data)
				}
				if _, ok := mdc.MD.Consumers[data.Cred.Id]; ok {
					mdc.MD.Consumers[data.Cred.Id] = &data
				} else {
					panic("Ub")
				}
			} else {
			}
		} else {
			ReturnErr = errors.New(ErrSourceNotExist)
		}
		mdc.mu.Unlock()
	case MetadataCommandUnRegisterConsumer:
		data, ok := Cmd.data.(ConsumerMD)
		if !ok {
			panic("Invalid Reflect Var")
		}
		mdc.mu.Lock()
		if tp, ok := mdc.MD.tpTree[data.FocalTopic]; !ok {
			ReturnErr = errors.New(ErrSourceNotExist)
		} else {
			for i2 := range data.FocalPartitions {
				for i := range tp.Partitions {
					if data.FocalPartitions[i2] == tp.Partitions[i].Name {
						for i3 := range tp.Partitions[i].ConsumerGroup.Consumers {
							if tp.Partitions[i].ConsumerGroup.Consumers[i3].Cred.Id == data.Cred.Id {
								tp.Partitions[i].ConsumerGroup.Consumers = append(tp.Partitions[i].ConsumerGroup.Consumers[:i3], tp.Partitions[i].ConsumerGroup.Consumers[i3+1:]...)
								goto next2
							}
						}
					}
				}
				// 不应该跑到这一步
				panic("Ub")
			next2:
			}
		}
		delete(mdc.MD.Consumers, data.Cred.Id)
		mdc.mu.Unlock()
	case MetadataCommandUnRegisterProducer:
		data, ok := Cmd.data.(ProducerMD)
		if !ok {
			panic("Invalid Reflect Var")
		}
		mdc.mu.Lock()
		if _, ok := mdc.MD.Producers[data.Cred.Id]; ok {
			delete(mdc.MD.Producers, data.Cred.Id)
		} else {
			ReturnErr = errors.New(ErrSourceNotExist)
		}
		mdc.mu.Unlock()
	case MetadataCommandDestroyTopic:
		targetName, ok := Cmd.data.(string)
		if !ok {
			panic("Invalid Reflect Var")
		}
		mdc.mu.Lock()
		if tp, ok := mdc.MD.tpTree[targetName]; ok {
			ConsumerIdList := []uint32{}
			for _, partition := range tp.Partitions {
				for _, consumer := range partition.ConsumerGroup.Consumers {
					ConsumerIdList = append(ConsumerIdList, consumer.Cred.Id)
				}
				for _, member := range partition.BrokerGroup.Members {
					atomic.AddUint32(&mdc.MD.Brokers[member.Name].PartitionNum, -1)
				}
			}
			for _, u := range ConsumerIdList {
				delete(mdc.MD.Consumers, u)
			}
		} else {
			ReturnErr = errors.New(ErrSourceNotExist)
		}
		mdc.mu.Unlock()
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
	err := Pack.NewDecoder(buffer).Decode(&i)
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
	if err == nil {
		mdc.mu.RUnlock()
		return &pb.CreateTopicResponse{Response: ErrResponse_ErrSourceAlreadyExist()}
	} else {
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
			}
			tp.Partitions = append(tp.Partitions, &PartitionMD{
				Name: p.PartitionName,
				BrokerGroup: BrokersGroupMD{
					Members: make([]*struct {
						Name string
						Url  string
					}, 0, len(i)),
				},
				ConsumerGroup: ConsumersGroupMD{Consumers: make([]*ConsumerMD, 0)},
			})
			for _, md := range i {
				tp.Partitions[len(tp.Partitions)-1].BrokerGroup.Members = append(tp.Partitions[len(tp.Partitions)-1].BrokerGroup.Members, &struct {
					Name string
					Url  string
				}{Name: md.Name, Url: md.Url})
			}
		}
		mdc.mu.RUnlock()
		err = mdc.commit(MetadataCommandCreateTopic, tp)

		// 构建response
		if err != nil {
			return &pb.CreateTopicResponse{Response: ErrToResponse(err)}
		}
		res := &pb.CreateTopicResponse{Response: ResponseSuccess(), PartitionDetails: make([]*pb.Partition, len(tp.Partitions))}
		for i := range tp.Partitions {
			res.PartitionDetails[i] = &pb.Partition{
				Topic: req.Topic,
				Name:  tp.Partitions[i].Name,
				Urls:  make([]string, len(tp.Partitions[i].BrokerGroup.Members)),
			}
			for m := range tp.Partitions[i].BrokerGroup.Members {
				res.PartitionDetails[m].Urls[i] = tp.Partitions[i].BrokerGroup.Members[m].Url
			}
		}
		return res
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
func (mdc *MetaDataController) RegisterConsumer(req *pb.RegisterConsumerRequest) *pb.RegisterConsumerResponse {
	if !mdc.IsLeader() {
		return &pb.RegisterConsumerResponse{Response: ErrResponse_ErrNotLeader()}
	}
	mdc.mu.RLock()
	tp, err := mdc.MD.QueryTopic(req.FocalTopic)
	mdc.mu.RUnlock()
	if err != nil {
		return &pb.RegisterConsumerResponse{Response: ErrResponse_ErrSourceAlreadyExist()}
	}
	parts := make([]*pb.Partition, 0, len(req.FocalPartitions))
	for i := range req.FocalPartitions {
		for _, partition := range tp.Partitions {
			if req.FocalPartitions[i] == partition.Name {
				urls := make([]string, 0, len(partition.BrokerGroup.Members))
				for _, m := range partition.BrokerGroup.Members {
					urls = append(urls, m.Url)
				}
				parts = append(parts, &pb.Partition{
					Topic: req.FocalTopic,
					Name:  partition.Name,
					Urls:  urls,
				})
				goto next
			}
		}
		return &pb.RegisterConsumerResponse{Response: ErrResponse_ErrSourceNotExist()}
	next:
	}
	c := ConsumerMD{
		Cred: CredentialsMD{
			Identity: pb.Credentials_Consumer,
			Id:       mdc.MD.GetIncreaseID(),
		},
		FocalTopic:           req.FocalTopic,
		FocalPartitions:      req.FocalPartitions,
		MaxReturnMessageSize: req.MaxReturnMessageSize,
		TimeoutSessionMsec:   req.TimeoutSessionMsec,
		OffsetResetMode:      req.OffsetResetMode,
	}
	err = mdc.commit(MetadataCommandRegisterConsumer, c)
	if err != nil {
		return &pb.RegisterConsumerResponse{Response: ErrToResponse(err)}
	}
	res := &pb.RegisterConsumerResponse{
		Response: ResponseSuccess(),
		Credential: &pb.Credentials{
			Identity: c.Cred.Identity,
			Id:       c.Cred.Id,
		},
		FocalPartitions: parts,
	}
	return res
}
func (mdc *MetaDataController) UnRegisterConsumer(req *pb.UnRegisterConsumerRequest) *pb.UnRegisterConsumerResponse {
	if mdc.IsLeader() == false {
		return &pb.UnRegisterConsumerResponse{Response: ErrResponse_ErrNotLeader()}
	}
	mdc.mu.RLock()
	Consumer, err := mdc.MD.QueryConsumer(req.Credential.Id)
	mdc.mu.RUnlock()
	if err != nil {
		return &pb.UnRegisterConsumerResponse{Response: ErrToResponse(err)}
	}
	err = mdc.commit(MetadataCommandUnRegisterConsumer, *Consumer)
	if err != nil {
		return &pb.UnRegisterConsumerResponse{Response: ErrToResponse(err)}
	}
	return &pb.UnRegisterConsumerResponse{Response: ResponseSuccess()}
}
func (mdc *MetaDataController) UnRegisterProducer(req *pb.UnRegisterProducerRequest) *pb.UnRegisterProducerResponse {
	if !mdc.IsLeader() {
		return &pb.UnRegisterProducerResponse{Response: ErrResponse_ErrNotLeader()}
	}
	mdc.mu.RLock()
	Producer, err := mdc.MD.QueryProducer(req.Credential.Id)
	mdc.mu.RUnlock()
	if err == nil {
		return &pb.UnRegisterProducerResponse{Response: ErrToResponse(err)}
	}
	err = mdc.commit(MetadataCommandUnRegisterProducer, *Producer)
	if err != nil {
		return &pb.UnRegisterProducerResponse{Response: ErrToResponse(err)}
	}
	return &pb.UnRegisterProducerResponse{Response: ResponseSuccess()}
}
func (mdc *MetaDataController) DestroyTopic(req *pb.DestroyTopicRequest) *pb.DestroyTopicResponse {
	if !mdc.IsLeader() {
		return &pb.DestroyTopicResponse{Response: ErrResponse_ErrNotLeader()}
	}
	mdc.mu.RLock()
	err := mdc.MD.CheckTopic(req.Topic)
	mdc.mu.RUnlock()
	if err != nil {
		return &pb.DestroyTopicResponse{Response: ErrToResponse(err)}
	}
	err = mdc.commit(MetadataCommandDestroyTopic, req.Topic)
	if err != nil {
		return &pb.DestroyTopicResponse{Response: ErrToResponse(err)}
	}
	return &pb.DestroyTopicResponse{Response: ResponseSuccess()}
}
