package MqServer

import (
	"MqServer/Raft"
	"MqServer/Raft/Pack"
	pb "MqServer/rpc"
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
	FocalPartitions      string
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
	// TODO: 应该是一个消费组 共享一套设置？？？
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
		FocalPartitions:      c.FocalPartitions,
		MaxReturnMessageSize: c.MaxReturnMessageSize,
		TimeoutSessionMsec:   c.TimeoutSessionMsec,
		OffsetResetMode:      c.OffsetResetMode,
	}
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
	bt, err := Pack.Marshal(*md)
	if err != nil {
		panic(err)
	}
	return bt
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
		return ResponseErrSourceNotExist()
	case ErrSourceAlreadyExist:
		return ResponseErrSourceAlreadyExist()
	case ErrSourceNotEnough:
		return ResponseErrSourceNotEnough()
	case ErrRequestIllegal:
		return ResponseErrRequestIllegal()
	case ErrRequestTimeout:
		return ResponseErrTimeout()
	case ErrRequestServerNotServe:
		return ResponseNotServer()
	case ErrRequestNotLeader:
		return ResponseErrNotLeader()
	default:
		panic(err)
	}
}

type MetadataCommand struct {
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
		return &pb.RegisterProducerResponse{Response: ResponseErrNotLeader()}
	}
	mdc.mu.RLock()
	tp, err := mdc.MD.QueryTopic(request.FocalTopic)
	mdc.mu.RUnlock()

	if err != nil {
		return &pb.RegisterProducerResponse{Response: ResponseErrSourceNotExist()}
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

func (mdc *MetaDataController) Handle(command interface{}) error {
	Cmd, ok := command.(MetadataCommand)
	var ReturnErr error = nil

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
			var tmpPar *PartitionMD
			for _, tpp := range tp.Partitions {
				if tpp.Name == data.FocalPartitions {
					tmpPar = tpp
					goto next1
				}
			}
			ReturnErr = errors.New(ErrSourceNotExist)
		next1:
			if ReturnErr != nil {
				if _, ok := mdc.MD.Consumers[data.Cred.Id]; !ok {
					mdc.MD.Consumers[data.Cred.Id] = &data
					tmpPar.ConsumerGroup.Consumers = append(tmpPar.ConsumerGroup.Consumers, &data)
				} else {
					ReturnErr = errors.New(ErrSourceAlreadyExist)
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
			for tpPaIndex := range tp.Partitions {
				if data.FocalPartitions == tp.Partitions[tpPaIndex].Name {
					for conIndex := range tp.Partitions[tpPaIndex].ConsumerGroup.Consumers {
						if tp.Partitions[tpPaIndex].ConsumerGroup.Consumers[conIndex].Cred.Id == data.Cred.Id {
							tp.Partitions[tpPaIndex].ConsumerGroup.Consumers = append(tp.Partitions[tpPaIndex].ConsumerGroup.Consumers[:conIndex], tp.Partitions[tpPaIndex].ConsumerGroup.Consumers[conIndex+1:]...)
							goto next2
						}
					}
				}
				// 不应该跑到这一步
				ReturnErr = errors.New(ErrSourceNotExist)
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
	return ReturnErr
}

func (mdc *MetaDataController) MakeSnapshot() []byte {
	mdc.mu.RLock()
	defer mdc.mu.RUnlock()
	return mdc.MD.SnapShot()
}
func (mdc *MetaDataController) LoadSnapshot(bt []byte) {
	i := MetaData{}
	err := Pack.Unmarshal(bt, &i)
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
		return &pb.CreateTopicResponse{Response: ResponseErrSourceAlreadyExist()}
	} else {
		for _, p := range req.Partition {
			if int(p.ReplicationNumber) > len(mdc.MD.Brokers) {
				return &pb.CreateTopicResponse{Response: ResponseErrSourceNotEnough()}
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
	err := mdc.MetaDataRaft.Commit(MetadataCommand{
		Mode: mode,
		data: data,
	})
	return err
}

func (mdc *MetaDataController) QueryTopic(req *pb.QueryTopicRequest) *pb.QueryTopicResponse {
	if !mdc.IsLeader() {
		return &pb.QueryTopicResponse{Response: ResponseErrNotLeader()}
	}
	mdc.mu.RLock()
	res, err := mdc.MD.QueryTopic(req.Topic)
	mdc.mu.RUnlock()
	if err != nil {
		return &pb.QueryTopicResponse{
			Response: ErrToResponse(err),
		}
	} else {
		ret := &pb.QueryTopicResponse{
			Response:         ResponseSuccess(),
			PartitionDetails: make([]*pb.Partition, 0, len(res.Partitions)),
		}
		if req.Credential.Identity == pb.Credentials_Producer || req.Credential.Identity == pb.Credentials_Broker {
			for _, partition := range res.Partitions {
				str := make([]string, 0, len(partition.BrokerGroup.Members))
				for _, i := range partition.BrokerGroup.Members {
					str = append(str, i.Url)
				}
				ret.PartitionDetails = append(ret.PartitionDetails, &pb.Partition{
					Topic: req.Topic,
					Name:  partition.Name,
					Urls:  str,
				})
			}
		} else if req.Credential.Identity == pb.Credentials_Consumer {
			mdc.mu.RLock()
			con, err := mdc.MD.QueryConsumer(req.Credential.Id)
			mdc.mu.RUnlock()
			if err != nil {
				return &pb.QueryTopicResponse{Response: ErrToResponse(err)}
			}
			for _, partition := range res.Partitions {
				if partition.Name == con.FocalPartitions {
					str := make([]string, 0, len(partition.BrokerGroup.Members))
					for _, i := range partition.BrokerGroup.Members {
						str = append(str, i.Url)
					}
					ret.PartitionDetails = append(ret.PartitionDetails, &pb.Partition{
						Topic: req.Topic,
						Name:  partition.Name,
						Urls:  str,
					})
					goto next3
				}
				return &pb.QueryTopicResponse{Response: ResponseErrSourceNotExist()}
			next3:
			}
		} else {
			panic("unreachable")
		}
		return ret
	}
}
func (mdc *MetaDataController) RegisterConsumer(req *pb.RegisterConsumerRequest) *pb.RegisterConsumerResponse {
	if !mdc.IsLeader() {
		return &pb.RegisterConsumerResponse{Response: ResponseErrNotLeader()}
	}
	mdc.mu.RLock()
	tp, err := mdc.MD.QueryTopic(req.FocalTopic)
	mdc.mu.RUnlock()
	if err != nil {
		return &pb.RegisterConsumerResponse{Response: ResponseErrSourceAlreadyExist()}
	}
	var parts *pb.Partition
	for _, partition := range tp.Partitions {
		if req.FocalPartitions == partition.Name {
			urls := make([]string, 0, len(partition.BrokerGroup.Members))
			for _, m := range partition.BrokerGroup.Members {
				urls = append(urls, m.Url)
			}
			parts = &pb.Partition{
				Topic: req.FocalTopic,
				Name:  partition.Name,
				Urls:  urls,
			}
			goto next
		}
		return &pb.RegisterConsumerResponse{Response: ResponseErrSourceNotExist()}
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
		return &pb.UnRegisterConsumerResponse{Response: ResponseErrNotLeader()}
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
		return &pb.UnRegisterProducerResponse{Response: ResponseErrNotLeader()}
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
		return &pb.DestroyTopicResponse{Response: ResponseErrNotLeader()}
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
