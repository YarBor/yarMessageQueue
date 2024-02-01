package MqServer

import (
	"MqServer/Err"
	"MqServer/RaftServer"
	"MqServer/RaftServer/Pack"
	pb "MqServer/rpc"
	"errors"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
)

type MetaData struct {
	SelfIncrementId uint32
	bkMu            sync.RWMutex
	Brokers         map[string]*BrokerMD
	tpMu            sync.RWMutex
	Topics          map[string]*TopicMD
	pMu             sync.RWMutex
	Producers       map[string]*ProducerMD
	cMu             sync.RWMutex
	Consumers       map[string]*ConsumerMD
	cgMu            sync.RWMutex
	ConsGroup       map[string]*ConsumersGroupMD
	ttMu            sync.RWMutex
	TpTerm          map[string]*int32 // key:Val TopicName : TopicTerm
	cgtMu           sync.RWMutex
	ConsGroupTerm   map[string]*int32 // key:Val ConsumerGroupID : ConsumerGroupTerm
}

func NewMetaData() *MetaData {
	return &MetaData{
		SelfIncrementId: 0,
		Brokers:         make(map[string]*BrokerMD),
		Topics:          make(map[string]*TopicMD),
		Producers:       make(map[string]*ProducerMD),
		Consumers:       make(map[string]*ConsumerMD),
		ConsGroup:       make(map[string]*ConsumersGroupMD),
		TpTerm:          make(map[string]*int32),
		ConsGroupTerm:   make(map[string]*int32),
	}
}

type ConsumerMD struct {
	SelfId  string
	GroupId string

	MaxReturnMessageSize int32
	TimeoutSessionMsec   int32
}

type ProducerMD struct {
	SelfId             string
	FocalTopic         string
	MaxPushMessageSize int32
}

type BrokerMD struct {
	BrokerData
	PartitionNum uint32
}

type BrokerData struct {
	Name string
	Url  string
}

type BrokersGroupMD struct {
	Members []*BrokerData
}

type ConsumersGroupMD struct {
	mu              sync.RWMutex
	GroupID         string
	FocalTopics     map[string]map[string][]BrokerData // [t][p][]urls...
	ConsumersFcPart map[string][]struct {
		TopicPart string // "T-P"
		Urls      []BrokerData
	} // [id][]Part...
	Mode      pb.RegisterConsumerGroupRequest_PullOptionMode
	GroupTerm int32
}

type PartitionMD struct {
	Topic       string
	Part        string
	BrokerGroup BrokersGroupMD
}

type MetaDataController struct {
	MetaDataRaft *RaftServer.RaftNode
	mu           sync.RWMutex
	MD           *MetaData
}

func (md *MetaData) GetIncreaseID() uint32 {
	return atomic.AddUint32(&md.SelfIncrementId, 1)
}

func (c *ConsumerMD) Copy() ConsumerMD {
	a := ConsumerMD{
		SelfId:               c.SelfId,
		GroupId:              c.GroupId,
		MaxReturnMessageSize: c.MaxReturnMessageSize,
		TimeoutSessionMsec:   c.TimeoutSessionMsec,
	}
	return a
}

func (p *ProducerMD) Copy() ProducerMD {
	a := ProducerMD{
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
		Members: append(make([]*BrokerData, 0, len(bg.Members)), bg.Members...),
	}
	return tbg
}

func (p *PartitionMD) Copy() PartitionMD {
	return PartitionMD{
		Topic:       p.Topic,
		Part:        p.Part,
		BrokerGroup: p.BrokerGroup.Copy(),
	}
}

type TopicMD struct {
	Name            string
	Partitions      []*PartitionMD
	FollowerGroupID []string
	TpTerm          int32
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
	bt, err := Pack.Marshal(md)
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
)

func ErrToResponse(err error) *pb.Response {
	if err == nil {
		return ResponseSuccess()
	}
	switch err.Error() {
	case Err.ErrSourceNotExist:
		return ResponseErrSourceNotExist()
	case Err.ErrSourceAlreadyExist:
		return ResponseErrSourceAlreadyExist()
	case Err.ErrSourceNotEnough:
		return ResponseErrSourceNotEnough()
	case Err.ErrRequestIllegal:
		return ResponseErrRequestIllegal()
	case Err.ErrRequestTimeout:
		return ResponseErrTimeout()
	case Err.ErrRequestServerNotServe:
		return ResponseNotServer()
	case Err.ErrRequestNotLeader:
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
	_, ok := md.Topics[t]
	if ok {
		return errors.New(Err.ErrSourceAlreadyExist)
	}
	return nil
}

func (md *MetaData) QueryTopic(t string) (*TopicMD, error) {
	//mdc.mu.RLock()
	//defer mdc.mu.RUnlock()
	if i, ok := md.Topics[t]; ok {
		p := i.Copy()
		return &p, nil
	} else {
		return nil, errors.New(Err.ErrSourceNotExist)
	}
}

func (md *MetaData) CheckConsumer(id string) error {
	//mdc.mu.RLock()
	//defer mdc.mu.RUnlock()
	//_, ok := md.Consumers[id]
	//return ok
	_, ok := md.Consumers[id]
	if ok {
		return errors.New(Err.ErrSourceAlreadyExist)
	}
	return nil
}

func (md *MetaData) QueryConsumer(id string) (*ConsumerMD, error) {
	//mdc.mu.RLock()
	//defer mdc.mu.RUnlock()
	if i, ok := md.Consumers[id]; ok {
		p := i.Copy()
		return &p, nil
	} else {
		return nil, errors.New(Err.ErrSourceNotExist)
	}
}

func (md *MetaData) CheckProducer(id string) error {
	//_, ok := md.Producers[id]
	//return ok
	_, ok := md.Producers[id]
	if ok {
		return errors.New(Err.ErrSourceAlreadyExist)
	}
	return nil
}

func (md *MetaData) QueryProducer(id string) (*ProducerMD, error) {
	//mdc.mu.RLock()
	//defer mdc.mu.RUnlock()
	if i, ok := md.Producers[id]; ok {
		p := i.Copy()
		return &p, nil
	} else {
		return nil, errors.New(Err.ErrSourceNotExist)
	}
}

func (md *MetaData) GetFreeBrokers(brokersNum int32) ([]*BrokerData, error) {
	if brokersNum == 0 {
		return nil, errors.New(Err.ErrRequestIllegal)
	}
	bks := make([]*BrokerMD, 0, len(md.Brokers))
	for _, bk := range md.Brokers {
		bks = append(bks, bk)
	}
	sort.Slice(bks, func(i, j int) bool {
		return atomic.LoadUint32(&bks[i].PartitionNum) < atomic.LoadUint32(&bks[j].PartitionNum)
	})
	tmp := make([]*BrokerData, brokersNum)
	for i := range tmp {
		tmp[i] = &bks[i%len(bks)].BrokerData
	}
	return tmp, nil
}

func (mdc *MetaDataController) RegisterProducer(request *pb.RegisterProducerRequest) *pb.RegisterProducerResponse {
	if !mdc.IsLeader() {
		return &pb.RegisterProducerResponse{Response: ResponseErrNotLeader()}
	}
	mdc.mu.RLock()
	defer mdc.mu.RUnlock()

	mdc.MD.tpMu.RLock()
	tp, err := mdc.MD.QueryTopic(request.FocalTopic)
	mdc.MD.tpMu.RUnlock()

	if err != nil {
		return &pb.RegisterProducerResponse{Response: ResponseErrSourceNotExist()}
	} else {
		// 在这里做好分配
		// 在handle中去做挂树？
		p := ProducerMD{
			FocalTopic:         request.GetFocalTopic(),
			MaxPushMessageSize: request.GetMaxPushMessageSize(),
		}

		err, data := mdc.commit(MetadataCommandRegisterProducer, p)

		if err != nil {
			return &pb.RegisterProducerResponse{Response: ErrToResponse(err)}
		}
		var ID, ok = data.(string)
		if !ok {
			panic("Reflect Err")
		}
		res := pb.RegisterProducerResponse{
			Response: ResponseSuccess(),
			Credential: &pb.Credentials{
				Identity: pb.Credentials_Producer,
				Id:       ID,
			},
			TpData: &pb.TpData{
				Topic:  request.FocalTopic,
				TpTerm: tp.TpTerm,
				Parts:  make([]*pb.Partition, 0, len(tp.Partitions)),
			},
		}
		for _, partition := range tp.Partitions {
			p := &pb.Partition{
				Topic:   request.FocalTopic,
				Part:    partition.Part,
				Brokers: make([]*pb.BrokerData, 0, len(partition.BrokerGroup.Members)),
			}
			for _, bk := range partition.BrokerGroup.Members {
				p.Brokers = append(p.Brokers, &pb.BrokerData{
					Name: bk.Name,
					Url:  bk.Url,
				})
			}
			res.TpData.Parts = append(res.TpData.Parts, p)
		}
		return &res
	}

}

func (mdc *MetaDataController) Handle(command interface{}) (error, interface{}) {
	mdc.mu.RLock()
	defer mdc.mu.RUnlock()

	mdCommand := command.(MetadataCommand)
	var err error = nil
	var retData interface{} = nil
	switch mdCommand.Mode {
	case MetadataCommandRegisterProducer:
		p := mdCommand.data.(ProducerMD)
		id := strconv.Itoa(int(mdc.MD.GetIncreaseID()))
		retData = id
		p.SelfId = id
		mdc.MD.pMu.Lock()
		if err = mdc.MD.CheckProducer(id); err == nil {
			panic("Ub")
		} else {
			mdc.MD.Producers[p.SelfId] = &p
		}
		mdc.MD.pMu.Unlock()
	case MetadataCommandCreateTopic:
		p := mdCommand.data.(TopicMD)
		mdc.MD.tpMu.Lock()
		if _, ok := mdc.MD.Topics[p.Name]; ok {
			err = errors.New(Err.ErrSourceAlreadyExist)
		} else {
			mdc.MD.Topics[p.Name] = &p
		}
		mdc.MD.tpMu.Unlock()
	}
	return err, retData
}

func (mdc *MetaDataController) MakeSnapshot() []byte {
	mdc.mu.RLock()
	defer mdc.mu.RUnlock()
	bt, err := Pack.Marshal(mdc.MD)
	if err != nil {
		panic(err)
	}
	return bt
}

func (mdc *MetaDataController) LoadSnapshot(bt []byte) {
	mdc.mu.Lock()
	defer mdc.mu.Unlock()
	var MD MetaData
	err := Pack.Unmarshal(bt, &MD)
	if err != nil {
		panic(err)
	}
	mdc.MD = &MD
}

func (mdc *MetaDataController) CreateTopic(req *pb.CreateTopicRequest) *pb.CreateTopicResponse {
	mdc.mu.RLock()
	defer mdc.mu.RUnlock()
	mdc.MD.tpMu.RLock()
	err := mdc.MD.CheckTopic(req.Topic)
	mdc.MD.tpMu.RUnlock()
	if err != nil {
		return &pb.CreateTopicResponse{
			Response: ErrToResponse(err),
		}
	}
	tp := TopicMD{
		Name:            req.Topic,
		Partitions:      make([]*PartitionMD, 0, len(req.Partition)),
		FollowerGroupID: make([]string, 0),
		TpTerm:          0,
	}
	for _, details := range req.Partition {
		if details.ReplicationNumber == 0 {
			return &pb.CreateTopicResponse{Response: ResponseErrRequestIllegal()}
		}
		bg := BrokersGroupMD{}
		bg.Members, err = mdc.MD.GetFreeBrokers(details.ReplicationNumber)
		if err != nil {
			return &pb.CreateTopicResponse{
				Response: ErrToResponse(err),
			}
		}
		tp.Partitions = append(tp.Partitions, &PartitionMD{
			Topic:       req.Topic,
			Part:        details.PartitionName,
			BrokerGroup: bg,
		})
	}

	err, _ = mdc.commit(MetadataCommandCreateTopic, tp)
	if err != nil {
		return &pb.CreateTopicResponse{Response: ErrToResponse(err)}
	}

	res := &pb.CreateTopicResponse{Response: ResponseSuccess(), Tp: &pb.TpData{
		Topic:  req.Topic,
		TpTerm: tp.TpTerm,
		Parts:  make([]*pb.Partition, 0, len(tp.Partitions)),
	}}

	for _, partition := range tp.Partitions {
		p := &pb.Partition{
			Topic:   req.Topic,
			Part:    partition.Part,
			Brokers: make([]*pb.BrokerData, 0, len(partition.BrokerGroup.Members)),
		}
		for _, member := range partition.BrokerGroup.Members {
			p.Brokers = append(p.Brokers, &pb.BrokerData{
				Name: member.Name,
				Url:  member.Url,
			})
		}
		res.Tp.Parts = append(res.Tp.Parts, p)
	}

	return res
}

func (mdc *MetaDataController) commit(mode string, data interface{}) (error, interface{}) {
	err, p := mdc.MetaDataRaft.Commit(MetadataCommand{
		Mode: mode,
		data: data,
	})
	return err, p
}

// todo:
func (mdc *MetaDataController) QueryTopic(req *pb.QueryTopicRequest) *pb.QueryTopicResponse {
	return nil
}
func (mdc *MetaDataController) RegisterConsumer(req *pb.RegisterConsumerRequest) *pb.RegisterConsumerResponse {
	return nil
}
func (mdc *MetaDataController) UnRegisterConsumer(req *pb.UnRegisterConsumerRequest) *pb.UnRegisterConsumerResponse {
	return nil
}
func (mdc *MetaDataController) UnRegisterProducer(req *pb.UnRegisterProducerRequest) *pb.UnRegisterProducerResponse {
	return nil
}
func (mdc *MetaDataController) DestroyTopic(req *pb.DestroyTopicRequest) *pb.DestroyTopicResponse {
	return nil
}
