package MqServer

import (
	"MqServer/Err"
	"MqServer/RaftServer"
	"MqServer/RaftServer/Pack"
	pb "MqServer/rpc"
	"errors"
	"fmt"
	"sort"
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

func (md *MetaData) getConsGroupTerm(ConsGroupID string) int32 {
	md.cgtMu.RLock()
	res, ok := md.ConsGroupTerm[ConsGroupID]
	md.cgtMu.RUnlock()
	if ok {
		return atomic.LoadInt32(res)
	} else {
		return -1
	}
}

func (md *MetaData) addConsGroupTerm(ConsGroupID string) int32 {
	md.cgtMu.RLock()
	res, ok := md.ConsGroupTerm[ConsGroupID]
	md.cgtMu.RUnlock()
	if ok {
		return atomic.AddInt32(res, 1)
	} else {
		i := int32(1)
		md.cgtMu.Lock()
		md.ConsGroupTerm[ConsGroupID] = &i
		md.cgtMu.Unlock()
		return i
	}
}
func (md *MetaData) getTpTerm(TpName string) int32 {
	md.ttMu.RLock()
	res, ok := md.TpTerm[TpName]
	md.ttMu.RUnlock()
	if ok {
		return atomic.LoadInt32(res)
	} else {
		return -1
	}
}

func (md *MetaData) addTpTerm(TpName string) int32 {
	md.ttMu.RLock()
	res, ok := md.TpTerm[TpName]
	md.ttMu.RUnlock()
	if ok {
		return atomic.AddInt32(res, 1)
	} else {
		i := int32(1)
		md.ttMu.Lock()
		md.TpTerm[TpName] = &i
		md.ttMu.Unlock()
		return i
	}
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
	FocalTopics     map[string]*PartitionSmD // [t][p][]urls...
	ConsumersFcPart map[string]*[]struct {
		Topic, Part string // "T-P"
		Urls        []*BrokerData
	} // [id][]Part...
	Mode      pb.RegisterConsumerGroupRequest_PullOptionMode
	GroupTerm int32
}

type PartitionMD struct {
	//Topic       string
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
		//Topic:       p.Topic,
		Part:        p.Part,
		BrokerGroup: p.BrokerGroup.Copy(),
	}
}

type PartitionSmD struct {
	Term  int32
	Parts []*PartitionMD
}
type TopicMD struct {
	Name            string
	Part            PartitionSmD
	FollowerGroupID []string
}

func (t *TopicMD) Copy() TopicMD {
	tt := TopicMD{
		Name: t.Name,
		Part: PartitionSmD{
			Term:  t.Part.Term,
			Parts: make([]*PartitionMD, len(t.Part.Parts))},
	}
	for i := range t.Part.Parts {
		p := t.Part.Parts[i].Copy()
		tt.Part.Parts[i] = &p
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
				TpTerm: tp.Part.Term,
				Parts:  make([]*pb.Partition, 0, len(tp.Part.Parts)),
			},
		}
		for _, partition := range tp.Part.Parts {
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
	case MetadataCommandRegisterConsumer:
		p := mdCommand.data.(ConsumerMD)
		id := fmt.Sprint(mdc.MD.GetIncreaseID())
		retData = id
		p.SelfId = id
		mdc.MD.cMu.Lock()
		if err = mdc.MD.CheckConsumer(id); err == nil {
			panic("Ub")
		} else {
			mdc.MD.Consumers[p.SelfId] = &p
		}
		mdc.MD.cMu.Unlock()
	case MetadataCommandRegisterProducer:
		p := mdCommand.data.(ProducerMD)
		id := fmt.Sprint(mdc.MD.GetIncreaseID())
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
	case MetadataCommandUnRegisterConsumer:
		ConId := mdCommand.data.(string)

		mdc.MD.cMu.Lock()
		targetConsumer, ok := mdc.MD.Consumers[ConId]
		if !ok {
			err = errors.New(Err.ErrSourceNotExist)
		} else {
			delete(mdc.MD.Consumers, ConId)
		}
		mdc.MD.cMu.Unlock()

		if err != nil || targetConsumer.GroupId == "" {
			break
		}

		mdc.MD.cgMu.RLock()
		var targetGroup *ConsumersGroupMD
		targetGroup, ok = mdc.MD.ConsGroup[targetConsumer.GroupId]
		mdc.MD.cgMu.RUnlock()

		if !ok {
			err = errors.New(Err.ErrSourceNotExist)
			break
		}

		targetGroup.mu.Lock()
		delete(targetGroup.ConsumersFcPart, ConId)
		IsClear := len(targetGroup.ConsumersFcPart) == 0
		targetGroup.mu.Unlock()

		if IsClear {
			mdc.MD.cgMu.Lock()
			delete(mdc.MD.ConsGroup, targetGroup.GroupID)
			mdc.MD.cgMu.Unlock()
		} else {
			err = mdc.MD.reBalance(targetGroup.GroupID)
		}

	}
	return err, retData
	// todo:
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
	if !mdc.IsLeader() {
		return &pb.CreateTopicResponse{Response: ResponseErrNotLeader()}
	}
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
		FollowerGroupID: make([]string, 0),
		Part: PartitionSmD{
			Term:  0,
			Parts: make([]*PartitionMD, 0, len(req.Partition)),
		},
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
		tp.Part.Parts = append(tp.Part.Parts, &PartitionMD{
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
		TpTerm: tp.Part.Term,
		Parts:  make([]*pb.Partition, 0, len(tp.Part.Parts)),
	}}

	for _, partition := range tp.Part.Parts {
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

func (mdc *MetaDataController) QueryTopic(req *pb.QueryTopicRequest) *pb.QueryTopicResponse {
	// todo:
	if mdc.IsLeader() == false {
		return &pb.QueryTopicResponse{Response: ResponseErrNotLeader()}
	}
	mdc.mu.RLock()
	defer mdc.mu.RUnlock()
	mdc.MD.tpMu.RLock()
	res, err := mdc.MD.QueryTopic(req.Topic)
	mdc.MD.tpMu.RUnlock()
	if err != nil {
		return &pb.QueryTopicResponse{Response: ErrToResponse(err)}
	}
	ret := pb.QueryTopicResponse{
		Response:         ResponseSuccess(),
		TopicTerm:        res.Part.Term,
		PartitionDetails: make([]*pb.Partition, 0, len(res.Part.Parts)),
	}
	for _, partition := range res.Part.Parts {
		p := &pb.Partition{
			Topic:   req.Topic,
			Part:    partition.Part,
			Brokers: make([]*pb.BrokerData, 0, len(partition.BrokerGroup.Members)),
		}
		for _, member := range partition.BrokerGroup.Members {
			bd := &pb.BrokerData{
				Name: member.Name,
				Url:  member.Url,
			}
			p.Brokers = append(p.Brokers, bd)
		}
		ret.PartitionDetails = append(ret.PartitionDetails, p)
	}
	return &ret
}

func (mdc *MetaDataController) RegisterConsumer(req *pb.RegisterConsumerRequest) *pb.RegisterConsumerResponse {
	// todo:
	if mdc.IsLeader() == false {
		return &pb.RegisterConsumerResponse{Response: ResponseErrNotLeader()}
	}
	mdc.mu.RLock()
	defer mdc.mu.RUnlock()
	c := ConsumerMD{
		SelfId:               "",
		GroupId:              "",
		MaxReturnMessageSize: req.MaxReturnMessageSize,
		TimeoutSessionMsec:   req.TimeoutSessionMsec,
	}
	err, data := mdc.commit(MetadataCommandRegisterConsumer, c)
	if err != nil {
		return &pb.RegisterConsumerResponse{Response: ErrToResponse(err)}
	}
	return &pb.RegisterConsumerResponse{
		Response: ResponseSuccess(),
		Credential: &pb.Credentials{
			Identity: pb.Credentials_Consumer,
			Id:       data.(string),
			Key:      "",
		},
	}
}

func (mdc *MetaDataController) UnRegisterConsumer(req *pb.UnRegisterConsumerRequest) *pb.UnRegisterConsumerResponse {
	// todo:
	if mdc.IsLeader() == false {
		return &pb.UnRegisterConsumerResponse{Response: ResponseErrNotLeader()}
	}
	mdc.mu.RLock()
	defer mdc.mu.RUnlock()
	mdc.MD.cMu.RLock()
	i, err := mdc.MD.QueryConsumer(req.Credential.Id)
	mdc.MD.cMu.RUnlock()
	if err != nil {
		return &pb.UnRegisterConsumerResponse{Response: ErrToResponse(err)}
	} else {
		err, _ = mdc.commit(MetadataCommandUnRegisterConsumer, i)
		return &pb.UnRegisterConsumerResponse{Response: ErrToResponse(err)}
	}
}
func (mdc *MetaDataController) UnRegisterProducer(req *pb.UnRegisterProducerRequest) *pb.UnRegisterProducerResponse {
	// todo:
	return nil
}

func (mdc *MetaDataController) RegisterConsumerGroup(req *pb.RegisterConsumerGroupRequest) *pb.RegisterConsumerGroupResponse {
	// todo
}

func (mdc *MetaDataController) UnRegisterConsumerGroup(req *pb.UnRegisterConsumerGroupRequest) *pb.UnRegisterConsumerGroupResponse {
	// todo
}

func (mdc *MetaDataController) JoinRegisterConsumerGroup(req *pb.JoinConsumerGroupRequest) *pb.JoinConsumerGroupResponse {
	// todo
}

func (mdc *MetaDataController) LeaveRegisterConsumerGroup(req *pb.LeaveConsumerGroupRequest) *pb.LeaveConsumerGroupResponse {
	// todo
}

func (mdc *MetaDataController) AddTopicRegisterConsumerGroup(req *pb.SubscribeTopicResponse) *pb.SubscribeTopicResponse {
	// todo
}

func (mdc *MetaDataController) DelTopicRegisterConsumerGroup(req *pb.UnSubscribeTopicRequest) *pb.UnSubscribeTopicResponse {
	// todo
}

func (mdc *MetaDataController) DestroyTopic(req *pb.DestroyTopicRequest) *pb.DestroyTopicResponse {
	// todo:
	return nil
}

func (md *MetaData) reBalance(GroupID string) error {
	// 获取cgMu互斥锁，确保对 ConsumersGroup表 映射的读访问
	md.cgMu.RLock()
	defer md.cgMu.RUnlock()

	// 根据给定的GroupID检索ConsumersGroup
	g, ok := md.ConsGroup[GroupID]
	if !ok {
		return errors.New(Err.ErrSourceNotExist)
	}

	// 获取ConsumersGroup互斥锁，确保对其字段的独占访问
	g.mu.Lock()
	defer g.mu.Unlock()

	// 创建切片以存储需要重新平衡的主题-分区URL
	TpUrls := make([]struct {
		Topic, Part string // "T-P"
		Urls        []*BrokerData
	}, 0)

	// 创建切片以存储需要从ConsumersGroup中删除的主题
	needDeleteTps := []string{}

	// 遍历ConsumersGroup的FocalTopics
	for T, val := range g.FocalTopics {
		// 检查主题是否已从元数据中删除
		if i := md.getTpTerm(T); i == -1 {
			needDeleteTps = append(needDeleteTps, T)
			continue
		} else if i != val.Term {
			// 获取主题的最新元数据
			md.tpMu.RLock()
			tp, err := md.QueryTopic(T)
			md.tpMu.RUnlock()
			if err != nil {
				// 更新ConsumersGroup中主题的分区和术语
				val.Parts = tp.Part.Parts
				val.Term = tp.Part.Term
			} else {
				continue
			}
		}

		// 遍历主题的分区
		for _, part := range val.Parts {
			// 将主题-分区URL添加到TpUrls切片中
			TpUrls = append(TpUrls, struct {
				Topic, Part string
				Urls        []*BrokerData
			}{Topic: T, Part: part.Part, Urls: part.BrokerGroup.Members})
		}
	}

	// 删除需要从ConsumersGroup中删除的主题
	for _, tp := range needDeleteTps {
		delete(g.FocalTopics, tp)
	}

	// 创建切片以存储ConsumersGroup的成员（消费者）
	members := make([]string, 0, len(g.ConsumersFcPart))

	for s, i := range g.ConsumersFcPart {
		members = append(members, s)
		*i = make([]struct {
			Topic, Part string
			Urls        []*BrokerData
		}, 0, len(TpUrls)/len(g.ConsumersFcPart)+1)
	}

	// map 遍历的顺序不同 通过sort 保持重新分配的 水平和一致
	sort.Slice(TpUrls, func(i, j int) bool {
		if TpUrls[i].Topic == TpUrls[j].Topic {
			return TpUrls[i].Part < TpUrls[j].Part
		}
		return TpUrls[i].Topic < TpUrls[j].Topic
	})
	sort.Slice(members, func(i, j int) bool {
		return members[i] < members[j]
	})

	// 在消费者之间重新平衡主题-分区URL
	for i, tpu := range TpUrls {
		*g.ConsumersFcPart[members[i/len(members)]] = append(*g.ConsumersFcPart[members[i/len(members)]], tpu)
	}

	// 更新ConsumersGroup的GroupTerm
	g.GroupTerm = md.addConsGroupTerm(g.GroupID)

	return nil
}
