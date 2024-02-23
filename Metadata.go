package MqServer

import (
	"MqServer/Err"
	"MqServer/RaftServer"
	"MqServer/RaftServer/Pack"
	"MqServer/Random"
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

type PartitionSmD struct {
	Term  int32
	Parts []*PartitionMD
}

type TopicMD struct {
	mu                sync.RWMutex
	Name              string
	Part              PartitionSmD
	FollowerGroupID   []string
	FollowerProducers []string
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

func (md *MetaData) getConsGroupTerm(ConsGroupID string) (int32, error) {
	md.cgtMu.RLock()
	res, ok := md.ConsGroupTerm[ConsGroupID]
	md.cgtMu.RUnlock()
	if ok {
		return atomic.LoadInt32(res), nil
	} else {
		return -1, errors.New(Err.ErrSourceNotExist)
	}
}

func (md *MetaData) addConsGroupTerm(ConsGroupID string) (int32, error) {
	md.cgtMu.RLock()
	res, ok := md.ConsGroupTerm[ConsGroupID]
	md.cgtMu.RUnlock()
	if ok {
		return atomic.AddInt32(res, 1), nil
	} else {
		return -1, errors.New(Err.ErrSourceNotExist)
	}
}

func (md *MetaData) createConsGroupTerm(ConsGroupID string) (int32, error) {
	md.cgtMu.Lock()
	defer md.cgtMu.Unlock()
	res, ok := md.ConsGroupTerm[ConsGroupID]
	if ok {
		return atomic.LoadInt32(res), errors.New(Err.ErrSourceAlreadyExist)
	} else {
		md.ConsGroupTerm[ConsGroupID] = new(int32)
		return 0, nil
	}
}

func (md *MetaData) getTpTerm(TpName string) (int32, error) {
	md.ttMu.RLock()
	res, ok := md.TpTerm[TpName]
	md.ttMu.RUnlock()
	if ok {
		return atomic.LoadInt32(res), nil
	} else {
		return -1, errors.New(Err.ErrSourceNotExist)
	}
}

func (md *MetaData) addTpTerm(TpName string) (int32, error) {
	md.ttMu.RLock()
	res, ok := md.TpTerm[TpName]
	md.ttMu.RUnlock()
	if ok {
		return atomic.AddInt32(res, 1), nil
	} else {
		return -1, errors.New(Err.ErrSourceNotExist)
	}
}

func (md *MetaData) createTpTerm(TpName string) (int32, error) {
	md.ttMu.Lock()
	defer md.ttMu.Unlock()
	res, ok := md.TpTerm[TpName]
	if ok {
		return *res, errors.New(Err.ErrSourceAlreadyExist)
	} else {
		md.TpTerm[TpName] = new(int32)
		return int32(0), nil
	}
}

func (tpmd *TopicMD) IsClear() bool {
	return len(tpmd.FollowerProducers) == 0 && len(tpmd.FollowerGroupID) == 0
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

func (md *MetaData) GetIncreaseID() uint32 {
	return atomic.AddUint32(&md.SelfIncrementId, 1)
}

func (c *ConsumerMD) Copy() *ConsumerMD {
	a := ConsumerMD{
		SelfId:               c.SelfId,
		GroupId:              c.GroupId,
		MaxReturnMessageSize: c.MaxReturnMessageSize,
		TimeoutSessionMsec:   c.TimeoutSessionMsec,
	}
	return &a
}

func (p *ProducerMD) Copy() *ProducerMD {
	a := ProducerMD{
		FocalTopic:         p.FocalTopic,
		MaxPushMessageSize: p.MaxPushMessageSize,
	}
	return &a
}

func (b *BrokerMD) Copy() *BrokerMD {
	tb := *b
	return &tb
}

func (bg *BrokersGroupMD) Copy() *BrokersGroupMD {
	tbg := BrokersGroupMD{
		Members: append(make([]*BrokerData, 0, len(bg.Members)), bg.Members...),
	}
	return &tbg
}

func (p *PartitionMD) Copy() *PartitionMD {
	return &PartitionMD{
		//Topic:       p.Topic,
		Part:        p.Part,
		BrokerGroup: *p.BrokerGroup.Copy(),
	}
}

func (t *TopicMD) Copy() *TopicMD {
	t.mu.RLock()
	defer t.mu.RUnlock()
	tt := TopicMD{
		Name: t.Name,
		Part: PartitionSmD{
			Term:  t.Part.Term,
			Parts: make([]*PartitionMD, len(t.Part.Parts))},
	}
	for i := range t.Part.Parts {
		p := t.Part.Parts[i].Copy()
		tt.Part.Parts[i] = p
	}
	return &tt
}

func (md *MetaData) SnapShot() []byte {
	bt, err := Pack.Marshal(md)
	if err != nil {
		panic(err)
	}
	return bt
}

const (
	RegisterProducer      = "rgp"
	RegisterConsumer      = "rgc"
	UnRegisterProducer    = "urp"
	UnRegisterConsumer    = "urc"
	CreateTopic           = "ct"
	RegisterConsGroup     = "rcg"
	UnRegisterConsGroup   = "urcg"
	JoinConsGroup         = "jcg"
	LeaveConsGroup        = "lcg"
	ConsGroupFocalTopic   = "cgft"
	ConsGroupUnFocalTopic = "cguft"
	DestroyTopic          = "dt"
	AddPart               = "ap"
	RemovePart            = "rp"
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
		return p, nil
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
		return p, nil
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
		return p, nil
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

		err, data := mdc.commit(
			RegisterProducer, p)

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
	case
		RegisterConsumer:
		p := mdCommand.data.(*ConsumerMD)
		id := fmt.Sprint(mdc.MD.GetIncreaseID())
		retData = id
		p.SelfId = id
		mdc.MD.cMu.Lock()
		if err = mdc.MD.CheckConsumer(id); err == nil {
			panic("Ub")
		} else {
			mdc.MD.Consumers[p.SelfId] = p
		}
		mdc.MD.cMu.Unlock()
	case
		RegisterProducer:
		p := mdCommand.data.(ProducerMD)
		id := fmt.Sprint(mdc.MD.GetIncreaseID())
		retData = id
		p.SelfId = id

		mdc.MD.tpMu.RLock()
		topic, ok := mdc.MD.Topics[p.FocalTopic]
		mdc.MD.tpMu.RUnlock()

		if !ok {
			err = errors.New(Err.ErrSourceNotExist)
			break
		} else {
			topic.mu.Lock()
			topic.FollowerProducers = append(topic.FollowerProducers, p.SelfId)
			_, _ = mdc.MD.addTpTerm(topic.Name)
			topic.mu.Unlock()
		}

		mdc.MD.pMu.Lock()
		if err = mdc.MD.CheckProducer(id); err == nil {
			panic("Ub")
		} else {
			mdc.MD.Producers[p.SelfId] = &p
		}
		mdc.MD.pMu.Unlock()

	case
		CreateTopic:
		p := mdCommand.data.(*TopicMD)
		mdc.MD.tpMu.Lock()
		if _, ok := mdc.MD.Topics[p.Name]; ok {
			err = errors.New(Err.ErrSourceAlreadyExist)
		} else {
			mdc.MD.Topics[p.Name] = p
		}
		mdc.MD.tpMu.Unlock()
	case
		UnRegisterConsumer:
		ConId := mdCommand.data.(*string)

		mdc.MD.cMu.Lock()
		targetConsumer, ok := mdc.MD.Consumers[*ConId]
		if !ok {
			err = errors.New(Err.ErrSourceNotExist)
		} else {
			delete(mdc.MD.Consumers, *ConId)
		}
		mdc.MD.cMu.Unlock()

		if err != nil || targetConsumer.GroupId == "" {
			break
		}

		var targetGroup *ConsumersGroupMD

		mdc.MD.cgMu.RLock()
		targetGroup, ok = mdc.MD.ConsGroup[targetConsumer.GroupId]
		mdc.MD.cgMu.RUnlock()

		if !ok {
			err = errors.New(Err.ErrSourceNotExist)
			break
		}

		targetGroup.mu.Lock()
		delete(targetGroup.ConsumersFcPart, *ConId)
		IsConsumerGroupClear := len(targetGroup.ConsumersFcPart) == 0
		if IsConsumerGroupClear == false {
			err = mdc.MD.reBalance(targetGroup)
		}
		targetGroup.mu.Unlock()

		if IsConsumerGroupClear {

			mdc.MD.cgMu.Lock()

			mdc.MD.cgtMu.Lock()
			delete(mdc.MD.ConsGroupTerm, targetGroup.GroupID)
			mdc.MD.cgtMu.Unlock()

			delete(mdc.MD.ConsGroup, targetGroup.GroupID)
			mdc.MD.cgMu.Unlock()

			NeedCheckClearTopics := []string{}
			mdc.MD.tpMu.RLock()
			for topic := range targetGroup.FocalTopics {
				i, ok := mdc.MD.Topics[topic]
				if ok {
					i.mu.Lock()
					for index := range i.FollowerGroupID {
						if i.FollowerGroupID[index] == targetGroup.GroupID {
							i.FollowerGroupID = append(i.FollowerGroupID[:index], i.FollowerGroupID[index+1:]...)
							_, _ = mdc.MD.addTpTerm(topic)
							break
						}
					}
					if i.IsClear() {
						NeedCheckClearTopics = append(NeedCheckClearTopics, i.Name)
					}
					i.mu.Unlock()
				}
			}
			mdc.MD.tpMu.RUnlock()

			mdc.MD.tpMu.Lock()
			trueDeleteList := []string{}
			for _, topic := range NeedCheckClearTopics {
				i, ok := mdc.MD.Topics[topic]
				if ok {
					i.mu.RLock()
					if i.IsClear() {
						trueDeleteList = append(trueDeleteList, topic)
					}
					i.mu.RUnlock()
				}
			}
			for _, s := range trueDeleteList {
				delete(mdc.MD.Topics, s)
			}
			mdc.MD.tpMu.Unlock()

		}

	case
		LeaveConsGroup:
		data := mdCommand.data.(*struct {
			GroupID string
			SelfID  string
		})

		mdc.MD.cMu.Lock()
		targetConsumer, ok := mdc.MD.Consumers[data.SelfID]
		if !ok || targetConsumer.GroupId != data.GroupID {
			err = errors.New(Err.ErrSourceNotExist)
		} else {
			targetConsumer.GroupId = ""
		}
		mdc.MD.cMu.Unlock()

		if err != nil {
			break
		}

		mdc.MD.cgMu.RLock()
		var targetGroup *ConsumersGroupMD
		targetGroup, ok = mdc.MD.ConsGroup[data.GroupID]
		mdc.MD.cgMu.RUnlock()

		if !ok {
			err = errors.New(Err.ErrSourceNotExist)
			break
		}

		targetGroup.mu.Lock()
		delete(targetGroup.ConsumersFcPart, data.SelfID)
		IsConsumerGroupClear := len(targetGroup.ConsumersFcPart) == 0
		if !IsConsumerGroupClear {
			err = mdc.MD.reBalance(targetGroup)
		}
		targetGroup.mu.Unlock()

		if IsConsumerGroupClear {

			mdc.MD.cgMu.Lock()

			mdc.MD.cgtMu.Lock()
			delete(mdc.MD.ConsGroupTerm, targetGroup.GroupID)
			mdc.MD.cgtMu.Unlock()

			delete(mdc.MD.ConsGroup, targetGroup.GroupID)
			mdc.MD.cgMu.Unlock()

			NeedCheckClearTopics := []string{}
			mdc.MD.tpMu.RLock()
			for topic := range targetGroup.FocalTopics {
				i, ok := mdc.MD.Topics[topic]
				if ok {
					i.mu.Lock()
					for index := range i.FollowerGroupID {
						if i.FollowerGroupID[index] == targetGroup.GroupID {
							i.FollowerGroupID = append(i.FollowerGroupID[:index], i.FollowerGroupID[index+1:]...)
							_, _ = mdc.MD.addTpTerm(topic)
							break
						}
					}
					if i.IsClear() {
						NeedCheckClearTopics = append(NeedCheckClearTopics, i.Name)
					}
					i.mu.Unlock()
				}
			}
			mdc.MD.tpMu.RUnlock()

			if len(NeedCheckClearTopics) != 0 {
				mdc.MD.tpMu.Lock()
				trueDeleteList := []string{}
				for _, topic := range NeedCheckClearTopics {
					i, ok := mdc.MD.Topics[topic]
					if ok {
						i.mu.RLock()
						if i.IsClear() {
							trueDeleteList = append(trueDeleteList, topic)
						}
						i.mu.RUnlock()
					}
				}
				for _, s := range trueDeleteList {
					delete(mdc.MD.Topics, s)
				}
				mdc.MD.tpMu.Unlock()
			}
		}
	case
		UnRegisterProducer:
		ProID := mdCommand.data.(*string)
		mdc.MD.pMu.Lock()
		p, ok := mdc.MD.Producers[*ProID]
		if !ok {
			err = errors.New(Err.ErrSourceNotExist)
		} else {
			delete(mdc.MD.Producers, *ProID)
		}
		mdc.MD.pMu.Unlock()

		mdc.MD.tpMu.RLock()
		topic, ok1 := mdc.MD.Topics[p.FocalTopic]
		mdc.MD.tpMu.RUnlock()
		if !ok1 {
			break
		}
		topic.mu.Lock()
		for i := range topic.FollowerProducers {
			if topic.FollowerProducers[i] == *ProID {
				topic.FollowerProducers = append(topic.FollowerProducers[:i], topic.FollowerProducers[i+1:]...)
				break
			}
		}
		isTopicClear := topic.IsClear()
		topic.mu.Unlock()

		// Remove Topic from TP-MAP and TP-Term-MAP
		if isTopicClear {
			mdc.MD.tpMu.Lock()
			topic, ok1 = mdc.MD.Topics[p.FocalTopic]
			if ok1 && topic.IsClear() {
				delete(mdc.MD.Topics, p.FocalTopic)

				mdc.MD.ttMu.Lock()
				delete(mdc.MD.TpTerm, topic.Name)
				mdc.MD.ttMu.Unlock()
			}
			mdc.MD.tpMu.Unlock()
		}

	case
		RegisterConsGroup:
		GroupData := mdCommand.data.(struct {
			GroupID string
			Mode    pb.RegisterConsumerGroupRequest_PullOptionMode
		})
		mdc.MD.cgMu.Lock()
		if _, ok := mdc.MD.ConsGroup[GroupData.GroupID]; ok {
			err = errors.New(Err.ErrSourceAlreadyExist)
		} else {
			Term := int32(0)
			Term, err = mdc.MD.createConsGroupTerm(GroupData.GroupID)
			if err == nil {
				mdc.MD.ConsGroup[GroupData.GroupID] = &ConsumersGroupMD{
					GroupID:     GroupData.GroupID,
					FocalTopics: make(map[string]*PartitionSmD),
					ConsumersFcPart: make(map[string]*[]struct {
						Topic, Part string
						Urls        []*BrokerData
					}),
					Mode:      GroupData.Mode,
					GroupTerm: Term,
				}
				retData = Term
			}
		}
		mdc.MD.cgMu.Unlock()
	case
		JoinConsGroup:
		data := mdCommand.data.(*struct {
			GroupID string
			SelfID  string
		})

		mdc.MD.cMu.RLock()
		mdc.MD.cgMu.RLock()

		_, ok := mdc.MD.Consumers[data.SelfID]
		if !ok {
			err = errors.New(Err.ErrSourceNotExist)
			goto JcgFinish
		}
		var group *ConsumersGroupMD
		group, ok = mdc.MD.ConsGroup[data.GroupID]
		if !ok {
			err = errors.New(Err.ErrSourceNotExist)
			goto JcgFinish
		}

		group.mu.Lock()

		group.ConsumersFcPart[data.SelfID] = nil
		err = mdc.MD.reBalance(group)

		group.mu.Unlock()

		if err != nil {
			goto JcgFinish
		}

		group.mu.RLock()

		// 构建返回的struct “retData_”
		t := group.ConsumersFcPart[data.SelfID]
		retData_ := struct {
			FcParts   []*pb.Partition
			GroupTerm int32
		}{
			FcParts:   make([]*pb.Partition, 0, len(*t)),
			GroupTerm: group.GroupTerm,
		}
		for _, Tpu := range *t {
			pa := &pb.Partition{
				Topic:   Tpu.Topic,
				Part:    Tpu.Part,
				Brokers: make([]*pb.BrokerData, 0, len(Tpu.Urls)),
			}
			for _, url := range Tpu.Urls {
				pa.Brokers = append(pa.Brokers, &pb.BrokerData{
					Name: url.Name,
					Url:  url.Url,
				})
			}
			retData_.FcParts = append(retData_.FcParts, pa)
		}
		retData = retData_

		group.mu.RUnlock()

	JcgFinish:
		mdc.MD.cgMu.RUnlock()
		mdc.MD.cMu.Unlock()
	case
		ConsGroupFocalTopic:
		data := mdCommand.data.(*struct {
			ConGiD string
			Topic  string
		})
		mdc.MD.cgMu.RLock()
		group, ok := mdc.MD.ConsGroup[data.ConGiD]
		mdc.MD.cgMu.RUnlock()

		mdc.MD.tpMu.RLock()
		topic, ok1 := mdc.MD.Topics[data.Topic]
		mdc.MD.tpMu.RUnlock()

		if !ok || !ok1 {
			err = errors.New(Err.ErrSourceNotExist)
			break
		}

		var partitions *PartitionSmD

		topic.mu.Lock()
		topic.FollowerGroupID = append(topic.FollowerGroupID, data.ConGiD)
		topic.Part.Term, err = mdc.MD.addTpTerm(topic.Name)
		partitions = topic.Part.Copy()
		topic.mu.RUnlock()

		if err != nil {
			break
		}

		group.mu.Lock()
		group.FocalTopics[topic.Name] = partitions
		err = mdc.MD.reBalance(group)
		group.mu.Unlock()

	case
		ConsGroupUnFocalTopic:
		data := mdCommand.data.(*struct {
			ConGiD string
			Topic  string
		})
		mdc.MD.cgMu.RLock()
		group, ok := mdc.MD.ConsGroup[data.ConGiD]
		mdc.MD.cgMu.RUnlock()

		mdc.MD.tpMu.RLock()
		topic, ok1 := mdc.MD.Topics[data.Topic]
		mdc.MD.tpMu.RUnlock()

		if !ok || !ok1 {
			err = errors.New(Err.ErrSourceNotExist)
			break
		}

		success := false
		topic.mu.Lock()
		for i, ID := range topic.FollowerGroupID {
			if ID == data.ConGiD {
				topic.FollowerGroupID = append(topic.FollowerGroupID[:i], topic.FollowerGroupID[i+1:]...)
				success = true
				break
			}
		}

		if !success {
			err = errors.New(Err.ErrSourceNotExist)
			goto Failure
		}

		IsTopicClear := topic.IsClear()
		var i int32
		i, err = mdc.MD.addTpTerm(topic.Name)
		topic.Part.Term = i

	Failure:
		topic.mu.Unlock()
		if err != nil {
			break
		}

		if IsTopicClear {
			mdc.MD.tpMu.Lock()
			topic, ok1 = mdc.MD.Topics[data.Topic]
			topic.mu.RLock()
			// Recheck for race condition during twice tpMu lock.
			if ok1 && topic.IsClear() {
				delete(mdc.MD.Topics, data.Topic)

				mdc.MD.ttMu.Lock()
				delete(mdc.MD.TpTerm, topic.Name)
				mdc.MD.ttMu.Unlock()
			}
			topic.mu.RUnlock()
			mdc.MD.tpMu.Unlock()
		}

		group.mu.Lock()
		if _, ok := group.FocalTopics[data.ConGiD]; ok {
			delete(group.FocalTopics, data.ConGiD)
			err = mdc.MD.reBalance(group)
		} else {
			err = errors.New(Err.ErrSourceNotExist)
		}
		group.mu.Unlock()
	case AddPart:
		data := mdCommand.data.(*struct {
			Topic   string
			Part    string
			Brokers []*BrokerData
		})
		mdc.MD.tpMu.RLock()
		mdc.MD.bkMu.RLock()
		for _, bk := range data.Brokers {
			if _, ok := mdc.MD.Brokers[bk.Name]; !ok {
				err = errors.New(Err.ErrSourceNotExist)
				break
			}
		}
		mdc.MD.bkMu.RUnlock()

		if err != nil {
			err = errors.New(Err.ErrSourceNotExist)
			break
		}

		mdc.MD.tpMu.RLock()
		t, ok := mdc.MD.Topics[data.Topic]
		mdc.MD.tpMu.RUnlock()

		if !ok {
			err = errors.New(Err.ErrSourceNotExist)
			break
		}

		t.mu.Lock()
		for _, part := range t.Part.Parts {
			if part.Part == data.Part {
				err = errors.New(Err.ErrSourceAlreadyExist)
				goto AddFalse
			}
		}
		t.Part.Parts = append(t.Part.Parts, &PartitionMD{
			Part: data.Part,
			BrokerGroup: BrokersGroupMD{
				Members: append(make([]*BrokerData, 0, len(data.Brokers)), data.Brokers...),
			},
		})

		t.Part.Term, err = mdc.MD.addTpTerm(t.Name)
		if err != nil {
			panic(err)
		}
		needReBalance := append(make([]string, 0, len(t.FollowerGroupID)), t.FollowerGroupID...)
	AddFalse:
		t.mu.Unlock()
		if err != nil {
			break
		}

		mdc.MD.cgMu.RLock()
		for _, i := range needReBalance {
			groupMD, ok := mdc.MD.ConsGroup[i]
			if ok {
				groupMD.mu.Lock()
				_ = mdc.MD.reBalance(groupMD)
				groupMD.mu.Unlock()
			}
		}
		mdc.MD.cgMu.RUnlock()
	case
		RemovePart:
		data := mdCommand.data.(struct {
			Topic string
			Part  string
		})

		mdc.MD.tpMu.RLock()
		t, ok := mdc.MD.Topics[data.Topic]
		mdc.MD.tpMu.RUnlock()
		if !ok {
			err = errors.New(Err.ErrSourceNotExist)
		}

		ok = false
		t.mu.Lock()

		for i, part := range t.Part.Parts {
			if part.Part == data.Part {
				ok = true
				t.Part.Parts = append(t.Part.Parts[:i], t.Part.Parts[i+1:]...)
			}
		}
		if !ok {
			err = errors.New(Err.ErrSourceNotExist)
			goto RemoveFalse
		}
		t.Part.Term, err = mdc.MD.addTpTerm(t.Name)
		if err != nil {
			panic(err)
		}
		needReBalance := append(make([]string, 0, len(t.FollowerGroupID)), t.FollowerGroupID...)

	RemoveFalse:
		t.mu.Unlock()
		if err != nil {
			break
		}

		mdc.MD.cgMu.RLock()
		for _, i := range needReBalance {
			groupMD, ok := mdc.MD.ConsGroup[i]
			if ok {
				groupMD.mu.Lock()
				_ = mdc.MD.reBalance(groupMD)
				groupMD.mu.Unlock()
			}
		}
		mdc.MD.cgMu.RUnlock()
	}

	return err, retData
}
func (psmd *PartitionSmD) Copy() *PartitionSmD {
	a := PartitionSmD{
		Term:  psmd.Term,
		Parts: make([]*PartitionMD, len(psmd.Parts)),
	}
	for i, part := range psmd.Parts {
		a.Parts[i] = part.Copy()
	}
	return &a
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

	err, _ = mdc.commit(
		CreateTopic, &tp)
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
	err, p := mdc.MetaDataRaft.Commit(
		MetadataCommand{
			Mode: mode,
			data: data,
		})
	return err, p
}

func (mdc *MetaDataController) QueryTopic(req *pb.QueryTopicRequest) *pb.QueryTopicResponse {
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
	err, data := mdc.commit(
		RegisterConsumer, &c)
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
	}
	err, _ = mdc.commit(
		UnRegisterConsumer, &i)
	return &pb.UnRegisterConsumerResponse{Response: ErrToResponse(err)}
}

func (mdc *MetaDataController) UnRegisterProducer(req *pb.UnRegisterProducerRequest) *pb.UnRegisterProducerResponse {
	if mdc.IsLeader() == false {
		return &pb.UnRegisterProducerResponse{Response: ResponseErrNotLeader()}
	}
	if req.Credential.Identity != pb.Credentials_Producer {
		return &pb.UnRegisterProducerResponse{Response: ResponseErrRequestIllegal()}
	}
	mdc.mu.RLock()
	defer mdc.mu.RUnlock()
	mdc.MD.pMu.RLock()
	err := mdc.MD.CheckProducer(req.Credential.Id)
	mdc.MD.pMu.RUnlock()
	if err != nil {
		return &pb.UnRegisterProducerResponse{Response: ErrToResponse(err)}
	}
	err, _ = mdc.commit(
		UnRegisterProducer, &req.Credential.Id)
	return &pb.UnRegisterProducerResponse{Response: ErrToResponse(err)}
}

func (mdc *MetaDataController) RegisterConsumerGroup(req *pb.RegisterConsumerGroupRequest) *pb.RegisterConsumerGroupResponse {
	if mdc.IsLeader() == false {
		return &pb.RegisterConsumerGroupResponse{
			Res: ResponseErrNotLeader(),
		}
	}
	IsAssign := true
reHash:
	if *req.GroupId == "" {
		IsAssign = false
		*req.GroupId = Random.RandStringBytes(16)
	}
	mdc.MD.cgMu.RLock()
	_, ok := mdc.MD.ConsGroup[*req.GroupId]
	mdc.MD.cgMu.RUnlock()
	if ok == false {
		if IsAssign {
			return &pb.RegisterConsumerGroupResponse{
				Res: ResponseErrSourceAlreadyExist(),
			}
		}
		*req.GroupId = ""
		goto reHash
	}

	Cg := struct {
		GroupID string
		Mode    pb.RegisterConsumerGroupRequest_PullOptionMode
	}{
		GroupID: *req.GroupId,
		Mode:    req.PullOption,
	}

	err, CgTerm := mdc.commit(
		RegisterConsGroup, &Cg)

	if err != nil {
		if err.Error() == Err.ErrSourceAlreadyExist && !IsAssign {
			goto reHash
		}
		return &pb.RegisterConsumerGroupResponse{Res: ErrToResponse(err)}
	}
	return &pb.RegisterConsumerGroupResponse{
		Res:       ResponseSuccess(),
		GroupTerm: CgTerm.(int32),
		Cred: &pb.Credentials{
			Identity: 0,
			Id:       *req.GroupId,
			Key:      "",
		},
	}
}

// 最后一个组员注销后自动注销
//func (mdc *MetaDataController) UnRegisterConsumerGroup(req *pb.UnRegisterConsumerGroupRequest) *pb.UnRegisterConsumerGroupResponse {
//}

func (mdc *MetaDataController) JoinRegisterConsumerGroup(req *pb.JoinConsumerGroupRequest) *pb.JoinConsumerGroupResponse {
	if mdc.IsLeader() == false {
		return &pb.JoinConsumerGroupResponse{Res: ResponseErrNotLeader()}
	}
	mdc.mu.RLock()
	defer mdc.mu.RUnlock()

	mdc.MD.cgMu.RLock()
	_, ok := mdc.MD.ConsGroup[req.ConsumerGroupId]
	mdc.MD.cgMu.RUnlock()

	if !ok {
		return &pb.JoinConsumerGroupResponse{Res: ResponseErrSourceNotExist()}
	}

	if !mdc.CreditCheck(req.Cred) {
		return &pb.JoinConsumerGroupResponse{Res: ResponseErrSourceNotExist()}
	}

	err, data_ := mdc.commit(
		JoinConsGroup, &struct {
			GroupID string
			SelfID  string
		}{
			GroupID: req.ConsumerGroupId,
			SelfID:  req.Cred.Id,
		})

	if err != nil {
		return &pb.JoinConsumerGroupResponse{Res: ErrToResponse(err)}
	}

	data := data_.(struct {
		FcParts   []*pb.Partition
		GroupTerm int32
	})
	return &pb.JoinConsumerGroupResponse{
		Res:       ResponseSuccess(),
		FcParts:   data.FcParts,
		GroupTerm: 0,
	}
}

func (mdc *MetaDataController) LeaveRegisterConsumerGroup(req *pb.LeaveConsumerGroupRequest) *pb.LeaveConsumerGroupResponse {
	if mdc.IsLeader() == false {
		return &pb.LeaveConsumerGroupResponse{Res: ResponseErrNotLeader()}
	}
	mdc.mu.RLock()
	defer mdc.mu.RUnlock()

	if !mdc.CreditCheck(req.ConsumerCred) || !mdc.CreditCheck(req.GroupCred) {
		return &pb.LeaveConsumerGroupResponse{Res: ResponseErrSourceNotExist()}
	}

	err, _ := mdc.commit(
		LeaveConsGroup, struct {
			GroupID string
			SelfID  string
		}{
			GroupID: req.GroupCred.Id,
			SelfID:  req.ConsumerCred.Id,
		})

	if err != nil {
		return &pb.LeaveConsumerGroupResponse{Res: ErrToResponse(err)}
	}

	return &pb.LeaveConsumerGroupResponse{Res: ResponseSuccess()}

}

func (mdc *MetaDataController) AddTopicRegisterConsumerGroup(req *pb.SubscribeTopicRequest) *pb.SubscribeTopicResponse {
	if !mdc.IsLeader() {
		return &pb.SubscribeTopicResponse{
			Res: ResponseErrNotLeader(),
		}
	}
	mdc.mu.RLock()
	defer mdc.mu.RUnlock()

	if !mdc.CreditCheck(req.CGCred) {
		return &pb.SubscribeTopicResponse{Res: ResponseErrSourceNotExist()}
	}

	err, _ := mdc.commit(
		ConsGroupFocalTopic, &struct {
			ConGiD string
			Topic  string
		}{
			ConGiD: req.CGCred.Id,
			Topic:  req.Tp,
		})
	if err != nil {
		return &pb.SubscribeTopicResponse{Res: ErrToResponse(err)}
	}

	return &pb.SubscribeTopicResponse{
		Res: ResponseSuccess(),
	}
}

func (mdc *MetaDataController) DelTopicRegisterConsumerGroup(req *pb.UnSubscribeTopicRequest) *pb.UnSubscribeTopicResponse {
	if !mdc.IsLeader() {
		return &pb.UnSubscribeTopicResponse{
			Res: ResponseErrNotLeader(),
		}
	}
	mdc.mu.RLock()
	defer mdc.mu.RUnlock()

	if !mdc.CreditCheck(req.CGCred) {
		return &pb.UnSubscribeTopicResponse{Res: ResponseErrSourceNotExist()}
	}

	err, _ := mdc.commit(
		ConsGroupUnFocalTopic, &struct {
			ConGiD string
			Topic  string
		}{
			ConGiD: req.CGCred.Id,
			Topic:  req.Tp,
		})
	if err != nil {
		return &pb.UnSubscribeTopicResponse{Res: ErrToResponse(err)}
	}

	return &pb.UnSubscribeTopicResponse{
		Res: ResponseSuccess(),
	}
}

//func (mdc *MetaDataController) DestroyTopic(req *pb.DestroyTopicRequest) *pb.DestroyTopicResponse {
//	return nil
//}

// hold g.mu.Lock()
func (md *MetaData) reBalance(g *ConsumersGroupMD) error {
	// 获取cgMu互斥锁，确保对 ConsumersGroup表 映射的读访问
	//md.cgMu.RLock()
	//defer md.cgMu.RUnlock()

	// 根据给定的GroupID检索ConsumersGroup
	//g, ok := md.ConsGroup[GroupID]
	//if !ok {
	//	return errors.New(Err.ErrSourceNotExist)
	//}

	// 获取ConsumersGroup互斥锁，确保对其字段的独占访问
	//g.mu.Lock()
	//defer g.mu.Unlock()

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
		if i, err := md.getTpTerm(T); err != nil {
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

	// 重写ConsumerFocalPartMap [包括重新为每个key-value分配slice]
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
	if len(members) > 0 {
		for i, tpu := range TpUrls {
			*g.ConsumersFcPart[members[i/len(members)]] = append(*g.ConsumersFcPart[members[i/len(members)]], tpu)
		}
	}

	// 更新ConsumersGroup的GroupTerm
	GroupTerm, err := md.addConsGroupTerm(g.GroupID)

	g.GroupTerm = GroupTerm

	if err != nil {
		return err
	}
	return nil
}

func (mdc *MetaDataController) AddPart(req *pb.AddPartRequest) *pb.AddPartResponse {
	if !mdc.IsLeader() {
		return &pb.AddPartResponse{Res: ResponseErrNotLeader()}
	}
	mdc.mu.RLock()
	defer mdc.mu.RUnlock()

	if mdc.CreditCheck(req.Cred) == false {
		return &pb.AddPartResponse{Res: ResponseErrSourceNotExist()}
	}

	var err error
	mdc.MD.bkMu.RLock()
	for _, bk := range req.Part.Brokers {
		if _, ok := mdc.MD.Brokers[bk.Name]; !ok {
			err = errors.New(Err.ErrSourceNotExist)
			break
		}
	}
	mdc.MD.bkMu.RUnlock()

	if err != nil {
		return &pb.AddPartResponse{Res: ErrToResponse(err)}
	}

	mdc.MD.tpMu.RLock()
	t, ok := mdc.MD.Topics[req.Part.Topic]
	mdc.MD.tpMu.RUnlock()
	if !ok {
		return &pb.AddPartResponse{Res: ResponseErrSourceNotExist()}
	}

	t.mu.RLock()
	for _, part := range t.Part.Parts {
		if part.Part == req.Part.Part {
			err = errors.New(Err.ErrSourceAlreadyExist)
			break
		}
	}
	t.mu.RUnlock()
	if err != nil {
		return &pb.AddPartResponse{Res: ErrToResponse(err)}
	}

	data := &struct {
		Topic   string
		Part    string
		Brokers []*BrokerData
	}{}
	data.Topic = req.Part.Topic
	data.Part = req.Part.Part
	for _, brokerData := range req.Part.Brokers {
		data.Brokers = append(data.Brokers, &BrokerData{
			Name: brokerData.Name,
			Url:  brokerData.Url,
		})
	}
	err, _ = mdc.commit(AddPart, data)
	return &pb.AddPartResponse{Res: ErrToResponse(err)}
}

func (mdc *MetaDataController) RemovePart(req *pb.RemovePartRequest) *pb.RemovePartResponse {
	if !mdc.IsLeader() {
		return &pb.RemovePartResponse{Res: ResponseErrNotLeader()}
	}
	mdc.mu.RLock()
	defer mdc.mu.RUnlock()

	if !mdc.CreditCheck(req.Cred) {
		return &pb.RemovePartResponse{Res: ResponseErrSourceNotExist()}
	}
	mdc.MD.tpMu.RLock()
	tp, ok := mdc.MD.Topics[req.Topic]
	mdc.MD.tpMu.RUnlock()
	if !ok {
		return &pb.RemovePartResponse{Res: ResponseErrSourceNotExist()}
	}

	ok = false
	tp.mu.RLock()
	for _, part := range tp.Part.Parts {
		if part.Part == req.Part {
			ok = true
			break
		}
	}
	tp.mu.RUnlock()

	if !ok {
		return &pb.RemovePartResponse{Res: ResponseErrSourceNotExist()}
	}
	data := &struct {
		Topic string
		Part  string
	}{}
	data.Topic = req.Topic
	data.Part = req.Part
	err, _ := mdc.commit(RemovePart, data)
	return &pb.RemovePartResponse{Res: ErrToResponse(err)}
}

func (mdc *MetaDataController) CreditCheck(Cred *pb.Credentials) bool {
	ok := false

	switch Cred.Identity {
	case pb.Credentials_Producer:
		mdc.MD.pMu.RLock()
		_, ok = mdc.MD.Producers[Cred.Id]
		mdc.MD.pMu.RUnlock()
	case pb.Credentials_Consumer:
		mdc.MD.cMu.RLock()
		_, ok = mdc.MD.Consumers[Cred.Id]
		mdc.MD.cMu.RUnlock()
	case pb.Credentials_ConsumerGroup:
		mdc.MD.cgMu.RLock()
		_, ok = mdc.MD.ConsGroup[Cred.Id]
		mdc.MD.cgMu.RUnlock()
	}

	return ok
}
