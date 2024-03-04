package MqServer

import (
	"MqServer/Err"
	Log "MqServer/Log"
	"MqServer/RaftServer"
	"MqServer/RaftServer/Pack"
	"MqServer/Random"
	pb "MqServer/rpc"
	"errors"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"time"
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

const (
	BrokerMode_BrokerConnected    = 1
	BrokerMode_BrokerDisconnected = 0
)

type BrokerMD struct {
	BrokerData
	// All Atomic operations
	HeartBeatSession int64
	TimeoutTime      int64
	IsMetadataNode   bool
	IsDisconnect     int32
	PartitionNum     uint32
}

func (md *BrokerMD) ResetTimeoutTime() {
	atomic.StoreInt64(&md.TimeoutTime, md.HeartBeatSession+time.Now().UnixMilli())
	atomic.StoreInt32(&md.IsDisconnect, BrokerMode_BrokerConnected)
}

func NewBrokerMD(IsMetadataNode bool, ID, url string, HeartBeatSession int64) *BrokerMD {
	return &BrokerMD{
		BrokerData: BrokerData{
			ID:  ID,
			Url: url,
		},
		HeartBeatSession: HeartBeatSession,
		TimeoutTime:      time.Now().UnixMilli(),
		IsMetadataNode:   IsMetadataNode,
		IsDisconnect:     BrokerMode_BrokerDisconnected,
		PartitionNum:     0,
	}
}

type BrokerData struct {
	ID  string
	Url string
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

func NewMetaDataController(node *RaftServer.RaftNode) *MetaDataController {
	return &MetaDataController{
		MetaDataRaft: node,
		mu:           sync.RWMutex{},
		MD:           NewMetaData(),
	}
}

func (mdc *MetaDataController) ConfirmIdentity(target *pb.Credentials) error {
	if !mdc.IsLeader() {
		return errors.New(Err.ErrRequestNotLeader)
	}
	mdc.mu.RLock()
	defer mdc.mu.RUnlock()
	if !mdc.CreditCheck(target) {
		return errors.New(Err.ErrRequestIllegal)
	} else {
		return nil
	}

}
func (mdc *MetaDataController) GetTopicTermDiff(Src map[string]int32) (map[string]int32, error) {
	if !mdc.IsLeader() {
		return nil, errors.New(Err.ErrRequestNotLeader)
	}
	mdc.mu.RLock()
	defer mdc.mu.RUnlock()
	returnData := map[string]int32{}
	for tp, term := range Src {
		mdc.MD.ttMu.RLock()
		i, ok := mdc.MD.TpTerm[tp]
		mdc.MD.ttMu.RUnlock()
		if ok {
			NewTerm := atomic.LoadInt32(i)
			if NewTerm != term {
				returnData[tp] = NewTerm
			}
		} else {
			returnData[tp] = -1
		}
	}
	return returnData, nil
}

func (mdc *MetaDataController) GetConsumerGroupTermDiff(Src map[string]int32) (map[string]int32, error) {
	if !mdc.IsLeader() {
		return nil, errors.New(Err.ErrRequestNotLeader)
	}
	mdc.mu.RLock()
	defer mdc.mu.RUnlock()
	returnData := map[string]int32{}
	for tp, term := range Src {
		mdc.MD.cgtMu.RLock()
		i, ok := mdc.MD.ConsGroupTerm[tp]
		mdc.MD.cgtMu.RUnlock()
		if ok {
			NewTerm := atomic.LoadInt32(i)
			if NewTerm != term {
				returnData[tp] = NewTerm
			}
		} else {
			returnData[tp] = -1
		}
	}
	return returnData, nil
}

func (mdc *MetaDataController) KeepBrokersAlive(id string) error {
	if mdc.IsLeader() == false {
		return errors.New(Err.ErrRequestNotLeader)
	}
	mdc.mu.RLock()
	defer mdc.mu.RUnlock()
	mdc.MD.bkMu.RLock()
	bk, ok := mdc.MD.Brokers[id]
	mdc.MD.bkMu.RUnlock()
	if !ok {
		return errors.New(Err.ErrSourceNotExist)
	} else {
		bk.ResetTimeoutTime()
		return nil
	}
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
	ConsGroupFocalTopic   = "ft"
	ConsGroupUnFocalTopic = "uft"
	DestroyTopic          = "dt"
	AddPart               = "ap"
	RemovePart            = "rp"
)

func ErrToResponse(err error) *pb.Response {
	if err == nil {
		return ResponseSuccess()
	}
	switch err.Error() {
	case Err.ErrFailure:
		return ResponseFailure()
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
		modeI := atomic.LoadInt32(&bks[i].IsDisconnect)
		modeJ := atomic.LoadInt32(&bks[j].IsDisconnect)
		if modeI == modeJ {
			return atomic.LoadUint32(&bks[i].PartitionNum) < atomic.LoadUint32(&bks[j].PartitionNum)
		}
		return modeI < modeJ
	})
	tmp := make([]*BrokerData, brokersNum)
	for i := range tmp {
		atomic.AddUint32(&bks[i%len(bks)].PartitionNum, 1)
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
					Id:  bk.ID,
					Url: bk.Url,
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
		data := mdCommand.data.(*struct {
			Name         string
			RequestParts []struct {
				PartitionName     string
				ReplicationNumber int32
			}
		})
		mdc.MD.tpMu.Lock()
		if _, ok := mdc.MD.Topics[data.Name]; ok {
			err = errors.New(Err.ErrSourceAlreadyExist)
		} else {
			tp := TopicMD{
				Name: data.Name,
				Part: PartitionSmD{
					Term:  0,
					Parts: make([]*PartitionMD, 0, len(data.RequestParts)),
				},
				FollowerGroupID:   make([]string, 0),
				FollowerProducers: make([]string, 0),
			}

			tp.Part.Term, err = mdc.MD.createTpTerm(tp.Name)
			if err != nil {
				panic(err)
			}

			for _, details := range data.RequestParts {
				bg := BrokersGroupMD{}
				bg.Members, err = mdc.MD.GetFreeBrokers(details.ReplicationNumber)
				if err != nil {

					mdc.MD.ttMu.Lock()
					delete(mdc.MD.TpTerm, tp.Name)
					mdc.MD.ttMu.Unlock()

					goto CreateFail
				}
				tp.Part.Parts = append(tp.Part.Parts, &PartitionMD{
					Part:        details.PartitionName,
					BrokerGroup: bg,
				})
			}

			if err != nil {
				panic("Ub")
			}
			retData = tp.Copy()
			mdc.MD.Topics[tp.Name] = &tp
		}
	CreateFail:
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
		if !isTopicClear {
			_, _ = mdc.MD.addTpTerm(topic.Name)
		}
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
					Id:  url.ID,
					Url: url.Url,
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
			if _, ok := mdc.MD.Brokers[bk.ID]; !ok {
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
	if err := mdc.MD.BrokersSourceCheck(); err != nil {
		return &pb.CreateTopicResponse{Response: ErrToResponse(err)}
	}
	mdc.MD.tpMu.RLock()
	err := mdc.MD.CheckTopic(req.Topic)
	mdc.MD.tpMu.RUnlock()

	if err != nil {
		return &pb.CreateTopicResponse{
			Response: ErrToResponse(err),
		}
	}

	var tp *TopicMD
	for _, details := range req.Partition {
		if details.ReplicationNumber == 0 {
			return &pb.CreateTopicResponse{Response: ResponseErrRequestIllegal()}
		}
	}

	info := struct {
		Name         string
		RequestParts []struct {
			PartitionName     string
			ReplicationNumber int32
		}
	}{
		Name: req.Topic,
		RequestParts: make([]struct {
			PartitionName     string
			ReplicationNumber int32
		}, 0, len(req.Partition)),
	}

	for _, details := range req.Partition {
		info.RequestParts = append(info.RequestParts, struct {
			PartitionName     string
			ReplicationNumber int32
		}{PartitionName: details.PartitionName, ReplicationNumber: details.ReplicationNumber})
	}
	if err, data := mdc.commit(CreateTopic, &info); err != nil {
		return &pb.CreateTopicResponse{Response: ErrToResponse(err)}
	} else {
		tp = data.(*TopicMD)
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
				Id:  member.ID,
				Url: member.Url,
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
				Id:  member.ID,
				Url: member.Url,
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
			Response: ResponseErrNotLeader(),
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
				Response: ResponseErrSourceAlreadyExist(),
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
		return &pb.RegisterConsumerGroupResponse{Response: ErrToResponse(err)}
	}
	return &pb.RegisterConsumerGroupResponse{
		Response:  ResponseSuccess(),
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
		return &pb.JoinConsumerGroupResponse{Response: ResponseErrNotLeader()}
	}
	mdc.mu.RLock()
	defer mdc.mu.RUnlock()

	mdc.MD.cgMu.RLock()
	_, ok := mdc.MD.ConsGroup[req.ConsumerGroupId]
	mdc.MD.cgMu.RUnlock()

	if !ok {
		return &pb.JoinConsumerGroupResponse{Response: ResponseErrSourceNotExist()}
	}

	if !mdc.CreditCheck(req.Cred) {
		return &pb.JoinConsumerGroupResponse{Response: ResponseErrSourceNotExist()}
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
		return &pb.JoinConsumerGroupResponse{Response: ErrToResponse(err)}
	}

	data := data_.(struct {
		FcParts   []*pb.Partition
		GroupTerm int32
	})
	return &pb.JoinConsumerGroupResponse{
		Response:  ResponseSuccess(),
		FcParts:   data.FcParts,
		GroupTerm: 0,
	}
}

func (mdc *MetaDataController) LeaveRegisterConsumerGroup(req *pb.LeaveConsumerGroupRequest) *pb.LeaveConsumerGroupResponse {
	if mdc.IsLeader() == false {
		return &pb.LeaveConsumerGroupResponse{Response: ResponseErrNotLeader()}
	}
	mdc.mu.RLock()
	defer mdc.mu.RUnlock()

	if !mdc.CreditCheck(req.ConsumerCred) || !mdc.CreditCheck(req.GroupCred) {
		return &pb.LeaveConsumerGroupResponse{Response: ResponseErrSourceNotExist()}
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
		return &pb.LeaveConsumerGroupResponse{Response: ErrToResponse(err)}
	}

	return &pb.LeaveConsumerGroupResponse{Response: ResponseSuccess()}

}

func (mdc *MetaDataController) AddTopicRegisterConsumerGroup(req *pb.SubscribeTopicRequest) *pb.SubscribeTopicResponse {
	if !mdc.IsLeader() {
		return &pb.SubscribeTopicResponse{
			Response: ResponseErrNotLeader(),
		}
	}
	mdc.mu.RLock()
	defer mdc.mu.RUnlock()

	if !mdc.CreditCheck(req.CGCred) {
		return &pb.SubscribeTopicResponse{Response: ResponseErrSourceNotExist()}
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
		return &pb.SubscribeTopicResponse{Response: ErrToResponse(err)}
	}

	return &pb.SubscribeTopicResponse{
		Response: ResponseSuccess(),
	}
}

func (mdc *MetaDataController) DelTopicRegisterConsumerGroup(req *pb.UnSubscribeTopicRequest) *pb.UnSubscribeTopicResponse {
	if !mdc.IsLeader() {
		return &pb.UnSubscribeTopicResponse{
			Response: ResponseErrNotLeader(),
		}
	}
	mdc.mu.RLock()
	defer mdc.mu.RUnlock()

	if !mdc.CreditCheck(req.CGCred) {
		return &pb.UnSubscribeTopicResponse{Response: ResponseErrSourceNotExist()}
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
		return &pb.UnSubscribeTopicResponse{Response: ErrToResponse(err)}
	}

	return &pb.UnSubscribeTopicResponse{
		Response: ResponseSuccess(),
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
		return &pb.AddPartResponse{Response: ResponseErrNotLeader()}
	}
	mdc.mu.RLock()
	defer mdc.mu.RUnlock()

	if mdc.CreditCheck(req.Cred) == false {
		return &pb.AddPartResponse{Response: ResponseErrSourceNotExist()}
	}

	var err error
	mdc.MD.bkMu.RLock()
	for _, bk := range req.Part.Brokers {
		if _, ok := mdc.MD.Brokers[bk.Id]; !ok {
			err = errors.New(Err.ErrSourceNotExist)
			break
		}
	}
	mdc.MD.bkMu.RUnlock()

	if err != nil {
		return &pb.AddPartResponse{Response: ErrToResponse(err)}
	}

	mdc.MD.tpMu.RLock()
	t, ok := mdc.MD.Topics[req.Part.Topic]
	mdc.MD.tpMu.RUnlock()
	if !ok {
		return &pb.AddPartResponse{Response: ResponseErrSourceNotExist()}
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
		return &pb.AddPartResponse{Response: ErrToResponse(err)}
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
			ID:  brokerData.Id,
			Url: brokerData.Url,
		})
	}
	err, _ = mdc.commit(AddPart, data)
	return &pb.AddPartResponse{Response: ErrToResponse(err)}
}

func (mdc *MetaDataController) RemovePart(req *pb.RemovePartRequest) *pb.RemovePartResponse {
	if !mdc.IsLeader() {
		return &pb.RemovePartResponse{Response: ResponseErrNotLeader()}
	}
	mdc.mu.RLock()
	defer mdc.mu.RUnlock()

	if !mdc.CreditCheck(req.Cred) {
		return &pb.RemovePartResponse{Response: ResponseErrSourceNotExist()}
	}
	mdc.MD.tpMu.RLock()
	tp, ok := mdc.MD.Topics[req.Topic]
	mdc.MD.tpMu.RUnlock()
	if !ok {
		return &pb.RemovePartResponse{Response: ResponseErrSourceNotExist()}
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
		return &pb.RemovePartResponse{Response: ResponseErrSourceNotExist()}
	}
	data := &struct {
		Topic string
		Part  string
	}{}
	data.Topic = req.Topic
	data.Part = req.Part
	err, _ := mdc.commit(RemovePart, data)
	return &pb.RemovePartResponse{Response: ErrToResponse(err)}
}

// Need mdc.mu.Rlock()
func (mdc *MetaDataController) CreditCheck(Cred *pb.Credentials) bool {
	ok := false
	if Cred == nil || Cred.Id == "" {
		return false
	}
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
	case pb.Credentials_Broker:
		mdc.MD.bkMu.RLock()
		_, ok = mdc.MD.Brokers[Cred.Id]
		mdc.MD.bkMu.RUnlock()
	}

	return ok
}

func (mdc *MetaDataController) CheckSourceTerm(req *pb.CheckSourceTermRequest) *pb.CheckSourceTermResponse {
	if !mdc.IsLeader() {
		return &pb.CheckSourceTermResponse{Response: ResponseErrNotLeader()}
	}
	if !mdc.CreditCheck(req.Self) {
		return &pb.CheckSourceTermResponse{Response: ResponseErrSourceNotExist()}
	}

	mdc.mu.RLock()
	defer mdc.mu.RUnlock()

	switch req.Self.Identity {
	case pb.Credentials_Producer:
		if req.TopicData == nil {
			return &pb.CheckSourceTermResponse{Response: ResponseErrRequestIllegal()}
		}
		tm, err := mdc.MD.getTpTerm(req.TopicData.Topic)
		if err != nil {
			return &pb.CheckSourceTermResponse{Response: ErrToResponse(err)}
		}
		if req.TopicData.TopicTerm == tm {
			return &pb.CheckSourceTermResponse{Response: ResponseSuccess()}
		} else {
			mdc.MD.tpMu.RLock()
			tp, err := mdc.MD.QueryTopic(req.TopicData.Topic)
			mdc.MD.tpMu.RUnlock()
			if err != nil {
				return &pb.CheckSourceTermResponse{Response: ErrToResponse(err)}
			} else {
				i := &pb.CheckSourceTermResponse{
					Response:  ResponseErrPartitionChanged(),
					TopicTerm: tp.Part.Term,
					GroupTerm: 0,
					TopicData: &pb.CheckSourceTermResponse_PartsData{
						FcParts: make([]*pb.CheckSourceTermResponse_PartsData_Parts, 0, len(tp.Part.Parts)),
					},
				}
				for _, part := range tp.Part.Parts {
					p := &pb.Partition{
						Topic:   tp.Name,
						Part:    part.Part,
						Brokers: make([]*pb.BrokerData, 0, len(part.BrokerGroup.Members)),
					}
					for _, member := range part.BrokerGroup.Members {
						p.Brokers = append(p.Brokers, &pb.BrokerData{
							Id:  member.ID,
							Url: member.Url,
						})
					}
					i.TopicData.FcParts = append(i.TopicData.FcParts, &pb.CheckSourceTermResponse_PartsData_Parts{
						FcParts: p,
					})
				}
				return i
			}
		}
		// 通过 Cid 下载所属的分区
	case pb.Credentials_ConsumerGroup:
		if req.ConsumerData == nil {
			return &pb.CheckSourceTermResponse{
				Response: ResponseErrRequestIllegal(),
			}
		}
		if req.ConsumerData.ConsumerId == nil {
			return &pb.CheckSourceTermResponse{Response: ResponseErrRequestIllegal()}
		}
		tm, err := mdc.MD.getConsGroupTerm(req.Self.Id)
		if err != nil {
			return &pb.CheckSourceTermResponse{Response: ErrToResponse(err)}
		}
		if req.ConsumerData.GroupTerm == tm {
			return &pb.CheckSourceTermResponse{
				Response: ResponseSuccess(),
			}
		} else {
			mdc.MD.cgMu.RLock()
			group, ok := mdc.MD.ConsGroup[req.Self.Id]
			mdc.MD.cgMu.RUnlock()
			if !ok {
				return &pb.CheckSourceTermResponse{Response: ResponseErrSourceNotExist()}
			}

			group.mu.RLock()
			data, ok1 := group.ConsumersFcPart[*req.ConsumerData.ConsumerId]
			if !ok1 {
				group.mu.RUnlock()
				return &pb.CheckSourceTermResponse{Response: ResponseErrSourceNotExist()}
			}
			i := &pb.CheckSourceTermResponse{
				Response:  ResponseErrPartitionChanged(),
				GroupTerm: group.GroupTerm,
				ConsumersData: &pb.CheckSourceTermResponse_PartsData{
					FcParts: make([]*pb.CheckSourceTermResponse_PartsData_Parts, 0, len(*data)),
				},
			}
			for _, fcp := range *data {
				p := &pb.Partition{
					Topic:   fcp.Topic,
					Part:    fcp.Part,
					Brokers: make([]*pb.BrokerData, 0, len(fcp.Urls)),
				}
				for _, url := range fcp.Urls {
					p.Brokers = append(p.Brokers, &pb.BrokerData{
						Id:  url.ID,
						Url: url.Url,
					})
				}
				i.ConsumersData.FcParts = append(i.ConsumersData.FcParts, &pb.CheckSourceTermResponse_PartsData_Parts{
					FcParts:    p,
					ConsumerID: req.ConsumerData.ConsumerId,
				})
			}
			group.mu.RUnlock()
			return i
		}

		// 通过Gid下载所有的分区
	case pb.Credentials_Broker:
		i := &pb.CheckSourceTermResponse{
			Response:      nil,
			TopicTerm:     0,
			GroupTerm:     0,
			ConsumersData: nil,
			TopicData:     nil,
		}
		if req.TopicData != nil && req.TopicData.Topic != "" {
			tm, err := mdc.MD.getTpTerm(req.TopicData.Topic)
			if err != nil {
				return &pb.CheckSourceTermResponse{Response: ErrToResponse(err)}
			}
			if req.TopicData.TopicTerm == tm {
				return &pb.CheckSourceTermResponse{Response: ResponseSuccess()}
			} else {
				mdc.MD.tpMu.RLock()
				tp, err := mdc.MD.QueryTopic(req.TopicData.Topic)
				mdc.MD.tpMu.RUnlock()
				if err != nil {
					return &pb.CheckSourceTermResponse{Response: ErrToResponse(err)}
				} else {
					i.Response = ResponseErrPartitionChanged()
					i.TopicTerm = tp.Part.Term
					i.TopicData = &pb.CheckSourceTermResponse_PartsData{
						FcParts:                  make([]*pb.CheckSourceTermResponse_PartsData_Parts, 0, len(tp.Part.Parts)),
						FollowerProducerIDs:      &pb.CheckSourceTermResponse_IDs{ID: append(make([]string, 0, len(tp.FollowerProducers)), tp.FollowerProducers...)},
						FollowerConsumerGroupIDs: &pb.CheckSourceTermResponse_IDs{ID: append(make([]string, 0, len(tp.FollowerGroupID)), tp.FollowerGroupID...)},
					}
					for _, part := range tp.Part.Parts {
						p := &pb.Partition{
							Topic:   tp.Name,
							Part:    part.Part,
							Brokers: make([]*pb.BrokerData, 0, len(part.BrokerGroup.Members)),
						}
						for _, member := range part.BrokerGroup.Members {
							p.Brokers = append(p.Brokers, &pb.BrokerData{
								Id:  member.ID,
								Url: member.Url,
							})
						}
						i.TopicData.FcParts = append(i.TopicData.FcParts, &pb.CheckSourceTermResponse_PartsData_Parts{
							FcParts: p,
						})
					}
				}
			}

		}
		if req.ConsumerData != nil && req.ConsumerData.ConsumerId != nil {
			tm, err := mdc.MD.getConsGroupTerm(req.Self.Id)
			if err != nil {
				return &pb.CheckSourceTermResponse{Response: ErrToResponse(err)}
			}
			if req.ConsumerData.GroupTerm == tm {
				return i
			} else {
				mdc.MD.cgMu.RLock()
				group, ok := mdc.MD.ConsGroup[req.ConsumerData.GroupID]
				mdc.MD.cgMu.RUnlock()
				if !ok {
					return &pb.CheckSourceTermResponse{Response: ResponseErrSourceNotExist()}
				}

				group.mu.RLock()

				i.Response = ResponseErrPartitionChanged()
				i.GroupTerm = group.GroupTerm
				i.ConsumersData = &pb.CheckSourceTermResponse_PartsData{
					FcParts: make([]*pb.CheckSourceTermResponse_PartsData_Parts, 0, len(group.ConsumersFcPart)),
				}
				for consID, fcps := range group.ConsumersFcPart {
					for _, fcp := range *fcps {
						p := &pb.Partition{
							Topic:   fcp.Topic,
							Part:    fcp.Part,
							Brokers: make([]*pb.BrokerData, 0, len(fcp.Urls)),
						}
						for _, url := range fcp.Urls {
							p.Brokers = append(p.Brokers, &pb.BrokerData{
								Id:  url.ID,
								Url: url.Url,
							})
						}
						cons, err := mdc.MD.QueryConsumer(consID)
						if err != nil {
							Log.ERROR("Error querying consumer")
						}
						i.ConsumersData.FcParts = append(i.ConsumersData.FcParts, &pb.CheckSourceTermResponse_PartsData_Parts{
							FcParts:                      p,
							ConsumerID:                   &consID,
							ConsumerTimeoutSession:       &cons.TimeoutSessionMsec,
							ConsumerMaxReturnMessageSize: &cons.MaxReturnMessageSize,
						})
					}
				}
				group.mu.RUnlock()
			}
		}
		return i
	default:
		return &pb.CheckSourceTermResponse{Response: ResponseErrRequestIllegal()}
	}
}

func (md *MetaData) BrokersSourceCheck() error {
	md.bkMu.RLock()
	defer md.bkMu.RUnlock()
	count := 0
	for _, brokerMD := range md.Brokers {
		if brokerMD.IsDisconnect == BrokerMode_BrokerDisconnected {
			count++
		}
	}
	if count > len(md.Brokers) {
		return errors.New(Err.ErrFailure)
	}
	return nil
}

func (mdc *MetaDataController) CheckBrokersAlive() {
	for {
		if mdc.mu.TryRLock() {
			if mdc.MD.bkMu.TryRLock() {
				now := time.Now().UnixMilli()
				for _, brokerMD := range mdc.MD.Brokers {
					if atomic.LoadInt64(&brokerMD.TimeoutTime) < now {
						// >> Mey here Race condition
						atomic.StoreInt32(&brokerMD.IsDisconnect, BrokerMode_BrokerDisconnected)
						// For Race
						if atomic.LoadInt64(&brokerMD.TimeoutTime) > now {
							atomic.StoreInt32(&brokerMD.IsDisconnect, BrokerMode_BrokerConnected)
						}
					}

				}
				mdc.MD.bkMu.RUnlock()
			}
			mdc.mu.RUnlock()
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// TODO: go
func (mdc *MetaDataController) ConsumerDisConnect(info *pb.DisConnectInfo) *pb.Response {
	if !mdc.IsLeader() {
		return ResponseErrNotLeader()
	}

	mdc.mu.RLock()
	defer mdc.mu.RUnlock()

	if !(mdc.CreditCheck(info.BrokerInfo) && mdc.CreditCheck(info.TargetInfo)) {
		return ResponseErrSourceNotExist()
	}

	data := mdc.CheckSourceTerm(&pb.CheckSourceTermRequest{
		Self: info.BrokerInfo,
		ConsumerData: &pb.CheckSourceTermRequest_ConsumerCheck{
			ConsumerId: &info.TargetInfo.Id,
			GroupTerm:  -1, // to get Newest parts data
		},
	})

	for _, part := range data.ConsumersData.FcParts {
		if part.ConsumerID == nil {
			Log.FATAL("Consumer Protocol Error")
		}
		if *part.ConsumerID == info.TargetInfo.Id {
			for _, brokerData := range part.FcParts.Brokers {
				if brokerData.Id == info.BrokerInfo.Id {
					go mdc.UnRegisterConsumer(&pb.UnRegisterConsumerRequest{Credential: info.TargetInfo})
					return ResponseSuccess()
				}
			}
		}
	}

	return ResponseSuccess()
}

func (mdc *MetaDataController) ProducerDisConnect(info *pb.DisConnectInfo) *pb.Response {
	if !mdc.IsLeader() {
		return ResponseErrNotLeader()
	}

	mdc.mu.RLock()
	defer mdc.mu.RUnlock()

	if !(mdc.CreditCheck(info.BrokerInfo) && mdc.CreditCheck(info.TargetInfo)) {
		return ResponseErrSourceNotExist()
	}

	go mdc.UnRegisterProducer(&pb.UnRegisterProducerRequest{Credential: info.TargetInfo})
	return ResponseSuccess()
}

// func for HeartBeat
func (mdc *MetaDataController) GetAllBrokers() []*BrokerMD {
	mdc.mu.RLock()
	mdc.MD.bkMu.RLock()
	data := []*BrokerMD{}
	for _, bk := range mdc.MD.Brokers {
		data = append(data, bk.Copy())
	}
	mdc.MD.bkMu.RUnlock()
	mdc.mu.RUnlock()
	return data
}

func (mdc *MetaDataController) SetBrokerAlive(ID string) error {
	if mdc.IsLeader() {
		mdc.mu.RLock()
		mdc.MD.bkMu.RLock()
		bk, ok := mdc.MD.Brokers[ID]
		mdc.MD.bkMu.RUnlock()
		mdc.mu.RUnlock()
		if !ok {
			return errors.New(Err.ErrSourceNotExist)
		} else {
			atomic.StoreInt64(&bk.TimeoutTime, time.Now().UnixMilli())
		}
		return nil
	}
	return errors.New(Err.ErrRequestNotLeader)
}
