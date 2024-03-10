package MqServer

import (
	"MqServer/ConsumerGroup"
	"MqServer/Err"
	"MqServer/MessageMem"
	"MqServer/RaftServer"
	"MqServer/RaftServer/Pack"
	"bytes"
	"sync"
	"sync/atomic"
)

var (
	Partition_Mode_ToDel  = int32(1)
	Partition_Mode_Normal = int32(0)
)

type Partition struct {
	Mode                 int32
	Node                 *RaftServer.RaftNode
	T                    string
	P                    string
	ConsumerGroupManager *ConsumerGroup.GroupsManager
	MessageEntry         *MessageMem.MessageEntry
}

var defaultEntryMaxSizeOf_1Block = int64(10)

var (
	PartitionCommand_Write      = "w"
	PartitionCommand_ModeChange = "c"
	PartitionCommand_Read       = "r"
)

func (p *Partition) IsLeader() bool {
	return p.Node.IsLeader()
}

func (p *Partition) Handle(i interface{}) (error, interface{}) {
	//TODO implement me
	panic("implement me")
}

func (p *Partition) MakeSnapshot() []byte {
	bf := bytes.NewBuffer(nil)
	encode := Pack.NewEncoder(bf)
	if err := encode.Encode(atomic.LoadInt32(&p.Mode)); err != nil {
		panic(err)
	}
	if err := encode.Encode(p.ConsumerGroupManager.MakeSnapshot()); err != nil {
		panic(err)
	}
	if err := encode.Encode(p.MessageEntry.MakeSnapshot()); err != nil {
		panic(err)
	}
	return bf.Bytes()
}

func (p *Partition) LoadSnapshot(data []byte) {
	bf := bytes.NewBuffer(data)
	decode := Pack.NewDecoder(bf)

	var mode int32
	if err := decode.Decode(&mode); err != nil {
		panic(err)
	} else {
		atomic.StoreInt32(&p.Mode, mode)
	}
	var data1 []byte
	if err := decode.Decode(&data1); err != nil {
		panic(err)
	} else {
		p.ConsumerGroupManager.LoadSnapshot(data1)
	}
	var data2 []byte
	if err := decode.Decode(&data2); err != nil {
		panic(err)
	} else {
		p.MessageEntry.LoadSnapshot(data2)
	}
}

type PartitionsController struct {
	ttMu              sync.RWMutex
	TopicTerm         map[string]*int32
	cgtMu             sync.RWMutex
	ConsumerGroupTerm map[string]*int32
	partsMu           sync.RWMutex
	P                 map[string]*map[string]*Partition // key: "Topic/Partition"
	handleTimeout     ConsumerGroup.SessionLogoutNotifier
}

func (c *PartitionsController) Stop() {
	c.ttMu.Lock()
	c.cgtMu.Lock()
	c.partsMu.Lock()
	for _, ps := range c.P {
		for _, partition := range *ps {
			partition.Stop()
		}
	}
}

func (p *PartitionsController) GetTopicTerm(id string) (int32, error) {
	p.ttMu.RLock()
	defer p.ttMu.RUnlock()
	i, ok := p.TopicTerm[id]
	if ok {
		return atomic.LoadInt32(i), nil
	} else {
		return -1, (Err.ErrSourceNotExist)
	}
}
func (p *PartitionsController) GetConsumerGroupTerm(id string) (int32, error) {
	p.cgtMu.RLock()
	defer p.cgtMu.RUnlock()
	i, ok := p.ConsumerGroupTerm[id]
	if ok {
		return atomic.LoadInt32(i), nil
	} else {
		return -1, (Err.ErrSourceNotExist)
	}
}

func (p *Partition) Stop() {
	p.ConsumerGroupManager.Stop()
}

func (p *Partition) ChangeModeToDel() {
	atomic.StoreInt32(&p.Mode, Partition_Mode_ToDel)
	p.ConsumerGroupManager.CorrespondPart2Del()
}

func (p *Partition) CheckToDel() bool {
	return atomic.LoadInt32(&p.Mode) == Partition_Mode_ToDel || p.ConsumerGroupManager.IsNoGroupExist()
}

func newPartition(t, p string,
	MaxEntries, MaxSize uint64,
	handleTimeout ConsumerGroup.SessionLogoutNotifier,
	server *RaftServer.RaftServer,
	peers ...struct{ ID, Url string },
) (*Partition, error) {
	if len(peers) <= 0 {
		return nil, Err.ErrRequestIllegal
	}
	part := &Partition{
		Mode:         Partition_Mode_Normal,
		Node:         nil,
		T:            t,
		P:            p,
		MessageEntry: MessageMem.NewMessageEntry(MaxEntries, MaxSize, defaultEntryMaxSizeOf_1Block),
	}
	part.ConsumerGroupManager = ConsumerGroup.NewGroupsManager(handleTimeout, part)
	node, err := server.RegisterRfNode(t, p, part, part, peers...)
	if err != nil {
		return nil, err
	}
	part.Node = node
	return part, nil
}

func (p *Partition) registerConsumerGroup(groupId string, term int32, consumer *ConsumerGroup.Consumer, consumeOff int64) (*ConsumerGroup.ConsumerGroup, error) {
	if atomic.LoadInt32(&p.Mode) == Partition_Mode_ToDel {
		return nil, Err.ErrSourceNotExist
	}
	return p.ConsumerGroupManager.RegisterConsumerGroup(ConsumerGroup.NewConsumerGroup(groupId, term, consumer, consumeOff))
}

func NewPartitionsController(handleTimeout ConsumerGroup.SessionLogoutNotifier) *PartitionsController {
	return &PartitionsController{
		P:             make(map[string]*map[string]*Partition),
		handleTimeout: handleTimeout,
	}
}

// return: data , ReadBeginOffset ,readEntries Num, IsAllow to del , err
// consider FirstTime , commitIndex == -1
// TODO : ADD Field to save Offset Consumer consumed offset
func (part *Partition) Read(consId, ConsGid string, CommitIndex int64) ([][]byte, int64, int64, bool, error) {
	g, err := part.ConsumerGroupManager.GetConsumerGroup(ConsGid)
	if err != nil {
		return nil, -1, -1, false, err
	}
	gMode := g.GetMode()
Begin:
	switch gMode {
	case
		ConsumerGroup.ConsumerGroupStart:
		if !g.CheckConsumer(consId) {
			return nil, -1, -1, false, Err.ErrRequestIllegal
		}
		BeginOffset, data, ReadNum := part.MessageEntry.Read(g.GetConsumeOffset(), g.Consumers.MaxReturnMessageEntries, g.Consumers.MaxReturnMessageSize)
		err := g.SetLastTimeOffset_Data(BeginOffset, &data)
		if err != nil {
			panic(err)
		}
		if !g.SetConsumeOffset(BeginOffset + ReadNum) {
			return nil, -1, -1, false, Err.ErrRequestIllegal
		}
		return data, BeginOffset, ReadNum, false, nil

	case
		ConsumerGroup.ConsumerGroupNormal:
		if !g.CheckConsumer(consId) {
			return nil, -1, -1, false, Err.ErrRequestIllegal
		}
		success, LastData, off := g.Commit(CommitIndex)
		if success {
			BeginOffset, data, ReadNum := part.MessageEntry.Read(g.GetConsumeOffset(), g.Consumers.MaxReturnMessageEntries, g.Consumers.MaxReturnMessageSize)
			err := g.SetLastTimeOffset_Data(BeginOffset, &data)
			if err != nil {
				panic(err)
			}
			if !g.SetConsumeOffset(BeginOffset + ReadNum) {
				return nil, -1, -1, false, Err.ErrRequestIllegal
			}
			return data, BeginOffset, ReadNum, false, nil
		} else {
			return LastData, off, int64(len(LastData)), false, nil
		}
	case
		ConsumerGroup.ConsumerGroupToDel:
		if !g.CheckConsumer(consId) {
			return nil, -1, -1, false, Err.ErrRequestIllegal
		}
		success, Lastdata, off := g.Commit(CommitIndex)
		if success {
			if part.MessageEntry.IsClearToDel(CommitIndex) {
				part.ConsumerGroupManager.DelGroup(ConsGid)
				// Part-Check-Del In Other Part where Call Read
				return nil, -1, -1, true, nil
			} else {
				BeginOffset, data, ReadNum := part.MessageEntry.Read(g.GetConsumeOffset(), g.Consumers.MaxReturnMessageEntries, g.Consumers.MaxReturnMessageSize)
				err := g.SetLastTimeOffset_Data(BeginOffset, &data)
				if err != nil {
					panic(err)
				}
				if !g.SetConsumeOffset(BeginOffset + ReadNum) {
					return nil, -1, -1, false, Err.ErrRequestIllegal
				}
				return data, BeginOffset, ReadNum, false, nil
			}
		} else {
			return Lastdata, off, int64(len(Lastdata)), false, nil
		}
	case
		ConsumerGroup.ConsumerGroupChangeAndWaitCommit:
		if g.CheckConsumer(consId) {
			Success, data, off := g.Commit(CommitIndex)
			if Success {
				err := g.ChangeState(ConsumerGroup.ConsumerGroupStart)
				if err != nil {
					return nil, -1, -1, false, err
				} else {
					return nil, -1, -1, true, nil
				}
			} else {
				return data, off, int64(len(data)), false, nil
			}
		} else {
			if err := g.ChangeConsumer(consId); err == nil {
				goto Begin
			} else {
				return nil, -1, -1, false, err
			}
		}
	}

	panic("unreachable")
}

//一个是Part的回收，一个是心跳监测去call_Part状态改变

func (pc *PartitionsController) CheckPartToDel(t, p string) {
	part, err := pc.getPartition(t, p)
	if err != nil {
		return
	}
	if part.CheckToDel() {
		pc.partsMu.Lock()
		(*pc.P[t])[p].ChangeModeToDel()
		delete(*pc.P[t], p)
		if len(*pc.P[t]) == 0 {
			delete(pc.P, t)
		}
		pc.partsMu.Unlock()
	}
}

func (pc *PartitionsController) getPartition(t, p string) (*Partition, error) {
	pc.partsMu.RLock()
	parts, ok := pc.P[t]
	pc.partsMu.RUnlock()
	if ok {
		part, ok := (*parts)[p]
		if ok {
			return part, nil
		}
	}
	return nil, (Err.ErrSourceNotExist)
}

func (ptc *PartitionsController) RegisterPart(t, p string,
	MaxEntries, MaxSize uint64,
	server *RaftServer.RaftServer,
	peers ...struct{ ID, Url string },
) (part *Partition, err error) {
	ptc.partsMu.Lock()
	defer ptc.partsMu.Unlock()
	if MaxSize == -1 {
		MaxSize = defaultMaxSize
	}
	if MaxEntries == -1 {
		MaxEntries = defaultMaxEntries
	}
	ok := false
	if _, ok = ptc.P[t]; ok {
	} else {
		ptc.P[t] = new(map[string]*Partition)
	}
	part, ok = (*ptc.P[t])[p]
	if !ok {
		part, err = newPartition(t, p, MaxEntries, MaxSize, ptc.handleTimeout, server, peers...)
		if err != nil {
			return nil, err
		}
		(*ptc.P[t])[p] = part
	}
	return part, nil
}

func (ptc *PartitionsController) GetPart(t, p string) (part *Partition) {
	ptc.partsMu.Lock()
	defer ptc.partsMu.Unlock()
	ok := false
	if _, ok = ptc.P[t]; ok {
	} else {
		return nil
	}
	part, ok = (*ptc.P[t])[p]
	if !ok {
		return nil
	}
	return part
}
