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

var defaultEntryMaxSizeOf_1Block = int64(1e3)

const (
	PartitionCommand_Write = "w"
	PartitionCommand_ToDel = "t"
	PartitionCommand_Read  = "r"
	//PartitionCommand_PartUpdate  = "p" // TODO Think:Should Do this in clusters ?
	PartitionCommand_GroupUpdate = "g"
)

func (p *Partition) Handle(i interface{}) (error, interface{}) {
	cmd, ok := i.(PartitionCommand)
	if !ok {
		cmd = PartitionCommand{
			Mode: i.(map[string]interface{})["Mode"].(string),
			Data: i.(map[string]interface{})["Data"],
		}
	}
	switch cmd.Mode {
	case PartitionCommand_ToDel:
		err := p.partToDel()
		return err, nil
	case PartitionCommand_Write:
		data, ok1 := cmd.Data.([][]byte)
		if !ok1 {
			td := cmd.Data.([]interface{})
			data = make([][]byte, 0)
			for _, i3 := range td {
				data = append(data, i3.([]byte))
			}
		}
		err := p.write(data)
		return err, nil
	case PartitionCommand_GroupUpdate:
		data, ok1 := cmd.Data.(struct {
			Gid  string
			Cons *ConsumerGroup.Consumer
		})
		if !ok1 {
			data = struct {
				Gid  string
				Cons *ConsumerGroup.Consumer
			}{Gid: cmd.Data.(map[string]interface{})["Gid"].(string), Cons: &ConsumerGroup.Consumer{
				Mode:                    cmd.Data.(map[string]interface{})["Cons"].(map[string]interface{})["Mode"].(int32),
				SelfId:                  cmd.Data.(map[string]interface{})["Cons"].(map[string]interface{})["SelfId"].(string),
				GroupId:                 cmd.Data.(map[string]interface{})["Cons"].(map[string]interface{})["GroupId"].(string),
				Time:                    cmd.Data.(map[string]interface{})["Cons"].(map[string]interface{})["Time"].(int64),
				TimeoutSessionMsec:      cmd.Data.(map[string]interface{})["Cons"].(map[string]interface{})["TimeoutSessionMsec"].(int32),
				MaxReturnMessageSize:    cmd.Data.(map[string]interface{})["Cons"].(map[string]interface{})["MaxReturnMessageSize"].(int32),
				MaxReturnMessageEntries: cmd.Data.(map[string]interface{})["Cons"].(map[string]interface{})["MaxReturnMessageEntries"].(int32),
			}}
		}
		err := p.updateGroupConsumer(data.Gid, data.Cons)
		return err, nil
	//case PartitionCommand_ModeChange:
	case PartitionCommand_Read:
		data, ok1 := cmd.Data.(struct {
			ConsId, ConsGid string
			CommitIndex     int64
			ReadEntryNum    int32
		})
		if !ok1 {
			data = struct {
				ConsId, ConsGid string
				CommitIndex     int64
				ReadEntryNum    int32
			}{
				ConsId:       cmd.Data.(map[string]interface{})["ConsId"].(string),
				ConsGid:      cmd.Data.(map[string]interface{})["ConsGid"].(string),
				CommitIndex:  cmd.Data.(map[string]interface{})["CommitIndex"].(int64),
				ReadEntryNum: cmd.Data.(map[string]interface{})["ReadEntryNum"].(int32),
			}
		}
		Data, ReadBeginOffset, ReadEntriesNum, IsAllow2Del, err := p.read(data.ConsId, data.ConsGid, data.CommitIndex, data.ReadEntryNum)
		if err != nil {
			return err, nil
		}
		return nil, struct {
			Data            [][]byte
			ReadBeginOffset int64
			ReadEntriesNum  int64
			IsAllow2Del     bool
			err             error
		}{Data, ReadBeginOffset, ReadEntriesNum, IsAllow2Del, nil}
	default:
		panic("unknown command")
	}
}
func (p *Partition) partToDel() error {
	p.ChangeModeToDel()
	return nil
}

func (p *Partition) PartToDel() error {
	err, _ := p.commit(PartitionCommand_ToDel, nil)
	return err
}
func (p *Partition) write(data [][]byte) error {
	for _, datum := range data {
		p.MessageEntry.Write(datum)
	}
	return nil
}

func (p *Partition) Write(data [][]byte) error {
	err, _ := p.commit(PartitionCommand_Write, data)
	return err
}

type PartitionCommand struct {
	Mode string
	Data interface{}
}

func (p *Partition) updateGroupConsumer(gid string, cons *ConsumerGroup.Consumer) error {
	g, err := p.ConsumerGroupManager.GetConsumerGroup(gid)
	if err != nil {
		return err
	}
	return g.SetWaitConsumer(cons)
}

func (p *Partition) UpdateGroupConsumer(gid string, cons *ConsumerGroup.Consumer) (err error) {
	err, _ = p.commit(PartitionCommand_GroupUpdate, struct {
		Gid  string
		Cons *ConsumerGroup.Consumer
	}{gid, cons})
	return
}
func (p *Partition) GetAllConsumerGroup() []*ConsumerGroup.ConsumerGroup {
	return p.ConsumerGroupManager.GetAllGroup()
}

func (p *Partition) commit(cmd string, data interface{}) (error, interface{}) {
	if p.IsLeader() == false {
		return Err.ErrRequestNotLeader, nil
	}
	return p.Node.Commit(
		PartitionCommand{
			Mode: cmd,
			Data: data,
		})
}

func (p *Partition) IsLeader() bool {
	return p.Node.IsLeader()
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
	rfServer          *RaftServer.RaftServer
	ttMu              sync.RWMutex
	TopicTerm         map[string]*int32
	cgtMu             sync.RWMutex
	ConsumerGroupTerm map[string]*int32
	partsMu           sync.RWMutex
	P                 map[string]*map[string]*Partition // key: "Topic/Partition"
	handleTimeout     ConsumerGroup.SessionLogoutNotifier
}

func (c *PartitionsController) GetAllPart() []*Partition {
	c.partsMu.RLock()
	defer c.partsMu.RUnlock()
	data := []*Partition{}
	for _, m := range c.P {
		for _, partition := range *m {
			data = append(data, partition)
		}
	}
	return data
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

// ChangeModeToDel TODO : Commit Handle
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

// TODO : Commit-Handle
func (p *Partition) registerConsumerGroup(groupId string, consumer *ConsumerGroup.Consumer, consumeOff int64) (*ConsumerGroup.ConsumerGroup, error) {
	if atomic.LoadInt32(&p.Mode) == Partition_Mode_ToDel {
		return nil, Err.ErrSourceNotExist
	}
	return p.ConsumerGroupManager.RegisterConsumerGroup(ConsumerGroup.NewConsumerGroup(groupId, consumer, consumeOff))
}

func NewPartitionsController(rf *RaftServer.RaftServer, handleTimeout ConsumerGroup.SessionLogoutNotifier) *PartitionsController {
	return &PartitionsController{
		rfServer:          rf,
		ttMu:              sync.RWMutex{},
		TopicTerm:         make(map[string]*int32),
		cgtMu:             sync.RWMutex{},
		ConsumerGroupTerm: make(map[string]*int32),
		partsMu:           sync.RWMutex{},
		P:                 make(map[string]*map[string]*Partition),
		handleTimeout:     handleTimeout,
	}
}
func (part *Partition) Read(consId, ConsGid string, CommitIndex int64, ReadEntryNum int32) ([][]byte, int64, int64, bool, error) {
	err, data := part.commit(PartitionCommand_Read, struct {
		consId, ConsGid string
		CommitIndex     int64
		ReadEntryNum    int32
	}{consId, ConsGid, CommitIndex, ReadEntryNum})
	if err != nil {
		return nil, 0, 0, false, err
	}
	res := data.(struct {
		Data            [][]byte
		ReadBeginOffset int64
		ReadEntriesNum  int64
		IsAllow2Del     bool
		err             error
	})
	return res.Data, res.ReadBeginOffset, res.ReadEntriesNum, res.IsAllow2Del, res.err
}

// return: data , ReadBeginOffset ,readEntries Num, IsAllow to del , err
// consider FirstTime , commitIndex == -1
// TODO : ADD Field to save Offset Consumer consumed offset
func (part *Partition) read(consId, ConsGid string, CommitIndex int64, ReadEntryNum int32) ([][]byte, int64, int64, bool, error) {
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
		g.Consumers.TimeUpdate()
		BeginOffset, data, ReadNum := part.MessageEntry.Read(g.GetConsumeOffset(), ReadEntryNum, g.Consumers.MaxReturnMessageSize)
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
		g.Consumers.TimeUpdate()
		success, LastData, off := g.Commit(CommitIndex)
		if success {
			BeginOffset, data, ReadNum := part.MessageEntry.Read(g.GetConsumeOffset(), ReadEntryNum, g.Consumers.MaxReturnMessageSize)
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
		g.Consumers.TimeUpdate()
		success, Lastdata, off := g.Commit(CommitIndex)
		if success {
			if part.MessageEntry.IsClearToDel(CommitIndex) {
				part.ConsumerGroupManager.DelGroup(ConsGid)
				// Part-Check-Del In Other Part where Call Read
				return nil, -1, -1, true, nil
			} else {
				BeginOffset, data, ReadNum := part.MessageEntry.Read(g.GetConsumeOffset(), ReadEntryNum, g.Consumers.MaxReturnMessageSize)
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
			g.Consumers.TimeUpdate()
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
	part, err := pc.GetPart(t, p)
	if err != nil {
		return
	}
	if part.CheckToDel() {
		pc.partsMu.Lock()
		//(*pc.P[t])[p].ChangeModeToDel()
		delete(*pc.P[t], p)
		if len(*pc.P[t]) == 0 {
			delete(pc.P, t)
		}
		pc.partsMu.Unlock()
	}
}

func (pc *PartitionsController) GetPart(t, p string) (*Partition, error) {
	pc.partsMu.RLock()
	parts, ok := pc.P[t]
	pc.partsMu.RUnlock()
	if ok {
		part, ok := (*parts)[p]
		if ok {
			return part, nil
		}
	}
	return nil, Err.ErrSourceNotExist
}

func (ptc *PartitionsController) RegisterPart(t, p string,
	MaxEntries, MaxSize uint64,
	peers ...struct{ ID, Url string },
) (part *Partition, err error) {
	ptc.partsMu.Lock()
	defer ptc.partsMu.Unlock()
	if MaxSize == 0 {
		MaxSize = defaultMaxSize
	}
	if MaxEntries == 0 {
		MaxEntries = defaultMaxEntries
	}
	ok := false
	if _, ok = ptc.P[t]; ok {
	} else {
		ptc.P[t] = new(map[string]*Partition)
	}
	part, ok = (*ptc.P[t])[p]
	if !ok {
		part, err = newPartition(t, p, MaxEntries, MaxSize, ptc.handleTimeout, ptc.rfServer, peers...)
		if err != nil {
			return nil, err
		}
		(*ptc.P[t])[p] = part
	}
	return part, nil
}
