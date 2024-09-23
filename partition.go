package MqServer

import (
	"bytes"
	"sync"
	"sync/atomic"
)

import (
	"github.com/YarBor/BorsMqServer/common"
	"github.com/YarBor/BorsMqServer/consumer_group"
	err_ "github.com/YarBor/BorsMqServer/error"
	"github.com/YarBor/BorsMqServer/message_memory"
	"github.com/YarBor/BorsMqServer/raft_server"
	Pack "github.com/YarBor/BorsMqServer/raft_server/pack"
)

const (
	Partition_Mode_ToDel  = int32(1)
	Partition_Mode_Normal = int32(0)
)

type Partition struct {
	Mode                 int32
	Node                 *raft_server.RaftNode
	T                    string
	P                    string
	ConsumerGroupManager *consumer_group.GroupsManager
	MessageEntry         *message_memory.MessageEntry
}

const (
	PartitionCommand_Write = "w"
	PartitionCommand_ToDel = "t"
	PartitionCommand_Read  = "r"
	//PartitionCommand_PartUpdate  = "p" // TODO Think:Should Do this in clusters ?
	PartitionCommand_GroupUpdate = "g"
)

func (_p *Partition) Handle(i interface{}) (error, interface{}) {
	cmd, ok := i.(PartitionCommand)
	if !ok {
		cmd = PartitionCommand{
			Mode: i.(map[string]interface{})["Mode"].(string),
			Data: i.(map[string]interface{})["Data"],
		}
	}
	switch cmd.Mode {
	case PartitionCommand_ToDel:
		err := _p.partToDel()
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
		err := _p.write(data)
		return err, nil
	case PartitionCommand_GroupUpdate:
		data, ok1 := cmd.Data.(struct {
			Gid  string
			Cons *consumer_group.Consumer
		})
		if !ok1 {
			_m := cmd.Data.(map[string]interface{})
			_m_Cons := _m["Cons"].(map[string]interface{})
			data = struct {
				Gid  string
				Cons *consumer_group.Consumer
			}{Gid: _m["Gid"].(string), Cons: &consumer_group.Consumer{
				Mode:                    _m_Cons["Mode"].(int32),
				SelfId:                  _m_Cons["SelfId"].(string),
				GroupId:                 _m_Cons["GroupId"].(string),
				Time:                    _m_Cons["Time"].(int64),
				TimeoutSessionMsec:      _m_Cons["TimeoutSessionMsec"].(int32),
				MaxReturnMessageSize:    _m_Cons["MaxReturnMessageSize"].(int32),
				MaxReturnMessageEntries: _m_Cons["MaxReturnMessageEntries"].(int32),
			}}
		}
		err := _p.updateGroupConsumer(data.Gid, data.Cons)
		return err, nil
	//case PartitionCommand_ModeChange:
	case PartitionCommand_Read:
		data, ok1 := cmd.Data.(struct {
			ConsId, ConsGid string
			CommitIndex     int64
			ReadEntryNum    int32
		})
		if !ok1 {
			_data := cmd.Data.(map[string]interface{})
			data = struct {
				ConsId, ConsGid string
				CommitIndex     int64
				ReadEntryNum    int32
			}{
				ConsId:       _data["ConsId"].(string),
				ConsGid:      _data["ConsGid"].(string),
				CommitIndex:  _data["CommitIndex"].(int64),
				ReadEntryNum: _data["ReadEntryNum"].(int32),
			}
		}
		Data, ReadBeginOffset, ReadEntriesNum, IsAllow2Del, err := _p.read(data.ConsId, data.ConsGid, data.CommitIndex, data.ReadEntryNum)
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
func (_p *Partition) partToDel() error {
	_p.ChangeModeToDel()
	return nil
}

func (_p *Partition) PartToDel() error {
	err, _ := _p.commit(PartitionCommand_ToDel, nil)
	return err
}
func (_p *Partition) write(data [][]byte) error {
	for _, datum := range data {
		_p.MessageEntry.Write(datum)
	}
	return nil
}

func (_p *Partition) Write(data [][]byte) error {
	err, _ := _p.commit(PartitionCommand_Write, data)
	return err
}

type PartitionCommand struct {
	Mode string
	Data interface{}
}

func (_p *Partition) updateGroupConsumer(gid string, cons *consumer_group.Consumer) error {
	g, err := _p.ConsumerGroupManager.GetConsumerGroup(gid)
	if err != nil {
		return err
	}
	return g.SetWaitConsumer(cons)
}

func (_p *Partition) UpdateGroupConsumer(gid string, cons *consumer_group.Consumer) (err error) {
	err, _ = _p.commit(PartitionCommand_GroupUpdate, struct {
		Gid  string
		Cons *consumer_group.Consumer
	}{gid, cons})
	return
}
func (_p *Partition) GetAllConsumerGroup() []*consumer_group.ConsumerGroup {
	return _p.ConsumerGroupManager.GetAllGroup()
}

func (_p *Partition) commit(cmd string, data interface{}) (error, interface{}) {
	if _p.IsLeader() == false {
		return err_.ErrRequestNotLeader, nil
	}
	return _p.Node.Commit(
		PartitionCommand{
			Mode: cmd,
			Data: data,
		})
}

func (_p *Partition) IsLeader() bool {
	return _p.Node.IsLeader()
}

func (_p *Partition) MakeSnapshot() []byte {
	bf := bytes.NewBuffer(nil)
	encode := Pack.NewEncoder(bf)
	if err := encode.Encode(atomic.LoadInt32(&_p.Mode)); err != nil {
		panic(err)
	}
	if err := encode.Encode(_p.ConsumerGroupManager.MakeSnapshot()); err != nil {
		panic(err)
	}
	if err := encode.Encode(_p.MessageEntry.MakeSnapshot()); err != nil {
		panic(err)
	}
	return bf.Bytes()
}

func (_p *Partition) LoadSnapshot(data []byte) {
	bf := bytes.NewBuffer(data)
	decode := Pack.NewDecoder(bf)

	var mode int32
	if err := decode.Decode(&mode); err != nil {
		panic(err)
	} else {
		atomic.StoreInt32(&_p.Mode, mode)
	}
	var data1 []byte
	if err := decode.Decode(&data1); err != nil {
		panic(err)
	} else {
		_p.ConsumerGroupManager.LoadSnapshot(data1)
	}
	var data2 []byte
	if err := decode.Decode(&data2); err != nil {
		panic(err)
	} else {
		_p.MessageEntry.LoadSnapshot(data2)
	}
}

type PartitionsController struct {
	rfServer          *raft_server.RaftServer
	ttMu              sync.RWMutex
	TopicTerm         map[string]*int32
	cgtMu             sync.RWMutex
	ConsumerGroupTerm map[string]*int32
	partsMu           sync.RWMutex
	P                 map[string]*map[string]*Partition // key: "Topic/Partition"
	handleTimeout     consumer_group.SessionLogoutNotifier
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
	defer c.ttMu.Unlock()
	c.cgtMu.Lock()
	defer c.cgtMu.Unlock()
	c.partsMu.Lock()
	defer c.partsMu.Unlock()
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
		return -1, (err_.ErrSourceNotExist)
	}
}

func (p *PartitionsController) GetConsumerGroupTerm(id string) (int32, error) {
	p.cgtMu.RLock()
	defer p.cgtMu.RUnlock()
	i, ok := p.ConsumerGroupTerm[id]
	if ok {
		return atomic.LoadInt32(i), nil
	} else {
		return -1, (err_.ErrSourceNotExist)
	}
}

func (_p *Partition) Stop() {
	_p.ConsumerGroupManager.Stop()
}

// ChangeModeToDel TODO : Commit Handle
func (_p *Partition) ChangeModeToDel() {
	atomic.StoreInt32(&_p.Mode, Partition_Mode_ToDel)
	_p.ConsumerGroupManager.CorrespondPart2Del()
}

func (_p *Partition) CheckToDel() bool {
	return atomic.LoadInt32(&_p.Mode) == Partition_Mode_ToDel || _p.ConsumerGroupManager.IsNoGroupExist()
}

func newPartition(t, p string,
	MaxEntries, MaxSize uint64,
	handleTimeout consumer_group.SessionLogoutNotifier,
	server *raft_server.RaftServer,
	peers ...struct{ ID, Url string },
) (*Partition, error) {
	if len(peers) <= 0 {
		return nil, err_.ErrRequestIllegal
	}
	part := &Partition{
		Mode:         Partition_Mode_Normal,
		Node:         nil,
		T:            t,
		P:            p,
		MessageEntry: message_memory.NewMessageEntry(MaxEntries, MaxSize, common.DefaultEntryMaxSizeOfEachBlock, t, p),
	}
	part.ConsumerGroupManager = consumer_group.NewGroupsManager(handleTimeout, part)
	node, err := server.RegisterRfNode(t, p, part, part, peers...)
	if err != nil {
		return nil, err
	}
	part.Node = node
	return part, nil
}
func (_p *Partition) Start() error {
	if _p.Node == nil {
		return err_.ErrRequestIllegal
	}
	_p.Node.Start()
	return nil
}

// TODO : Commit-Handle
func (_p *Partition) registerConsumerGroup(groupId string, consumer *consumer_group.Consumer, consumeOff int64) (*consumer_group.ConsumerGroup, error) {
	if atomic.LoadInt32(&_p.Mode) == Partition_Mode_ToDel {
		return nil, err_.ErrSourceNotExist
	}
	return _p.ConsumerGroupManager.RegisterConsumerGroup(consumer_group.NewConsumerGroup(groupId, consumer, consumeOff))
}

func NewPartitionsController(rf *raft_server.RaftServer, handleTimeout consumer_group.SessionLogoutNotifier) *PartitionsController {
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

func (_p *Partition) Read(ConsId, ConsGid string, CommitIndex int64, ReadEntryNum int32) ([][]byte, int64, int64, bool, error) {
	err, data := _p.commit(PartitionCommand_Read, struct {
		ConsId, ConsGid string
		CommitIndex     int64
		ReadEntryNum    int32
	}{ConsId, ConsGid, CommitIndex, ReadEntryNum})
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

// return: data , ReadBeginOffset ,readEntries Num, IsAllow to del , err_
// consider FirstTime , commitIndex == -1
// TODO : ADD Field to save Offset Consumer consumed offset
func (_p *Partition) read(ConsId, ConsGid string, CommitIndex int64, ReadEntryNum int32) ([][]byte, int64, int64, bool, error) {
	g, err := _p.ConsumerGroupManager.GetConsumerGroup(ConsGid)
	if err != nil {
		return nil, -1, -1, false, err
	}
	gMode := g.GetMode()
Begin:
	switch gMode {
	case
		consumer_group.ConsumerGroupStart:
		if !g.CheckConsumer(ConsId) {
			return nil, -1, -1, false, err_.ErrRequestIllegal
		}
		g.Consumers.TimeUpdate()
		if ReadEntryNum != 0 {
			BeginOffset, data, ReadNum := _p.MessageEntry.Read(g.GetConsumeOffset(), ReadEntryNum, g.Consumers.MaxReturnMessageSize)
			err := g.SetLastTimeOffset_Data(BeginOffset, &data)
			if err != nil {
				panic(err)
			}
			err = g.ChangeState(consumer_group.ConsumerGroupNormal)
			if err != nil {
				panic(err)
			}
			if !g.SetConsumeOffset(BeginOffset + ReadNum) {
				return nil, -1, -1, false, err_.ErrRequestIllegal
			}
			return data, BeginOffset, ReadNum, false, nil
		}
	case
		consumer_group.ConsumerGroupNormal:
		if !g.CheckConsumer(ConsId) {
			return nil, -1, -1, false, err_.ErrRequestIllegal
		}
		g.Consumers.TimeUpdate()
		if ReadEntryNum != 0 {
			success, LastData, off := g.Commit(CommitIndex)
			if success {
				BeginOffset, data, ReadNum := _p.MessageEntry.Read(g.GetConsumeOffset(), ReadEntryNum, g.Consumers.MaxReturnMessageSize)
				err := g.SetLastTimeOffset_Data(BeginOffset, &data)
				if err != nil {
					panic(err)
				}
				if !g.SetConsumeOffset(BeginOffset + ReadNum) {
					return nil, -1, -1, false, err_.ErrRequestIllegal
				}
				return data, BeginOffset, ReadNum, false, nil
			} else {
				return LastData, off, int64(len(LastData)), false, nil
			}
		}
	case
		consumer_group.ConsumerGroupToDel:
		if !g.CheckConsumer(ConsId) {
			return nil, -1, -1, false, err_.ErrRequestIllegal
		}
		g.Consumers.TimeUpdate()
		if ReadEntryNum != 0 {
			success, Lastdata, off := g.Commit(CommitIndex)
			if success {
				if _p.MessageEntry.IsClearToDel(CommitIndex) {
					_p.ConsumerGroupManager.DelGroup(ConsGid)
					// Part-Check-Del In Other Part where Call Read
					return nil, -1, -1, true, nil
				} else {
					BeginOffset, data, ReadNum := _p.MessageEntry.Read(g.GetConsumeOffset(), ReadEntryNum, g.Consumers.MaxReturnMessageSize)
					err := g.SetLastTimeOffset_Data(BeginOffset, &data)
					if err != nil {
						panic(err)
					}
					if !g.SetConsumeOffset(BeginOffset + ReadNum) {
						return nil, -1, -1, false, err_.ErrRequestIllegal
					}
					return data, BeginOffset, ReadNum, false, nil
				}
			} else {
				return Lastdata, off, int64(len(Lastdata)), false, nil
			}
		}

	case
		consumer_group.ConsumerGroupChangeAndWaitCommit:
		if g.CheckConsumer(ConsId) {
			g.Consumers.TimeUpdate()
			if ReadEntryNum == 0 {
				Success, data, off := g.Commit(CommitIndex)
				if Success {
					err := g.ChangeState(consumer_group.ConsumerGroupStart)
					if err != nil {
						return nil, -1, -1, false, err
					} else {
						return nil, -1, -1, true, nil
					}
				} else {
					return data, off, int64(len(data)), false, nil
				}
			} else {
			}
		} else {
			if err := g.ChangeConsumer(ConsId); err == nil {
				goto Begin
			} else {
				return nil, -1, -1, false, err
			}
		}
	}
	return nil, CommitIndex, 0, false, nil
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
	return nil, err_.ErrSourceNotExist
}

func (ptc *PartitionsController) RegisterPart(t, p string,
	MaxEntries, MaxSize uint64,
	peers ...struct{ ID, Url string },
) (part *Partition, err error) {
	ptc.partsMu.Lock()
	defer ptc.partsMu.Unlock()
	if MaxSize == 0 {
		MaxSize = uint64(common.PartDefaultMaxSize)
	}
	if MaxEntries == 0 {
		MaxEntries = uint64(common.PartDefaultMaxEntries)
	}
	ok := false
	if data, ok := ptc.P[t]; !ok || data == nil {
		data = &map[string]*Partition{}
		ptc.P[t] = data
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
