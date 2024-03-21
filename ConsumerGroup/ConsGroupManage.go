package ConsumerGroup

import (
	"BorsMqServer/Err"
	"BorsMqServer/RaftServer/Pack"
	"bytes"
	"io"
	"sync"
	"sync/atomic"
	"time"
)

type leaderCheck interface {
	IsLeader() bool
}

type GroupsManager struct {
	wg               sync.WaitGroup
	mu               sync.RWMutex
	IsStop           bool
	ID2ConsumerGroup map[string]*ConsumerGroup // CredId -- Index
	offlineCall      SessionLogoutNotifier
	leaderCheck
}

func (gm *GroupsManager) GetAllGroup() []*ConsumerGroup {
	gm.mu.RLock()
	defer gm.mu.RUnlock()
	data := []*ConsumerGroup{}
	for _, group := range gm.ID2ConsumerGroup {
		data = append(data, group)
	}
	return data
}
func (gm *GroupsManager) MakeSnapshot() []byte {
	bf := bytes.NewBuffer(nil)
	encoder := Pack.NewEncoder(bf)
	gm.mu.RLock()
	defer gm.mu.RUnlock()
	if err := encoder.Encode(gm.IsStop); err != nil {
		panic(err)
	}
	for _, group := range gm.ID2ConsumerGroup {
		if err := encoder.Encode(group.MakeSnapshot()); err != nil {
			panic(err)
		}
	}
	return bf.Bytes()
}

func (gm *GroupsManager) LoadSnapshot(bt []byte) {
	gm.mu.Lock()
	defer gm.mu.RUnlock()
	decoder := Pack.NewDecoder(bytes.NewBuffer(bt))
	err := decoder.Decode(&gm.IsStop)
	if err != nil {
		panic(err)
	}
	gm.ID2ConsumerGroup = make(map[string]*ConsumerGroup)
	for {
		var bts []byte
		err = decoder.Decode(&bts)
		if err != nil {
			if err == io.EOF {
				break
			} else {
				panic(err)
			}
		}
		i, g := SnapShotToConsumerGroup(bts)
		gm.ID2ConsumerGroup[i] = g
	}
}

func (gm *GroupsManager) CorrespondPart2Del() {
	gm.mu.RLock()
	defer gm.mu.RUnlock()
	for _, group := range gm.ID2ConsumerGroup {
		_ = group.ChangeState(ConsumerGroupToDel)
	}
}
func (gm *GroupsManager) IsNoGroupExist() bool {
	gm.mu.RLock()
	defer gm.mu.RUnlock()
	return len(gm.ID2ConsumerGroup) == 0
}

func (gm *GroupsManager) DelGroup(ID string) {
	gm.mu.Lock()
	defer gm.mu.Unlock()
	delete(gm.ID2ConsumerGroup, ID)
}
func NewGroupsManager(sessionLogoutNotifier SessionLogoutNotifier, check leaderCheck) *GroupsManager {
	group := &GroupsManager{
		IsStop:           false,
		ID2ConsumerGroup: make(map[string]*ConsumerGroup),
		offlineCall:      sessionLogoutNotifier,
		leaderCheck:      check,
	}
	group.wg.Add(1)
	go group.HeartbeatCheck()
	return group
}

func (cg *ConsumerGroup) CheckLastTimeCommit(lastTimeCommitIndex int64) (*[][]byte, error) {
	cg.mu.RLock()
	if lastTimeCommitIndex != cg.ConsumeOffsetLastTime {
		return cg.LastConsumeData, nil
	}
	cg.mu.RUnlock()
	return nil, nil
}

const (
	ConsumerGroupStart = iota
	ConsumerGroupNormal
	ConsumerGroupChangeAndWaitCommit
	ConsumerGroupToDel
)

// ConsumerGroup TODO: 重写Pull，UpdateCg逻辑，添加了字段Mode，考虑消费者转换时的行为，最后一次提交
type ConsumerGroup struct {
	Mode                  int32
	mu                    sync.RWMutex
	GroupId               string
	Consumers             *Consumer
	WaitingConsumers      *Consumer
	ConsumeOffset         int64 //ConsumeLastTimeGetDataOffset
	ConsumeOffsetLastTime int64 //ConsumeLastTimeGetDataOffset
	LastConsumeData       *[][]byte
}

func (cg *ConsumerGroup) MakeSnapshot() []byte {
	bf := bytes.NewBuffer(nil)
	encoder := Pack.NewEncoder(bf)

	cg.mu.RLock()
	defer cg.mu.RUnlock()

	if err := encoder.Encode(cg.Mode); err != nil {
		panic(err)
	}
	if err := encoder.Encode(cg.GroupId); err != nil {
		panic(err)
	}
	if err := encoder.Encode(cg.Consumers); err != nil {
		panic(err)
	}
	if err := encoder.Encode(cg.WaitingConsumers); err != nil {
		panic(err)
	}
	if err := encoder.Encode(cg.ConsumeOffset); err != nil {
		panic(err)
	}
	if err := encoder.Encode(cg.ConsumeOffsetLastTime); err != nil {
		panic(err)
	}
	if err := encoder.Encode(cg.LastConsumeData); err != nil {
		panic(err)
	}
	return bf.Bytes()
}

func SnapShotToConsumerGroup(snapshot []byte) (string, *ConsumerGroup) {
	cgNew := &ConsumerGroup{}
	decoder := Pack.NewDecoder(bytes.NewBuffer(snapshot))
	var err error
	if err = decoder.Decode(&cgNew.Mode); err != nil {
		panic(err)
	}
	if err = decoder.Decode(&cgNew.GroupId); err != nil {
		panic(err)
	}
	if err = decoder.Decode(&cgNew.Consumers); err != nil {
		panic(err)
	}
	if err = decoder.Decode(&cgNew.WaitingConsumers); err != nil {
		panic(err)
	}
	if err = decoder.Decode(&cgNew.ConsumeOffset); err != nil {
		panic(err)
	}
	if err = decoder.Decode(&cgNew.ConsumeOffsetLastTime); err != nil {
		panic(err)
	}
	if err = decoder.Decode(&cgNew.LastConsumeData); err != nil {
		panic(err)
	}
	return cgNew.GroupId, cgNew
}
func (c *ConsumerGroup) CheckConsumer(consID string) bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.Consumers != nil && c.Consumers.SelfId == consID
}

func (c *ConsumerGroup) GetMode() int32 {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.Mode
}

func (cg *ConsumerGroup) getLastTimeOffset_Data() (int64, *[][]byte, error) {
	cg.mu.RLock()
	defer cg.mu.RUnlock()
	if cg.LastConsumeData == nil {
		return 0, nil, (Err.ErrSourceNotExist)
	} else {
		return cg.ConsumeOffsetLastTime, cg.LastConsumeData, nil
	}
}

func (cg *ConsumerGroup) SetLastTimeOffset_Data(off int64, data *[][]byte) error {
	cg.mu.Lock()
	defer cg.mu.Unlock()
	if off <= cg.ConsumeOffsetLastTime {
		return Err.ErrRequestIllegal
	} else {
		cg.ConsumeOffsetLastTime = off
		cg.LastConsumeData = data
		cg.Consumers.TimeUpdate()
		return nil
	}
}

func (cg *ConsumerGroup) GetConsumeOffset() int64 {
	cg.mu.RLock()
	defer cg.mu.RUnlock()
	return cg.ConsumeOffset
}

func (cg *ConsumerGroup) SetConsumeOffset(offset int64) bool {
	cg.mu.Lock()
	defer cg.mu.Unlock()
	if offset > cg.ConsumeOffset {
		cg.ConsumeOffset = offset
		return true
	} else {
		return false
	}
}

func (cg *ConsumerGroup) Commit(off int64) (IsSuccess bool, LastTimeData [][]byte, LastTimeOffset int64) {
	cg.mu.RLock()
	defer cg.mu.RUnlock()
	if off == cg.ConsumeOffsetLastTime {
		return true, nil, -1
	} else {
		return false, *cg.LastConsumeData, cg.ConsumeOffsetLastTime
	}
	//cg.mu.RLock()
	//if cg.LastConsumeData == nil || cg.ConsumeOffsetLastTime == -1 {
	//	cg.mu.RUnlock()
	//	return nil, -1, false, nil
	//}
	//cg.mu.RUnlock()
	//
	//lastOff, data, err := cg.getLastTimeOffset_Data()
	//if err != nil {
	//	return nil, 0, false, err
	//}
	//if lastOff == off {
	//	cg.mu.Lock()
	//	cg.LastConsumeData = nil
	//	cg.ConsumeOffsetLastTime = -1
	//	cg.mu.Unlock()
	//	if cg.GetMode() == ConsumerGroupToDel {
	//		return nil, lastOff, true, nil
	//	}
	//	return nil, lastOff, false, nil
	//} else if off < lastOff {
	//	return nil, lastOff, false, (Err.ErrRequestIllegal)
	//} else {
	//	return *data, lastOff, false, nil
	//}
}

func (cg *ConsumerGroup) ChangeState(state int) error {
	cg.mu.Lock()
	defer cg.mu.Unlock()
	switch state {
	case ConsumerGroupStart:
		if cg.Mode != ConsumerGroupChangeAndWaitCommit {
			return Err.ErrRequestIllegal
		}
		cg.ConsumeOffsetLastTime = -1
		cg.LastConsumeData = nil
		cg.Mode = ConsumerGroupStart
	case ConsumerGroupChangeAndWaitCommit:
		if cg.Mode == ConsumerGroupToDel {
			return Err.ErrRequestIllegal
		}
		cg.Mode = ConsumerGroupChangeAndWaitCommit
	case ConsumerGroupToDel:
		cg.Mode = ConsumerGroupToDel
	case ConsumerGroupNormal:
		if cg.Mode != ConsumerGroupStart {
			return Err.ErrRequestIllegal
		} else {
			cg.Mode = ConsumerGroupNormal
		}
	default:
		return Err.ErrRequestIllegal
	}
	return nil
}
func (cg *ConsumerGroup) SetWaitConsumer(cons *Consumer) error {
	err := cg.ChangeState(ConsumerGroupChangeAndWaitCommit)
	if err != nil {
		return err
	}
	cg.mu.Lock()
	cg.WaitingConsumers = cons
	cg.mu.Unlock()
	return nil
}

// TODO : Part of Change Mode to ChangeWait...
func (cg *ConsumerGroup) ChangeConsumer(newConsID string) error {
	cg.mu.Lock()
	defer cg.mu.Unlock()

	if cg.Mode != ConsumerGroupChangeAndWaitCommit || cg.WaitingConsumers == nil || cg.WaitingConsumers.SelfId != newConsID {
		return Err.ErrRequestIllegal
	}

	if cg.Consumers == nil {
		cg.Mode = ConsumerGroupNormal
		cg.Consumers = cg.WaitingConsumers
		cg.Consumers.TimeUpdate()
		cg.WaitingConsumers = nil
		return nil
	} else {
		//cg.WaitingConsumer = newCons
		if cg.Consumers.Time < time.Now().UnixMilli() {
			cg.Consumers = cg.WaitingConsumers
			cg.ConsumeOffset = cg.ConsumeOffsetLastTime
			cg.ConsumeOffsetLastTime = -1
			cg.WaitingConsumers.TimeUpdate()
			cg.Mode = ConsumerGroupStart
			cg.WaitingConsumers = nil
			return nil
		}
		return Err.ErrNeedToWait
	}
}

func NewConsumerGroup(groupId string, consumer *Consumer, consumeOff int64) *ConsumerGroup {
	return &ConsumerGroup{
		Mode:                  ConsumerGroupStart,
		GroupId:               groupId,
		Consumers:             consumer,
		WaitingConsumers:      nil,
		ConsumeOffset:         consumeOff,
		ConsumeOffsetLastTime: -1,
		LastConsumeData:       nil,
	}
}

const (
	ConsumerMode_Connect    = 1
	ConsumerMode_DisConnect = 2
)

type Consumer struct {
	Mode                    int32
	SelfId                  string
	GroupId                 string
	Time                    int64 //NextTimeOutTime
	TimeoutSessionMsec      int32
	MaxReturnMessageSize    int32
	MaxReturnMessageEntries int32
}

func NewConsumer(selfId string, groupId string, timeoutSessionMsec, maxReturnMessageSize, MaxReturnMessageEntries int32) *Consumer {
	return &Consumer{
		Mode:                 ConsumerMode_Connect,
		SelfId:               selfId,
		GroupId:              groupId,
		Time:                 time.Now().UnixMilli() + int64(timeoutSessionMsec*2),
		TimeoutSessionMsec:   timeoutSessionMsec,
		MaxReturnMessageSize: maxReturnMessageSize, MaxReturnMessageEntries: MaxReturnMessageEntries,
	}
}

func (c *Consumer) CheckTimeout(now int64) bool {
	if atomic.LoadInt32(&c.Mode) == ConsumerMode_DisConnect {
		return true
	} else {
		if now > atomic.LoadInt64(&c.Time) {
			atomic.StoreInt32(&c.Mode, ConsumerMode_DisConnect)
			return true
		}
		return false
	}
}

func (c *Consumer) TimeUpdate() {
	atomic.StoreInt64(&c.Time, time.Now().UnixMilli()+int64(c.TimeoutSessionMsec))
}

type SessionLogoutNotifier interface {
	CancelReg2Cluster(consumer *Consumer)
	//Notify metadata service to log out due to session termination
}

func (gm *GroupsManager) HeartbeatCheck() {
	defer gm.wg.Done()
	for {
		time.Sleep(35 * time.Millisecond)
		if gm.IsStop {
			return
		}
		if gm.IsLeader() == false || !gm.mu.TryRLock() {
			continue
		} else {
			now := time.Now().UnixMilli()
			groups := make([]*ConsumerGroup, 0, len(gm.ID2ConsumerGroup))
			for _, Group := range gm.ID2ConsumerGroup {
				groups = append(groups, Group)
			}
			gm.mu.RUnlock()
			for _, Group := range groups {
				if Group.mu.TryRLock() {
					Cons := Group.Consumers
					Group.mu.RUnlock()
					IsTimeout := Cons.CheckTimeout(now)
					if IsTimeout {
						if Group.mu.TryLock() {
							Group.Consumers = nil
							if Group.Mode == ConsumerGroupToDel {
								gm.wg.Add(1)
								go func() {
									gm.DelGroup(Group.GroupId)
									gm.wg.Done()
								}()
							}
							Group.mu.Unlock()
							gm.wg.Add(1)
							go func() {
								gm.offlineCall.CancelReg2Cluster(Cons)
								gm.wg.Done()
							}()
						}
					}
				}
			}
		}
	}
}

func (gm *GroupsManager) UnregisterConsumerGroup(GroupId string) error {
	if gm.IsStop {
		return Err.ErrSourceNotExist
	}
	gm.mu.Lock()
	defer gm.mu.Unlock()
	if _, ok := gm.ID2ConsumerGroup[GroupId]; ok {
		return Err.ErrSourceNotExist
	} else {
		delete(gm.ID2ConsumerGroup, GroupId)
		return nil
	}
}

func (gm *GroupsManager) RegisterConsumerGroup(consumersGroup *ConsumerGroup) (*ConsumerGroup, error) {
	if gm.IsStop {
		return nil, Err.ErrSourceNotExist
	}
	gm.mu.Lock()
	defer gm.mu.Unlock()
	if _, ok := gm.ID2ConsumerGroup[consumersGroup.GroupId]; !ok {
		gm.ID2ConsumerGroup[consumersGroup.GroupId] = consumersGroup
		return consumersGroup, nil
	} else {
		return nil, Err.ErrSourceAlreadyExist
	}
}

func (gm *GroupsManager) GetConsumerGroup(groupId string) (*ConsumerGroup, error) {
	if gm.IsStop {
		return nil, (Err.ErrSourceNotExist)
	}
	gm.mu.RLock()
	defer gm.mu.RUnlock()
	i, ok := gm.ID2ConsumerGroup[groupId]
	if ok {
		return i, nil
	} else {
		return nil, (Err.ErrSourceNotExist)
	}
}

func (gm *GroupsManager) Stop() {
	gm.IsStop = true
	gm.wg.Wait()
}
