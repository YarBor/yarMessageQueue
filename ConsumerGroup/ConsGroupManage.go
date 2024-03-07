package ConsumerGroup

import (
	"MqServer/Err"
	"sync"
	"sync/atomic"
	"time"
)

type GroupsManager struct {
	wg               sync.WaitGroup
	mu               sync.RWMutex
	IsStop           bool
	ID2ConsumerGroup map[string]*ConsumerGroup // CredId -- Index
	SessionLogoutNotifier
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
func NewGroupsManager(sessionLogoutNotifier SessionLogoutNotifier) *GroupsManager {
	return &GroupsManager{
		IsStop:                false,
		ID2ConsumerGroup:      make(map[string]*ConsumerGroup),
		SessionLogoutNotifier: sessionLogoutNotifier,
	}
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
	GroupTerm             int32
	Consumers             *Consumer
	WaitingConsumers      *Consumer
	ConsumeOffset         int64 //ConsumeLastTimeGetDataOffset
	ConsumeOffsetLastTime int64 //ConsumeLastTimeGetDataOffset
	LastConsumeData       *[][]byte
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
		cg.Mode = ConsumerGroupStart
	case ConsumerGroupChangeAndWaitCommit:
		if cg.Mode == ConsumerGroupToDel {
			return Err.ErrRequestIllegal
		}
		cg.Mode = ConsumerGroupChangeAndWaitCommit
	case ConsumerGroupToDel:
		cg.Mode = ConsumerGroupToDel
	default:
		return Err.ErrRequestIllegal
	}
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
			cg.WaitingConsumers.TimeUpdate()
			cg.Mode = ConsumerGroupStart
			cg.WaitingConsumers = nil
			return nil
		}
		return (Err.ErrNeedToWait)
	}
}

func NewConsumerGroup(groupId string, maxReturnMessageSize int32) *ConsumerGroup {
	return &ConsumerGroup{
		Mode:                  ConsumerGroupStart,
		GroupId:               groupId,
		Consumers:             nil,
		ConsumeOffsetLastTime: 0,
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
		if !gm.mu.TryRLock() {
			continue
		} else {
			now := time.Now().UnixMilli()
			for _, Group := range gm.ID2ConsumerGroup {
				Group.mu.TryRLock()
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
							gm.CancelReg2Cluster(Cons)
							gm.wg.Done()
						}()
					}
				}
			}
		}
		gm.mu.RUnlock()
	}
}

func (gm *GroupsManager) UnregisterConsumerGroup(GroupId string) error {
	if gm.IsStop {
		return (Err.ErrSourceNotExist)
	}
	gm.mu.Lock()
	defer gm.mu.Unlock()
	if _, ok := gm.ID2ConsumerGroup[GroupId]; ok {
		return (Err.ErrSourceNotExist)
	} else {
		delete(gm.ID2ConsumerGroup, GroupId)
		return nil
	}
}

func (gm *GroupsManager) RegisterConsumerGroup(consumersGroup *ConsumerGroup) (*ConsumerGroup, error) {
	if gm.IsStop {
		return nil, (Err.ErrSourceNotExist)
	}
	gm.mu.Lock()
	defer gm.mu.Unlock()
	if _, ok := gm.ID2ConsumerGroup[consumersGroup.GroupId]; !ok {
		gm.ID2ConsumerGroup[consumersGroup.GroupId] = consumersGroup
		return consumersGroup, nil
	} else {
		return nil, (Err.ErrSourceAlreadyExist)
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
