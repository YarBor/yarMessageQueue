package ConsumerGroup

import (
	"MqServer/Err"
	"errors"
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

func NewGroupsManager(sessionLogoutNotifier SessionLogoutNotifier) *GroupsManager {
	return &GroupsManager{
		IsStop:                false,
		ID2ConsumerGroup:      make(map[string]*ConsumerGroup),
		SessionLogoutNotifier: sessionLogoutNotifier,
	}
}

type ConsumerGroup struct {
	mu                   sync.RWMutex
	GroupId              string
	GroupTerm            int32
	Consumers            *Consumer
	MaxReturnMessageSize int32
	ConsumeOffset        uint64 //ConsumeLastTimeGetDataOffset
	LastConsumeData      *[][]byte
}

func NewConsumerGroup(groupId string, maxReturnMessageSize int32) *ConsumerGroup {
	return &ConsumerGroup{
		GroupId:              groupId,
		Consumers:            nil,
		MaxReturnMessageSize: maxReturnMessageSize,
		ConsumeOffset:        0,
		LastConsumeData:      nil,
	}
}

const (
	ConsumerMode_Connect    = 1
	ConsumerMode_DisConnect = 2
)

type Consumer struct {
	Mode               int32
	SelfId             string
	GroupId            string
	Time               int64 //NextTimeOutTime
	TimeoutSessionMsec int32
}

func NewConsumer(selfId string, groupId string, timeoutSessionMsec int32) *Consumer {
	return &Consumer{
		Mode:               ConsumerMode_Connect,
		SelfId:             selfId,
		GroupId:            groupId,
		Time:               time.Now().UnixMilli() + int64(timeoutSessionMsec*2),
		TimeoutSessionMsec: timeoutSessionMsec,
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
					gm.wg.Add(1)
					go func() {
						gm.CancelReg2Cluster(Cons)
						gm.wg.Done()
					}()
				}
			}
		}
		gm.mu.RUnlock()
	}
}

func (gm *GroupsManager) UnregisterConsumerGroup(GroupId string) error {
	if gm.IsStop {
		return errors.New(Err.ErrSourceNotExist)
	}
	gm.mu.Lock()
	defer gm.mu.Unlock()
	if _, ok := gm.ID2ConsumerGroup[GroupId]; ok {
		return errors.New(Err.ErrSourceNotExist)
	} else {
		delete(gm.ID2ConsumerGroup, GroupId)
		return nil
	}
}

func (gm *GroupsManager) RegisterConsumerGroup(consumersGroup *ConsumerGroup) error {
	if gm.IsStop {
		return errors.New(Err.ErrSourceNotExist)
	}
	gm.mu.Lock()
	defer gm.mu.Unlock()
	if _, ok := gm.ID2ConsumerGroup[consumersGroup.GroupId]; !ok {
		gm.ID2ConsumerGroup[consumersGroup.GroupId] = consumersGroup
		return nil
	} else {
		return errors.New(Err.ErrSourceAlreadyExist)
	}
}

func (gm *GroupsManager) Stop() {
	gm.IsStop = true
	gm.wg.Wait()
}
