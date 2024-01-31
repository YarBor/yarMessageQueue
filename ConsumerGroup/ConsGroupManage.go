package ConsumerGroup

import (
	"MqServer"
	"errors"
	"sync"
	"sync/atomic"
	"time"
)

type GroupManager struct {
	mu        sync.RWMutex
	wg        sync.WaitGroup
	IsStop    bool
	IdHash    map[uint32]int // CredId -- Index
	Consumers []*Consumer
	SessionLogoutNotifier
}

func NewConsumerHeartBeatManager(s SessionLogoutNotifier) *GroupManager {
	res := &GroupManager{
		IsStop:                false,
		IdHash:                make(map[uint32]int),
		Consumers:             make([]*Consumer, 0),
		SessionLogoutNotifier: s,
	}
	res.wg.Add(1)
	go res.heartbeatCheck()
	return res
}

type Consumer struct {
	MqServer.ConsumerMD
	mu sync.Mutex
	//TODO:
	//ConsumeLastTimeGetData
	ConsumeOffset uint64
	ConsumeData   [][]byte
	//NextTimeOutTime
	Time int64
}

func (c *Consumer) TimeUpdate() {
	atomic.StoreInt64(&c.Time, time.Now().UnixMilli()+int64(c.TimeoutSessionMsec))
}

type SessionLogoutNotifier interface {
	cancelReg2Cluster(consumer *Consumer)
	//Notify metadata service to log out due to session termination
}

func (gm *GroupManager) heartbeatCheck() {
	defer gm.wg.Done()
	for {
		time.Sleep(10 * time.Millisecond)
		if !gm.mu.TryLock() {
			continue
		}
		if gm.IsStop {
			return
		} else {
			l := make([]*Consumer, 0, len(gm.Consumers))
			for i := range gm.Consumers {
				if atomic.LoadInt64(&gm.Consumers[i].Time) > time.Now().UnixMilli() {
					l = append(l, gm.Consumers[i])
				} else {
					gm.wg.Add(1)
					go func() {
						gm.cancelReg2Cluster(gm.Consumers[i])
						gm.wg.Done()
					}()
				}
			}
			gm.Consumers = l
		}
		gm.mu.Unlock()
	}
}

func (gm *GroupManager) RegisterConsumer(consumer *Consumer) error {
	if gm.IsStop {
		return errors.New(MqServer.ErrSourceNotExist)
	}
	gm.mu.Lock()
	defer gm.mu.Unlock()
	if _, ok := gm.IdHash[consumer.Cred.Id]; !ok {
		gm.IdHash[consumer.Cred.Id] = len(gm.Consumers) - 1
		gm.Consumers = append(gm.Consumers, consumer)
		return nil
	} else {
		return errors.New(MqServer.ErrSourceAlreadyExist)
	}
}

func (gm *GroupManager) Stop() {
	gm.IsStop = true
	gm.wg.Wait()
}
