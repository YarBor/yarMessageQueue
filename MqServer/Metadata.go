package MqServer

import (
	"MqServer/Log"
	"MqServer/Raft"
	pb "MqServer/rpc"
	"sync"
)

type CompetitionGroup struct {
	CompeteConsumers []Consumer
	LoadBalanceFunc  func(group *CompetitionGroup) int
}

type TopicPartition struct {
	FileName    string
	FileSize    int32
	Messages    []pb.Message
	StartIndex  int32
	NowIndex    int32
	MemoryLimit int32
	EntryLimit  int32
	Group       CompetitionGroup
}

type Topic struct {
	// all
	Consumers  []Consumer
	Partitions []TopicPartition
}

type MetadataHandler struct {
	mu sync.RWMutex
	*AccountController
	IsMultipleClusters bool
	Brokers            []Broker
	Topics             []Topic
	RaftApplyChan      chan Raft.ApplyMsg
	wg                 sync.WaitGroup
	stop               chan struct{}
}

func (h *MetadataHandler) Stop() {
	h.stop <- struct{}{}
	h.wg.Wait()
}

type AccountController struct {
	Set map[interface{}]interface{}
}

func MakeAccountController() *AccountController {
	return &AccountController{
		Set: make(map[interface{}]interface{}),
	}
}
func MakeMetadataHandler() *MetadataHandler {
	s := &MetadataHandler{
		mu:                 sync.RWMutex{},
		AccountController:  MakeAccountController(),
		Brokers:            make([]Broker, 0),
		Topics:             make([]Topic, 0),
		RaftApplyChan:      make(chan Raft.ApplyMsg),
		stop:               make(chan struct{}),
		IsMultipleClusters: false,
	}
	return s
}
func (h *MetadataHandler) Start(isMultipleClusters bool) {
	if isMultipleClusters {
		h.IsMultipleClusters = true
	}
	h.wg.Add(1)
	go h.metadataHandle()
}

type MetaDataCommand struct {
}

func (h *MetadataHandler) metadataHandle() {
	defer h.wg.Done()
	select {
	case <-h.stop:
		return
	case mess, ok := <-h.RaftApplyChan:
		if !ok {
			Log.FATAL("Metadata handler ApplyChan Closed")
		} else if command, ok := mess.Command.(MetaDataCommand); !ok {
			Log.FATAL("Metadata handler Message Translate Fail")
		} else {
			h.Handle(command)
		}
	}
}
func (h *MetadataHandler) Handle(c MetaDataCommand) {
}
