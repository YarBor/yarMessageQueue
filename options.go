package MqServer

import (
	"MqServer/common"
	"errors"
	"fmt"
	"net"
	"strconv"
	"time"
)

type BuildOptions func() (string, interface{}, error)

type BrokerOptions struct {
	data map[string]interface{}
	opts []BuildOptions
	err  error
}

func NewBrokerOptions() *BrokerOptions {
	return &BrokerOptions{
		opts: []BuildOptions{},
		data: make(map[string]interface{}),
		err:  errors.New("Options Need Build And Check"),
	}
}

func NewBrokerOptionsFormFields(data map[string]interface{}) *BrokerOptions {
	return &BrokerOptions{
		data: data,
		opts: []BuildOptions{},
		err:  nil,
	}
}

func (o *BrokerOptions) With(options ...BuildOptions) *BrokerOptions {
	o.opts = append(o.opts, options...)
	return o
}

func (o *BrokerOptions) Build() (*BrokerOptions, error) {
	for _, option := range o.opts {
		service, input, err := option()
		if err != nil {
			o.err = err
			return nil, err
		}
		existingData, exists := o.data[service]
		if exists {
			// 合并数据
			if existingMap, ok := existingData.(map[string]interface{}); !ok {
				panic(o)
			} else {
				inputMap, ok := input.(map[string]interface{})
				if !ok {
					panic(o)
				}
				for key, value := range inputMap {
					existingMap[key] = value
				}
			}
		}
	}
	return o.Check()
}

func (o *BrokerOptions) Check() (*BrokerOptions, error) {
	o.err = nil
	return o, nil
}

func IsMetaDataServer() BuildOptions {
	return func() (string, interface{}, error) {
		return "IsMetaDataServer", true, nil
	}
}

func RaftServerAddr(ID, IP, Port string) BuildOptions {
	return func() (string, interface{}, error) {
		if net.ParseIP(IP) != nil {
			return "", nil, fmt.Errorf("Illegal IP address")
		}
		portNum, err := strconv.Atoi(Port)
		if err != nil || portNum < 0 || portNum > 65535 {
			return "", nil, fmt.Errorf("Illegal Port Number :%s", Port)
		}
		return "RaftServerAddr", map[string]interface{}{ID: IP + ":" + Port}, nil
	}
}

func BrokerKey(key string) BuildOptions {
	return func() (string, interface{}, error) {
		return "BrokerKey", key, nil
	}
}

func MetadataServerInfo(ID, IP, Port string, HeartBeatSession int64) BuildOptions {
	return func() (string, interface{}, error) {
		return "MetadataServerInfo", map[string]interface{}{ID: map[string]interface{}{"Url": IP + ":" + Port, "HeartBeatSession": HeartBeatSession}}, nil
	}
}

func BrokerID(ID string) BuildOptions {
	return func() (string, interface{}, error) {
		return "BrokerID", ID, nil
	}
}

func BrokerAddr(IP, Port string) BuildOptions {
	return func() (string, interface{}, error) {
		if net.ParseIP(IP) != nil {
			return "", nil, fmt.Errorf("Illegal IP address")
		}
		portNum, err := strconv.Atoi(Port)
		if err != nil || portNum < 0 || portNum > 65535 {
			return "", nil, fmt.Errorf("Illegal Port Number :%s", Port)
		}
		return "BrokerAddr", map[string]interface{}{"IP": IP, "Port": Port}, nil
	}
}

func SetDefaultEntryMaxSizeOfEachBlock(i int64) BuildOptions {
	return func() (string, interface{}, error) {
		common.DefaultEntryMaxSizeOfEachBlock = i
		return "DefaultEntryMaxSizeOfEachBlock", i, nil
	}
}
func SetPartDefaultMaxEntries(i int32) BuildOptions {
	return func() (string, interface{}, error) {
		common.PartDefaultMaxEntries = i
		return "PartDefaultMaxEntries", i, nil
	}
}
func SetPartDefaultMaxSize(i int32) BuildOptions {
	return func() (string, interface{}, error) {
		common.PartDefaultMaxSize = i
		return "PartDefaultMaxSize", i, nil
	}
}
func SetDefaultMaxEntriesOf1Read(i int32) BuildOptions {
	return func() (string, interface{}, error) {
		common.DefaultMaxEntriesOf1Read = i
		return "DefaultMaxEntriesOf1Read", i, nil
	}
}
func SetDefaultMaxSizeOf1Read(i int32) BuildOptions {
	return func() (string, interface{}, error) {
		common.DefaultMaxSizeOf1Read = i
		return "DefaultMaxSizeOf1Read", i, nil
	}
}
func SetRaftHeartbeatTimeout(i time.Duration) BuildOptions {
	return func() (string, interface{}, error) {
		common.RaftHeartbeatTimeout = i
		return "RaftHeartbeatTimeout", i, nil
	}
}
func SetRaftVoteTimeOut(i time.Duration) BuildOptions {
	return func() (string, interface{}, error) {
		common.RaftVoteTimeOut = i
		return "RaftVoteTimeOut", i, nil
	}
}
func SetMQCommitTimeout(i time.Duration) BuildOptions {
	return func() (string, interface{}, error) {
		common.MQCommitTimeout = i
		return "MQCommitTimeout", i, nil
	}
}
func SetRaftLogSize(i int) BuildOptions {
	return func() (string, interface{}, error) {
		common.RaftLogSize = i
		return "RaftLogSize", i, nil
	}
}
func SetCacheStayTime_Ms(ms int32) BuildOptions {
	return func() (string, interface{}, error) {
		common.CacheStayTime_Ms = ms
		return "CacheStayTime_Ms", ms, nil
	}
}
func SetMQRequestTimeoutSessions_Ms(ms int32) BuildOptions {
	return func() (string, interface{}, error) {
		common.MQRequestTimeoutSessions_Ms = ms
		return "MQRequestTimeoutSessions_Ms", ms, nil
	}
}
