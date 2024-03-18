package common

import (
	"MqServer/Random"
	"fmt"
	"net"
	"strconv"
	"time"
)

var (
	DefaultEntryMaxSizeOfEachBlock int64         = int64(1e5)
	PartDefaultMaxEntries          int32         = int32(500)
	PartDefaultMaxSize             int32         = int32(1024 * 1024)
	DefaultMaxEntriesOf1Read       int32         = int32(10)
	DefaultMaxSizeOf1Read          int32         = int32(10 * 1024)
	RaftHeartbeatTimeout           time.Duration = 200 * time.Millisecond
	RaftVoteTimeOut                time.Duration = 400 * time.Millisecond
	MQCommitTimeout                time.Duration = time.Millisecond * 500
	RaftLogSize                    int           = 1024 * 1024 * 2
	CacheStayTime_Ms               int32         = int32(5 * time.Second.Milliseconds())
	MQRequestTimeoutSessions_Ms    int32         = 300
	MetadataHeartBeatSession_ms    int64         = 300
)

type BuildOptions func() (string, interface{}, error)

func IsMetaDataServer(i bool) BuildOptions {
	return func() (string, interface{}, error) {
		return "IsMetaDataServer", i, nil
	}
}

func RaftServerAddr(ID, IP, Port string) BuildOptions {
	if ID == "" {
		ID = Random.RandStringBytes(16)
	}
	return func() (string, interface{}, error) {
		if net.ParseIP(IP) == nil {
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

func MetadataServerInfo(ID, RaftServer_IP, RaftServer_Port string, HeartBeatSession_ms int64) BuildOptions {
	return func() (string, interface{}, error) {
		if HeartBeatSession_ms <= 0 {
			HeartBeatSession_ms = MetadataHeartBeatSession_ms
		}
		return "MetadataServerInfo", map[string]interface{}{ID: map[string]interface{}{"Url": RaftServer_IP + ":" + RaftServer_Port, "HeartBeatSession": HeartBeatSession_ms}}, nil
	}
}

func BrokerID(ID string) BuildOptions {
	return func() (string, interface{}, error) {
		return "BrokerID", ID, nil
	}
}

func BrokerAddr(IP, Port string) BuildOptions {
	return func() (string, interface{}, error) {
		if net.ParseIP(IP) == nil {
			return "", nil, fmt.Errorf("Illegal IP address")
		}
		portNum, err := strconv.Atoi(Port)
		if err != nil || portNum < 0 || portNum > 65535 {
			return "", nil, fmt.Errorf("Illegal Port Number :%s", Port)
		}
		return "BrokerAddr", IP + ":" + Port, nil
	}
}

func SetDefaultEntryMaxSizeOfEachBlock(i int64) BuildOptions {
	return func() (string, interface{}, error) {
		DefaultEntryMaxSizeOfEachBlock = i
		return "DefaultEntryMaxSizeOfEachBlock", i, nil
	}
}
func SetPartDefaultMaxEntries(i int32) BuildOptions {
	return func() (string, interface{}, error) {
		PartDefaultMaxEntries = i
		return "PartDefaultMaxEntries", i, nil
	}
}
func SetPartDefaultMaxSize(i int32) BuildOptions {
	return func() (string, interface{}, error) {
		PartDefaultMaxSize = i
		return "PartDefaultMaxSize", i, nil
	}
}
func SetDefaultMaxEntriesOf1Read(i int32) BuildOptions {
	return func() (string, interface{}, error) {
		DefaultMaxEntriesOf1Read = i
		return "DefaultMaxEntriesOf1Read", i, nil
	}
}
func SetDefaultMaxSizeOf1Read(i int32) BuildOptions {
	return func() (string, interface{}, error) {
		DefaultMaxSizeOf1Read = i
		return "DefaultMaxSizeOf1Read", i, nil
	}
}
func SetRaftHeartbeatTimeout(i time.Duration) BuildOptions {
	return func() (string, interface{}, error) {
		RaftHeartbeatTimeout = i
		return "RaftHeartbeatTimeout", i, nil
	}
}
func SetRaftVoteTimeOut(i time.Duration) BuildOptions {
	return func() (string, interface{}, error) {
		RaftVoteTimeOut = i
		return "RaftVoteTimeOut", i, nil
	}
}
func SetMQCommitTimeout(i time.Duration) BuildOptions {
	return func() (string, interface{}, error) {
		MQCommitTimeout = i
		return "MQCommitTimeout", i, nil
	}
}
func SetRaftLogSize(i int) BuildOptions {
	return func() (string, interface{}, error) {
		RaftLogSize = i
		return "RaftLogSize", i, nil
	}
}
func SetCacheStayTime_Ms(ms int32) BuildOptions {
	return func() (string, interface{}, error) {
		CacheStayTime_Ms = ms
		return "CacheStayTime_Ms", ms, nil
	}
}
func SetMQRequestTimeoutSessions_Ms(ms int32) BuildOptions {
	return func() (string, interface{}, error) {
		MQRequestTimeoutSessions_Ms = ms
		return "MQRequestTimeoutSessions_Ms", ms, nil
	}
}
