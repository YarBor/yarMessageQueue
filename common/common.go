package common

import (
	"time"
)

var (
	DefaultEntryMaxSizeOfEachBlock int64         = int64(1e5)
	PartDefaultMaxEntries          int32         = int32(500)
	PartDefaultMaxSize             int32         = int32(1024 * 1024)
	DefaultMaxEntriesOf1Read       int32         = int32(10)
	DefaultMaxSizeOf1Read          int32         = int32(10 * 1024)
	RaftHeartbeatTimeout           time.Duration = 300 * time.Millisecond
	RaftVoteTimeOut                time.Duration = 300 * time.Millisecond
	MQCommitTimeout                time.Duration = time.Millisecond * 500
	RaftLogSize                    int           = 1024 * 1024 * 2
	CacheStayTime_Ms               int32         = int32(5 * time.Second.Milliseconds())
	MQRequestTimeoutSessions_Ms    int32         = 300
)
