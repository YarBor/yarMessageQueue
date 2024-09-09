package PgLog

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"sync/atomic"
)

func SetLogOutput(w io.Writer) {
	log.SetOutput(w)
}

func SetLogFlags(flags int) {
	log.SetFlags(flags)
}

const (
	LogLevel_TRACE = iota
	LogLevel_INFO
	LogLevel_DEBUG
	LogLevel_WARN
	LogLevel_ERROR
	LogLevel_FATAL
)

var logLevel int32 = LogLevel_DEBUG

func init() {
	log.SetFlags(log.Lmicroseconds)
}

func ToJson(a interface{}) string {
	if str, ok := a.(string); ok {
		return str
	}
	data, err := json.Marshal(a)
	if err != nil {
		panic(err)
	}
	return string(data)
}
func SetLogLevel(l int) error {
	switch l {
	case LogLevel_TRACE:
	case LogLevel_DEBUG:
	case LogLevel_INFO:
	case LogLevel_WARN:
	case LogLevel_ERROR:
	case LogLevel_FATAL:
	default:
		return errors.New("Invalid log level")
	}
	atomic.StoreInt32(&logLevel, int32(l))
	return nil
}

func TRACE(i ...interface{}) {
	if atomic.LoadInt32(&logLevel) <= LogLevel_TRACE {
		st1 := ""
		for _, i2 := range i {
			st1 += fmt.Sprintf("%s ", ToJson(i2))
		}
		log.Printf("TRACE : %s", st1)
	}
}

func DEBUG(i ...interface{}) {
	if atomic.LoadInt32(&logLevel) <= LogLevel_DEBUG {
		st1 := ""
		for _, i2 := range i {
			st1 += fmt.Sprintf("%s ", ToJson(i2))
		}
		log.Printf("DEBUG : %s", st1)
	}
}

func INFO(i ...interface{}) {
	if atomic.LoadInt32(&logLevel) <= LogLevel_INFO {
		st1 := ""
		for _, i2 := range i {
			st1 += fmt.Sprintf("%s ", ToJson(i2))
		}
		log.Printf("INFO : %s", st1)
	}
}

func WARN(i ...interface{}) {
	if atomic.LoadInt32(&logLevel) <= LogLevel_WARN {
		st1 := ""
		for _, i2 := range i {
			st1 += fmt.Sprintf("%s ", ToJson(i2))
		}
		log.Printf("WARN : %s", st1)
	}
}

func ERROR(i ...interface{}) {
	if atomic.LoadInt32(&logLevel) <= LogLevel_ERROR {
		st1 := ""
		for _, i2 := range i {
			st1 += fmt.Sprintf("%s ", ToJson(i2))
		}
		log.Printf("ERROR : %s", st1)
	}
}

func FATAL(i ...interface{}) {
	if atomic.LoadInt32(&logLevel) <= LogLevel_FATAL {
		st1 := ""
		for _, i2 := range i {
			st1 += fmt.Sprintf("%s ", ToJson(i2))
		}
		log.Fatalf("FATAL : %s", st1)
	}
}

func PANIC(i ...interface{}) {
	st1 := ""
	for _, i2 := range i {
		st1 += fmt.Sprintf("%s ", ToJson(i2))
	}
	log.Printf("PANIC : %s", st1)
	panic(i)
}
