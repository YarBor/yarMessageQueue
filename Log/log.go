package PgLog

import (
	"errors"
	"io"
	"log"
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

var logLevel int = LogLevel_DEBUG

func init() {
	log.SetFlags(log.Lmicroseconds)
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
	logLevel = l
	return nil
}

func TRACE(i ...interface{}) {
	if logLevel <= LogLevel_TRACE {
		log.Printf("TRACE : %#v", i)
	}
}

func DEBUG(i ...interface{}) {
	if logLevel <= LogLevel_DEBUG {
		log.Printf("DEBUG : %#v", i)
	}
}

func INFO(i ...interface{}) {
	if logLevel <= LogLevel_INFO {
		log.Printf("INFO : %#v", i)
	}
}

func WARN(i ...interface{}) {
	if logLevel <= LogLevel_WARN {
		log.Printf("WARN : %#v", i)
	}
}

func ERROR(i ...interface{}) {
	if logLevel <= LogLevel_ERROR {
		log.Printf("ERROR : %#v", i)
	}
}

func FATAL(i ...interface{}) {
	if logLevel <= LogLevel_FATAL {
		log.Fatalf("FATAL : %#v", i)
	}
}

func PANIC(i ...interface{}) {
	log.Printf("PANIC : %#v", i)
	panic(i)
}
