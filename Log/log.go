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
	LogLevel_DEBUG
	LogLevel_INFO
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
		break
	default:
		return errors.New("Invalid log level")
	}
	logLevel = l
	return nil
}

func TRACE(i ...interface{}) {
	if logLevel <= LogLevel_TRACE {
		log.Print("TRACE : ", i)
	}
}

func DEBUG(i ...interface{}) {
	if logLevel <= LogLevel_DEBUG {
		log.Print("DEBUG : ", i)
	}
}

func INFO(i ...interface{}) {
	if logLevel <= LogLevel_INFO {
		log.Print("INFO : ", i)
	}
}

func WARN(i ...interface{}) {
	if logLevel <= LogLevel_WARN {
		log.Print("WARN : ", i)
	}
}

func ERROR(i ...interface{}) {
	if logLevel <= LogLevel_ERROR {
		log.Print("ERROR : ", i)
	}
}

func FATAL(i ...interface{}) {
	if logLevel <= LogLevel_FATAL {
		log.Fatal("FATAL : ", i)
	}
}
