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
	_TRACE = 1
	_DEBUG = 2
	_INFO  = 3
	_WARN  = 4
	_ERROR = 5
	_FATAL = 6
)

var logLevel int = 2

func init() {
	log.SetFlags(log.Lmicroseconds)
}

func SetLogLevel(l int) error {
	switch l {
	case _TRACE:
	case _DEBUG:
	case _INFO:
	case _WARN:
	case _ERROR:
	case _FATAL:
		break
	default:
		return errors.New("Invalid log level")
	}
	logLevel = l
	return nil
}

func TRACE(i ...interface{}) {
	if logLevel <= _TRACE {
		log.Printf("TRACE : %v\n", i...)
	}
}

func DEBUG(i ...interface{}) {
	if logLevel <= _DEBUG {
		log.Printf("DEBUG : %v\n", i...)
	}
}

func INFO(i ...interface{}) {
	if logLevel <= _INFO {
		log.Printf("INFO : %v\n", i...)
	}
}

func WARN(i ...interface{}) {
	if logLevel <= _WARN {
		log.Printf("WARN : %v\n", i...)
	}
}

func ERROR(i ...interface{}) {
	if logLevel <= _ERROR {
		log.Printf("ERROR : %v\n", i...)
	}
}

func FATAL(i ...interface{}) {
	if logLevel <= _FATAL {
		log.Fatalf("FATAL : %v\n", i)
	}
}
