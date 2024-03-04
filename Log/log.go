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
	_TRACE = iota
	_DEBUG
	_INFO
	_WARN
	_ERROR
	_FATAL
)

var logLevel int = _DEBUG

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
		log.Print("TRACE : ", i)
	}
}

func DEBUG(i ...interface{}) {
	if logLevel <= _DEBUG {
		log.Print("DEBUG : ", i)
	}
}

func INFO(i ...interface{}) {
	if logLevel <= _INFO {
		log.Print("INFO : ", i)
	}
}

func WARN(i ...interface{}) {
	if logLevel <= _WARN {
		log.Print("WARN : ", i)
	}
}

func ERROR(i ...interface{}) {
	if logLevel <= _ERROR {
		log.Print("ERROR : ", i)
	}
}

func FATAL(i ...interface{}) {
	if logLevel <= _FATAL {
		log.Fatal("FATAL : ", i)
	}
}
