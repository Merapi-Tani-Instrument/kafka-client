package kafkaClient

import (
	"io"
	"log"
)

type StdLogger interface {
	Print(v ...interface{})
	Printf(format string, v ...interface{})
	Println(v ...interface{})
}

var (
	PanicHandler    func(interface{})
	MaxRequestSize  int32     = 100 * 1024 * 1024
	MaxResponseSize int32     = 100 * 1024 * 1024
	Logger          StdLogger = log.New(io.Discard, "[Mertani] ", log.LstdFlags)
)
