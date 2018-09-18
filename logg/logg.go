package logg

import (
	"bytes"
	"fmt"
	"net"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

const (
	LvDebug = iota - 1
	LvLog
	LvWarning
	LvError
	LvOff
	LvPrint = 99
)

type Logger struct {
	logLevel    int
	logPath     string
	logFile     *os.File
	logFileTmp  bytes.Buffer
	logFileSize int64
	sync.Mutex
}

func (l *Logger) SetLevel(lv string) int {
	switch lv {
	case "dbg":
		l.logLevel = -1
	case "log":
		l.logLevel = 0
	case "warn":
		l.logLevel = 1
	case "err":
		l.logLevel = 2
	case "off":
		l.logLevel = 3
	case "pp":
		l.logLevel = 99
	default:
		panic("unexpected log level: " + lv)
	}

	return l.logLevel
}

func (l *Logger) GetLevel() int {
	return l.logLevel
}

func (l *Logger) LogFile(fn string, rotateSize int64) {
	l.logPath = fn
	fn += "." + time.Now().UTC().Format("2006-01-02_15-04-05.000")

	var err error
	l.logFile, err = os.Create(fn)
	if err != nil {
		panic(err)
	}
	l.logFileSize = rotateSize
	l.logFileTmp.Reset()
}

func trunc(fn string) string {
	idx := strings.LastIndex(fn, "/")
	if idx == -1 {
		idx = strings.LastIndex(fn, "\\")
	}
	return fn[idx+1:]
}

// Widnows WSA error messages are way too long to print
// ex: An established connection was aborted by the software in your host machine.write tcp 127.0.0.1:8100->127.0.0.1:52466: wsasend: An established connection was aborted by the software in your host machine.
func tryShortenWSAError(err interface{}) (ret string) {
	defer func() {
		if recover() != nil {
			ret = fmt.Sprintf("%v", err)
		}
	}()

	if e, sysok := err.(*net.OpError).Err.(*os.SyscallError); sysok {
		errno := e.Err.(syscall.Errno)
		if msg, ok := WSAErrno[int(errno)]; ok {
			ret = msg
		} else {
			// messages on linux are short enough
			ret = fmt.Sprintf("C%d, %s", uintptr(errno), e.Error())
		}

		return
	}

	ret = err.(*net.OpError).Err.Error()
	return
}

func (l *Logger) print(lvs string, params ...interface{}) {
	_, fn, line, _ := runtime.Caller(2)
	now := time.Now()
	m := csvbuffer{}
	m.Write(now.Format("2006-01-02 15:04:05.000 MST"), trunc(fn)+":"+strconv.Itoa(line), lvs)

	for _, p := range params {
		switch p.(type) {
		case *net.OpError:
			op := p.(*net.OpError)

			if op.Source == nil && op.Addr == nil {
				m.Write(fmt.Sprintf("%s, %s", op.Op, tryShortenWSAError(p)))
			} else {
				m.Write(fmt.Sprintf("%s %v, %s", op.Op, op.Addr, tryShortenWSAError(p)))
			}
		case *net.DNSError:
			op := p.(*net.DNSError)
			m.Write(fmt.Sprintf("DNS lookup error timeout: %v, name: %s", op.IsTimeout, op.Name))
		default:
			m.Write(fmt.Sprintf("%+v", p))
		}
	}

	m.NewLine()

	if l.logFile != nil {
		l.logFileTmp.Write(m.Bytes())

		l.Lock()
		if l.logFileTmp.Len() > 4096 {
			l.logFile.Write(l.logFileTmp.Bytes())
			l.logFileTmp.Reset()

			if st, _ := l.logFile.Stat(); st.Size() > l.logFileSize {
				l.logFile.Sync()
				l.logFile.Close()
				l.LogFile(l.logPath, l.logFileSize)
			}
		}
		l.Unlock()
	} else {
		os.Stderr.Write(m.Bytes())
	}
}

func (l *Logger) D(params ...interface{}) {
	if l.logLevel <= -1 {
		l.print("  DEBUG  ", params...)
	}
}

func (l *Logger) L(params ...interface{}) {
	if l.logLevel <= 0 {
		l.print("  LOG  ", params...)
	}
}

func (l *Logger) W(params ...interface{}) {
	if l.logLevel <= 1 {
		l.print("  WARNING  ", params...)
	}
}

func (l *Logger) E(params ...interface{}) {
	if l.logLevel <= 2 {
		l.print("  ERROR  ", params...)
	}
}

func (l *Logger) P(params ...interface{}) {
	if l.logLevel == 99 {
		l.print("  PPRINT  ", params...)
	}
}

func (l *Logger) F(params ...interface{}) {
	l.print("  FATAL  ", params...)
	os.Exit(1)
}
