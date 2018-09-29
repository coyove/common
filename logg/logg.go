package logg

import (
	"bytes"
	"encoding/csv"
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

type Format byte

const (
	FmtLongTime Format = 1 + iota
	FmtLongTimeUTC
	FmtShortTime
	FmtShortTimeSec
	FmtElapsedTime
	FmtElapsedTimeSec
	FmtLongFile
	FmtShortFile
	FmtLevel
	FmtGoroutine
)

type Logger struct {
	logLevel    int
	logPath     string
	formats     []Format
	logFile     *os.File
	logFileTmp  bytes.Buffer
	logFileSize int64
	lastFlush   int64
	start       int64
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

func (l *Logger) SetFormats(formats ...Format) {
	l.formats = formats
	l.start = time.Now().UnixNano()
}

func (l *Logger) Parse(config string) {
	parts := strings.Split(config, ",")
	if len(parts) == 0 {
		return
	}

	x := parts[0]
	if strings.Contains(x, ":") {
		fn := x[strings.Index(x, ":")+1:]
		x = x[:len(x)-len(fn)-1]
		if parts := strings.Split(fn, "+"); len(parts) == 2 {
			if rs, err := strconv.Atoi(parts[0]); err == nil {
				l.LogFile(parts[1], int64(rs))
			}
		} else {
			l.LogFile(fn, 1024*1024)
		}
	}
	l.SetLevel(x)

	r := csv.NewReader(strings.NewReader(config))
	parts, _ = r.Read()
	formats := make([]Format, 0, len(parts))

	for i := 1; i < len(parts); i++ {
		switch x := parts[i]; x {
		case "longtime", "lt":
			formats = append(formats, FmtLongTime)
		case "longtimeutc", "ltu":
			formats = append(formats, FmtLongTimeUTC)
		case "shorttime", "st":
			formats = append(formats, FmtShortTime)
		case "shorttimesec", "sts":
			formats = append(formats, FmtShortTimeSec)
		case "elapsedtime", "et":
			formats = append(formats, FmtElapsedTime)
		case "elapsedtimesec", "ets":
			formats = append(formats, FmtElapsedTimeSec)
		case "shortfile", "sf":
			formats = append(formats, FmtShortFile)
		case "longfile", "lf":
			formats = append(formats, FmtLongFile)
		case "level", "lv", "l":
			formats = append(formats, FmtLevel)
		case "goroutine", "go", "g":
			formats = append(formats, FmtGoroutine)
		}
	}

	if len(formats) > 0 {
		l.SetFormats(formats...)
	}
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

func (l *Logger) print(lvs string, format string, params ...interface{}) {
	_, fn, line, _ := runtime.Caller(2)
	now := time.Now()
	m := csvbuffer{}
	for _, f := range l.formats {
		switch f {
		case FmtLongTime:
			m.Write(now.Format("2006-01-02 15:04:05.000 MST"))
		case FmtLongTimeUTC:
			m.Write(now.UTC().Format("2006-01-02 15:04:05.000"))
		case FmtLongFile:
			m.Write(fn + ":" + strconv.Itoa(line))
		case FmtShortFile:
			m.Write(trunc(fn) + ":" + strconv.Itoa(line))
		case FmtShortTime:
			now = now.UTC()
			m.Write(fmt.Sprintf("%d%02d%02d%02d%02d%02d.%03d", now.Year(), now.Month(), now.Day(), now.Hour(), now.Minute(), now.Second(), (now.UnixNano()%1e9)/1e6))
		case FmtShortTimeSec:
			now = now.UTC()
			m.Write(fmt.Sprintf("%d%02d%02d%02d%02d%02d", now.Year(), now.Month(), now.Day(), now.Hour(), now.Minute(), now.Second()))
		case FmtElapsedTime:
			m.Write(strconv.FormatFloat(float64(time.Now().UnixNano()-l.start)/1e9, 'f', 6, 64))
		case FmtElapsedTimeSec:
			m.Write(strconv.FormatInt((time.Now().UnixNano()-l.start)/1e9, 10))
		case FmtLevel:
			m.Write(lvs)
		case FmtGoroutine:
			buf := [32]byte{}
			runtime.Stack(buf[:], false)
			startidx := bytes.Index(buf[:], []byte(" "))
			endidx := bytes.Index(buf[:], []byte("["))
			m.Write(string(buf[startidx+1 : endidx-1]))
		}
	}

	for i := 0; i < len(params); i++ {
		p := params[i]
		switch p.(type) {
		case *net.OpError:
			op := p.(*net.OpError)

			if op.Source == nil && op.Addr == nil {
				params[i] = fmt.Sprintf("%s, %s", op.Op, tryShortenWSAError(p))
			} else {
				params[i] = fmt.Sprintf("%s %v, %s", op.Op, op.Addr, tryShortenWSAError(p))
			}
		case *net.DNSError:
			op := p.(*net.DNSError)
			params[i] = fmt.Sprintf("DNS lookup error timeout: %v, name: %s", op.IsTimeout, op.Name)
		}
	}

	m.Write(fmt.Sprintf(format, params...))
	m.NewLine()

	if l.logFile != nil {
		l.logFileTmp.Write(m.Bytes())
		l.flush(lvs == LvFATALText)
	} else {
		os.Stderr.Write(m.Bytes())
	}
}

func (l *Logger) flush(force bool) {
	l.Lock()
	now := time.Now().UnixNano()
	if l.logFileTmp.Len() > 4096 || now-l.lastFlush > 1e9 || force {
		if l.logFile != nil {
			l.logFile.Write(l.logFileTmp.Bytes())
			l.lastFlush = now
			l.logFileTmp.Reset()

			if st, _ := l.logFile.Stat(); st.Size() > l.logFileSize {
				l.logFile.Sync()
				l.logFile.Close()
				l.LogFile(l.logPath, l.logFileSize)
			}
		}
	}
	l.Unlock()
}

var (
	LvFATALText   = "  FATAL  "
	LvPPRINTText  = "  PPRINT  "
	LvERRORText   = "  ERROR  "
	LvWARNINGText = "  WARNING  "
	LvLOGText     = "  INFO  "
	LvDEBUGText   = " DEBUG "
)

func (l *Logger) D(format string, params ...interface{}) {
	if l == nil {
		return
	}
	if l.logLevel <= -1 {
		l.print(LvDEBUGText, format, params...)
	}
}

func (l *Logger) L(format string, params ...interface{}) {
	if l == nil {
		return
	}
	if l.logLevel <= 0 {
		l.print(LvLOGText, format, params...)
	}
}

func (l *Logger) W(format string, params ...interface{}) {
	if l == nil {
		return
	}
	if l.logLevel <= 1 {
		l.print(LvWARNINGText, format, params...)
	}
}

func (l *Logger) E(format string, params ...interface{}) {
	if l == nil {
		return
	}
	if l.logLevel <= 2 {
		l.print(LvERRORText, format, params...)
	}
}

func (l *Logger) P(format string, params ...interface{}) {
	if l == nil {
		return
	}
	if l.logLevel == 99 {
		l.print(LvPPRINTText, format, params...)
	}
}

func (l *Logger) F(format string, params ...interface{}) {
	if l == nil {
		return
	}
	l.print(LvFATALText, format, params...)
	os.Exit(1)
}
