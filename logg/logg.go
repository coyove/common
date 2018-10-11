package logg

import (
	"bytes"
	"encoding/csv"
	"fmt"
	"io"
	"net"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

type _Level int

const (
	LvDebug0 _Level = iota - 2
	LvDebug
	LvLog
	LvInfo
	LvWarning
	LvError
	LvFatal
	LvOff
)

type _Format byte

const (
	FmtLongTime _Format = 1 + iota
	FmtLongTimeUTC
	FmtShortTime
	FmtShortTimeSec
	FmtElapsedTime
	FmtElapsedTimeSec
	FmtLongFile
	FmtShortFile
	FmtLevel
	FmtGoroutine
	FmtVoid
)

type Logger struct {
	Writer       io.Writer
	logLevel     _Level
	ignoreLevels []_Level
	logPath      string
	formats      []_Format
	logFile      *os.File
	logFileTmp   bytes.Buffer
	logFileSize  int64
	lastFlush    int64
	start        int64
	sync.Mutex
}

var (
	LvTexts  = []string{" FATAL ", " ERROR ", " WARNING ", " INFO ", " LOG ", " DEBUG ", " DEBUG0 "}
	lvlookup = map[string]_Level{"dbg0": LvDebug0, "dbg": LvDebug, "info": LvInfo, "log": LvLog, "warn": LvWarning, "err": LvError, "fatal": LvFatal, "off": LvOff}
)

func (l *Logger) SetLevel(lv string) _Level {
	l.ignoreLevels = nil
	for i, lv := range strings.Split(lv, "^") {
		n, ok := lvlookup[lv]
		if !ok {
			panic("unexpected log level: " + lv)
		}

		if i == 0 {
			l.logLevel = n
		} else {
			if l.ignoreLevels == nil {
				l.ignoreLevels = []_Level{n}
			} else {
				l.ignoreLevels = append(l.ignoreLevels, n)
			}
		}
	}

	return l.logLevel
}

func NewLogger(config string) *Logger {
	l := &Logger{}
	l.formats = []_Format{FmtLongTime, FmtShortFile, FmtLevel}
	l.start = time.Now().UnixNano()

	parts := strings.Split(config, ",")
	if len(parts) == 0 {
		return l
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
	formats := make([]_Format, 0, len(parts))

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
		case "void":
			formats = append(formats, FmtVoid)
		}
	}

	if len(formats) > 0 {
		l.formats = formats
	}

	return l
}

func (l *Logger) GetLevel() _Level {
	return l.logLevel
}

func (l *Logger) LogFile(fn string, rotateSize int64) {
	if l.logFile != nil {
		l.logFile.Sync()
		l.logFile.Close()
	}

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
	_, fn, line, _ := runtime.Caller(3)
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
		x := ""
		switch op := p.(type) {
		case *net.OpError:
			if op.Source == nil && op.Addr == nil {
				x = fmt.Sprintf("%s, %s", op.Op, tryShortenWSAError(p))
			} else {
				x = fmt.Sprintf("%s %v, %s", op.Op, op.Addr, tryShortenWSAError(p))
			}
			params[i] = x
		case *net.DNSError:
			x = fmt.Sprintf("DNS lookup failed: %v", op)
			params[i] = x
		default:
			if format == "" {
				x = fmt.Sprintf("%v", op)
			}
		}

		if format == "" {
			m.Write(x)
		}
	}

	if format != "" {
		m.Write(fmt.Sprintf(format, params...))
	}
	m.NewLine()

	if l.logFile != nil {
		l.logFileTmp.Write(m.Bytes())
		l.flush(lvs == LvTexts[0])
	} else if l.Writer != nil {
		l.Writer.Write(m.Bytes())
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
				l.LogFile(l.logPath, l.logFileSize)
			}
		}
	}
	l.Unlock()
}

func (l *Logger) If(b bool) *Logger {
	if b {
		return l
	}
	return nil
}

func (l *Logger) level(lv _Level, format string, params ...interface{}) *Logger {
	if l == nil {
		return nil
	}
	for _, n := range l.ignoreLevels {
		if n == lv {
			return l
		}
	}
	if l.logLevel <= lv {
		l.print(LvTexts[LvOff-lv-1], format, params...)
	}
	if lv == LvFatal {
		os.Exit(1)
	}
	return l
}

func (l *Logger) Dbg0f(f string, a ...interface{}) *Logger  { return l.level(LvDebug0, f, a...) }
func (l *Logger) Dbg0(a ...interface{}) *Logger             { return l.level(LvDebug0, "", a...) }
func (l *Logger) Dbgf(f string, a ...interface{}) *Logger   { return l.level(LvDebug, f, a...) }
func (l *Logger) Dbg(a ...interface{}) *Logger              { return l.level(LvDebug, "", a...) }
func (l *Logger) Logf(f string, a ...interface{}) *Logger   { return l.level(LvLog, f, a...) }
func (l *Logger) Log(a ...interface{}) *Logger              { return l.level(LvLog, "", a...) }
func (l *Logger) Infof(f string, a ...interface{}) *Logger  { return l.level(LvInfo, f, a...) }
func (l *Logger) Info(a ...interface{}) *Logger             { return l.level(LvInfo, "", a...) }
func (l *Logger) Warnf(f string, a ...interface{}) *Logger  { return l.level(LvWarning, f, a...) }
func (l *Logger) Warn(a ...interface{}) *Logger             { return l.level(LvWarning, "", a...) }
func (l *Logger) Errorf(f string, a ...interface{}) *Logger { return l.level(LvError, f, a...) }
func (l *Logger) Error(a ...interface{}) *Logger            { return l.level(LvError, "", a...) }
func (l *Logger) Fatalf(f string, a ...interface{}) *Logger { return l.level(LvFatal, f, a...) }
func (l *Logger) Fatal(a ...interface{}) *Logger            { return l.level(LvFatal, "", a...) }
