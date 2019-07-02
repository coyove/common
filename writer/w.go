package writer

import (
	"bytes"
	"crypto/sha1"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/coyove/common/sched"
	"github.com/docker/docker/pkg/reexec"
	"github.com/edsrzf/mmap-go"
)

const (
	forkName   = "writer-agent"
	mmapCount  = 1024
	bufferSize = 1024 * 1024
)

var (
	qk       = make([]byte, 4096)
	testMode = ""
)

func hashName(n string) string {
	x := sha1.Sum([]byte(n))
	return filepath.Join(os.TempDir(), fmt.Sprintf("wa-%x", x))
}

func init() {
	reexec.Register(forkName, func() {
		path := hashName(os.Args[1])

		ln, err := net.Listen("unix", path)
		if err != nil {
			panic(err)
		}

		f, err := os.OpenFile(path+"mmap", os.O_RDWR, 0755)
		if err != nil {
			panic(err)
		}

		output, err := os.OpenFile(os.Args[1], os.O_RDWR|os.O_CREATE, 0755)
		if err != nil {
			panic(err)
		}

		var mmaps []mmap.MMap
		for i := 0; i < mmapCount; i++ {
			var p mmap.MMap
			p, err = mmap.MapRegion(f, len(qk), mmap.RDWR, 0, int64(len(qk)*i))
			if err != nil {
				panic(err)
			}
			p.Lock()
			mmaps = append(mmaps, p)
		}

		conn, err := ln.Accept()
		if err != nil {
			panic(err)
		}

		var buffer struct {
			sync.Mutex
			bytes.Buffer
		}

		var closed bool
		var closeConfirmed = make(chan bool)

		write := func(p []byte) {
			buffer.Lock()
			defer buffer.Unlock()

			buffer.Write(p)
			if buffer.Len() > bufferSize || len(p) == 0 {
				output.Write(buffer.Bytes())
				buffer.Reset()
			}
		}
		flush := func() {
			write(nil)
		}

		sk := sched.ScheduleSync(flush, time.Now().Add(time.Second))

		go func() {
		LOOP:
			flag := false
			for _, m := range mmaps {
				if m[0] == 0 {
					continue
				}

				n := int(binary.BigEndian.Uint32(m[1:5]))
				p := m[5 : 5+n]
				write(p)

				m[0] = 0
				flag = true
				sk.RescheduleSync(flush, time.Now().Add(time.Second))
				break
			}

			if !flag {
				// If we can't find any incoming data in mmap, and we are told to exit
				// We confirm that and then exit
				if closed {
					closeConfirmed <- true
					return
				}

				time.Sleep(time.Second)
			}

			goto LOOP
		}()

		var hdr = make([]byte, 4)
		var p []byte
		for {
			if _, err := io.ReadAtLeast(conn, hdr, 4); err != nil {
				break
			}
			n := int(binary.BigEndian.Uint32(hdr))
			if n > len(p) {
				p = make([]byte, n)
			} else {
				p = p[:n]
			}
			if _, err := io.ReadAtLeast(conn, p, n); err != nil {
				break
			}
			write(p)
			sk.RescheduleSync(flush, time.Now().Add(time.Second))
		}

		closed = true
		select {
		case <-closeConfirmed:
		}

		// Wait all cleaned up, do final flush to file
		flush()

		f.Close()
		output.Close()
		os.Remove(path + "mmap")
		os.Remove(path)
		fmt.Println("Writer agent out, cleaned:", path, "(mmap)", output.Name())
	})

	if reexec.Init() {
		os.Exit(0)
	}

}

type Writer struct {
	mu     sync.Mutex
	conn   net.Conn
	mmaps  []mmap.MMap
	f      *os.File
	buffer bytes.Buffer
	schW   sched.SchedKey
	output string

	Classifier func(p []byte) bool
}

func (w *Writer) writeMMap(p []byte) (ok bool) {
	defer func() {
		if r := recover(); r != nil {
			ok = false
		}
	}()

	if len(p) < len(qk) {
		for _, m := range w.mmaps {
			if m[0] == 0 {
				copy(m[1:], p)
				m[0] = 1
				return true
			}
		}
	}
	return false
}

func (w *Writer) Write(p []byte) (n int, err error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.Classifier != nil && !w.Classifier(p) {
		w.buffer.Write(p)
		if w.buffer.Len() > bufferSize {
			_, err := w.writeLocked(w.buffer.Bytes())
			if err != nil {
				return 0, err
			}
			w.buffer.Reset()
			return len(p), nil
		}

		w.schW.Reschedule(func() {
			w.mu.Lock()
			w.writeLocked(w.buffer.Bytes())
			w.buffer.Reset()
			w.mu.Unlock()
		}, time.Now().Add(time.Second))
		return len(p), nil
	}

	return w.writeLocked(p)
}

func (w *Writer) writeLocked(p []byte) (n int, err error) {
	N := len(p)
	if N == 0 {
		return 0, nil
	}

	p = append(p, 0, 0, 0, 0)
	copy(p[4:], p)
	binary.BigEndian.PutUint32(p, uint32(len(p)-4))

	if w.writeMMap(p) {
		return N, nil
	}

AGAIN:
	if _, err = w.conn.Write(p); err != nil {
		fmt.Println("Writer met error:", err, ", retry now, delete old unix socket:", os.Remove(hashName(w.output)))

		for deadline := time.Now().Add(time.Second * 5); time.Now().Before(deadline); {
			if w2, err := New(w.output); err == nil {
				w.conn.Close()
				w.conn = w2.conn
				goto AGAIN
			}
		}

		panic("lost writer agent permanently")
	}

	return N, nil
}

func New(output string) (*Writer, error) {
	cmd := reexec.Command(forkName, output)
	cmd.Stdin = os.Stdin
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	err := cmd.Start()
	if err != nil {
		return nil, err
	}

	w := &Writer{output: output}
	path := hashName(output)
	os.Remove(path)

	w.f, err = os.OpenFile(path+"mmap", os.O_RDWR|os.O_CREATE, 0755)
	if err != nil {
		return nil, err
	}

	for i := 0; i < mmapCount; i++ {
		if _, err = w.f.Write(qk); err != nil {
			goto ERROR
		}
	}

	for i := 0; i < mmapCount; i++ {
		var p mmap.MMap
		p, err = mmap.MapRegion(w.f, len(qk), mmap.RDWR, 0, int64(len(qk)*i))
		if err != nil {
			goto ERROR
		}
		p.Lock()
		w.mmaps = append(w.mmaps, p)
	}

	for deadline := time.Now().Add(time.Second); time.Now().Before(deadline); {
		w.conn, err = net.Dial("unix", path)
		if err != nil {
			continue
		}
		return w, nil
	}

ERROR:
	w.f.Close()
	cmd.Wait()
	return nil, err
}
