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
)

const (
	forkName   = "writer-agent"
	bufferSize = 1024 * 1024
)

func hashName(n string) string {
	x := sha1.Sum([]byte(n + forforkName))
	return filepath.Join(os.TempDir(), fmt.Sprintf("wa-%x", x))
}

func init() {
	reexec.Register(forkName, func() {
		path := hashName(os.Args[1])

		ln, err := net.Listen("unix", path)
		if err != nil {
			panic(err)
		}

		output, err := os.OpenFile(os.Args[1], os.O_RDWR|os.O_CREATE, 0755)
		if err != nil {
			panic(err)
		}

		conn, err := ln.Accept()
		if err != nil {
			panic(err)
		}

		var buffer struct {
			sync.Mutex
			bytes.Buffer
		}

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

		sk := sched.Schedule(flush, time.Second)

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
			sk.Reschedule(flush, time.Now().Add(time.Second))
		}

		flush()
		output.Close()
		os.Remove(path)

		fmt.Println("Writer agent out, cleaned:", path, output.Name())
	})

	if reexec.Init() {
		os.Exit(0)
	}

}

type Writer struct {
	mu     sync.Mutex
	conn   net.Conn
	buffer bytes.Buffer
	schW   sched.SchedKey
	output string
}

func (w *Writer) Write(p []byte) (n int, err error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.writeLocked(p)
}

func (w *Writer) SlowWrite(p []byte) (n int, err error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	w.buffer.Write(p)
	if w.buffer.Len() > bufferSize {
		_, err := w.writeLocked(w.buffer.Bytes())
		if err != nil {
			return 0, err
		}
		w.buffer.Reset()
	}

	w.schW.Reschedule(func() {
		w.mu.Lock()
		w.writeLocked(w.buffer.Bytes())
		w.buffer.Reset()
		w.mu.Unlock()
	}, time.Now().Add(time.Second))
	return len(p), nil
}

func (w *Writer) writeLocked(p []byte) (n int, err error) {
	if len(p) == 0 {
		return 0, nil
	}

	if len(p) >= 1<<32 {
		return 0, fmt.Errorf("buffer too large")
	}

	p = append(p, 0, 0, 0, 0)
	copy(p[4:], p)
	binary.BigEndian.PutUint32(p, uint32(len(p)-4))

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

	return len(p) - 4, nil
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

	for deadline := time.Now().Add(time.Second); time.Now().Before(deadline); {
		w.conn, err = net.Dial("unix", path)
		if err != nil {
			continue
		}
		return w, nil
	}

	cmd.Wait()
	return nil, err
}
