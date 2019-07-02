package writer

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"testing"
	"time"
)

func TestWrite(t *testing.T) {
	w, err := New("/tmp/zzz")
	if err != nil {
		panic(err)
	}
	for i := 0; i < 1e6; i++ {
		j := make([]byte, 8)
		binary.BigEndian.PutUint64(j, uint64(i))
		w.Write(j)
	}

	time.Sleep(time.Second * 2)

	buf, _ := ioutil.ReadFile("/tmp/zzz")
	test := map[uint64]bool{}
	for i := 0; i < len(buf); i += 8 {
		j := binary.BigEndian.Uint64(buf[i:])
		test[j] = true
	}

	fmt.Println(len(test))

	fmt.Println("exit")
	os.Remove("/tmp/zzz")
}

func TestWrite2(t *testing.T) {
	w, err := New("/tmp/zzz")
	if err != nil {
		panic(err)
	}
	for i := 0; i < 1e6; i++ {
		j := make([]byte, 8)
		binary.BigEndian.PutUint64(j, uint64(i))
		w.Write(j)
		//fmt.Println(i)
	}
}

func foo(a ...interface{}) string {
	return fmt.Sprint(a...)
}

func BenchmarkUnixSocket(b *testing.B) {
	w, _ := New("/tmp/zzz")
	for i := 0; i < b.N; i++ {
		w.Write([]byte(foo(i)))
	}

	fi, _ := os.Stat("/tmp/zzz")
	b.Log(fi.Size())
	os.Remove("/tmp/zzz")
}

func BenchmarkUnixSocketClass(b *testing.B) {
	w, _ := New("/tmp/zzz")

	for i := 0; i < b.N; i++ {
		if rand.Intn(2) == 0 {
			w.Write([]byte(foo(i)))
		} else {
			w.SlowWrite([]byte(foo(i)))
		}
	}

	b.Log(os.Stat("/tmp/zzz"))
	os.Remove("/tmp/zzz")
}

func BenchmarkMemory(b *testing.B) {
	p := bytes.Buffer{}
	of, _ := os.Create("/tmp/zzz")
	for i := 0; i < b.N; i++ {
		p.Write([]byte(foo(i)))
		if p.Len() > 1024*1024 {
			of.Write(p.Bytes())
			p.Reset()
		}
	}
	of.Write(p.Bytes())
	of.Close()
	os.Remove("/tmp/zzz")
}
