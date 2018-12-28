package fz

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"strconv"
	"testing"

	"github.com/coyove/common/rand"
)

const COUNT = 1 << 10

func genReader(r interface{}) io.Reader {
	buf := &bytes.Buffer{}
	switch x := r.(type) {
	case *rand.Rand:
		buf.Write(x.Fetch(8))
	case []byte:
		buf.Write(x)
	case int64:
		p := [8]byte{}
		binary.BigEndian.PutUint64(p[:], uint64(x))
		buf.Write(p[:])
	case int:
		p := [8]byte{}
		binary.BigEndian.PutUint64(p[:], uint64(x))
		buf.Write(p[:])
	case string:
		buf.WriteString(x)
	}
	return buf
}

var marker = []byte{1, 2, 3, 4, 5, 6, 7, 8}

func TestOpenFZ(t *testing.T) {
	os.Remove("test")
	f, err := OpenFZ("test", true)
	if f == nil {
		t.Fatal(err)
	}

	r := rand.New()
	for i := 0; i < COUNT; i++ {
		f.Add(strconv.Itoa(i), genReader(r))
		fmt.Print("\r", i)
	}
	fmt.Print("\r")

	f.Add("13739", genReader(marker))
	f.Close()

	f, err = OpenFZ("test", false)
	if f == nil {
		t.Fatal(err)
	}

	v, _ := f.Get("13739")
	buf := v.ReadAllAndClose()
	if !bytes.Equal(buf, marker) {
		t.Error(buf)
	}

	if v, err := f.Get(strconv.Itoa(COUNT / 2)); err != nil {
		t.Error(err)
	} else {
		v.Close()
	}

	f.Close()
}

func TestOpenFZ2(t *testing.T) {
	f, err := OpenFZ("map", true)
	if f == nil {
		t.Fatal(err)
	}

	for i := 0; i < 256; i++ {
		f.Add(strconv.Itoa(i), genReader(int64(i)))
		if f.Count() != i+1 {
			t.Error("Count() failed")
		}
		if f.Size() != int64(i+1)*8 {
			t.Error("Size() failed")
		}
		for j := 0; j < i; j++ {
			v, _ := f.Get(strconv.Itoa(j))
			buf := v.ReadAllAndClose()
			vj := int64(binary.BigEndian.Uint64(buf))

			if vj != int64(j) {
				t.Error(vj, j)
			}
		}
	}

	f.Close()
	os.Remove("map")
}

func TestOpenFZ2Random(t *testing.T) {
	f, err := OpenFZ("map", true)
	if f == nil {
		t.Fatal(err)
	}

	f.SetFlag(LsAsyncCommit)
	m := map[string]int{}

	r := rand.New()
	for i := 0; i < COUNT*2; i++ {
		ir := int(r.Uint64())
		si := strconv.Itoa(ir)
		f.Add(si, genReader(int64(ir)))
		m[si] = ir

		if r.Intn(3) == 1 {
			f.Commit()
		}

		if r.Intn(5) == 1 {
			f.Commit()
			f.Close()
			f, err = OpenFZ("map", false)
			f.SetFlag(LsAsyncCommit)
			if f == nil {
				t.Fatal(err)
			}
		}

		fmt.Print("\r", i)
	}

	fmt.Print("\r")

	f.Commit()
	f.Close()

	f, err = OpenFZ("map", false)
	if f == nil {
		t.Fatal(err)
	}

	for k, vi := range m {
		v2, _ := f.Get(k)
		v2i := int(binary.BigEndian.Uint64(v2.ReadAllAndClose()))
		if v2i != vi {
			t.Error(v2i, vi)
		}
	}

	f.Close()

	os.Remove("map")
}

func TestOpenFZ2Async(t *testing.T) {
	f, err := OpenFZ("map", true)
	if f == nil {
		t.Fatal(err)
	}

	f.SetFlag(LsAsyncCommit)

	for i := 0; i < COUNT; i++ {
		f.Add(strconv.Itoa(i), genReader(int64(i)))
		if i%10 == 0 {
			f.Commit()
		}
		m := map[uint128]int64{}
		for j := 0; j <= i; j++ {
			v, _ := f.Get(strconv.Itoa(j))
			buf := v.ReadAllAndClose()
			m[v.key] = int64(binary.BigEndian.Uint64(buf))
		}
		f.Walk(true, func(k string, value *Data) error {
			key := hashString(k)
			v := int64(binary.BigEndian.Uint64(value.ReadAllAndClose()))
			if v != m[key] {
				t.Error(key, v, m[key], len(m))
			}
			delete(m, key)
			return nil
		})

		if len(m) > 0 {
			t.Error("We have a non-empty map")
		}

		fmt.Print("\r", i)
	}
	fmt.Print("\r")

	f.Commit()
	for j := 0; j < COUNT; j++ {
		v, _ := f.Get(strconv.Itoa(j))

		buf := v.ReadAllAndClose()
		vj := int64(binary.BigEndian.Uint64(buf))

		if vj != int64(j) {
			t.Error(vj, j)
		}
	}

	f.Close()
	os.Remove("map")
}

func BenchmarkFZ(b *testing.B) {
	f, err := OpenFZ("test", false)
	if f == nil {
		b.Fatal(err)
	}

	r := rand.New()
	for i := 0; i < b.N; i++ {
		v, _ := f.Get(strconv.Itoa(r.Intn(COUNT)))
		if v != nil {
			v.ReadAllAndClose()
		}
	}

	f.Close()
}

func BenchmarkFile(b *testing.B) {

	r := rand.New()
	for i := 0; i < b.N; i++ {
		f, _ := os.Open("test2/" + strconv.Itoa(r.Intn(COUNT)))
		buf := make([]byte, 8)
		f.Seek(0, 0)
		io.ReadAtLeast(f, buf, 8)
		f.Close()
	}
}

func TestMain(m *testing.M) {
	os.RemoveAll("test2")
	os.Remove("test")
	os.Remove("map")

	os.Mkdir("test2", 0777)
	rbuf := make([]byte, 8)
	for i := 0; i < COUNT; i++ {
		ioutil.WriteFile("test2/"+strconv.Itoa(i), rbuf, 0666)
		fmt.Print("\r OS:", i)
	}
	fmt.Print("\r        ")

	f, err := OpenFZ("test", true)
	if err != nil {
		panic(err)
	}

	r := rand.New()
	for i := 0; i < COUNT; i++ {
		f.Add(strconv.Itoa(i), genReader(r))
		fmt.Print("\r FZ:", i)
	}
	fmt.Print("\r")

	f.Close()

	code := m.Run()

	os.RemoveAll("test2")
	os.Remove("test")
	os.Exit(code)
}
