package fz

import (
	"io"
	"io/ioutil"
	"log"
	"os"
	"strconv"
	"testing"

	"github.com/coyove/common/rand"
)

const COUNT = 1 << 10

func TestOpenFZ(t *testing.T) {
	os.Remove("test")
	f, err := OpenFZ("test", true)
	if f == nil {
		t.Fatal(err)
	}

	r := rand.New()
	for i := 0; i < COUNT; i++ {
		f.Put(uint128{r.Uint64(), r.Uint64()}, int64(r.Uint64()))
		if i%100 == 0 {
			log.Println(i)
		}
	}

	f.Put(uint128{0, 13739}, 13739)
	f.Close()

	f, err = OpenFZ("test", false)
	if f == nil {
		t.Fatal(err)
	}

	if v, _ := f.Get(uint128{0, 13739}); v != 13739 {
		t.Error(v)
	}

	f.Close()
}

func BenchmarkFZ(b *testing.B) {
	f, err := OpenFZ("test", false)
	if f == nil {
		b.Fatal(err)
	}

	r := rand.New()
	for i := 0; i < b.N; i++ {
		f.Get(uint128{0, r.Uint64()})
	}

	f.Close()
}

func TestA_Begin(t *testing.T) {
	os.Mkdir("test2", 0777)
	rbuf := make([]byte, 8)

	for i := 0; i < COUNT; i++ {
		ioutil.WriteFile("test2/"+strconv.Itoa(i), rbuf, 0666)
	}
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

func BenchmarkZ_End(b *testing.B) {
	os.RemoveAll("test2")
}
