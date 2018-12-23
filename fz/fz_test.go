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
	rbuf := r.Fetch(200)
	for i := 0; i < COUNT; i++ {
		f.Put(uint128{r.Uint64(), r.Uint64()}, rbuf)
		if i%1000 == 0 {
			log.Println(i)
		}
	}

	f.Put(uint128{0, 13739}, rbuf)
	f.Close()

	f, err = OpenFZ("test", false)
	if f == nil {
		t.Fatal(err)
	}

	//	if v, _ := f.Get(uint128{0, 13739}); v != 13739 {
	//		t.Error(v)
	//	}

	f.Close()
}

func BenchmarkFZ(b *testing.B) {
	f, err := OpenFZ("test", false)
	if f == nil {
		b.Fatal(err)
	}

	for i := 0; i < b.N; i++ {
		f.Get(uint128{0, 13739})
	}

	f.Close()
}

func TestA_Begin(t *testing.T) {
	os.Mkdir("test2", 0777)
	r := rand.New()
	rbuf := r.Fetch(200)

	for i := 0; i < COUNT; i++ {
		ioutil.WriteFile("test2/"+strconv.Itoa(i), rbuf, 0666)
	}
}

func BenchmarkFile(b *testing.B) {

	for i := 0; i < b.N; i++ {
		f, _ := os.Open("test2/100")
		buf := make([]byte, 200)
		f.Seek(0, 0)
		io.ReadAtLeast(f, buf, 200)
		f.Close()
	}

}

func BenchmarkZ_End(b *testing.B) {
	os.RemoveAll("test2")
}
