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

func TestOpenFZ(t *testing.T) {
	os.Remove("test")
	f, err := OpenFZ("test", true)
	if f == nil {
		t.Fatal(err)
	}

	r := rand.New()
	for i := 0; i < 1<<14; i++ {
		f.Put(uint128{r.Uint64(), r.Uint64()}, r.Uint64())
		if i%1000 == 0 {
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

	for i := 0; i < b.N; i++ {
		if v, _ := f.Get(uint128{0, 13739}); v != 13739 {
			b.Error(v)
		}
	}

	f.Close()
}

func TestA_Begin(t *testing.T) {
	os.Mkdir("test2", 0777)

	for i := 0; i < 1<<14; i++ {
		ioutil.WriteFile("test2/"+strconv.Itoa(i), []byte{1, 2, 3, 4, 5, 6, 7, 8}, 0666)
	}
}

func BenchmarkFile(b *testing.B) {
	f, _ := os.Open("test2/10000")

	for i := 0; i < b.N; i++ {
		buf := make([]byte, 8)
		f.Seek(0, 0)
		io.ReadAtLeast(f, buf, 8)
	}

	f.Close()
}

func BenchmarkZ_End(b *testing.B) {
	os.RemoveAll("test2")
}
