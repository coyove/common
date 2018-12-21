package fz

import (
	"log"
	"os"
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
	for i := 0; i < 1<<12; i++ {
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
