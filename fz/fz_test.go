package fz

import (
	"os"
	"testing"

	"github.com/coyove/common/rand"
)

func TestOpenFZ(t *testing.T) {
	f, err := OpenFZ("test", true)
	if f == nil {
		t.Fatal(err)
	}
	f.Close()
	f, err = OpenFZ("test", false)
	if f == nil {
		t.Fatal(err)
	}
	f.Close()
	os.Remove("test")
}

func BenchmarkBTree(b *testing.B) {
	//	tr.ReplaceOrInsert(pair{0, 0})
	//	tr.ReplaceOrInsert(pair{1, 1})
	//	tr.ReplaceOrInsert(pair{2, 2})
	//	tr.ReplaceOrInsert(pair{3, 3})
	//	tr.ReplaceOrInsert(pair{4, 4})

	r := rand.New()
	for i := 0; i < b.N; i++ {
		m := map[uint64]uint64{}
		for i := 0; i < 2000; i++ {
			m[uint64(uint32(r.Uint64()))] = uint64(uint32(r.Uint64()))
		}

		tr := NewTree()
		for k, v := range m {
			tr.ReplaceOrInsert(pair{k, v})
		}

		for k, v := range m {
			v2 := tr.Get(k)
			if v2.value != v {
				b.Fatal(v2, v)
			}
		}
	}

	//t.Log(tr.Get(2))
}
