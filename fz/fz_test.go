package fz

import (
	"testing"

	"github.com/coyove/common/rand"
)

func TestBlock(t *testing.T) {
	r := rand.New()
	for i := 0; i < 10000; i++ {
		m := map[uint64]uint64{}
		for i := 0; i < 20; i++ {
			m[uint64(uint32(r.Uint64()))] = uint64(uint32(r.Uint64()))
		}

		b := Block{}
		for k, v := range m {
			b.Put(k, v)
		}

		for k, v := range m {
			v2, _ := b.Get(k)
			if v2 != v {
				t.Fatal(v2, v)
			}
		}
	}
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
		for i := 0; i < 20; i++ {
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
