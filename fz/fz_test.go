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
