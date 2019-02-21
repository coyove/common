package timeframe

import (
	"math/rand"
	"testing"
	"time"
)

func TestBasic(t *testing.T) {
	for i := 0; i < 4; i++ {
		f := NewTimeframe(1)
		v := f.Borrow()
		if !f.Return(v) {
			t.Log(f.m)
			t.FailNow()
		}

		v = f.Borrow()
		time.Sleep(2 * time.Second)
		if f.Return(v) {
			t.Log(f.m, v)
			t.FailNow()
		}
	}
}

func TestBasic2(t *testing.T) {
	f := NewTimeframe(2)
	count := int(1e6)

	v := make([]uint64, count)
	for i := 0; i < count; i++ {
		v[i] = f.Borrow()
	}

	r := rand.New(rand.NewSource(int64(v[0])))
	for i, idx := range r.Perm(count) {
		if !f.Return(v[idx]) {
			t.Log(i, idx)
			t.FailNow()
		}
	}
	for i, idx := range r.Perm(count) {
		if f.Return(v[idx]) {
			t.Log(i, idx)
			t.FailNow()
		}
	}
	time.Sleep(2 * time.Second)

	if f.Return(v[0]) {
		t.FailNow()
	}

}
