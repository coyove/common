package fz

import (
	"encoding/binary"
	"os"
	"testing"
)

func TestFailCase1(t *testing.T) {
	f, err := OpenFZ("map", true)
	if f == nil {
		t.Fatal(err)
	}

	f.SetFlag(LsAsyncCommit)
	for i := 0; i < maxItems; i++ {
		f.Put(uint128{0, uint64(i)}, genReader(int64(i)))
	}
	f.Commit()

	f.UnsetFlag(LsAsyncCommit)

	testCase1 = true
	f.Put(uint128{0, 13739}, genReader(int64(13739))) // will fail

	if f.Count() != maxItems {
		t.Error(f.Count())
	}

	for i := 0; i < maxItems; i++ {
		v, _ := f.Get(uint128{0, uint64(i)})
		vj := int64(binary.BigEndian.Uint64(v.ReadAllAndClose()))
		if vj != int64(i) {
			t.Error(vj, i)
		}

	}

	f.Close()
	os.Remove("map")

	f, err = OpenFZ("map", true)
	if f == nil {
		t.Fatal(err)
	}

	f.Put(uint128{0, 13739}, genReader(int64(13739))) // will fail
	if f.Count() != 0 {
		t.Error(f.Count())
	}

	f.Walk(func(k uint128, v *Data) error {
		t.Error("There shouldn't be any elements inside")
		return nil
	})

	f.Close()
	os.Remove("map")

	testCase1 = false
}
