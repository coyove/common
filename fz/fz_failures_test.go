package fz

import (
	"encoding/binary"
	"os"
	"strconv"
	"testing"
)

func TestFailCase1(t *testing.T) {
	f, err := OpenFZ("map", true)
	if f == nil {
		t.Fatal(err)
	}

	f.SetFlag(LsAsyncCommit)
	for i := 0; i < maxItems; i++ {
		f.Add(strconv.Itoa(i), genReader(int64(i)))
	}
	f.Commit()

	f.UnsetFlag(LsAsyncCommit)

	testCase1 = true
	f.Add("13739", genReader(int64(13739))) // will fail

	if f.Count() != maxItems {
		t.Error(f.Count())
	}

	for i := 0; i < maxItems; i++ {
		v, _ := f.Get(strconv.Itoa(i))
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

	f.Add("13739", genReader(int64(13739))) // will fail
	if f.Count() != 0 {
		t.Error(f.Count())
	}

	f.Walk(false, func(k uint128, v *Data) error {
		t.Error("There shouldn't be any elements inside")
		return nil
	})

	f.Close()
	os.Remove("map")

	testCase1 = false
}
