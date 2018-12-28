package fz

import (
	"encoding/binary"
	"os"
	"strconv"
	"testing"
)

func TestFailCase1(t *testing.T) {
	f, err := Open("map", nil)
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

	if f.Size() != maxItems*8 {
		t.Error(f.Size())
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

	f, err = Open("map", nil)
	if f == nil {
		t.Fatal(err)
	}

	f.Add("13739", genReader(int64(13739))) // will fail
	if f.Count() != 0 {
		t.Error(f.Count())
	}

	f.Walk(false, func(k string, v *Data) error {
		t.Error("There shouldn't be any elements inside")
		return nil
	})

	f.Close()
	os.Remove("map")

	testCase1 = false
}

func TestFailCase1_2(t *testing.T) {
	f, err := Open("map", nil)
	if f == nil {
		t.Fatal(err)
	}

	testCase1 = true
	f.Add("13739", genReader(int64(13739))) // will fail

	if f.Count() != 0 {
		t.Error(f.Count())
	}
	if f.Size() != 0 {
		t.Error(f.Size())
	}

	f.Close()
	os.Remove("map")

	testCase1 = false
}

func TestFailCase2(t *testing.T) {
	testCase2 = true

	f, err := Open("map", nil)
	if f == nil {
		t.Fatal(err)
	}

	for i := 0; i < maxItems; i++ {
		f.Add(strconv.Itoa(i), genReader(int64(i)))
	}

	f.SetFlag(LsAsyncCommit)
	f.Add(strconv.Itoa(maxItems), genReader(0))
	f.Add(strconv.Itoa(maxItems-1), genReader(0)) // fail

	if f.Count() != maxItems || f.Size() != maxItems*8 {
		t.Error("Count() or Size() failed")
	}

	f.Close()
	os.Remove("map")

	testCase2 = false
}

func TestFailCase3(t *testing.T) {

	f, err := Open("map", nil)
	if f == nil {
		t.Fatal(err)
	}

	f.SetFlag(LsAsyncCommit)
	m := map[int]bool{COUNT: true}
	for i := 0; i < COUNT; i++ {
		f.Add(strconv.Itoa(i), genReader(int64(i)))
		m[i] = true
	}
	f.Commit()
	f.UnsetFlag(LsAsyncCommit)

	f.Add(strconv.Itoa(COUNT), genReader(int64(COUNT)))
	testCase3 = true
	fatal := f.Add("case3", genReader(0)).(*Fatal)
	testCase3 = false

	f.Close()

	Recover("map", fatal.Snapshot)

	f, err = Open("map", nil)
	if f == nil {
		t.Fatal(err)
	}

	if f.Count() != COUNT+1 {
		t.Error("Count() failed")
	}

	f.Walk(true, func(k string, v *Data) error {
		if k != strconv.Itoa(int(binary.BigEndian.Uint64(v.ReadAllAndClose()))) {
			t.Error(k)
		}
		i, _ := strconv.Atoi(k)
		delete(m, i)
		return nil
	})

	if len(m) > 0 {
		t.Error("There shouldn't be any elements inside")
	}

	f.Close()
	os.Remove("map")
}
