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

	for i := 0; i < maxItems; i++ {
		f.Add(strconv.Itoa(i), genReader(int64(i)))
	}

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

	f, err := Open("map", nil)
	if f == nil {
		t.Fatal(err)
	}

	for i := 0; i < maxItems; i++ {
		f.Add(strconv.Itoa(i), genReader(int64(i)))
	}

	testCase2 = true
	f.Add(strconv.Itoa(maxItems), genReader(0))
	testCase2 = false

	f.Close()

	if _, err := Open("map", nil); err != ErrInvalidSnapshot {
		t.Fatal(err)
	}

	f, err = Open("map", &Options{
		IgnoreSnapshot: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if f.Count() != maxItems {
		t.Error("Count() failed")
	}

	for i := 0; i < maxItems; i++ {
		v, _ := f.Get(strconv.Itoa(i))
		if i != int(binary.BigEndian.Uint64(v.ReadAllAndClose())) {
			t.Error(i)
		}
	}

	f.Close()
	os.Remove("map")

}

func TestFailCase3(t *testing.T) {

	f, err := Open("map", nil)
	if f == nil {
		t.Fatal(err)
	}

	m := map[int]bool{}
	for i := 0; i < COUNT; i++ {
		f.Add(strconv.Itoa(i), genReader(int64(i)))
		m[i] = true
	}

	testCase3 = true
	f.Add("case3", genReader(0))
	testCase3 = false

	f.Close()

	for i := 0; i < 2; i++ {
		f, err = Open("map", nil)
		if f == nil {
			t.Fatal(err)
		}

		if f.Count() != COUNT+i {
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
			t.Error("There shouldn't be any elements inside:", m)
		}

		m[COUNT*2] = true
		f.Add(strconv.Itoa(COUNT*2), genReader(int(COUNT*2)))

		f.Close()
	}
	os.Remove("map")
}
