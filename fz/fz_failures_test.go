package fz

import (
	"encoding/binary"
	"io/ioutil"
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

	// case 2: fail on an empty file
	f, err = Open("map", nil)
	if f == nil {
		t.Fatal(err)
	}

	f.Add("13739", genReader(int64(13739))) // will fail
	if f.Count() != 0 {
		t.Error(f.Count())
	}

	f.Walk(nil, func(k string, v *Data) error {
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
	(func() {
		defer func() { recover() }()
		f.Add(strconv.Itoa(maxItems), genReader(0))
	}())
	testCase2 = false

	f.Close()

	for i := 0; i < 2; i++ {
		f, err = Open("map", nil)
		if err != nil {
			t.Fatal(err)
		}
		if f.Count() != maxItems+i {
			t.Error("Count() failed")
		}

		for i := 0; i < maxItems; i++ {
			v, _ := f.Get(strconv.Itoa(i))
			if i != int(binary.BigEndian.Uint64(v.ReadAllAndClose())) {
				t.Error(i)
			}
		}
		f.Add("1234567890", genReader(1234567890))
		f.Close()
	}

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
	(func() {
		defer func() { recover() }()
		f.Add("case3", genReader(0))
	}())
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

		f.Walk(nil, func(k string, v *Data) error {
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

func TestFailCase4(t *testing.T) {

	f, err := Open("map", nil)
	if f == nil {
		t.Fatal(err)
	}

	for i := 0; i < maxItems; i++ {
		f.Add(strconv.Itoa(i), genReader(int64(i)))
	}

	testCase4 = true
	(func() {
		defer func() { recover() }()
		f.Add(strconv.Itoa(maxItems), genReader(0))
	}())

	f.Close()

	f, err = Open("map", nil)
	if err != ErrSnapshotRecoveryFailed {
		t.Fatal(err)
	}
	testCase4 = false

	f, err = Open("map", nil)
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

func TestFailCaseCorruptedFile(t *testing.T) {
	f, err := Open("map", nil)
	if f == nil {
		t.Fatal(err)
	}
	for i := 0; i < 256; i++ {
		f.Add(strconv.Itoa(i), genReader(int64(i)))
	}
	f.Close()

	raw, _ := os.OpenFile("map", os.O_RDWR, 0666)
	raw.Seek(-8, os.SEEK_END)
	raw.Write([]byte{99})
	raw.Close()

	f, err = Open("map", nil)
	if f == nil {
		t.Fatal(err)
	}

	x, _ := f.Get("255")
	_, err = ioutil.ReadAll(x)
	if err == nil {
		t.Fatal("data should be corrupted")
	}
	x.Close()

	f.Close()
	os.Remove("map")
}
