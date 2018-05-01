package slice

import (
	"strconv"
	"strings"
	"testing"

	"github.com/coyove/common/rand"
)

func TestRemoveElems(t *testing.T) {
	a := []int{1, 2, 3, 4, 5}
	Remove(&a, 2)

	if !Equal(&a, &[]int{1, 2, 4, 5}) {
		t.Error("TestRemoveElems failed 1")
	}

	b := []chan bool{nil, make(chan bool), make(chan bool, 1)}
	b[2] <- true
	Remove(&b, 1)
	if !<-b[1] {
		t.Error("TestRemoveElems failed 2")
	}
}

func TestEqual(t *testing.T) {
	a := rand.New().Fetch(16)
	b := make([]byte, 16)
	copy(b, a)

	if !Equal(&a, &a) {
		t.Error("TestEqual failed 1")
	}

	if !Equal(&a, &b) {
		t.Error("TestEqual failed 2")
	}

	b[1]++
	if Equal(&a, &b) {
		t.Error("TestEqual failed 3")
	}

	s, s2 := make([]string, len(a)), make([]string, len(a))
	for i, x := range a {
		s[i] = strconv.Itoa(int(x))
		s2[i] = s[i]
	}

	if !Equal(&s, &s2) {
		t.Error("TestEqual failed 4")
	}

	Remove(&s2, 2)
	if Equal(&s, &s2) {
		t.Error("TestEqual failed 5")
	}
}

func TestClone(t *testing.T) {
	a := []string{"a", "0", "1"}
	b := Clone(&a).([]string)
	b[1] = "changed"

	if strings.Join(a, "") == strings.Join(b, "") {
		t.Error("TestClone failed")
	}
}
