package sched

import (
	"testing"
	"time"
)

func TestSched(t *testing.T) {
	ok := false
	Schedule(func() { ok = true }, time.Now().Add(time.Second))
	time.Sleep(time.Second * 2)
	if !ok {
		t.FailNow()
	}
}

func TestSchedTimedoutAlready(t *testing.T) {
	ok := false
	key := Schedule(func() { ok = true }, time.Now().Add(-time.Second))
	if !ok || key != 0 {
		t.FailNow()
	}
	ok = false
	key = Schedule(func() { ok = true }, time.Now())
	if !ok || key != 0 {
		t.FailNow()
	}
}
