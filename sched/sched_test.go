package sched

import (
	"log"
	"math/rand"
	"os"
	"runtime/pprof"
	"sync/atomic"
	"testing"
	"time"
)

func TestSched(t *testing.T) {
	var x int64
	for i := 0; i < 1e6; i++ {
		func() {
			var a SchedKey
			go func() {
				time.Sleep(time.Second)
				if rand.Intn(2) == 0 {
					a.Cancel()
					atomic.AddInt64(&x, 1)
				}
			}()
			a = Schedule(func() {
				a.Cancel()
			}, time.Now().Add(2*time.Second))
		}()
	}
	time.Sleep(time.Second * 2)
	log.Println(1e6 - x)
	of, _ := os.Create("sclog")
	pprof.Lookup("heap").WriteTo(of, 1)
	of.Close()
	select {}
}

func TestSchedTimedoutAlready(t *testing.T) {
	ok := false
	key := Schedule(func() { ok = true }, time.Now().Add(-time.Second))
	if ok || key != 0 {
		t.FailNow()
	}
	key = Schedule(func() { ok = true }, time.Now())
	if !ok || key != 0 {
		t.FailNow()
	}
}
