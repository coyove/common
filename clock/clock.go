package clock

import (
	"runtime"
	"sync/atomic"
	"time"
	"unsafe"
)

const hasMonotonic = 1 << 63

var (
	counter int64 = -1
	start   int64
	lastSec int64
)

func init() {
	x := time.Now()
	s := *(*[2]uint64)(unsafe.Pointer(&x))
	if s[0]&hasMonotonic == 1 {
		panic("monotonic clock not found on platform: " + runtime.GOOS + "/" + runtime.GOARCH)
	}
	start = x.Unix()
	lastSec = start
}

func timeNow() (int64, int64) {
	x := time.Now()
	s := *(*[2]int64)(unsafe.Pointer(&x))
	// s[1] -> time.Time.ext -> nsec since process started

	sec := start + s[1]/1e9
	ctr := atomic.AddInt64(&counter, 1)

	if atomic.SwapInt64(&lastSec, sec) != sec {
		// We have crossed a full second, so we will try to reset the counter
		// Only one caller will successfully swap the value to 0, the rest will do the atomic adding
		if atomic.CompareAndSwapInt64(&counter, atomic.LoadInt64(&counter), 0) {
			ctr = 0
		} else {
			ctr = atomic.AddInt64(&counter, 1)
		}
	}

	// 30bits for the counter, which allow 1 billion effective values
	return sec, sec<<30 | (ctr & 0x4fffffff)
}

func Now() int64 {
	_, v := timeNow()
	return v
}

func Unix() int64 {
	v, _ := timeNow()
	return v
}
