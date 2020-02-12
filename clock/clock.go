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

	if ctr&0xffffff != ctr {
		// Worst case, the local machine is so fast that 16M values is just not enough for the counter
		// We have to manually delay the whole process by sleeping
		time.Sleep(time.Millisecond * 10)
		return timeNow()
	}

	// 24bits for the counter, which allow ~16M effective values
	return sec, sec<<24 | (ctr & 0xffffff)
}

// Timestamp returns a timestamp that is guaranteed to be
// goroutine-safe and globally unique on this machine as long as the process persists
func Timestamp() int64 {
	_, v := timeNow()
	return v
}

// Unix returns the unix timestamp based on when the process started
// so its returned value will not affected by the changing of system wall timer
func Unix() int64 {
	v, _ := timeNow()
	return v
}
