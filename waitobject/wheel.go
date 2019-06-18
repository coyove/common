package waitobject

import (
	"sync"
	"sync/atomic"
	"time"
	"unsafe"
)

type notifier struct {
	deadline int64
	obj      *Object
}

func (n *notifier) invalidate() {
	atomic.StorePointer((*unsafe.Pointer)(unsafe.Pointer(&n.obj)), unsafe.Pointer(uintptr(0)))
}

func (n *notifier) isvalid() bool {
	return atomic.LoadPointer((*unsafe.Pointer)(unsafe.Pointer(&n.obj))) != unsafe.Pointer(uintptr(0))
}

var (
	timeoutWheel struct {
		secmin [60][60]struct {
			sync.Mutex
			list []*notifier
		}
	}
	debug bool

	Eternal = (time.Time{}).Add((1 << 60) * time.Nanosecond)
)

func init() {
	go func() {
		for t := range time.Tick(time.Second) {
			s, m, now := t.Second(), t.Minute(), t.Unix()

			repeat := false

		REPEAT:
			ts := &timeoutWheel.secmin[s][m]
			ts.Lock()
			for i := len(ts.list) - 1; i >= 0; i-- {
				n := ts.list[i]
				debugprint("repeated: ", repeat, ", notifier: ", n, ", now: ", now, ", timedout: ", n.deadline > now)

				if n.deadline > now && n.isvalid() {
					continue
				}

				// Remove the notifier, and if it is valid, tell its object to time out
				ts.list = append(ts.list[:i], ts.list[i+1:]...)
				if n.isvalid() {
					debugprint("broadcast by wheel")
					n.obj.mu.Lock()
					n.obj.sig.Broadcast()
					n.obj.mu.Unlock()
				}
			}
			ts.Unlock()

			if !repeat {
				// Dial back 1 second to check if any objects which should time out at "this second"
				// are added to the "previous second" because of clock precision
				t = time.Unix(now-1, 0)
				s, m = t.Second(), t.Minute()
				repeat = true
				goto REPEAT
			}
		}
	}()
}
