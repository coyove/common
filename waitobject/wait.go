package waitobject

import (
	"log"
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

				if debug {
					log.Println("[debug]", repeat, "notifier:", n, now, n.deadline > now)
				}

				if n.deadline > now && n.isvalid() {
					continue
				}

				// Remove the notifier, and if it is valid, tell its object to time out
				ts.list = append(ts.list[:i], ts.list[i+1:]...)
				if n.isvalid() {
					n.obj.mu.Lock()
					if debug {
						log.Println("[debug] broadcast by wheel")
					}
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

	log.SetFlags(log.Lshortfile | log.Lmicroseconds | log.Ltime)
}

type Object struct {
	mu  sync.Mutex
	v   interface{}
	sig *sync.Cond
	rev *notifier
}

func New() *Object {
	o := &Object{}
	o.sig = sync.NewCond(&o.mu)
	return o
}

func (o *Object) Touch(v interface{}) {
	o.mu.Lock()
	if debug {
		log.Println("[debug] broadcast by touching")
	}
	o.v = v
	o.sig.Broadcast()
	o.mu.Unlock()
}

func (o *Object) SetWaitDeadline(t time.Time) {
	o.mu.Lock()
	defer o.mu.Unlock()

	if o.rev != nil {
		// Current object has a notifier in the timeoutwheel
		// invalidate to prevent it from firing any old timeout events in the future
		o.rev.invalidate()
		o.rev = nil
	}

	if t.IsZero() {
		return
	}

	o.rev = &notifier{deadline: t.Unix(), obj: o}
	if o.isTimedout() {
		if debug {
			log.Println("direct (already) timeout")
		}
		o.sig.Broadcast()
		return
	}

	ts := &timeoutWheel.secmin[t.Second()][t.Minute()]
	ts.Lock()
	ts.list = append(ts.list, o.rev)
	ts.Unlock()
}

func (o *Object) isTimedout() bool {
	if o.rev == nil {
		return false
	}

	ns := time.Now().Unix()
	cd := o.rev.deadline

	a := ns >= cd

	if debug {
		log.Println("timed out, now:", ns, "deadline:", cd)
	}
	return a
}

func (o *Object) Wait() (interface{}, bool) {
	o.mu.Lock()
	defer o.mu.Unlock()

	// Before waiting for any data, return early if it is already timed out
	if o.isTimedout() {
		return nil, false
	}

	if !debug {
		o.sig.Wait()
	} else {
		log.Println("wait start", o.v)
		o.sig.Wait()
		log.Println("wait end", o.v)
	}

	// After receiving any data, return early if received data is a timeout event
	if o.isTimedout() {
		return nil, false
	}

	return o.v, true
}
