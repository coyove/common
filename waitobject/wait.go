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

var timeoutWheel struct {
	secmin [60][60]struct {
		sync.Mutex
		list []*notifier
	}
	timeoutmark *byte
}

func init() {
	timeoutWheel.timeoutmark = new(byte)
	go func() {
		for t := range time.Tick(time.Second) {
			s, m, now := t.Second(), t.Minute(), t.UnixNano()

			repeat := false

		REPEAT:
			ts := &timeoutWheel.secmin[s][m]
			ts.Lock()
			for i := len(ts.list) - 1; i >= 0; i-- {
				n := ts.list[i]

				if n.deadline > now+1e9 {
					continue
				}

				if !n.isvalid() {
					continue
				}

				// object timedout, remove it from the wheel and send each listener a timeoutmark
				ts.list = append(ts.list[:i], ts.list[i+1:]...)
				n.obj.Touch(timeoutWheel.timeoutmark)
			}
			ts.Unlock()

			if !repeat {
				// Dial back 1 second to check if any objects which should time out at "this second"
				// are added to the "previous second" because of clock precision
				t = time.Unix(0, now-1e9)
				s, m = t.Second(), t.Minute()
				repeat = true
				goto REPEAT
			}
		}
	}()
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
	defer o.mu.Unlock()
	o.v = v
	o.sig.Broadcast()
}

func (o *Object) SetWaitDeadline(t time.Time) {
	o.mu.Lock()
	defer o.mu.Unlock()

	if o.rev != nil {
		// Current object has a notifier in the timeoutwheel
		// invalidate to prevent it from firing any timeout events in the future
		o.rev.invalidate()
		o.rev = nil
	}

	if t.IsZero() {
		o.v = nil
		return
	}

	s, m := t.Second(), t.Minute()
	ts := &timeoutWheel.secmin[s][m]

	ts.Lock()
	o.rev = &notifier{deadline: t.UnixNano(), obj: o}
	ts.list = append(ts.list, o.rev)
	ts.Unlock()
}

func (o *Object) isTimedout() bool {
	if o.rev != nil && time.Now().UnixNano() > o.rev.deadline {
		return true
	}
	if m, _ := o.v.(*byte); m == timeoutWheel.timeoutmark {
		return true
	}
	return false
}

func (o *Object) Wait() (interface{}, bool) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.isTimedout() {
		return nil, false
	}

	o.sig.Wait()

	if o.isTimedout() {
		return nil, false
	}

	return o.v, true
}
