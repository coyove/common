package waitobject

import (
	"fmt"
	"sync"
	"time"
)

func debugprint(v ...interface{}) {
	if debug {
		fmt.Println(time.Now().Format(time.RFC3339), fmt.Sprint(v...))
	}
}

type Object struct {
	mu      sync.Mutex
	v       interface{}
	sig     *sync.Cond
	rev     *notifier
	touched bool
}

func New() *Object {
	o := &Object{}
	o.sig = sync.NewCond(&o.mu)
	return o
}

func (o *Object) Touch(f func(old interface{}) interface{}) {
	o.mu.Lock()
	debugprint("broadcast by touching")
	o.v = f(o.v)
	o.touched = true
	o.sig.Signal()
	o.mu.Unlock()
}

func (o *Object) SetValue(f func(v interface{}) interface{}) interface{} {
	o.mu.Lock()
	defer o.mu.Unlock()
	old := o.v
	if f != nil {
		o.touched = false
		o.v = f(o.v)
	}
	return old
}

// SetWaitDeadline sets the deadline of Wait(), note that its precision is 1s
func (o *Object) SetWaitDeadline(t time.Time) {
	o.mu.Lock()
	defer o.mu.Unlock()

	if o.rev != nil {
		// Current object has a notifier in the timeoutwheel
		// invalidate to prevent it from firing any old timeout events in the future
		o.rev.invalidate()
		o.rev = nil
	}

	if t.IsZero() || t == Eternal {
		// Caller wants to cancel the deadline
		return
	}

	o.rev = &notifier{deadline: t.Unix(), obj: o}
	if o.isTimedout() {
		debugprint("direct (already) timeout")
		o.sig.Broadcast()
		return
	}

	ts := &timeoutWheel.secmin[t.Second()][t.Minute()]
	ts.Lock()
	ts.list = append(ts.list, o.rev)
	ts.Unlock()
}

func (o *Object) IsTimedout() bool {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.isTimedout()
}

func (o *Object) isTimedout() bool {
	if o.rev == nil {
		return false
	}

	now := time.Now().Unix()
	out := now >= o.rev.deadline

	debugprint("isTimedout: ", out, ", now: ", now, ", deadline: ", o.rev.deadline)
	return out
}

func (o *Object) Wait() (interface{}, bool) {
	o.mu.Lock()
	defer o.mu.Unlock()

	// Before waiting for any data, return early if it is already timed out
	if o.isTimedout() {
		return nil, false
	}

	if o.touched {
		o.touched = false
		return o.v, true
	}

	debugprint("wait start, v: ", o.v)
	o.sig.Wait()
	o.touched = false
	debugprint("wait end, v: ", o.v)

	// After receiving any data, return early if it is already timed out
	if o.isTimedout() {
		return nil, false
	}

	return o.v, true
}
