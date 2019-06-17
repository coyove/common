package sched

import (
	"sync"
	"time"
)

type notifier struct {
	key      SchedKey
	deadline int64
	callback func()
}

var timeoutWheel struct {
	secmin [60][60]struct {
		sync.Mutex
		counter uint64
		list    []*notifier
	}
}

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

				if n.deadline > now {
					continue
				}

				ts.list = append(ts.list[:i], ts.list[i+1:]...)
				go n.callback()
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

type SchedKey uint64

func Schedule(callback func(), deadline time.Time) SchedKey {
	if time.Now().Unix() >= deadline.Unix() {
		// timed out already
		callback()
		return 0
	}

	s, m := deadline.Second(), deadline.Minute()
	ts := &timeoutWheel.secmin[s][m]
	ts.Lock()

	ts.counter++

	// sec (6bit) | min (6bit) | counter (52bit)
	// key will never be 0
	key := SchedKey(uint64(s+1)<<58 | uint64(m+1)<<52 | (ts.counter & 0xfffffffffffff))

	ts.list = append(ts.list, &notifier{
		key:      key,
		deadline: deadline.Unix(),
		callback: callback,
	})

	ts.Unlock()
	return key
}

func (key SchedKey) Cancel() {
	s := int(key>>58) - 1
	m := int(key<<6>>58) - 1
	if s < 0 || s > 59 || m < 0 || m > 59 {
		return
	}
	ts := &timeoutWheel.secmin[s][m]
	ts.Lock()
	for i := len(ts.list) - 1; i >= 0; i-- {
		if ts.list[i].key == key {
			ts.list = append(ts.list[:i], ts.list[i+1:]...)
			break
		}
	}
	ts.Unlock()
}

func (key *SchedKey) Reschedule(callback func(), deadline time.Time) {
	key.Cancel()
	*key = Schedule(callback, deadline)
}
