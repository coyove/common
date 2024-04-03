package sched

import (
	"runtime"
	"sync"
	"sync/atomic"
	"time"
	_ "unsafe"
)

//go:linkname runtimeNano runtime.nanotime
func runtimeNano() int64

var taskCanceled = new(int)

type timer struct {
	real  *time.Timer
	lock  int32
	dead  int32
	tasks []any
}

func (t *timer) spinlock() {
	for !atomic.CompareAndSwapInt32(&t.lock, 0, 1) {
		if atomic.LoadInt32(&t.dead) == 1 {
			return
		}
		runtime.Gosched()
	}
}

func (t *timer) spinunlock() {
	atomic.StoreInt32(&t.lock, 0)
}

type shard struct {
	timers sync.Map
	mgr    *Group
}

func (m *shard) start(d time.Duration, data any) Key {
	nano := time.Duration(runtimeNano())
	now := nano / time.Second
	at := (nano + d) / time.Second
	if at == now {
		go m.mgr.wakeup([]any{data})
		return Key{}
	}

	v, loaded := m.timers.LoadOrStore(at, &timer{lock: 1})
	t := v.(*timer)
	if !loaded {
		// runtime.SetFinalizer(t, func(t *timer) {
		// 	atomic.StoreInt32(&t.dead, 1)
		// })
		t.real = time.AfterFunc(d, func() {
			t.spinlock()
			defer func() {
				atomic.StoreInt32(&t.dead, 1)
				t.spinunlock()
				m.timers.Delete(at)
			}()
			end := len(t.tasks) - 1
			for i := 0; i <= end; i++ {
				if t.tasks[i] == taskCanceled {
					for ; end >= i; end-- {
						if t.tasks[end] != taskCanceled {
							t.tasks[end], t.tasks[i] = t.tasks[i], t.tasks[end]
							break
						}
					}
				}
			}
			tt := t.tasks[:end+1]
			if len(tt) > 0 {
				m.mgr.wakeup(tt)
			}
		})
	} else {
		t.spinlock()
	}
	i := len(t.tasks)
	t.tasks = append(t.tasks, data)
	t.spinunlock()
	return Key{tm: t, index: i}
}

type Key struct {
	tm    *timer
	index int
}

type Group struct {
	shards   []shard
	shardCtr atomic.Int64
	wakeup   func([]any)
}

// NewGroup creates a schedule group, where payloads can be queued and fired at specific
// time, use wakeup function to handle fired payloads.
func NewGroup(wakeup func([]any)) *Group {
	m := &Group{
		shards: make([]shard, runtime.NumCPU()),
		wakeup: wakeup,
	}
	for i := range m.shards {
		m.shards[i].mgr = m
	}
	return m
}

// Schedule schedules the payload to be fired after d, which is counted in seconds.
// If d is less than a second, the payload will be fired immediately.
func (m *Group) Schedule(d time.Duration, payload any) Key {
	mgr := &m.shards[m.shardCtr.Add(1)%int64(runtime.NumCPU())]
	return mgr.start(d, payload)
}

// Cancel cancels the payload associated with the key anyway, which may be fired or not.
// The caller should implement its own coordinate logic if this matters.
func (m *Group) Cancel(key Key) {
	if key.tm == nil {
		return
	}
	t := key.tm
	t.spinlock()
	defer t.spinunlock()
	if atomic.LoadInt32(&t.dead) == 1 {
		return
	}
	t.tasks[key.index] = taskCanceled
}
