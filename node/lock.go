package node

import (
	"math/rand"
	"sync"
	"time"

	"github.com/coyove/common/sched"
)

type lk struct {
	unlockTime time.Time
	uuid       string
	schedDeath sched.SchedKey
}

type KeyLocks struct {
	mu sync.Mutex
	l  map[string]*lk
}

func NewKeyLocks() *KeyLocks {
	return &KeyLocks{
		l: make(map[string]*lk),
	}
}

func (l *KeyLocks) MustLock(k string, uuid string, timeout time.Duration) string {
	if uuid == "" {
		uuid = randomString(k)
	}

	if timeout == -1 {
		timeout = time.Duration(1 << 60)
	}

	for {
		if l.Lock(k, uuid, timeout) {
			return uuid
		}
		time.Sleep(time.Millisecond * time.Duration(rand.Intn(50)+50))
	}
}

func (l *KeyLocks) Lock(v string, uuid string, timeout time.Duration) bool {
	l.mu.Lock()
	defer l.mu.Unlock()

	if uuid == "" {
		panic("must provide uuid")
	}

	now := time.Now()
	lv := l.l[v]

	if lv == nil || lv.unlockTime.Before(now) {
		l.l[v] = &lk{
			unlockTime: now.Add(timeout),
			uuid:       uuid,
			schedDeath: sched.Schedule(func() {
				l.mu.Lock()
				delete(l.l, v)
				l.mu.Unlock()
			}, timeout+time.Second),
		}
		return true
	}

	return false
}

func (l *KeyLocks) Unlock(v string, uuid string) {
	l.mu.Lock()
	defer l.mu.Unlock()

	if uuid == "" {
		panic("must provide uuid")
	}

	lv := l.l[v]
	if lv == nil || lv.uuid != uuid {
		return
	}

	delete(l.l, v)
	lv.schedDeath.Cancel()
}

func (l *KeyLocks) IsLocked(k string, uuid string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()

	lv := l.l[k]
	if lv != nil && lv.uuid == uuid && time.Now().Before(lv.unlockTime) {
		// Extend the life of the lock for some time
		lv.unlockTime = lv.unlockTime.Add(heartbeat)
		lv.schedDeath.Reschedule(nil, lv.unlockTime.Add(time.Second))
		return true
	}

	return false
}
