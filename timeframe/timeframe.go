package timeframe

import (
	"math/rand"
	"sync"
	"time"
)

type Timeframe struct {
	sync.Mutex
	lifetime uint32
	m        map[uint32][]byte
	r        *rand.Rand
}

func NewTimeframe(lifetimeSec uint32) *Timeframe {
	return &Timeframe{
		m:        make(map[uint32][]byte),
		r:        rand.New(rand.NewSource(time.Now().UnixNano())),
		lifetime: lifetimeSec,
	}
}

func (t *Timeframe) now() uint32 {
	return uint32(time.Now().Unix())
}

func (t *Timeframe) purge(now uint32) {
	if t.r.Intn(10) > 0 {
		return
	}
	for ts := range t.m {
		if ts < now-t.lifetime {
			delete(t.m, ts)
		}
	}
}

func (t *Timeframe) Borrow() uint64 {
	t.Lock()
	defer t.Unlock()

	now := t.now()
	v := t.m[now]

	if v == nil {
		v = make([]byte, 1)
	}

	idx := 0
REPEAT:
	lastSlot := v[len(v)-1]
	if lastSlot>>6 == 0 {
		lastSlot |= 1 << 6
	} else if lastSlot<<2>>6 == 0 {
		lastSlot |= 1 << 4
		idx = 1
	} else if lastSlot<<4>>6 == 0 {
		lastSlot |= 1 << 2
		idx = 2
	} else if lastSlot<<6>>6 == 0 {
		lastSlot |= 1
		idx = 3
	} else {
		v = append(v, 0)
		goto REPEAT
	}
	v[len(v)-1] = lastSlot

	t.m[now] = v
	t.purge(now)
	return uint64(now)<<32 + uint64((len(v)-1)*4+idx)
}

func (t *Timeframe) Return(token uint64) bool {
	now := t.now()
	ts := uint32(token >> 32)

	if ts < now-t.lifetime {
		return false
	}

	t.Lock()
	defer t.Unlock()

	t.purge(now)

	index := uint32(token)
	idx := index % 4
	index = index / 4

	v := t.m[ts]
	if len(v) < int(index+1) {
		return false
	}

	slot := v[index]
	state := slot << uint(idx*2) >> 6

	if state != 1 { // 0 or 3
		return false
	}

	slot |= 1 << uint(7-idx*2)
	v[index] = slot
	return true
}
