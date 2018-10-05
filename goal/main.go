package goal

import (
	"sync"
)

type Goal struct {
	goal       uint64
	overflowed map[uint64]uint64
	overflow   uint64
	sync.Mutex
}

func New() *Goal {
	return &Goal{
		overflowed: make(map[uint64]uint64),
	}
}

const bit58_6 = 0xffffffffffffffc0

func (c *Goal) Meet(num uint64) bool {
	c.Lock()
	defer c.Unlock()

	if num == c.goal {
		c.goal = num + 1
		return true
	}

	if num < c.goal {
		return false
	}

	if num-c.goal > 1<<31 {
		return false
	}

	c.overflow++
	score := uint64(0)
	for i := c.goal; i < num; {
		x, exist := c.overflowed[i&bit58_6]
		if !exist {
			break
		}

	AGAIN:
		if x&(uint64(1)<<(i&0x3f)) > 0 {
			score++
			if (i+1)&bit58_6 == i&bit58_6 && i < num {
				i++
				goto AGAIN
			}
			i++
		} else {
			break
		}
	}

	if score == num-c.goal {
		for i := c.goal; i < num; i++ {
			tag := i & bit58_6
			c.overflowed[tag] &= ^(uint64(1) << (i & 0x3f))
			if c.overflowed[tag] == 0 {
				delete(c.overflowed, tag)
			}
		}
		c.goal = num + 1
		return true
	}

	c.overflowed[num&bit58_6] |= uint64(1) << (num & 0x3f)
	return true
}

func (c *Goal) Goal() uint64 {
	return c.goal
}

func (c *Goal) Overflow() int {
	c.Lock()
	defer c.Unlock()
	return len(c.overflowed)
}
