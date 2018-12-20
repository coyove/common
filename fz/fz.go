package fz

import (
	"bytes"
	"strconv"
)

func (n *Block) Get(k uint64) (v uint64, ok bool) {
	start, end, mid := int8(0), n.size-1, int8(0)
	for start <= end {
		mid = (start + end) / 2
		mk := uint64From6Bytes(n.data[int(mid)*12:])
		if k == mk {
			v, ok = uint64From6Bytes(n.data[int(mid)*12+6:]), true
			return
		}

		if k < mk {
			end = mid - 1
		} else {
			start = mid + 1
		}
	}
	return 0, false
}

func (n *Block) Put(k, v uint64) bool {

	if n.size == 42 {
		return false
	}

	if n.size == 0 {
		n.size = 1
		uint64To6Bytes(n.data[0:], k)
		uint64To6Bytes(n.data[6:], v)
		return true
	}

	start, end, mid := int8(0), n.size-1, int8(0)
	for start <= end {
		mid = (start + end) / 2
		mk := uint64From6Bytes(n.data[int(mid)*12:])
		if k == mk {
			uint64To6Bytes(n.data[int(mid)*12+6:], v)
			return true
		}

		if k < mk {
			end = mid - 1
		} else {
			start = mid + 1
		}
	}

	if start == mid+1 {
		copy(n.data[int(mid+2)*12:], n.data[int(mid+1)*12:])
		uint64To6Bytes(n.data[int(mid+1)*12:], k)
		uint64To6Bytes(n.data[int(mid+1)*12+6:], v)

	} else {
		copy(n.data[int(mid+1)*12:], n.data[int(mid)*12:])
		uint64To6Bytes(n.data[int(mid)*12:], k)
		uint64To6Bytes(n.data[int(mid)*12+6:], v)
	}

	n.size++
	return true
}

func (n *Block) String() string {
	b := bytes.Buffer{}
	for i := int8(0); i < n.size; i++ {
		b.WriteString(strconv.Itoa(int(uint64From6Bytes(n.data[int(i)*12:]))) + " ")
	}
	return b.String()
}
