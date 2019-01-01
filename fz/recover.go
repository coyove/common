package fz

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/fnv"
	"unsafe"
)

var (
	ErrSnapshotRecoveryFailed = fmt.Errorf("failed to recover the snapshot")
)

func recoverSB(sb *SuperBlock, snapshot []byte) (result byte) {
	defer func() {
		if r := recover(); r != nil {
			result = 'E'
		}

		switch result {
		case 'I', 'S':
			// we also clear the invalid snapshot
			binary.BigEndian.PutUint64(sb._mmap[superBlockSize:], 0)
		}
	}()

	if len(snapshot) < superBlockSize+16 {
		return 'I'
	}

	h := fnv.New128()
	h.Write(snapshot[:len(snapshot)-16])
	if !bytes.Equal(h.Sum(nil), snapshot[len(snapshot)-16:]) {
		return 'I'
	}

	f := sb._fd
	copy(sb._mmap, snapshot[:superBlockSize])

	i := superBlockSize
	for i < len(snapshot)-16 {
		x := snapshot[i : i+nodeBlockSize]
		node := (*nodeBlock)(unsafe.Pointer(&x[0]))

		if node.offset > int64(sb.mmapSize) {
			if _, err := f.Seek(node.offset, 0); err != nil {
				return 'E'
			}

			if _, err := f.Write(x); err != nil {
				return 'E'
			}
		} else {
			copy(sb._mmap[node.offset:], x)
		}

		if testCase4 {
			return 'E'
		}

		i += nodeBlockSize
	}

	// all done
	return 'S'
}
