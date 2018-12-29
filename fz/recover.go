package fz

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/fnv"
	"unsafe"
)

var (
	ErrInvalidSnapshot = fmt.Errorf("invalid snapshot bytes")
)

func recoverSB(sb *SuperBlock, snapshot []byte) error {
	if len(snapshot) < superBlockSize+16 {
		return ErrInvalidSnapshot
	}

	h := fnv.New128()
	h.Write(snapshot[:len(snapshot)-16])
	if !bytes.Equal(h.Sum(nil), snapshot[len(snapshot)-16:]) {
		return ErrInvalidSnapshot
	}

	f := sb._fd
	copy(sb._mmap, snapshot[:superBlockSize])

	i := superBlockSize
	for i < len(snapshot)-16 {
		x := snapshot[i : i+nodeBlockSize]
		node := (*nodeBlock)(unsafe.Pointer(&x[0]))

		if node.offset > int64(sb.mmapSize) {
			if _, err := f.Seek(node.offset, 0); err != nil {
				return err
			}

			if _, err := f.Write(x); err != nil {
				return err
			}
		} else {
			copy(sb._mmap[node.offset:], x)
		}

		i += nodeBlockSize
	}

	// all done
	binary.BigEndian.PutUint64(sb._mmap[superBlockSize:], 0)
	return nil
}
