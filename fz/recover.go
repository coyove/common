package fz

import (
	"bytes"
	"fmt"
	"hash/fnv"
	"os"
	"unsafe"
)

var (
	ErrInvalidSnapshot = fmt.Errorf("invalid snapshot bytes")
)

func Recover(path string, snapshot []byte) error {
	if len(snapshot) < superBlockSize+16 {
		return ErrInvalidSnapshot
	}

	h := fnv.New128()
	h.Write(snapshot[:len(snapshot)-16])
	if !bytes.Equal(h.Sum(nil), snapshot[len(snapshot)-16:]) {
		return ErrInvalidSnapshot
	}

	f, err := os.OpenFile(path, os.O_RDWR, 0666)
	if err != nil {
		return err
	}

	defer f.Close()

	if _, err := f.Seek(0, 0); err != nil {
		return err
	}

	if _, err := f.Write(snapshot[:superBlockSize]); err != nil {
		return err
	}

	i := superBlockSize
	for i < len(snapshot)-16 {
		x := snapshot[i : i+nodeBlockSize]
		node := (*nodeBlock)(unsafe.Pointer(&x[0]))

		if _, err := f.Seek(node.offset, 0); err != nil {
			return err
		}

		if _, err := f.Write(x); err != nil {
			return err
		}

		i += nodeBlockSize
	}

	return nil
}
