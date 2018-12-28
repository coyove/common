package fz

import (
	"fmt"
	"hash"
	"hash/fnv"
	"io"
	"io/ioutil"
	"os"
	"time"
	"unsafe"

	"github.com/coyove/common/rand"
	mmap "github.com/edsrzf/mmap-go"
)

var (
	testCase1 bool // Simulate: error when copying data to disk
	testCase2 bool // Simulate: rollback if key duplicates
	testCase3 bool // Simulate: fatal error: sync dirties to disk
	testError = fmt.Errorf("test")
)

var (
	_one    uint16 = 1
	_endian byte   = *(*byte)(unsafe.Pointer(&_one))
)

var (
	ErrWrongMagic    = fmt.Errorf("wrong magic code")
	ErrEndianness    = fmt.Errorf("wrong endianness")
	ErrKeyNotFound   = fmt.Errorf("key not found")
	ErrKeyExisted    = fmt.Errorf("key already existed")
	ErrKeyTooLong    = fmt.Errorf("key too long")
	ErrCriticalState = fmt.Errorf("critical state")
)

const (
	LsAsyncCommit = 1 << iota
	LsCritical
)

const mmapSize = 1024 * 1024

func OpenFZ(path string, create bool) (*SuperBlock, error) {
	return OpenFZWithFds(path, create, 4)
}

func OpenFZWithFds(path string, create bool, maxFds int) (_sb *SuperBlock, _err error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		return nil, err
	}

	defer func() {
		if _err != nil {
			f.Close()
		}
	}()

	blockHdr := [superBlockSize]byte{}
	if _, err := io.ReadAtLeast(f, blockHdr[:], superBlockSize); err != nil {
		if !create {
			return nil, err
		}
	}

	sb := &SuperBlock{
		_fd:         f,
		_dirtyNodes: map[*nodeBlock]bool{},
		_filename:   path,
	}
	h := fnv.New64()

	if create {
		var payload [64]byte
		for i := 0; i < mmapSize; i += 64 {
			if _, err := sb._fd.Write(payload[:]); err != nil {
				return nil, err
			}
		}

		r := rand.New()
		sb.magic = superBlockMagic
		sb.endian = _endian
		sb.createdAt = uint32(time.Now().Unix())
		sb.mmapSize = int32(mmapSize)
		sb.mmapSizeUsed = int32(superBlockSize)
		copy(sb.salt[:], r.Fetch(16))
		if err := sb.sync(); err != nil {
			return nil, err
		}
	} else {
		*(*[superBlockSize]byte)(unsafe.Pointer(sb)) = blockHdr
		if sb.magic != superBlockMagic {
			return nil, ErrWrongMagic
		}
		if sb.endian != _endian {
			return nil, ErrEndianness
		}

		h.Write(blockHdr[:superBlockSize-8])
		if sb.superHash != h.Sum64() {
			return nil, fmt.Errorf("wrong super hash: %x, expected: %x", h.Sum64(), sb.superHash)
		}

		fi, _ := f.Stat()
		if sb.rootNode >= fi.Size() {
			return nil, fmt.Errorf("corrupted root node")
		}
	}

	sb._snapshot = *(*[superBlockSize]byte)(unsafe.Pointer(sb))
	sb._snapshotChPending = map[*nodeBlock][nodeBlockSize]byte{}
	sb._cacheFds = make(chan *os.File, maxFds)
	sb._mmap, err = mmap.MapRegion(sb._fd, mmapSize, mmap.RDWR, 0, 0)
	if err != nil {
		return nil, err
	}
	sb._mmap.Lock()
	sb._maxFds = int32(maxFds)

	for i := 0; i < maxFds; i++ {
		f, err := os.OpenFile(path, os.O_RDONLY, 0666)
		if err != nil {
			return nil, err
		}

		sb._cacheFds <- f
	}

	return sb, nil
}

type Data struct {
	pair
	_fd     *os.File
	_closed bool
	_super  *SuperBlock
	h       hash.Hash64
	depth   int
	index   int
}

func (d *Data) Read(p []byte) (int, error) {
	if d.size <= 0 {
		return 0, io.EOF
	}

	n, err := d._fd.Read(p)

	if int64(n) > d.size {
		n = int(d.size)
		d.size = 0
	} else {
		d.size -= int64(n)
	}

	d.h.Write(p[:n])
	if d.size == 0 {
		if d.hash != d.h.Sum64() {
			return 0, fmt.Errorf("invalid hash: %x, expect: %x", d.h.Sum64(), d.hash)
		}
	}

	return n, err
}

func (d *Data) Close() {
	if d._closed {
		return
	}
	d._closed = true
	d._super._cacheFds <- d._fd
}

func (d *Data) ReadAllAndClose() []byte {
	buf, _ := ioutil.ReadAll(d)
	d.Close()
	return buf
}

type Fatal struct {
	Err      error
	Snapshot []byte
}

func (f *Fatal) Error() string {
	return f.Err.Error()
}
