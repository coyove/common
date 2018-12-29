package fz

import (
	"encoding/binary"
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
	LsCritical = 1 << iota
)

type Options struct {
	MaxFds         int
	MMapSize       int
	IgnoreSnapshot bool
}

func Open(path string, opt *Options) (_sb *SuperBlock, _err error) {
	create := true
	if _, err := os.Stat(path); err == nil {
		create = false
	}

	if opt == nil {
		opt = &Options{
			MMapSize: 1024 * 1024,
			MaxFds:   4,
		}
	}
	if opt.MaxFds == 0 {
		opt.MaxFds = 4
	}
	if opt.MMapSize == 0 {
		opt.MMapSize = 1024 * 1024
	}

	if opt.MMapSize/1024*1024 != opt.MMapSize {
		return nil, fmt.Errorf("mmap size should be multiple of 1024")
	}

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

	defer func() {
		if _err != nil && sb._mmap != nil {
			sb._mmap.Unlock()
			sb._mmap.Unmap()
		}
	}()

	h := fnv.New64()

	if create {
		var payload [1024]byte
		for i := 0; i < opt.MMapSize; i += 1024 {
			if _, err := sb._fd.Write(payload[:]); err != nil {
				return nil, err
			}
		}

		r := rand.New()
		sb.magic = superBlockMagic
		sb.endian = _endian
		sb.createdAt = uint32(time.Now().Unix())
		sb.mmapSize = int32(opt.MMapSize)
		sb.mmapSizeUsed = int32(superBlockSize + snapshotSize)
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

	sb._mmap, err = mmap.MapRegion(sb._fd, int(sb.mmapSize), mmap.RDWR, 0, 0)
	if err != nil {
		return nil, err
	}
	sb._mmap.Lock()

	if ln := int(binary.BigEndian.Uint32(sb._mmap[superBlockSize : superBlockSize+4])); ln != 0 {
		var snapshot []byte
		if ln > snapshotSize {
			snapshot, err = ioutil.ReadFile(sb._filename + ".snapshot")
			if err != nil {
				return nil, err
			}
		} else {
			snapshot = make([]byte, ln)
			copy(snapshot, sb._mmap[superBlockSize+4:])
		}

		if !opt.IgnoreSnapshot {
			if err := recoverSB(sb, snapshot); err != nil {
				return nil, err
			}

			copy((*(*[superBlockSize]byte)(unsafe.Pointer(sb)))[:], sb._mmap[:])
			os.Remove(sb._filename + ".snapshot")
		} else {
			// clear the first 8 bytes of any potential snapshot bytes
			binary.BigEndian.PutUint64(sb._mmap[superBlockSize:], 0)
		}
	}

	sb._snapshot = *(*[superBlockSize]byte)(unsafe.Pointer(sb))
	sb._snapshotChPending = map[*nodeBlock][nodeBlockSize]byte{}
	maxFds := opt.MaxFds
	sb._cacheFds = make(chan *os.File, maxFds)
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
