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
	testSetMem func()
	testCase1  bool // Simulate: error when copying data to disk
	testCase2  bool // Simulate: fatal error: sync dirties to disk, incomplete snapshot
	testCase3  bool // Simulate: fatal error: sync dirties to disk
	testCase4  bool // Simulate: failed to recover snapshot
	testError  = fmt.Errorf("test")
)

var (
	_one    uint16 = 1
	_endian byte   = *(*byte)(unsafe.Pointer(&_one))
)

type _padding struct {
	a byte
	b [11]byte
	c uint32
	d uint64
}

func init() {
	var p [2]_padding
	var p0, p1 = uintptr(unsafe.Pointer(&p[0])), uintptr(unsafe.Pointer(&p[1]))
	if p1-p0 != 24 {
		panic("golang changed struct padding, again")
	}
}

var (
	defaultMMapSize = 1024 * 1024 * 4
	defaultInitSize = 1024 * 1024 * 4
)

var (
	ErrWrongMagic   = fmt.Errorf("wrong magic code")
	ErrEndianness   = fmt.Errorf("wrong endianness")
	ErrKeyNotFound  = fmt.Errorf("key not found")
	ErrKeyExisted   = fmt.Errorf("key already existed")
	ErrKeyTooLong   = fmt.Errorf("key too long")
	ErrKeyNilReader = fmt.Errorf("nil reader")
)

type Options struct {
	MaxFds      int
	MMapSize    int
	InitSize    int
	ForceCreate bool
}

func Open(path string, opt *Options) (_sb *SuperBlock, _err error) {
	create := true
	if _, err := os.Stat(path); err == nil {
		create = false
	}

	if opt == nil {
		opt = &Options{
			MMapSize: defaultMMapSize,
			InitSize: defaultInitSize,
			MaxFds:   4,
		}
	}
	if opt.MaxFds == 0 {
		opt.MaxFds = 4
	}
	if opt.MMapSize == 0 {
		opt.MMapSize = defaultMMapSize
	}
	if opt.InitSize == 0 {
		opt.InitSize = defaultInitSize
	}
	if opt.MMapSize >= 1024*1024*1024*2 {
		return nil, fmt.Errorf("mmap size can't exceed 2 GiB")
	}
	if opt.MMapSize/4096*4096 != opt.MMapSize || opt.InitSize/4096*4096 != opt.InitSize {
		return nil, fmt.Errorf("mmap/init size should be multiple of 4096")
	}
	if opt.MMapSize > opt.InitSize {
		return nil, fmt.Errorf("mmap size should be smaller than init size")
	}
	if opt.ForceCreate {
		create = true
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
		var payload [4096]byte
		for i := 0; i < opt.InitSize; i += 4096 {
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
		sb.tailptr = int64(opt.MMapSize)
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
	if err := sb._mmap.Lock(); err != nil {
		return nil, err
	}

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

		switch x := recoverSB(sb, snapshot); x {
		case 'S':
			copy((*(*[superBlockSize]byte)(unsafe.Pointer(sb)))[:], sb._mmap[:])
			fallthrough
		case 'I':
			os.Remove(sb._filename + ".snapshot")
		case 'E':
			return nil, ErrSnapshotRecoveryFailed
		}
	}

	sb._snapshot = *(*[superBlockSize]byte)(unsafe.Pointer(sb))
	sb._snapshotChPending = map[*nodeBlock][nodeBlockSize]byte{}
	maxFds := opt.MaxFds
	sb._cacheFds = make(chan *os.File, maxFds)
	sb._maxFds = int16(maxFds)

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
	Metadata
	_fd       *os.File
	_closed   bool
	_super    *SuperBlock
	h         hash.Hash32
	depth     int
	index     int
	remaining int
}

func (d *Data) Read(p []byte) (int, error) {
	if d.remaining <= 0 {
		return 0, io.EOF
	}

	n, err := d._fd.Read(p)

	if n > d.remaining {
		n = d.remaining
		d.remaining = 0
	} else {
		d.remaining -= n
	}

	d.h.Write(p[:n])
	if d.remaining == 0 {
		if d.crc32 != d.h.Sum32() {
			return 0, fmt.Errorf("invalid hash: %x, expect: %x", d.h.Sum32(), d.crc32)
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
