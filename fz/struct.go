package fz

import (
	"bytes"
	"fmt"
	"hash/fnv"
	"io"
	"os"
	"sort"
	"sync"
	"time"
	"unsafe"

	"github.com/coyove/common/rand"
)

var (
	superBlockMagic        = [4]byte{'z', 'z', 'z', '0'}
	nodeMagic              = [4]byte{'x', 'x', 'x', '0'}
	_one            uint16 = 1
	_endian         byte   = *(*byte)(unsafe.Pointer(&_one))

	ErrWrongMagic  = fmt.Errorf("wrong magic code")
	ErrKeyNotFound = fmt.Errorf("key not found")
	ErrKeyInserted = fmt.Errorf("key inserted")
	ErrKeyExisted  = fmt.Errorf("key already existed")
)

const itemSize = 56
const superBlockSize = 64
const nodeBlockSize = 16 + maxItems*itemSize + maxChildren*8
const nodeBlockSizeFast = 16

const (
	LsAsyncCommit = 1 << iota
)

type SuperBlock struct {
	magic     [4]byte
	endian    byte
	reserved  [7]byte
	createdAt uint32
	size      uint64
	count     uint64
	salt      [16]byte
	rootNode  int64
	superHash uint64

	_fd         *os.File
	_dirtyNodes map[*nodeBlock]bool
	_root       *nodeBlock
	_lock       sync.RWMutex
	_reader     io.Reader
	_flag       uint32

	_snapshot       [superBlockSize]byte
	_masterSnapshot bytes.Buffer
}

type nodeBlock struct {
	magic          [4]byte
	itemsSize      uint16
	childrenSize   uint16
	offset         int64
	items          [maxItems]pair
	childrenOffset [maxChildren]int64

	_children [maxChildren]*nodeBlock
	_super    *SuperBlock
	_snapshot [nodeBlockSize]byte
}

type pair struct {
	key    uint128
	value  int64
	size   int64
	tstamp uint32
	flag   uint32
	flag2  uint64
	hash   uint64
}

func (b *SuperBlock) newNode() *nodeBlock {
	return &nodeBlock{
		magic:  nodeMagic,
		_super: b,
	}
}

func (b *SuperBlock) addDirtyNode(n *nodeBlock) {
	b._dirtyNodes[n] = true
}

func (b *SuperBlock) SetFlag(flag uint32) {
	b._flag |= flag
}

func (b *SuperBlock) Sync() error {
	h := fnv.New64()
	blockHdr := *(*[superBlockSize]byte)(unsafe.Pointer(b))
	h.Write(blockHdr[:superBlockSize-8])
	b.superHash = h.Sum64()
	blockHdr = *(*[superBlockSize]byte)(unsafe.Pointer(b))
	b._snapshot = blockHdr

	b._fd.Seek(0, 0)
	if _, err := b._fd.Write(blockHdr[:]); err != nil {
		return err
	}
	return b._fd.Sync()
}

func (b *SuperBlock) Count() int {
	return int(b.count)
}

func (b *SuperBlock) Close() {
	b._fd.Close()
}

func OpenFZ(path string, create bool) (_sb *SuperBlock, _err error) {
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

	sb := &SuperBlock{_fd: f, _dirtyNodes: map[*nodeBlock]bool{}}
	h := fnv.New64()

	if create {
		r := rand.New()
		sb.magic = superBlockMagic
		sb.endian = _endian
		sb.createdAt = uint32(time.Now().Unix())
		copy(sb.salt[:], r.Fetch(16))
		if err := sb.Sync(); err != nil {
			return nil, err
		}
	} else {
		copy((*(*[superBlockSize]byte)(unsafe.Pointer(sb)))[:], blockHdr[:])
		if sb.magic != superBlockMagic {
			return nil, ErrWrongMagic
		}
		if sb.endian != _endian {
			return nil, fmt.Errorf("wrong endianness")
		}

		h.Write(blockHdr[:superBlockSize-8])
		if sb.superHash != h.Sum64() {
			return nil, fmt.Errorf("wrong super hash: %x and %x", sb.superHash, h.Sum64())
		}

		fi, _ := f.Stat()
		if sb.rootNode >= fi.Size() {
			return nil, fmt.Errorf("corrupted root node")
		}

		sb._snapshot = blockHdr
	}

	return sb, nil
}

func (sb *SuperBlock) loadNodeBlockBytes(offset int64) (nodeHdr [nodeBlockSize]byte, err error) {
	_, err = sb._fd.Seek(offset, 0)
	if err != nil {
		return
	}

	if _, err = io.ReadAtLeast(sb._fd, nodeHdr[:], nodeBlockSize); err != nil {
		return
	}

	return
}

func (sb *SuperBlock) loadNodeBlock(offset int64) (*nodeBlock, error) {
	var n = &nodeBlock{_super: sb}
	x, err := sb.loadNodeBlockBytes(offset)
	if err != nil {
		return nil, err
	}

	*(*[nodeBlockSize]byte)(unsafe.Pointer(n)) = x
	if n.magic != nodeMagic {
		return nil, ErrWrongMagic
	}

	n._snapshot = x
	return n, nil
}

func (b *nodeBlock) fastchild(i int) (*nodeBlock, error) {
	sb := b._super

	_, err := sb._fd.Seek(b.offset+16+itemSize*maxItems+int64(i)*8, 0)
	if err != nil {
		return nil, err
	}

	var addr [8]byte
	if _, err := io.ReadAtLeast(sb._fd, addr[:], len(addr)); err != nil {
		return nil, err
	}

	offset := *(*int64)(unsafe.Pointer(&addr))

	_, err = sb._fd.Seek(offset, 0)
	if err != nil {
		return nil, err
	}

	var nodeHdr [nodeBlockSizeFast]byte
	var node = &nodeBlock{}
	if _, err := io.ReadAtLeast(sb._fd, nodeHdr[:], len(nodeHdr)); err != nil {
		return nil, err
	}
	*(*[nodeBlockSizeFast]byte)(unsafe.Pointer(node)) = nodeHdr

	if node.magic != nodeMagic {
		return nil, ErrWrongMagic
	}

	return node, nil
}

func (b *nodeBlock) fastitem(i int) (*pair, error) {
	sb := b._super

	_, err := sb._fd.Seek(b.offset+16+int64(i)*itemSize, 0)
	if err != nil {
		return nil, err
	}

	var addr [itemSize]byte
	if _, err := io.ReadAtLeast(sb._fd, addr[:], len(addr)); err != nil {
		return nil, err
	}

	return (*pair)(unsafe.Pointer(&addr)), nil
}

func (s *nodeBlock) fastfind(key uint128) (index int, p *pair, found bool) {
	i := sort.Search(int(s.itemsSize), func(i int) bool {
		it, _ := s.fastitem(i)
		return key.less(it.key)
	})
	it, _ := s.fastitem(i - 1)
	if i > 0 && !(it.key.less(key)) {
		return i - 1, it, true
	}
	return i, nil, false
}
