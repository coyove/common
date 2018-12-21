package fz

import (
	"fmt"
	"hash/fnv"
	"io"
	"os"
	"time"
	"unsafe"

	"github.com/coyove/common/rand"
)

var (
	superBlockMagic        = [4]byte{'z', 'z', 'z', '0'}
	nodeMagic              = [4]byte{'x', 'x', 'x', '0'}
	_one            uint16 = 1
	_endian         byte   = *(*byte)(unsafe.Pointer(&_one))
)

const superBlockSize = 64
const nodeBlockSize = 16 + maxItems*24 + maxChildren*8

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
}

type pair struct {
	key   uint128
	value uint64
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

func (b *SuperBlock) Sync() error {
	h := fnv.New64()
	blockHdr := *(*[superBlockSize]byte)(unsafe.Pointer(b))
	h.Write(blockHdr[:superBlockSize-8])
	b.superHash = h.Sum64()
	blockHdr = *(*[superBlockSize]byte)(unsafe.Pointer(b))

	b._fd.Seek(0, 0)
	if _, err := b._fd.Write(blockHdr[:]); err != nil {
		return err
	}
	return b._fd.Sync()
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
			return nil, fmt.Errorf("wrong magic code")
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
	}

	return sb, nil
}

func (sb *SuperBlock) loadNodeBlock(offset int64) (*nodeBlock, error) {
	_, err := sb._fd.Seek(offset, 0)
	if err != nil {
		return nil, err
	}

	var nodeHdr [nodeBlockSize]byte
	var n = &nodeBlock{_super: sb}

	if _, err := io.ReadAtLeast(sb._fd, nodeHdr[:], nodeBlockSize); err != nil {
		return nil, err
	}

	*(*[nodeBlockSize]byte)(unsafe.Pointer(n)) = nodeHdr
	if n.magic != nodeMagic {
		return nil, fmt.Errorf("wrong magic code")
	}

	return n, nil
}
