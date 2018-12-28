package fz

import (
	"bytes"
	"encoding/binary"
	"hash/fnv"
	"io"
	"os"
	"strings"
	"sync"
	"time"
	"unsafe"

	mmap "github.com/edsrzf/mmap-go"
)

var superBlockMagic = [4]byte{'z', 'z', 'z', '0'}

const (
	itemSize       = 56
	superBlockSize = 72
	nodeBlockSize  = 16 + maxItems*itemSize + maxChildren*8
	snapshotSize   = superBlockSize + 4*nodeBlockSize + 16
)

type SuperBlock struct {
	magic        [4]byte
	endian       byte
	reserved     [7]byte
	mmapSize     int32
	mmapSizeUsed int32
	createdAt    uint32
	size         int64
	count        uint64
	salt         [16]byte
	rootNode     int64
	superHash    uint64

	_fd         *os.File
	_mmap       mmap.MMap
	_cacheFds   chan *os.File
	_filename   string
	_dirtyNodes map[*nodeBlock]bool
	_root       *nodeBlock
	_lock       sync.RWMutex
	_reader     io.Reader
	_keystr     string
	_flag       uint32
	_maxFds     int32

	_snapshot          [superBlockSize]byte
	_snapshotPending   [superBlockSize]byte
	_snapshotChPending map[*nodeBlock][nodeBlockSize]byte
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

func (b *SuperBlock) revertToLastSnapshot() {
	*(*[superBlockSize]byte)(unsafe.Pointer(b)) = b._snapshot
	b._root = nil
}

func (b *SuperBlock) SetFlag(flag uint32) {
	b._flag |= flag
}

func (b *SuperBlock) UnsetFlag(flag uint32) {
	b._flag &= ^flag
}

func (b *SuperBlock) sync() error {
	h := fnv.New64()
	blockHdr := *(*[superBlockSize]byte)(unsafe.Pointer(b))
	h.Write(blockHdr[:superBlockSize-8])
	b.superHash = h.Sum64()
	blockHdr = *(*[superBlockSize]byte)(unsafe.Pointer(b))

	var err error
	if b._mmap == nil {
		b._fd.Seek(0, 0)
		if _, err := b._fd.Write(blockHdr[:]); err != nil {
			return err
		}
		err = b._fd.Sync()
	} else {
		copy(b._mmap, blockHdr[:])
		//err = b._mmap.Flush()
	}
	if err == nil {
		b._snapshotPending = blockHdr
	}
	return err
}

func (b *SuperBlock) Count() int {
	return int(b.count)
}

func (b *SuperBlock) Size() int64 {
	return b.size
}

func (b *SuperBlock) Close() {
	b._mmap.Unlock()
	b._mmap.Unmap()
	b._fd.Close()
	for i := 0; i < int(b._maxFds); i++ {
		f := <-b._cacheFds
		f.Close()
	}
}

func (sb *SuperBlock) Walk(readData bool, callback func(key string, data *Data) error) error {
	sb._lock.RLock()
	defer sb._lock.RUnlock()

	var err error
	if sb.rootNode == 0 && sb._root == nil {
		return nil
	}

	if sb._root == nil {
		sb._root, err = sb.loadNodeBlock(sb.rootNode)
		if err != nil {
			return err
		}
	}

	return sb._root.iterate(callback, readData, 0)
}

func (sb *SuperBlock) syncDirties() error {
	if sb._flag&LsAsyncCommit > 0 {
		return nil
	}

	return sb._syncDirties()
}

func (sb *SuperBlock) _syncDirties() error {
	if sb._root == nil {
		// nothing in the tree
		return nil
	}

	buf := bytes.Buffer{}
	buf.Write(sb._snapshot[:])

	for node := range sb._dirtyNodes {
		if node.offset == 0 {
			continue
		}
		buf.Write(node._snapshot[:])
	}

	newFatal := func(err error) *Fatal {
		sb.SetFlag(LsCritical)
		h := fnv.New128()
		h.Write(buf.Bytes())
		buf.Write(h.Sum(nil))
		return &Fatal{Err: err, Snapshot: buf.Bytes()}
	}

	nodes := make([]*nodeBlock, 0, len(sb._dirtyNodes))
	for len(sb._dirtyNodes) > 0 {
		for node := range sb._dirtyNodes {
			if !node.areChildrenSynced() {
				continue
			}

			if err := node.sync(); err != nil {
				return newFatal(err)
			}

			nodes = append(nodes, node)
			delete(sb._dirtyNodes, node)
		}
	}

	sb.rootNode = sb._root.offset
	var err error
	if testCase3 {
		err = testError
	} else {
		err = sb.sync()
	}

	if err == nil {
		// all clear, let's commit the snapshots
		for _, node := range nodes {
			node._snapshot = sb._snapshotChPending[node]
			delete(sb._snapshotChPending, node)
		}
		if len(sb._snapshotChPending) != 0 {
			panic("shouldn't happen")
		}
		sb._snapshot = sb._snapshotPending
	} else {
		return newFatal(err)
	}
	return err
}

func (sb *SuperBlock) revertDirties() {
	for node := range sb._dirtyNodes {
		node.revertToLastSnapshot()
		delete(sb._dirtyNodes, node)
	}
	sb.revertToLastSnapshot()
}

func (sb *SuperBlock) writePair(key uint128) (pair, error) {
	if sb._reader == nil {
		panic("wtf")
	}

	if testCase1 {
		return pair{}, testError
	}

	v, err := sb._fd.Seek(0, os.SEEK_END)
	if err != nil {
		return pair{}, err
	}

	var keystrbuf [2]byte
	binary.BigEndian.PutUint16(keystrbuf[:], uint16(len(sb._keystr)))
	if _, err := sb._fd.Write(keystrbuf[:]); err != nil {
		return pair{}, err
	}
	if _, err := io.Copy(sb._fd, strings.NewReader(sb._keystr)); err != nil {
		return pair{}, err
	}

	buf := make([]byte, 32*1024)
	written := int64(0)
	h := fnv.New64()
	for {
		nr, er := sb._reader.Read(buf)
		if nr > 0 {
			nw, ew := sb._fd.Write(buf[0:nr])
			if nw > 0 {
				written += int64(nw)
				h.Write(buf[0:nr])
			}
			if ew != nil {
				err = ew
				break
			}
			if nr != nw {
				err = io.ErrShortWrite
				break
			}
		}
		if er != nil {
			if er != io.EOF {
				err = er
			}
			break
		}
	}
	if err != nil {
		return pair{}, err
	}

	p := pair{key: key, tstamp: uint32(time.Now().Unix()), value: v, size: written, hash: h.Sum64()}
	sb.size += written
	return p, nil
}

func (sb *SuperBlock) Add(key string, r io.Reader) (err error) {
	sb._lock.Lock()
	defer sb._lock.Unlock()

	if sb._flag&LsCritical > 0 {
		return ErrCriticalState
	}
	if len(key) >= 65536 {
		return ErrKeyTooLong
	}

	fatal := false
	defer func() {
		if err != nil && !fatal {
			if testCase2 {
				sb.revertDirties()
			}

			if err != ErrKeyExisted {
				sb.revertDirties()
			}
		}
	}()

	sb._reader = r
	sb._keystr = key
	k := hashString(key)

	if sb.rootNode == 0 && sb._root == nil {
		p, err := sb.writePair(k)
		if err != nil {
			return err
		}

		sb._root = sb.newNode()
		sb._root.itemsSize = 1
		sb._root.items[0] = p
		sb._root.markDirty()
		goto SYNC
	}

	if sb.rootNode != 0 && sb._root == nil {
		sb._root, err = sb.loadNodeBlock(sb.rootNode)
		if err != nil {
			return err
		}
	}

	if sb._root.itemsSize >= maxItems {
		item2, second := sb._root.split(maxItems / 2)
		oldroot := sb._root
		sb._root = sb.newNode()
		sb._root.appendItems(item2)
		sb._root.appendChildren(oldroot, second)
		sb._root.markDirty()
	}

	if err := sb._root.insert(k); err != nil {
		return err
	}

SYNC:
	sb.count++
	if err := sb.syncDirties(); err != nil {
		fatal = true
		return err
	}

	return nil
}

func (sb *SuperBlock) Get(key string) (*Data, error) {
	sb._lock.RLock()
	defer sb._lock.RUnlock()

	if sb._flag&LsCritical > 0 {
		return nil, ErrCriticalState
	}

	load := func(node pair) (*Data, error) {
		d := &Data{}
		d._fd = <-sb._cacheFds
		d._super = sb

		if _, err := d._fd.Seek(node.value, 0); err != nil {
			return nil, err
		}

		var keystrbuf [2]byte
		if _, err := io.ReadAtLeast(d._fd, keystrbuf[:], 2); err != nil {
			return nil, err
		}
		ln := int(binary.BigEndian.Uint16(keystrbuf[:]))
		keyname := make([]byte, ln)
		if _, err := io.ReadAtLeast(d._fd, keyname, ln); err != nil {
			return nil, err
		}

		d.h = fnv.New64()
		d.pair = node
		return d, nil
	}

	//	if cached, ok := sb._cache.Get(key); ok {
	//		return load(cached.(pair))
	//	}

	k := hashString(key)

	var err error
	if sb.rootNode == 0 && sb._root == nil {
		return nil, ErrKeyNotFound
	}

	if sb.rootNode != 0 && sb._root == nil {
		sb._root, err = sb.loadNodeBlock(sb.rootNode)
		if err != nil {
			return nil, err
		}
	}

	node, err := sb._root.get(k)
	if err != nil {
		return nil, err
	}

	//	sb._cache.Add(key, node)
	return load(node)
}

func (sb *SuperBlock) Commit() error {
	if sb._flag&LsAsyncCommit == 0 {
		panic("SuperBlock is not in async state")
	}

	if sb._flag&LsCritical > 0 {
		return ErrCriticalState
	}

	sb._lock.Lock()
	defer sb._lock.Unlock()

	return sb._syncDirties()
}

func (sb *SuperBlock) loadNodeBlock(offset int64) (*nodeBlock, error) {
	var err error
	var nodeHdr [nodeBlockSize]byte

	if offset < int64(sb.mmapSize) {
		copy(nodeHdr[:], sb._mmap[offset:])
	} else {
		_, err = sb._fd.Seek(offset, 0)
		if err != nil {
			return nil, err
		}

		if _, err := io.ReadAtLeast(sb._fd, nodeHdr[:], nodeBlockSize); err != nil {
			return nil, err
		}
	}

	n := &nodeBlock{_super: sb}
	*(*[nodeBlockSize]byte)(unsafe.Pointer(n)) = nodeHdr
	if n.magic != nodeMagic {
		return nil, ErrWrongMagic
	}

	n._snapshot = nodeHdr
	return n, nil
}
