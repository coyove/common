package fz

import (
	"bytes"
	"encoding/binary"
	"hash/crc32"
	"hash/fnv"
	"io"
	"io/ioutil"
	"os"
	"strings"
	"sync"
	"time"
	"unsafe"

	mmap "github.com/edsrzf/mmap-go"
)

var superBlockMagic = [4]byte{'z', 'z', 'z', '0'}

const (
	superBlockSize = 72
	nodeBlockSize  = 16 + maxItems*itemSize + maxChildren*8

	// normally an insert op won't affect more than 8 nodes, if it does, we have to save snapshot to an external file
	snapshotSize = 4 + superBlockSize + 8*nodeBlockSize + 16 // <32K
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
	_maxFds     int16
	_closed     bool

	// snapshots store the "stable" states of SuperBlock (and nodeBlock)
	// when dirty nodes are about to sync, new states will go to pending snapshots,
	// only when all dirty nodes are updated without errors, pending snapshots become stable snapshots
	// if any error happened, all nodes revert back to last snapshots.
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

func (b *SuperBlock) Close() error {
	b._lock.Lock()
	defer b._lock.Unlock()

	if b._closed {
		return nil
	}
	b._closed = true
	if err := b._mmap.Unlock(); err != nil {
		return err
	}
	if err := b._mmap.Unmap(); err != nil {
		return err
	}
	if err := b._fd.Close(); err != nil {
		return err
	}
	for i := 0; i < int(b._maxFds); i++ {
		f := <-b._cacheFds
		if err := f.Close(); err != nil {
			return err
		}
	}
	return nil
}

func (sb *SuperBlock) Walk(filter func(Metadata) bool, callback func(key string, data *Data) error) error {
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

	return sb._root.iterate(filter, callback, 0)
}

// syncDirties shall only be called by Add()/Flag()
func (sb *SuperBlock) syncDirties() error {
	if sb._root == nil || len(sb._dirtyNodes) == 0 {
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

	ext := false
	h := fnv.New128()
	h.Write(buf.Bytes())
	buf.Write(h.Sum(nil))

	binary.BigEndian.PutUint32(sb._mmap[superBlockSize:], uint32(buf.Len()))

	if testCase2 {
		// normally we won't have a fatal error here,
		// let's simulate that copy(sb._mmap[superBlockSize+4:], buf.Bytes()) fails
		panic(testError)
	}

	if buf.Len() <= snapshotSize {
		copy(sb._mmap[superBlockSize+4:], buf.Bytes())
	} else {
		ext = true
		if err := ioutil.WriteFile(sb._filename+".snapshot", buf.Bytes(), 0666); err != nil {
			binary.BigEndian.PutUint32(sb._mmap[superBlockSize:], 0)
			return err
		}
	}

	// we have done writing the master snapshot
	// if the above code failed, we are fine because we will directly revertDirties
	// from now on we are entering the critical area,
	// if the belowed code failed, we have to panic,
	// users can recover it, but SuperBlock must not be used anymore.

	nodes := make([]*nodeBlock, 0, len(sb._dirtyNodes))
	for len(sb._dirtyNodes) > 0 {
		for node := range sb._dirtyNodes {
			if !node.areChildrenSynced() {
				continue
			}

			if err := node.sync(); err != nil {
				panic(err)
			}

			nodes = append(nodes, node)
			delete(sb._dirtyNodes, node)
		}
	}

	sb.rootNode = sb._root.offset
	var err error
	if testCase3 || testCase4 {
		err = testError
	} else {
		err = sb.sync()
	}

	if err != nil {
		panic(err)
	}

	// all clear, let's commit the pending snapshots
	for _, node := range nodes {
		node._snapshot = sb._snapshotChPending[node]
		delete(sb._snapshotChPending, node)
	}

	// after that the pending snapshots should be emptied
	if len(sb._snapshotChPending) != 0 {
		panic("shouldn't happen")
	}
	sb._snapshot = sb._snapshotPending
	binary.BigEndian.PutUint64(sb._mmap[superBlockSize:], 0)
	if ext {
		os.Remove(sb._filename + ".snapshot")
	}

	return nil
}

func (sb *SuperBlock) revertDirties() {
	for node := range sb._dirtyNodes {
		node.revertToLastSnapshot()
		delete(sb._dirtyNodes, node)
	}
	sb.revertToLastSnapshot()
}

func (sb *SuperBlock) writeMetadata(key uint128) (Metadata, error) {
	if sb._reader == nil {
		panic("wtf")
	}

	if testCase1 {
		return Metadata{}, testError
	}

	v, err := sb._fd.Seek(0, os.SEEK_END)
	if err != nil {
		return Metadata{}, err
	}

	var keylen uint16 = uint16(len(sb._keystr))
	if keylen > 8 {
		if _, err := io.Copy(sb._fd, strings.NewReader(sb._keystr)); err != nil {
			return Metadata{}, err
		}
	}

	buf := make([]byte, 32*1024)
	written := int64(0)
	h := crc32.NewIEEE()
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
		return Metadata{}, err
	}

	p := Metadata{
		key:    key,
		tstamp: uint32(time.Now().Unix()),
		offset: v,
		crc32:  h.Sum32(),
	}

	p.setKeyLen(keylen)
	p.setBufLen(written)

	sb.size += written
	return p, nil
}

func (sb *SuperBlock) Add(key string, r io.Reader) (err error) {
	sb._lock.Lock()
	defer sb._lock.Unlock()

	if len(key) >= 65536 {
		return ErrKeyTooLong
	}
	if r == nil {
		return ErrKeyNilReader
	}

	defer func() {
		if err != nil {
			sb.revertDirties()
		}
	}()

	sb._reader = r
	sb._keystr = key
	k := sb.hashString(key)

	if sb.rootNode == 0 && sb._root == nil {
		p, err := sb.writeMetadata(k)
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
		return err
	}

	return nil
}

func (sb *SuperBlock) Get(key string) (*Data, error) {
	sb._lock.RLock()
	defer sb._lock.RUnlock()

	load := func(node Metadata) (*Data, error) {
		d := &Data{}
		d._fd = <-sb._cacheFds
		d._super = sb

		if _, err := d._fd.Seek(node.offset, 0); err != nil {
			return nil, err
		}

		if node.KeyLen() > 8 {
			ln := int64(node.KeyLen())
			if _, err := d._fd.Seek(ln, os.SEEK_CUR); err != nil {
				return nil, err
			}
		}

		d.h = crc32.NewIEEE()
		d.Metadata = node
		d.remaining = int(node.BufLen())
		return d, nil
	}

	//	if cached, ok := sb._cache.Get(key); ok {
	//		return load(cached.(Metadata))
	//	}

	k := sb.hashString(key)

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

	node, err := sb._root.getOrFlag(k, nil)
	if err != nil {
		return nil, err
	}

	//	sb._cache.Add(key, node)
	return load(node)
}

func (sb *SuperBlock) Flag(key string, callback func(uint64) uint64) (uint64, error) {
	sb._lock.Lock()
	defer sb._lock.Unlock()

	k := sb.hashString(key)

	var err error
	if sb.rootNode == 0 && sb._root == nil {
		return 0, ErrKeyNotFound
	}

	if sb.rootNode != 0 && sb._root == nil {
		sb._root, err = sb.loadNodeBlock(sb.rootNode)
		if err != nil {
			return 0, err
		}
	}

	node, err := sb._root.getOrFlag(k, callback)
	if err != nil {
		return 0, err
	}

	if err := sb.syncDirties(); err != nil {
		return 0, err
	}

	return node.flag, nil
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
