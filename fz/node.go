// This file was derived from https://github.com/google/btree
// Below are the original copyright info
// Copyright 2014 Google Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package fz

import (
	"hash/crc32"
	"io"
	"os"
	"sort"
	"unsafe"
)

var nodeMagic = [4]byte{'x', 'x', 'x', '0'}

const maxItems = 63
const maxChildren = maxItems + 1

type nodeBlock struct {
	magic          [4]byte
	itemsSize      uint16
	childrenSize   uint16
	offset         int64
	items          [maxItems]Metadata
	childrenOffset [maxChildren]int64

	_children [maxChildren]*nodeBlock
	_super    *SuperBlock
	_snapshot [nodeBlockSize]byte
}

func (s *nodeBlock) markDirty() {
	s._super.addDirtyNode(s)
}

// insertAt inserts a value into the given index, pushing all subsequent values
// forward.
func (s *nodeBlock) insertItemAt(index int, item Metadata) {
	_ = s.items[maxItems-1-s.itemsSize]
	copy(s.items[index+1:], s.items[index:])
	s.items[index] = item
	s.itemsSize++
	s.markDirty()
}

func (s *nodeBlock) appendItems(data ...Metadata) {
	copy(s.items[s.itemsSize:], data)
	s.itemsSize += uint16(len(data))
	if s.itemsSize > maxItems {
		panic("wtf")
	}
	s.markDirty()
}

// find returns the index where the given item should be inserted into this
// list.  'found' is true if the item already exists in the list at the given
// index.
func (s *nodeBlock) find(key uint128) (index int, found bool) {
	i := sort.Search(int(s.itemsSize), func(i int) bool {
		return key.less(s.items[i].key)
	})
	if i > 0 && s.items[i-1].key == key {
		return i - 1, true
	}
	return i, false
}

// insertAt inserts a value into the given index, pushing all subsequent values
// forward.
func (s *nodeBlock) insertChildAt(index int, n *nodeBlock) {
	_ = s._children[maxChildren-1-s.childrenSize]
	copy(s._children[index+1:], s._children[index:])
	copy(s.childrenOffset[index+1:], s.childrenOffset[index:])
	s._children[index] = n
	s.childrenOffset[index] = n.offset
	s.childrenSize++
	s.markDirty()
}

func (s *nodeBlock) appendChildren(nodes ...*nodeBlock) {
	copy(s._children[s.childrenSize:], nodes)
	for i := uint16(0); i < uint16(len(nodes)); i++ {
		if nodes[i] != nil {
			s.childrenOffset[s.childrenSize+i] = nodes[i].offset
		}
	}

	s.childrenSize += uint16(len(nodes))
	if s.childrenSize > maxChildren {
		panic("wtf")
	}
	s.markDirty()
}

func (s *nodeBlock) appendChildrenAndOffsets(nodes []*nodeBlock, offsets []int64) {
	copy(s._children[s.childrenSize:], nodes)
	for i := uint16(0); i < uint16(len(nodes)); i++ {
		s.childrenOffset[s.childrenSize+i] = offsets[i]
	}

	s.childrenSize += uint16(len(nodes))
	if s.childrenSize > maxChildren {
		panic("wtf")
	}
	s.markDirty()
}

// split splits the given node at the given index.  The current node shrinks,
// and this function returns the item that existed at that index and a new node
// containing all items/children after it.
func (n *nodeBlock) split(i int) (Metadata, *nodeBlock) {
	item := n.items[i]
	next := n._super.newNode()
	next.appendItems(n.items[i+1:]...)
	n.itemsSize = uint16(i)
	if n.childrenSize > 0 {
		next.appendChildrenAndOffsets(n._children[i+1:], n.childrenOffset[i+1:])
		n.childrenSize = uint16(i + 1)
	}
	next.markDirty()
	n.markDirty()
	return item, next
}

// maybeSplitChild checks if a child should be split, and if so splits it.
// Returns whether or not a split occurred.
func (n *nodeBlock) maybeSplitChild(i int) bool {
	ch, _ := n.child(i)
	if ch == nil || ch.itemsSize < maxItems {
		return false
	}
	item, second := ch.split(maxItems / 2)
	n.insertItemAt(i, item)
	n.insertChildAt(i+1, second)
	n.markDirty()
	return true
}

// insert inserts an item into the subtree rooted at this node
func (n *nodeBlock) insert(key uint128) error {
	i, found := n.find(key)
	//log.Println(n.children, n.items, item, i, found)
	if found {
		return ErrKeyExisted
	}

	if n.childrenSize == 0 {
		p, err := n._super.writeMetadata(key)
		if err != nil {
			return err
		}
		n.insertItemAt(i, p)
		return nil
	}

	if n.maybeSplitChild(i) {
		inTree := n.items[i]
		switch {
		case key.less(inTree.key):
			// no change, we want first split node
		case inTree.key.less(key):
			i++ // we want second split node
		default:
			return ErrKeyExisted
		}
	}

	//log.Println(n._children, n.childrenOffset, n.childrenSize)
	ch, err := n.child(i)
	if err != nil {
		return err
	}
	return ch.insert(key)
}

func (n *nodeBlock) child(i int) (*nodeBlock, error) {
	var err error
	if n._children[i] == nil {
		if n.childrenOffset[i] == 0 {
			return nil, nil
		}

		n._children[i], err = n._super.loadNodeBlock(n.childrenOffset[i])
	}
	return n._children[i], err
}

func (n *nodeBlock) areChildrenSynced() bool {
	for i := uint16(0); i < n.childrenSize; i++ {
		if n.childrenOffset[i] == 0 {
			if n._children[i] != nil && n._children[i].offset > 0 {
				n.childrenOffset[i] = n._children[i].offset
				continue
			}
			return false
		}
	}
	return true
}

// sync syncs the node to disk, and add it as the pending snapshot
func (n *nodeBlock) sync() (err error) {
	mm := false
	if n.offset == 0 {
		if n._super.mmapSizeUsed+nodeBlockSize <= n._super.mmapSize {
			n.offset = int64(n._super.mmapSizeUsed)
			n._super.mmapSizeUsed += nodeBlockSize
			mm = true
		} else {
			n.offset, err = n._super._fd.Seek(0, os.SEEK_END)
			if err != nil {
				return
			}
		}
	}

	x := *(*[nodeBlockSize]byte)(unsafe.Pointer(n))

	if mm {
		copy(n._super._mmap[n.offset:], x[:])
	} else {
		_, err = n._super._fd.Seek(n.offset, 0)
		if err != nil {
			return
		}
		_, err = n._super._fd.Write(x[:])
	}

	if err == nil {
		n._super._snapshotChPending[n] = x
	}
	return
}

func (n *nodeBlock) revertToLastSnapshot() {
	*(*[nodeBlockSize]byte)(unsafe.Pointer(n)) = n._snapshot
	n._children = [maxChildren]*nodeBlock{}
}

// get finds the given key in the subtree and returns it.
func (n *nodeBlock) getOrFlag(key uint128, callback func(uint64) uint64) (Metadata, error) {
	i, found := n.find(key)
	if found {
		m := n.items[i]
		if callback != nil {
			old := m.flag
			m.flag = callback(old)
			n.items[i] = m

			if old != m.flag {
				n.markDirty()
			}
		}
		return m, nil
	} else if n.childrenSize > 0 {
		ch, err := n.child(i)
		if err != nil {
			return Metadata{}, err
		}
		return ch.getOrFlag(key, callback)
	}
	return Metadata{}, ErrKeyNotFound
}

func (n *nodeBlock) iterate(filter func(Metadata) bool, callback func(string, *Data) error, depth int) error {
	for i := uint16(0); i < n.itemsSize; i++ {
		if n.childrenSize > 0 {
			ch, err := n.child(int(i))
			if err != nil {
				return err
			}
			if err := ch.iterate(filter, callback, depth+1); err != nil {
				return err
			}
		}

		var d *Data
		var keyname []byte
		var readData = true

		if filter != nil {
			readData = filter(n.items[i])
		}

		if !readData {
			continue
		}

		d = &Data{}
		d._super = n._super
		d._fd = <-n._super._cacheFds

		node := n.items[i]
		if _, err := d._fd.Seek(node.offset, 0); err != nil {
			return err
		}

		if ln := int(node.KeyLen()); ln > 8 {
			keyname = make([]byte, ln)
			if _, err := io.ReadAtLeast(d._fd, keyname, ln); err != nil {
				return err
			}
		} else {
			keyname = make([]byte, ln)
			x := node.key[0]
			copy(keyname, (*(*[8]byte)(unsafe.Pointer(&x)))[:])
		}

		d.h = crc32.NewIEEE()
		d.Metadata = node
		d.depth = depth
		d.index = int(i)
		d.remaining = int(node.BufLen())

		callback(string(keyname), d)

		if d != nil {
			d.Close()
		}
	}

	if n.childrenSize > 0 {
		ch, err := n.child(int(n.childrenSize - 1))
		if err != nil {
			return err
		}

		if err := ch.iterate(filter, callback, depth+1); err != nil {
			return err
		}
	}
	return nil
}
