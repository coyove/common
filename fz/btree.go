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

// Package btree implements in-memory B-Trees of arbitrary degree.
//
// btree implements an in-memory B-Tree for use as an ordered data structure.
// It is not meant for persistent storage solutions.
//
// It has a flatter structure than an equivalent red-black or other binary tree,
// which in some cases yields better memory usage and/or performance.
// See some discussion on the matter here:
//   http://google-opensource.blogspot.com/2013/01/c-containers-that-save-memory-and-time.html
// Note, though, that this project is in no way related to the C++ B-Tree
// implementation written about there.
//
// Within this tree, each node contains a slice of items and a (possibly nil)
// slice of children.  For basic numeric values or raw structs, this can cause
// efficiency differences when compared to equivalent C++ template code that
// stores values in arrays within the node:
//   * Due to the overhead of storing values as interfaces (each
//     value needs to be stored as the value itself, then 2 words for the
//     interface pointing to that value and its type), resulting in higher
//     memory use.
//   * Since interfaces can point to values anywhere in memory, values are
//     most likely not stored in contiguous blocks, resulting in a higher
//     number of cache misses.
// These issues don't tend to matter, though, when working with strings or other
// heap-allocated structures, since C++-equivalent structures also must store
// pointers and also distribute their values across the heap.
//
// This implementation is designed to be a drop-in replacement to gollrb.LLRB
// trees, (http://github.com/petar/gollrb), an excellent and probably the most
// widely used ordered tree implementation in the Go ecosystem currently.
// Its functions, therefore, exactly mirror those of
// llrb.LLRB where possible.  Unlike gollrb, though, we currently don't
// support storing multiple equivalent values.
package fz

import (
	"sort"
)

// NewWithFreeList creates a new B-Tree that uses the given node free list.
func NewTree() *BTree {
	return &BTree{}
}

const maxPairs = 63
const maxChildren = maxPairs + 1

type uint128 [2]uint64

func (l uint128) less(r uint128) bool {
	if l[0] == r[0] {
		return l[1] < r[1]
	}
	return l[0] < r[0]
}

type pair struct {
	key   uint64
	value uint64
}

// items stores items in a node.
type items struct {
	size int
	data [maxPairs]pair
}

// insertAt inserts a value into the given index, pushing all subsequent values
// forward.
func (s *items) insertAt(index int, item pair) {
	if s.size >= maxPairs {
		panic("already full")
	}

	copy(s.data[index+1:], s.data[index:])
	s.data[index] = item
	s.size++
}

// removeAt removes a value at a given index, pulling all subsequent values
// back.
func (s *items) removeAt(index int) pair {
	item := s.data[index]
	copy(s.data[index:], s.data[index+1:])
	s.size--
	return item
}

// pop removes and returns the last element in the list.
func (s *items) pop() (out pair) {
	index := s.size - 1
	out = s.data[index]
	s.size--
	return
}

// truncate truncates this instance at index so that it contains only the
// first index items. index must be less than or equal to length.
func (s *items) truncate(index int) {
	s.size = index
}

func (s *items) append(pairs ...pair) {
	copy(s.data[s.size:], pairs)
	s.size += len(pairs)
	if s.size > maxPairs {
		panic("wtf")
	}
}

// find returns the index where the given item should be inserted into this
// list.  'found' is true if the item already exists in the list at the given
// index.
func (s items) find(item uint64) (index int, found bool) {
	i := sort.Search(s.size, func(i int) bool {
		return item < s.data[i].key
	})
	if i > 0 && s.data[i-1].key >= item {
		return i - 1, true
	}
	return i, false
}

// children stores child nodes in a node.
type children struct {
	size int
	data [maxChildren]*node
}

// insertAt inserts a value into the given index, pushing all subsequent values
// forward.
func (s *children) insertAt(index int, n *node) {
	if s.size >= maxChildren {
		panic("already full")
	}

	copy(s.data[index+1:], s.data[index:])
	s.data[index] = n
	s.size++
}

// removeAt removes a value at a given index, pulling all subsequent values
// back.
func (s *children) removeAt(index int) *node {
	item := s.data[index]
	copy(s.data[index:], s.data[index+1:])
	s.size--
	return item
}

// pop removes and returns the last element in the list.
func (s *children) pop() (out *node) {
	index := s.size - 1
	out = s.data[index]
	s.size--
	return
}

// truncate truncates this instance at index so that it contains only the
// first index children. index must be less than or equal to length.
func (s *children) truncate(index int) {
	s.size = index
}

func (s *children) append(nodes ...*node) {
	copy(s.data[s.size:], nodes)
	s.size += len(nodes)
	if s.size > maxChildren {
		panic("wtf")
	}
}

// node is an internal node in a tree.
//
// It must at all times maintain the invariant that either
//   * len(children) == 0, len(items) unconstrained
//   * len(children) == len(items) + 1
type node struct {
	items    items
	children children
}

// split splits the given node at the given index.  The current node shrinks,
// and this function returns the item that existed at that index and a new node
// containing all items/children after it.
func (n *node) split(i int) (pair, node) {
	item := n.items.data[i]
	next := node{}
	next.items.append(n.items.data[i+1:]...)
	n.items.truncate(i)
	if n.children.size > 0 {
		next.children.append(n.children.data[i+1:]...)
		n.children.truncate(i + 1)
	}
	return item, next
}

// maybeSplitChild checks if a child should be split, and if so splits it.
// Returns whether or not a split occurred.
func (n *node) maybeSplitChild(i, maxpairs int) bool {
	if n.children.data[i] == nil || n.children.data[i].items.size < maxpairs {
		return false
	}
	first := n.children.data[i]
	item, second := first.split(maxpairs / 2)
	n.items.insertAt(i, item)
	n.children.insertAt(i+1, &second)
	return true
}

// insert inserts an item into the subtree rooted at this node, making sure
// no nodes in the subtree exceed maxpairs items.  Should an equivalent item be
// be found/replaced by insert, it will be returned.
func (n *node) insert(item uint64, value uint64, maxpairs int) (pair, bool) {
	i, found := n.items.find(item)
	//log.Println(n.children, n.items, item, i, found)
	if found {
		out := n.items.data[i]
		n.items.data[i] = pair{item, value}
		return out, false
	}
	if n.children.size == 0 {
		n.items.insertAt(i, pair{item, value})
		return pair{}, true
	}
	if n.maybeSplitChild(i, maxpairs) {
		inTree := n.items.data[i]
		switch {
		case item < inTree.key:
			// no change, we want first split node
		case inTree.key < item:
			i++ // we want second split node
		default:
			out := n.items.data[i]
			n.items.data[i] = pair{item, value}
			return out, false
		}
	}

	return n.children.data[i].insert(item, value, maxpairs)
}

// get finds the given key in the subtree and returns it.
func (n *node) get(key uint64) pair {
	i, found := n.items.find(key)
	if found {
		return n.items.data[i]
	} else if n.children.size > 0 {
		return n.children.data[i].get(key)
	}
	return pair{}
}

// BTree is an implementation of a B-Tree.
//
// BTree stores pair instances in an ordered structure, allowing easy insertion,
// removal, and iteration.
//
// Write operations are not safe for concurrent mutation by multiple
// goroutines, but Read operations are.
type BTree struct {
	length int
	root   *node
}

// ReplaceOrInsert adds the given item to the tree.  If an item in the tree
// already equals the given one, it is removed from the tree and returned.
// Otherwise, nil is returned.
//
// nil cannot be added to the tree (will panic).
func (t *BTree) ReplaceOrInsert(item pair) pair {
	if t.root == nil {
		t.root = &node{}
		t.root.items.size = 1
		t.root.items.data[0] = item
		t.length++
		return pair{}
	}

	if t.root.items.size >= maxPairs {
		item2, second := t.root.split(maxPairs / 2)
		oldroot := t.root
		t.root = &node{}
		t.root.items.append(item2)
		t.root.children.append(oldroot, &second)
	}

	out, zero := t.root.insert(item.key, item.value, maxPairs)
	if zero {
		t.length++
	}
	return out
}

// Get looks for the key item in the tree, returning it.  It returns nil if
// unable to find that item.
func (t *BTree) Get(key uint64) pair {
	if t.root == nil {
		return pair{}
	}
	return t.root.get(key)
}

// Len returns the number of items currently in the tree.
func (t *BTree) Len() int {
	return t.length
}
