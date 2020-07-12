package quadtree

import (
	"encoding/json"
	"fmt"
	"strconv"
	"sync"
)

type Database interface {
	Load(id string) (QuadTree, error)
	Store(t QuadTree) error
	StoreElement(id string, e Element) error
	DeleteAllElements(id string) error
	DeleteElement(id string, e Element) error
	StoreOrthant(id string, o int, oid string) (existed bool, err error)
}

type MemoryDatabase struct {
	sync.Mutex
	m map[string]map[string][]byte
	e map[string]map[Point]Element
}

func NewMemoryDatabase() *MemoryDatabase {
	return &MemoryDatabase{
		m: map[string]map[string][]byte{},
		e: map[string]map[Point]Element{},
	}
}

func (m *MemoryDatabase) Load(id string) (QuadTree, error) {
	m.Lock()
	h := m.m[id]
	m.Unlock()
	if h == nil {
		return QuadTree{}, fmt.Errorf("%q not found", id)
	}
	t := QuadTree{}
	if err := json.Unmarshal(h["t"], &t); err != nil {
		return QuadTree{}, err
	}
	m.Lock()
	t.O[0], t.O[1], t.O[2], t.O[3] = string(h["0"]), string(h["1"]), string(h["2"]), string(h["3"])
	t.Elems = m.e[id]
	m.Unlock()
	return t, nil
}

func (m *MemoryDatabase) Store(t QuadTree) error {
	buf, _ := json.Marshal(t)
	m.Lock()
	m.m[t.ID] = map[string][]byte{"t": buf}
	m.Unlock()
	return nil
}

func (m *MemoryDatabase) StoreElement(id string, e Element) error {
	m.Lock()
	h := m.e[id]
	if h == nil {
		h = map[Point]Element{}
		m.e[id] = h
	}
	h[e.Point] = e
	m.Unlock()
	return nil
}

func (m *MemoryDatabase) DeleteAllElements(id string) error {
	m.Lock()
	delete(m.e, id)
	m.Unlock()
	return nil
}

func (m *MemoryDatabase) DeleteElement(id string, e Element) error {
	m.Lock()
	h := m.e[id]
	delete(h, e.Point)
	m.Unlock()
	return nil
}

func (m *MemoryDatabase) StoreOrthant(id string, o int, oid string) (existed bool, err error) {
	m.Lock()
	defer m.Unlock()
	h := m.m[id]
	if m == nil {
		return false, fmt.Errorf("store %q orthant #%d: not found", id, o)
	}
	if _, exist := h[strconv.Itoa(o)]; exist {
		return true, nil
	}
	h[strconv.Itoa(o)] = []byte(oid)
	return false, nil
}
