// Simple quad-tree
package quadtree

import (
	"bytes"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"math"
	"strconv"
	"strings"
	"sync"
)

var (
	directions      = []string{"u", "d", "l", "r", "ul", "ur", "dl", "dr"}
	memStorage      = map[string]map[string][]byte{}
	memStorageElems = map[string]map[Point]Element{}
	memStoragemu    = sync.Mutex{}

	MaxElems    = 8
	ErrNotFound = fmt.Errorf("not found")
	Load        = func(id string) (QuadTree, error) {
		memStoragemu.Lock()
		h := memStorage[id]
		memStoragemu.Unlock()
		if h == nil {
			return QuadTree{}, ErrNotFound
		}
		t := QuadTree{}
		if err := json.Unmarshal(h["t"], &t); err != nil {
			return QuadTree{}, err
		}
		t.O[0], t.O[1], t.O[2], t.O[3] = string(h["0"]), string(h["1"]), string(h["2"]), string(h["3"])
		memStoragemu.Lock()
		t.Elems = memStorageElems[id]
		memStoragemu.Unlock()
		return t, nil
	}
	Store = func(t QuadTree) error {
		buf, _ := json.Marshal(t)
		memStoragemu.Lock()
		memStorage[t.ID] = map[string][]byte{"t": buf}
		memStoragemu.Unlock()
		return nil
	}
	StoreElement = func(id string, e Element) error {
		memStoragemu.Lock()
		m := memStorageElems[id]
		if m == nil {
			m = map[Point]Element{}
			memStorageElems[id] = m
		}
		m[e.Point] = e
		memStoragemu.Unlock()
		return nil
	}
	DeleteAllElements = func(id string) error {
		memStoragemu.Lock()
		delete(memStorageElems, id)
		memStoragemu.Unlock()
		return nil
	}
	DeleteElement = func(id string, e Element) error {
		memStoragemu.Lock()
		m := memStorageElems[id]
		delete(m, e.Point)
		memStoragemu.Unlock()
		return nil
	}
	StoreOrthant = func(id string, o int, oid string) (existed bool, err error) {
		memStoragemu.Lock()
		defer memStoragemu.Unlock()
		m := memStorage[id]
		if m == nil {
			return false, ErrNotFound
		}
		if _, exist := m[strconv.Itoa(o)]; exist {
			return true, nil
		}
		m[strconv.Itoa(o)] = []byte(oid)
		return false, nil
	}
)

type Element struct {
	Point
	Data []byte
}

type Point struct {
	Ix, Iy uint64
}

func Pt(x, y float64) Point               { return Point{^math.Float64bits(x), ^math.Float64bits(y)} }
func (p Point) X() float64                { return math.Float64frombits(^p.Ix) }
func (p Point) Y() float64                { return math.Float64frombits(^p.Iy) }
func (p Point) Sub(p2 Point) Point        { return Pt(p.X()-p2.X(), p.Y()-p2.Y()) }
func (p Point) Distance(p2 Point) float64 { p = p.Sub(p2); return math.Sqrt(p.X()*p.X() + p.Y()*p.Y()) }
func (p Point) String() string            { return fmt.Sprintf("(%.2f,%.2f)", p.X(), p.Y()) }
func (e Element) String() string          { return fmt.Sprintf("<%q-%v>", e.Data, e.Point) }

type QuadTree struct {
	ID     string
	Parent string
	AABB   [2]Point
	MinBox float64
	O      [4]string         `json:"-"` // stored as "0", "1", "2" and "3"
	Elems  map[Point]Element `json:"-"` // stored as a hashmap: Point -> Element
}

func NewQuadTree(tl, br Point, fill func(t *QuadTree)) (QuadTree, error) {
	var id [12]byte
	rand.Read(id[:])
	t := QuadTree{ID: base64.URLEncoding.EncodeToString(id[:]), AABB: [2]Point{tl, br}}
	if fill != nil {
		fill(&t)
	}
	return t, Store(t)
}

func (t QuadTree) insideOrth(p Point) (orthantIndex int, topLeft, bottomRight Point) { // returns 0-3
	tl, br := t.AABB[0], t.AABB[1]
	if p.X() > br.X() || p.X() < tl.X() {
		panic("x outside")
	} else if p.Y() > tl.Y() || p.Y() < br.Y() {
		panic("y outside")
	}

	center := Pt((tl.X()+br.X())/2, (tl.Y()+br.Y())/2)
	if p.X() >= center.X() && p.Y() > center.Y() {
		return 0, Pt(center.X(), tl.Y()), Pt(br.X(), center.Y())
	} else if p.X() < center.X() && p.Y() >= center.Y() {
		return 1, tl, center
	} else if p.X() <= center.X() && p.Y() < center.Y() {
		return 2, Pt(tl.X(), center.Y()), Pt(center.X(), br.Y())
	}
	return 3, center, br // center itself will be inside 3
}

func (t QuadTree) Put(p Point, v []byte) error {
	if t.isleaf() {
		if len(t.Elems) < MaxElems {
			// Have spare room
			return StoreElement(t.ID, Element{p, v})
		}

		if size := t.AABB[0].Sub(t.AABB[1]); math.Abs(size.X())/2 < t.MinBox || math.Abs(size.Y())/2 < t.MinBox {
			// Cannot split anymore
			return StoreElement(t.ID, Element{p, v})
		}

		// Split node
		for _, e := range t.Elems {
			if err := t.calcPutOrth(e.Point, e.Data); err != nil {
				return err
			}
		}

		if err := DeleteAllElements(t.ID); err != nil {
			return err
		}
	}
	return t.calcPutOrth(p, v)
}

func (t QuadTree) calcPutOrth(p Point, v []byte) error {
	if p == (Point{}) {
		return nil
	}

	i, iul, idr := t.insideOrth(p)
	if t.O[i] == "" {
		tr, err := NewQuadTree(iul, idr, func(nt *QuadTree) {
			nt.MinBox = t.MinBox
			nt.Parent = t.ID
		})
		if err != nil {
			return err
		}
		if err := StoreElement(tr.ID, Element{p, v}); err != nil {
			return err
		}
		existed, err := StoreOrthant(t.ID, i, tr.ID)
		if err != nil {
			return err
		}
		if existed {
			t, err := Load(t.ID) // reload
			if err != nil {
				return err
			}
			return t.calcPutOrth(p, v)
		}
		return nil
	}
	t, err := Load(t.O[i])
	if err != nil {
		return err
	}
	return t.Put(p, v)
}

func (t QuadTree) Get(p Point) (Element, error) {
	e, _, err := t.find(nil, p)
	return e, err
}

func (t QuadTree) Remove(p Point) (Element, error) {
	e, tid, err := t.find(nil, p)
	if err != nil {
		return e, err
	}
	return e, DeleteElement(tid, e)
}

func (t QuadTree) find(buf *bytes.Buffer, p Point) (Element, string, error) {
	if t.isleaf() {
		if e, ok := t.Elems[p]; ok {
			return e, t.ID, nil
		}
	}
	i, _, _ := t.insideOrth(p)
	// fmt.Println(p, t.Pos, i, t.O)
	if t.O[i] == "" {
		return Element{}, t.ID, ErrNotFound
	}
	if buf != nil {
		buf.WriteByte(byte(i))
	}
	t, err := Load(t.O[i])
	if err != nil {
		return Element{}, "", err
	}
	return t.find(buf, p)
}

func (t QuadTree) Iterate(cb func(Element) error) error {
	if t.isleaf() {
		for _, e := range t.Elems {
			if err := cb(e); err != nil {
				return err
			}
		}
	} else {
		for _, o := range t.O {
			if o != "" {
				ot, err := Load(o)
				if err != nil {
					return err
				}
				if err := ot.Iterate(cb); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func (t QuadTree) MaxDepth() (depth int, leaves int, err error) {
	d, n := 0, len(t.Elems)
	if !t.isleaf() {
		n = 0
	}
	for _, o := range t.O {
		if o != "" {
			ot, err := Load(o)
			if err != nil {
				return 0, 0, err
			}
			od, on, err := ot.MaxDepth()
			if err != nil {
				return 0, 0, err
			}
			if od > d {
				d = od
			}
			n += on
		}
	}
	return d + 1, n, nil
}

func (t QuadTree) String() string { return t.str(0, "") }

func (t QuadTree) str(ident int, locode string) string {
	p := bytes.Buffer{}
	prefix := strings.Repeat("  ", ident)
	if t.isleaf() {
		p.WriteString(prefix)
		for _, e := range t.Elems {
			p.WriteString(e.String())
			p.WriteString(" ")
		}
		p.WriteString("\n")
	} else {
		for i, o := range t.O {
			if o != "" {
				p.WriteString(prefix)
				p.WriteString(locode)
				p.WriteString(strconv.Itoa(i))
				p.WriteString(":\n")

				ot, err := Load(o)
				if err != nil {
					p.WriteString(prefix)
					p.WriteString("  error: ")
					p.WriteString(err.Error())
				} else {
					p.WriteString(ot.str(ident+1, locode+strconv.Itoa(i)))
				}
			}
		}
	}
	return p.String()
}

func (t QuadTree) isleaf() bool { return len(t.O[0])+len(t.O[1])+len(t.O[2])+len(t.O[3]) == 0 }
