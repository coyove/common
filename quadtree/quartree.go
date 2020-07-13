// Simple quad-tree
package quadtree

import (
	"bytes"
	"crypto/rand"
	"encoding/base64"
	"fmt"
	"math"
	"strconv"
	"strings"
)

var (
	directions = []string{"u", "d", "l", "r", "ul", "ur", "dl", "dr"}
	MaxElems   = 8
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
	mgr    Database
}

func NewQuadTree(mgr Database, tl, br Point, fill func(t *QuadTree)) (QuadTree, error) {
	var id [12]byte
	rand.Read(id[:])
	t := QuadTree{ID: base64.URLEncoding.EncodeToString(id[:]), AABB: [2]Point{tl, br}, mgr: mgr}
	if fill != nil {
		fill(&t)
	}
	return t, mgr.Store(t)
}

func (t QuadTree) insideOrth(p Point) (orthantIndex int, topLeft, bottomRight Point, err error) { // returns 0-3
	tl, br := t.AABB[0], t.AABB[1]
	if p.X() > br.X() || p.X() < tl.X() {
		return 0, Point{}, Point{}, fmt.Errorf("x outside")
	} else if p.Y() > tl.Y() || p.Y() < br.Y() {
		return 0, Point{}, Point{}, fmt.Errorf("y outside")
	}

	center := Pt((tl.X()+br.X())/2, (tl.Y()+br.Y())/2)
	if p.X() >= center.X() && p.Y() > center.Y() {
		return 0, Pt(center.X(), tl.Y()), Pt(br.X(), center.Y()), nil
	} else if p.X() < center.X() && p.Y() >= center.Y() {
		return 1, tl, center, nil
	} else if p.X() <= center.X() && p.Y() < center.Y() {
		return 2, Pt(tl.X(), center.Y()), Pt(center.X(), br.Y()), nil
	}
	return 3, center, br, nil // center itself will be inside 3
}

func (t QuadTree) Put(p Point, v []byte) error {
	if t.isleaf() {
		if len(t.Elems) < MaxElems {
			// Have spare room
			return t.mgr.StoreElement(t.ID, Element{p, v})
		}

		if size := t.AABB[0].Sub(t.AABB[1]); math.Abs(size.X())/2 < t.MinBox || math.Abs(size.Y())/2 < t.MinBox {
			// Cannot split anymore
			return t.mgr.StoreElement(t.ID, Element{p, v})
		}

		// Split node
		for _, e := range t.Elems {
			if err := t.calcPutOrth(e.Point, e.Data); err != nil {
				return err
			}
		}

		if err := t.mgr.DeleteAllElements(t.ID); err != nil {
			return err
		}
	}
	return t.calcPutOrth(p, v)
}

func (t QuadTree) calcPutOrth(p Point, v []byte) error {
	if p == (Point{}) {
		return nil
	}

	i, iul, idr, err := t.insideOrth(p)
	if err != nil {
		return err
	}

	if t.O[i] == "" {
		tr, err := NewQuadTree(t.mgr, iul, idr, func(nt *QuadTree) {
			nt.MinBox = t.MinBox
			nt.Parent = t.ID
		})
		if err != nil {
			return err
		}
		if err := t.mgr.StoreElement(tr.ID, Element{p, v}); err != nil {
			return err
		}
		existed, err := t.mgr.StoreOrthant(t.ID, i, tr.ID)
		if err != nil {
			return err
		}
		if existed {
			t, err := t.LoadTree(t.ID) // reload
			if err != nil {
				return err
			}
			return t.calcPutOrth(p, v)
		}
		return nil
	}
	t, err = t.LoadTree(t.O[i])
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
	return e, t.mgr.DeleteElement(tid, e)
}

func (t QuadTree) find(buf *bytes.Buffer, p Point) (Element, string, error) {
	if t.isleaf() {
		if e, ok := t.Elems[p]; ok {
			return e, t.ID, nil
		}
	}
	i, _, _, err := t.insideOrth(p)
	if err != nil {
		return Element{}, "", err
	}
	// fmt.Println(p, t.Pos, i, t.O)
	if t.O[i] == "" {
		return Element{}, t.ID, fmt.Errorf("%v not found", p)
	}
	if buf != nil {
		buf.WriteByte(byte(i))
	}
	t, err = t.LoadTree(t.O[i])
	if err != nil {
		return Element{}, "", err
	}
	return t.find(buf, p)
}

func (t QuadTree) Iterate(cb func(Element) error) error {
	if t.isleaf() {
		for _, e := range t.Elems {
			if x, y, tl, br := e.Point.X(), e.Point.Y(), t.AABB[0], t.AABB[1]; !(x >= tl.X() && x <= br.X() &&
				y >= br.Y() && y <= tl.Y()) {
				return fmt.Errorf("invalid point outside quadrant: p=%v, aabb=%v-%v", e.Point, tl, br)
			}
			if err := cb(e); err != nil {
				return err
			}
		}
	} else {
		for _, o := range t.O {
			if o != "" {
				ot, err := t.LoadTree(o)
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
			ot, err := t.LoadTree(o)
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

				ot, err := t.LoadTree(o)
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

func (t QuadTree) LoadTree(id string) (QuadTree, error) {
	lt, err := t.mgr.Load(id)
	lt.mgr = t.mgr
	return lt, err
}

func (t QuadTree) SetDataSource(mgr Database) QuadTree {
	t.mgr = mgr
	return t
}
