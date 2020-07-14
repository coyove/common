package quadtree

import (
	"bytes"
	"sort"
	"strings"
)

// http://web.archive.org/web/20120907211934/http://ww1.ucmss.com/books/LFS/CSREA2006/MSV4517.pdf
func walk(code []byte, dir string, newcode []byte) {
	copy(newcode, code)
	for i := len(code) - 1; i >= 0; i-- {
		c := code[i]
		newcode[i], dir = walkFSM(c, dir)
		if dir == "" {
			break
		}
	}
}

func walkFSM(startOrth byte, dir string) (byte, string) {
	walkOrth := func(startOrth byte, dir byte) (byte, byte) {
		// 1 0 1 0
		// 2 3 2 3
		// 1 0 1 0
		// 2 3 2 3
		switch dir {
		case 'u':
			return ("\x03\x02\x01\x00"[startOrth]), "uu\x00\x00"[startOrth]
		case 'd':
			return ("\x03\x02\x01\x00"[startOrth]), "\x00\x00dd"[startOrth]
		case 'l':
			return ("\x01\x00\x03\x02"[startOrth]), "\x00ll\x00"[startOrth]
		case 'r':
			return ("\x01\x00\x03\x02"[startOrth]), "r\x00\x00r"[startOrth]
		default:
			panic(dir)
		}
	}
	switch dir {
	case "u", "d", "l", "r":
		o, d := walkOrth(startOrth, dir[0])
		return o, strings.Trim(string(d), "\x00")
	case "ul", "ur", "dl", "dr":
		o, d := walkOrth(startOrth, dir[0])
		o2, d2 := walkOrth(o, dir[1])
		return o2, strings.Trim(string(d)+string(d2), "\x00")
	default:
		panic(dir)
	}
}

func (t QuadTree) getByCode(locode []byte) (interface{}, error) {
	var err error
	for _, code := range locode {
		if t.isleaf() {
			return t.Elems, nil
		}
		o := t.O[code]
		if o == "" {
			return nil, nil
		}
		t, err = t.LoadTree(o)
		if err != nil {
			return nil, err
		}
	}
	// d, n := t.MaxDepth()
	// fmt.Println("####code:", locode, d, n)
	return t, nil // return the parent tree whose children all share the same code prefix
}

func (t QuadTree) FindNeig(src Point, distance func(p Point) float64) ([]Element, error) {
	if distance == nil { // Simple Euclidean distance
		distance = src.Distance
	}

	buf := &bytes.Buffer{}
	cands := map[Point]Element{}

	_, tid, _ := t.find(buf, src)
	pt, err := t.LoadTree(tid)
	if err != nil {
		return nil, err
	}
	for _, e := range pt.Elems {
		if e.Point != src {
			cands[e.Point] = e
		}
	}

	x, tmp := buf.Bytes(), make([]byte, buf.Len()*8)
NEXT_DIR:
	for i, dir := range directions {
		y := tmp[i*len(x) : (i+1)*len(x)]
		walk(x, dir, y) // walk dir from x to y
		for ii := 0; ii < i; ii++ {
			if bytes.Equal(tmp[ii*len(x):(ii+1)*len(x)], y) {
				continue NEXT_DIR
			}
		}
		v, err := t.getByCode(y)
		if err != nil {
			return nil, err
		}
		switch v := v.(type) {
		case map[Point]Element:
			for p, e := range v {
				cands[p] = e
			}
		case QuadTree:
			v.Iterate(func(e Element) error { cands[e.Point] = e; return nil })
		}
	}
	els := make([]Element, 0, len(cands))
	for p, k := range cands {
		if p != (Point{}) {
			els = append(els, k)
		}
	}
	sort.Slice(els, func(i, j int) bool { return distance(els[i].Point) < distance(els[j].Point) })
	return els, nil
}
