package quadtree

import (
	"fmt"
	"math/rand"
	"strconv"
	"sync"
	"testing"
	"time"
)

func TestQuadTree(t *testing.T) {
	rand.Seed(time.Now().Unix())
	_tr, _ := NewQuadTree(NewMemoryDatabase(), Pt(-180, 90), Pt(180, -90), func(t *QuadTree) {
		t.MinBox = 3
	})
	tr := func() QuadTree {
		tr, _ := _tr.LoadTree(_tr.ID)
		return tr
	}

	fmt.Print("find null tree neig: ")
	fmt.Println(tr().FindNeig(Point{}, nil))

	randPoint := func() Point {
		x := rand.Float64()*360 - 180
		y := rand.Float64()*180 - 90
		if rand.Intn(4) == 0 {
			x = float64(int64(x))
			y = float64(int64(y))
		}
		if rand.Intn(10) == 0 {
			x, y = 0, 0
		}
		return Pt(x, y)
	}

	start := time.Now()
	m := map[Point]interface{}{}
	allpoints := []Point{}
	for i := 0; i < 1e5; i++ {
		p := randPoint()
		m[p] = i
		tr().Put(p, itob(i))
		allpoints = append(allpoints, p)
	}

	length := float64(len(m))
	fmt.Println("size:", len(m), time.Since(start).Seconds()/length)

	start = time.Now()
	idx := 0
	for p, v := range m {
		v2, err := tr().Get(p)
		if err != nil {
			t.Fatal(p, "idx=", idx, "expect=", v, "err=", err)
		}
		if btoi(v2.Data) != v {
			t.Fatal(p, idx, "got:", v2.Data, "expect:", v)
		}
		idx++
	}
	fmt.Println(time.Since(start).Seconds() / length)
	// fmt.Println(tr())
	rp := allpoints[rand.Intn(len(allpoints))]
	fmt.Print("rand point=", rp, " neighbours=")
	nn, _ := tr().FindNeig(rp, nil)
	fmt.Println(len(nn))
}

func TestQuadTreeConcurrent(t *testing.T) {
	rand.Seed(time.Now().Unix())
	_tr, _ := NewQuadTree(NewMemoryDatabase(), Pt(-10, 10), Pt(10, -10), nil)
	tr := func() QuadTree {
		tr, _ := _tr.LoadTree(_tr.ID)
		return tr
	}

	randPoint := func() Point {
		x := rand.Float64()*5 - 10
		y := rand.Float64()*5 - 10
		if rand.Intn(4) == 0 {
			x = float64(int64(x))
			y = float64(int64(y))
		}
		if rand.Intn(10) == 0 {
			x, y = 0, 0
		}
		return Pt(x, y)
	}

	m := map[Point]interface{}{}
	mu := sync.Mutex{}
	allpoints := []Point{}

	wait := sync.WaitGroup{}
	for i := 0; i < 50; i++ {
		wait.Add(1)
		go func(i int) {
			p := randPoint()
			tr().Put(p, itob(i))

			mu.Lock()
			m[p] = i
			allpoints = append(allpoints, p)
			mu.Unlock()
			wait.Done()
		}(i)
	}
	wait.Wait()

	depth, nodes, _ := tr().MaxDepth()
	fmt.Println("size:", len(m), "tree-depth=", depth, "tree-leaves=", nodes)

	idx := 0
	for p, v := range m {
		v2, err := tr().Get(p)
		if err != nil {
			t.Log(p, "tree=", tr(), "idx=", idx, "expect=", v, "err=", err)
		}
		if btoi(v2.Data) != v {
			t.Log(p, idx, "got:", v2.Data, "expect:", v)
		}
		idx++
	}
	// fmt.Println(tr())
	rp := allpoints[rand.Intn(len(allpoints))]
	fmt.Print("rand point=", rp, " neigbours=")
	fmt.Println(tr().FindNeig(rp, nil))

	dedup := map[Point]bool{}
	fmt.Println("iterate result:", tr().Iterate(func(e Element) error {
		if dedup[e.Point] {
			t.Fatal("dedup", e.Point)
		}
		dedup[e.Point] = true
		return nil
	}))
}

func TestQuadTreeNeigSimple(t *testing.T) {
	_tr, _ := NewQuadTree(NewMemoryDatabase(), Pt(-10, 10), Pt(10, -10), func(t *QuadTree) {
		t.MinBox = 0.5
	})
	tr := func() QuadTree {
		tr, _ := _tr.LoadTree(_tr.ID)
		return tr
	}
	tr().Put(Pt(1, 1), itob(1))
	tr().Put(Pt(-1, 1), itob(2))
	fmt.Println(tr().FindNeig(Point{0, 0}, nil))
	tr().Put(Pt(-1, -1), itob(3))
	tr().Put(Pt(1, -1), itob(4))
	fmt.Println(tr().FindNeig(Pt(0.1, 0.1), nil)) // 1 should be the first
	fmt.Println(tr())

	MaxElems = 4
	for i := 0; i < 100; i++ {
		tr().Put(Pt(10-float64(i)*0.01, 10-float64(i)*0.01), itob(i))
	}

	// 100 points inside (9,9)-(10,10)
	// root:  (-10,-10)-(10,10)
	// 0:     (0,0)-(10,10)
	// 00:    (5,5)-(10,10)
	// 000:   (7.5,7.5)-(10,10)
	// 0000:  (8.75,8.75)-(10,10)
	// 00000: (>9,>9),(10,10)
	d, _, _ := tr().MaxDepth()
	if d != 6 {
		t.Fatal(d, tr())
	}
}

func TestPrintQuadFSM(t *testing.T) {
	for _, d := range []string{"u", "d", "l", "r", "ul", "ur", "dl", "dr"} {
		for i := 0; i < 4; i++ {
			o, d := walkFSM(byte(i), d)
			fmt.Printf("% 2s: %d ", d, o)
		}
		fmt.Println()
	}
}

func itob(i int) []byte { return []byte(strconv.Itoa(i)) }

func btoi(b []byte) int { i, _ := strconv.Atoi(string(b)); return i }
