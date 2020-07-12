package redis_adapter

import (
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/coyove/common/quadtree"
)

func TestSimpleRedis(t *testing.T) {
	rc := New(os.Getenv("QT"))
	quadtree.MaxElems = 2
	rand.Seed(time.Now().Unix())
	_tr, _ := quadtree.NewQuadTree(rc, quadtree.Pt(-180, 90), quadtree.Pt(180, -90), func(t *quadtree.QuadTree) {
		t.MinBox = 3
	})
	tr := func() quadtree.QuadTree {
		tr, _ := _tr.LoadTree(_tr.ID)
		return tr
	}

	randPoint := func() quadtree.Point {
		x := rand.Float64()*360 - 180
		y := rand.Float64()*180 - 90
		if rand.Intn(4) == 0 {
			x = float64(int64(x))
			y = float64(int64(y))
		}
		if rand.Intn(10) == 0 {
			x, y = 0, 0
		}
		return quadtree.Pt(x, y)
	}

	start := time.Now()
	m := map[quadtree.Point]interface{}{}
	allpoints := []quadtree.Point{}
	for i := 0; i < 20; i++ {
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
	fmt.Println(tr())
	rp := allpoints[rand.Intn(len(allpoints))]
	fmt.Print("rand point=", rp, " neighbours=")
	nn, _ := tr().FindNeig(rp, nil)
	fmt.Println(len(nn))
}

func itob(i int) []byte { return []byte(strconv.Itoa(i)) }

func btoi(b []byte) int { i, _ := strconv.Atoi(string(b)); return i }
