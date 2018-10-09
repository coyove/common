package goal

import (
	"math/rand"
	"testing"
	"time"
)

func TestGoal(t *testing.T) {
	g := New()
	g.Meet(0)
	g.Meet(0)
	g.Meet(1)
	g.Meet(2)
	g.Meet(4)
	g.Meet(3)
	for i := 5; i < 10; i++ {
		g.Meet(uint64(i))
	}

	if g.Goal() != 10 || g.Overflow() != 0 {
		t.FailNow()
	}

	g.Meet(11)
	if g.Goal() != 10 || g.Overflow() != 1 {
		t.FailNow()
	}

	g.Meet(10)
	// we don't check 11 now, if we add 12 (or onward), we will then cehck 11
	if g.Goal() != 11 || g.Overflow() != 1 {
		t.FailNow()
	}

	g.Meet(12)
	if g.Goal() != 13 || g.Overflow() != 0 {
		t.FailNow()
	}

	// 12 has been met, meeting it again will return false
	if g.Meet(12) != false {
		t.FailNow()
	}
}

func TestGoalRandom(t *testing.T) {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	x := r.Perm(65536)
	g := New()
	for _, num := range x {
		g.Meet(uint64(num))
	}

	g.Meet(65536)
	if g.Goal() != 65537 || g.Overflow() != 0 {
		t.FailNow()
	}

	for i := 0; i < 65537; i++ {
		if g.Meet(uint64(i)) != false {
			t.FailNow()
		}
	}
}

func BenchmarkGoalRandom(b *testing.B) {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	x := r.Perm(65536)
	for i := 0; i < b.N; i++ {
		g := New()
		for _, num := range x {
			g.Meet(uint64(num))
		}
		g.Meet(65536)
		if g.Goal() != 65537 || g.Overflow() != 0 {
			b.FailNow()
		}
	}
}

func BenchmarkGoalLessRandom(b *testing.B) {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	x := make([]uint64, 65536)
	for i := 0; i < len(x); i++ {
		x[i] = uint64(i)
	}

	for i := 0; i < len(x); i++ {
		if r.Intn(100) != 0 {
			continue
		}

		j := r.Intn(10) + i - 20

		if j >= 0 && i < len(x) {
			x[i], x[j] = x[j], x[i]
		}
	}

	for i := 0; i < b.N; i++ {
		g := New()
		for _, num := range x {
			g.Meet(uint64(num))
		}
		g.Meet(65536)
		if g.Goal() != 65537 || g.Overflow() != 0 {
			b.FailNow()
		}
	}
}
