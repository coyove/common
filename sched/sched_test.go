package sched

import (
	"fmt"
	"math/rand"
	"sync/atomic"
	"testing"
	"time"
)

func TestSched(t *testing.T) {
	rand.Seed(time.Now().Unix())

	var x atomic.Int64

	m := NewGroup(func(data []any) {
		// fmt.Println(len(data))
		for _, d := range data {
			x.Add(int64(d.(int)))
		}
	})

	const N = 1e6
	ref := map[int]Key{}
	for i := 0; i < N; i++ {
		d := time.Second + time.Duration(rand.Intn(2000))*time.Millisecond
		k := m.Schedule(d, i)
		ref[i] = k
	}

	c := 0
	deletes := 0
	for i, key := range ref {
		m.Cancel(key)
		deletes += i
		if c++; c > 10 {
			break
		}
	}

	time.Sleep(time.Second * 4)
	fmt.Println(x.Load(), N*(N-1)/2-deletes)

	{
		m := NewGroup(func(data []any) {
			fmt.Println(data)
			if len(data) != 1 || data[0] != 2 {
				t.Fatal(data)
			}
		})
		a := m.Schedule(time.Second+1, 1)
		m.Schedule(time.Second+2, 2)
		m.Cancel(a)
	}

	select {}
}
