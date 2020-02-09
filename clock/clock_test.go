package clock

import (
	"sync"
	"testing"
	"time"
)

func TestNow(t *testing.T) {
	for {
		if time.Now().Nanosecond()/1e6 > 900 {
			break
		}
	}

	wg := sync.WaitGroup{}
	m := sync.Map{}

	for i := 0; i < 1e3; i++ {
		wg.Add(1)
		go func() {
			for i := 0; i < 1e3; i++ {
				n := Timestamp()
				if _, ok := m.Load(n); ok {
					t.Fatal(n)
				}
				m.Store(n, true)
			}
			wg.Done()
		}()
	}
	wg.Wait()
}

func BenchmarkThumb(b *testing.B) {
	for i := 0; i < b.N; i++ {
		timeNow()
	}
}

func BenchmarkTime(b *testing.B) {
	for i := 0; i < b.N; i++ {
		time.Now().UnixNano()
	}
}
