package waitobject

import (
	"fmt"
	"sync/atomic"
	"testing"
	"time"
)

func TestObjectTimeout(t *testing.T) {
	debug = true
	o := New()
	o.SetWaitDeadline(time.Now().Add(time.Second))
	_, ok := o.Wait()
	if ok {
		t.FailNow()
	}
}

func TestObjectTimeout2(t *testing.T) {
	debug = true
	o := New()
	start := time.Now()

	go func() {
		time.Sleep(time.Second)
		o.SetWaitDeadline(time.Now())
		time.Sleep(time.Second)
		o.Touch(1)
	}()

	_, ok := o.Wait()
	if ok {
		t.FailNow()
	}

	if time.Since(start).Seconds() < 1 {
		t.FailNow()
	}

	_, ok = o.Wait()
	if ok {
		t.FailNow()
	}

	o.SetWaitDeadline(time.Time{})
	v, _ := o.Wait()
	if v.(int) != 1 {
		t.FailNow()
	}
}

func TestObjectTimeout3(t *testing.T) {
	o := New()
	go func() {
		for i := 0; i < 5; i++ {
			time.Sleep(time.Second * 2)
			o.Touch(i)
		}
	}()
	o.SetWaitDeadline(time.Now().Add(time.Second * 5))
	for {
		v, ok := o.Wait()
		if !ok {
			break
		}
		fmt.Println("a", v)
		// 0, 1
	}
	if _, ok := o.Wait(); ok {
		t.FailNow()
	}

	// wait more than 1 sec to omit '2'
	time.Sleep(time.Second + 200*time.Millisecond)
	o.SetWaitDeadline(time.Time{})
	for {
		v, ok := o.Wait()
		if !ok {
			break
		}
		fmt.Println("b", v)
		// 3, 4
		if v.(int) == 4 {
			o.SetWaitDeadline(time.Now())
		}
	}
}

func TestObjectTimeoutThenRead(t *testing.T) {
	debug = true
	o := New()
	go func() {
		time.Sleep(1000 * time.Millisecond)
		o.Touch(1)
		time.Sleep(3000 * time.Millisecond)
		o.Touch(2)
	}()
	o.SetWaitDeadline(time.Now().Add(time.Second * 2))
	v, _ := o.Wait()
	if v.(int) != 1 {
		t.FailNow()
	}
	time.Sleep(time.Second)
	o.SetWaitDeadline(time.Time{})
	v, _ = o.Wait()
	if v.(int) != 2 {
		t.FailNow()
	}
}

func TestObject(t *testing.T) {
	o := New()
	o.Touch(2)

	go func() {
		time.Sleep(time.Second)
		o.Touch(1)
	}()

	var count uint64
	for i := 0; i < 10; i++ {
		go func() {
			v, _ := o.Wait()
			if v.(int) != 1 {
				t.FailNow()
			}
			atomic.AddUint64(&count, 1)
		}()
	}

	time.Sleep(1500 * time.Millisecond)
	if count != 10 {
		t.FailNow()
	}
}
