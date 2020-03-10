package oneliner

import (
	"fmt"
	"testing"
)

func TestOne(t *testing.T) {
	it := New(nil)
	it.Install("assert", "", func(v bool) error {
		if !v {
			return (fmt.Errorf("assertion failed"))
		}
		return nil
	})
	it.Install("map", "hello world", func() (interface{}, error) {
		return map[string]string{"a": "a"}, nil
	})
	it.Install("var-test", "", func(v int64, a ...int64) ([]int64, error) {
		a[0] += v
		return a, nil
	})

	assert := func(v string) {
		r, _, err := it.Run(v)
		if err != nil {
			t.Fatal(v, err)
		}
		t.Log(v, r)
	}

	assert(`(= 1 (int "1"))`)
	assert(`(= (k (var-test 1 2 3) 0) 3)`)
	assert(`(map-keys (
		map
	))`)
	assert("(sloppy true)")
	assert(`(json (var-test 1 2 3`)
	assert(`(assert (= true (< 1 2`)
	assert(`(assert (if true true false`)
	assert(`(assert (if (not true) false true`)
}
