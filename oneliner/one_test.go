package oneliner

import (
	"testing"
)

func TestOne(t *testing.T) {
	it := New(nil)
	it.Install("map", "hello world", func() (interface{}, error) {
		return map[string]string{"a": "a"}, nil
	})
	it.Install("var-test", "", func(v int64, a ...int64) ([]int64, error) {
		a[0] += v
		return a, nil
	})
	t.Log(it.Run(`(k (var-test 1 2 3) 0)`))
	t.Log(it.Run(`(map-keys (
		map
	))`))
	it.Run("(sloppy true)")
	t.Log(it.Run(`(json (var-test 1 2 3`))
	// t.Log(strings.Join(it.Funcs(), "\n"))
}
