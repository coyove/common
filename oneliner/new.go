package oneliner

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"math"
	"reflect"
	"strconv"
	"strings"
	"sync"
)

type Interpreter struct {
	onMissing func(k string) (interface{}, error)
	fn        map[string]*Func
	counter   int64
	history   sync.Map
	Sloppy    bool
}

var errorInterface = reflect.TypeOf((*error)(nil)).Elem()

func (it *Interpreter) Install(name, doc string, f interface{}) {
	rf := reflect.ValueOf(f)
	t := rf.Type()

	if t.NumOut() > 2 {
		panic("function should return at most 2 values")
	} else if t.NumOut() == 2 && !t.Out(1).Implements(errorInterface) {
		panic("function should return 2 values: (value, error)")
	}

	it.fn[name] = &Func{
		nargs:  t.NumIn(),
		f:      rf,
		name:   name,
		error1: t.NumOut() == 1 && t.Out(0).Implements(errorInterface),
		vararg: t.IsVariadic(),
	}

	if doc != "" {
		it.fn[name].comment = " - " + doc
	}
}

func New(onMissing func(k string) (interface{}, error)) *Interpreter {
	var it *Interpreter
	it = &Interpreter{
		fn: map[string]*Func{},
		onMissing: func(k string) (interface{}, error) {
			switch k {
			case "true", "on", "yes":
				return true, nil
			case "false", "off", "no":
				return false, nil
			}
			if strings.HasPrefix(k, "$") {
				if v, _ := strconv.ParseInt(k[1:], 10, 64); v > 0 && v <= it.counter {
					v, _ := it.history.Load(v)
					return v, nil
				}
			}
			if onMissing != nil {
				return onMissing(k)
			}
			return nil, fmt.Errorf("not found")
		},
	}

	less := func(a, b interface{}) bool {
		switch a.(type) {
		case int64:
			return a.(int64) < b.(int64)
		case float64:
			return a.(float64) < b.(float64)
		case string:
			return a.(string) < b.(string)
		case uint64:
			return a.(uint64) < b.(uint64)
		default:
			panic(fmt.Errorf("%v and %v are no comparable", a, b))
		}
	}

	it.Install("not", "", func(a bool) bool { return !a })
	it.Install("=", "", func(a, b interface{}) bool { return a == b })
	it.Install("==", "", func(a, b interface{}) bool { return a == b })
	it.Install("!=", "", func(a, b interface{}) bool { return a != b })
	it.Install("<>", "", func(a, b interface{}) bool { return a != b })

	it.Install("^", "concat strings", func(c ...string) string { return strings.Join(c, "") })

	it.Install("+", "", func(c ...int64) (x int64) {
		for _, c := range c {
			x += c
		}
		return
	})
	it.Install("-", "", func(a int64, c ...int64) (x int64) {
		x = a
		for _, c := range c {
			x -= c
		}
		return
	})
	it.Install("*", "", func(c ...int64) (x int64) {
		x = 1
		for _, c := range c {
			x *= c
		}
		return
	})
	it.Install("/", "", func(a int64, c ...int64) (x int64) {
		x = a
		for _, c := range c {
			x /= c
		}
		return
	})
	it.Install("%", "", func(a int64, c ...int64) (x int64) {
		x = a
		for _, c := range c {
			x %= c
		}
		return
	})
	it.Install("+.", "", func(c ...float64) (x float64) {
		for _, c := range c {
			x += c
		}
		return
	})
	it.Install("-.", "", func(a float64, c ...float64) (x float64) {
		x = a
		for _, c := range c {
			x -= c
		}
		return
	})
	it.Install("*.", "", func(c ...float64) (x float64) {
		x = 1
		for _, c := range c {
			x *= c
		}
		return
	})
	it.Install("/.", "", func(a float64, c ...float64) (x float64) {
		x = a
		for _, c := range c {
			x /= c
		}
		return
	})
	it.Install("%.", "", func(a float64, c ...float64) (x float64) {
		x = a
		for _, c := range c {
			x = math.Remainder(x, c)
		}
		return
	})

	it.Install(">", "", func(a, b interface{}) bool { return !less(a, b) && a != b })
	it.Install(">=", "", func(a, b interface{}) bool { return !less(a, b) })
	it.Install("<", "", func(a, b interface{}) bool { return less(a, b) })
	it.Install("<=", "", func(a, b interface{}) bool { return less(a, b) || a == b })

	it.Install("json", "", func(a interface{}) (string, error) {
		buf, err := json.MarshalIndent(a, "", "  ")
		return string(buf), err
	})
	it.Install("write-file", "", func(fn string, a interface{}) error {
		buf, _ := json.MarshalIndent(a, "", "  ")
		return ioutil.WriteFile(fn, buf, 0777)
	})
	it.Install("read-file", "", func(fn string) (string, error) {
		buf, err := ioutil.ReadFile(fn)
		return string(buf), err
	})

	it.Install("map-keys", "", func(a interface{}) ([]interface{}, error) {
		keys := reflect.ValueOf(a).MapKeys()
		ret := make([]interface{}, len(keys))
		for i := range keys {
			ret[i] = keys[i].Interface()
		}
		return ret, nil
	})
	it.Install("k", "", func(m, k interface{}) (interface{}, error) {
		rv := reflect.ValueOf(m)
		switch rv.Kind() {
		case reflect.Map:
			return rv.MapIndex(reflect.ValueOf(k)).Interface(), nil
		case reflect.Slice, reflect.Array:
			return rv.Index(int(k.(int64))).Interface(), nil
		}
		return nil, fmt.Errorf("error calling %v on %v", k, m)
	})
	it.Install("int", "", func(a interface{}) (int64, error) {
		switch a := a.(type) {
		case int64:
			return int64(a), nil
		case float64:
			return int64(a), nil
		case string:
			return strconv.ParseInt(a, 10, 64)
		default:
			rv := reflect.ValueOf(a)
			switch rv.Kind() {
			case reflect.Int:
				return rv.Int(), nil
			case reflect.Uint:
				return int64(rv.Uint()), nil
			}
			return 0, fmt.Errorf("can't convert %v to int64", a)
		}
	})
	it.Install("float", "", func(a interface{}) (float64, error) {
		switch a := a.(type) {
		case int64:
			return float64(a), nil
		case float64:
			return float64(a), nil
		case string:
			return strconv.ParseFloat(a, 64)
		default:
			rv := reflect.ValueOf(a)
			switch rv.Kind() {
			case reflect.Int:
				return float64(rv.Int()), nil
			case reflect.Uint:
				return float64(rv.Uint()), nil
			}
			return 0, fmt.Errorf("can't convert %v to float64", a)
		}
	})
	it.Install("uint", "", func(a interface{}) (uint64, error) {
		switch a := a.(type) {
		case int64:
			return uint64(a), nil
		case float64:
			return uint64(a), nil
		case string:
			return strconv.ParseUint(a, 10, 64)
		default:
			rv := reflect.ValueOf(a)
			switch rv.Kind() {
			case reflect.Int:
				return uint64(rv.Int()), nil
			case reflect.Uint:
				return rv.Uint(), nil
			}
			return 0, fmt.Errorf("can't convert %v to uint64", a)
		}
	})
	it.Install("sloppy", "", func(v bool) (bool, error) { it.Sloppy = v; return true, nil })
	return it
}
