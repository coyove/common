package oneliner

import (
	"encoding/json"
	"fmt"
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
	it.Install("^", "concat two strings", func(a, b string) (string, error) { return a + b, nil })
	it.Install("+", "add two integer numbers", func(a, b int64) (int64, error) { return a + b, nil })
	it.Install("-", "subtract two integer numbers", func(a, b int64) (int64, error) { return a - b, nil })
	it.Install("*", "", func(a, b int64) (int64, error) { return a * b, nil })
	it.Install("/", "", func(a, b int64) (int64, error) { return a / b, nil })
	it.Install("%", "", func(a, b int64) (int64, error) { return a % b, nil })
	it.Install("+.", "add two float numbers", func(a, b float64) (float64, error) { return a + b, nil })
	it.Install("-.", "subtract two float numbers", func(a, b float64) (float64, error) { return a - b, nil })
	it.Install("*.", "", func(a, b float64) (float64, error) { return a * b, nil })
	it.Install("**.", "", func(a, b float64) (float64, error) { return math.Pow(a, b), nil })
	it.Install("/.", "", func(a, b float64) (float64, error) { return a / b, nil })
	it.Install("%.", "", func(a, b float64) (float64, error) { return math.Remainder(a, b), nil })
	it.Install("json", "", func(a interface{}) (string, error) {
		buf, err := json.MarshalIndent(a, "", "  ")
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
