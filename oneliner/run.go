package oneliner

import (
	"bytes"
	"fmt"
	"reflect"
	"runtime/debug"
	"strconv"
	"strings"
	"sync/atomic"
	"text/scanner"
	"unicode"
)

type Func struct {
	nargs         int
	name, comment string
	f             reflect.Value
	error1        bool
}

var errorInterface = reflect.TypeOf((*error)(nil)).Elem()

func (it *Interpreter) Install(name, doc string, f interface{}, comment ...string) {
	rf := reflect.ValueOf(f)
	if rf.Type().NumOut() == 2 && !rf.Type().Out(1).Implements(errorInterface) {
		panic("function should return 2 values: (value, error)")
	}
	it.fn[name] = &Func{
		nargs:  rf.Type().NumIn(),
		f:      rf,
		name:   name,
		error1: rf.Type().NumOut() == 1 && rf.Type().Out(0).Implements(errorInterface),
	}
	if doc != "" {
		it.fn[name].comment = " - " + doc
	}
}

type (
	_atom     string
	_compound []interface{}
	_string   string
	_float    float64
	_int      int64
)

func exec(expr interface{}, it *Interpreter) (interface{}, error) {
	switch a := expr.(type) {
	case _atom:
		v, err := it.onMissing(string(a))
		if err != nil {
			return nil, (fmt.Errorf("atom: %v not supported, error: %v", a, err))
		}
		return v, nil
	case _string:
		return string(a), nil
	case _float:
		return float64(a), nil
	case _int:
		return int64(a), nil
	}

	c := expr.(_compound)
	if len(c) == 0 {
		return nil, nil
	}

	var args []reflect.Value
	var lastarg interface{}
	var callee *Func
	var calleeType byte

	switch a := c[0].(type) {
	case _atom:
		switch a {
		case "->":
			calleeType = 'c'
		case "if":
			switch len(c) {
			case 1, 2:
				return nil, fmt.Errorf("too few arguments to call 'if'")
			case 3, 4:
				cond, err := exec(c[1], it)
				if err != nil {
					return nil, err
				}
				if cond == true {
					return exec(c[2], it)
				} else if len(c) == 4 {
					return exec(c[3], it)
				} else {
					return nil, nil
				}
			}
		default:
			if callee = it.fn[string(a)]; callee == nil {
				return nil, fmt.Errorf("func %v not found", a)
			}
			calleeType = 0
		}
	default:
		return nil, (fmt.Errorf("closure not supported"))
	}

	for i := 1; i < len(c); i++ {
		res, err := exec(c[i], it)
		if err != nil {
			return nil, err
		}
		args = append(args, reflect.ValueOf(res))
		lastarg = res
	}

	if calleeType == 'c' {
		return lastarg, nil
	}

	if t := callee.f.Type(); !t.IsVariadic() && callee.nargs != len(args) {
		return nil, (fmt.Errorf("func %v requires %d arguments", callee.name, callee.nargs))
	} else if v := t.IsVariadic(); v && callee.nargs-1 > len(args) {
		return nil, (fmt.Errorf("variadic func %v requires at least %d arguments", callee.name, callee.nargs-1))
	}

	res := callee.f.Call(args)
	if len(res) == 0 {
		return nil, nil
	}

	if len(res) == 1 {
		if callee.error1 {
			e, _ := res[0].Interface().(error)
			return nil, e
		}
		return res[0].Interface(), nil
	}

	// log.Println((*(*[3]uintptr)(unsafe.Pointer(&res[1])))[2])
	if err, _ := res[1].Interface().(error); err != nil {
		return nil, err
	}
	return res[0].Interface(), nil
}

func (it *Interpreter) Run(tmpl string) (result interface{}, counter int64, err error) {
	var s scanner.Scanner
	s.Init(strings.NewReader(tmpl))

	defer func() {
		if r := recover(); r != nil {
			err, _ = r.(error)
			if err == nil {
				s := debug.Stack()
				if idx := bytes.Index(s, []byte("reflect.Value.Call")); idx > -1 {
					s = bytes.TrimSpace(s[:idx])
				}
				err = fmt.Errorf("recovered from panic: %v %v", r, string(s))
			}
		}
	}()

	c, err := scan(&s, true, it.Sloppy)
	if err != nil {
		return nil, 0, err
	}

	v, err := exec(c, it)
	if err != nil {
		return nil, 0, err
	}

	ctr := atomic.AddInt64(&it.counter, 1)
	it.history.Store(ctr, v)
	return v, ctr, nil
}

func (it *Interpreter) Funcs() []string {
	p := []string{}
	for k, f := range it.fn {
		p = append(p, fmt.Sprintf("%s/%d%s", k, f.nargs, f.comment))
	}
	return p
}

func scan(s *scanner.Scanner, toplevel, sloppy bool) (comp _compound, err error) {
	if toplevel {
		comp = append(comp, _atom("->"))
	}
	for tok := s.Scan(); tok != scanner.EOF; tok = s.Scan() {
		// fmt.Println(s.TokenText())
		switch tok {
		case scanner.Float:
			v, _ := strconv.ParseFloat(s.TokenText(), 64)
			comp = append(comp, _float(v))
		case scanner.Int:
			v, _ := strconv.ParseInt(s.TokenText(), 10, 64)
			comp = append(comp, _int(v))
		case scanner.String, scanner.RawString:
			t, err := strconv.Unquote(s.TokenText())
			if err != nil {
				return nil, err
			}
			comp = append(comp, _string(t))
		case '(':
			c, err := scan(s, false, sloppy)
			if err != nil {
				return nil, err
			}
			comp = append(comp, c)
		case ')':
			return _compound(comp), nil
		// case scanner.Ident:
		// 	comp = append(comp, _atom(s.TokenText()))
		default:
			p := bytes.Buffer{}
			for p.WriteString(s.TokenText()); ; {
				next := s.Peek()
				if unicode.IsSpace(next) || next < 0 || next == '(' || next == ')' { // special scanner.X
					break
				}
				s.Scan()
				p.WriteString(s.TokenText())
			}
			comp = append(comp, _atom(p.String()))
		}
	}
	if toplevel || sloppy {
		return comp, nil
	}
	return nil, fmt.Errorf("unexpected end")
}
