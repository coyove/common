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
}

var errorInterface = reflect.TypeOf((*error)(nil)).Elem()

func (it *Interpreter) Install(name, doc string, f interface{}, comment ...string) {
	rf := reflect.ValueOf(f)
	if rf.Type().NumOut() != 2 || !rf.Type().Out(1).Implements(errorInterface) {
		panic("function should return 2 values: (value, error)")
	}
	it.fn[name] = &Func{
		nargs: rf.Type().NumIn(),
		f:     rf,
		name:  name,
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

func (c _compound) exec(it *Interpreter) (interface{}, error) {
	if len(c) == 0 {
		return nil, nil
	}

	var args []reflect.Value
	var callee *Func

	switch a := c[0].(type) {
	case _atom:
		if a != "->" {
			callee = it.fn[string(a)]
			if callee == nil {
				return nil, (fmt.Errorf("func %v not found", a))
			}
		}
	default:
		return nil, (fmt.Errorf("closure not supported"))
	}

	for i := 1; i < len(c); i++ {
		switch a := c[i].(type) {
		case _atom:
			v, err := it.onMissing(string(a))
			if err != nil {
				return nil, (fmt.Errorf("atom: %v not supported, error: %v", a, err))
			}
			args = append(args, reflect.ValueOf(v))
		case _string:
			args = append(args, reflect.ValueOf(string(a)))
		case _float:
			args = append(args, reflect.ValueOf(float64(a)))
		case _int:
			args = append(args, reflect.ValueOf(int64(a)))
		case _compound:
			res, err := a.exec(it)
			if err != nil {
				return nil, err
			}
			args = append(args, reflect.ValueOf(res))
		}
	}

	if callee == nil {
		if len(args) == 0 {
			return nil, nil
		}
		return args[len(args)-1].Interface(), nil
	}

	if t := callee.f.Type(); !t.IsVariadic() && callee.nargs != len(args) {
		return nil, (fmt.Errorf("func %v requires %d arguments", callee.name, callee.nargs))
	} else if v := t.IsVariadic(); v && callee.nargs-1 > len(args) {
		return nil, (fmt.Errorf("variadic func %v requires at least %d arguments", callee.name, callee.nargs-1))
	}

	res := callee.f.Call(args)
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
			s := debug.Stack()
			if idx := bytes.Index(s, []byte("reflect.Value.Call")); idx > -1 {
				s = bytes.TrimSpace(s[:idx])
			}
			err = fmt.Errorf("recovered from panic: %v %v", r, string(s))
		}
	}()

	c, err := scan(&s, true, it.Sloppy)
	if err != nil {
		return nil, 0, err
	}

	v, err := c.exec(it)
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
