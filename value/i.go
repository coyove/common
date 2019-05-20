package value

import (
	"math"
	"reflect"
	"strconv"
	"unsafe"
)

type valueType byte

var (
	typeNil       valueType = 0
	typeString    valueType = 1
	typeBytes     valueType = 2
	typeInterface valueType = 3
	typeXInt                = unsafe.Pointer(&typeNil)
	typeXFloat              = unsafe.Pointer(&typeString)
)

type Value struct {
	a uint64
	p unsafe.Pointer
}

func init() {
	if strconv.IntSize != 64 && strconv.IntSize != 32 {
		panic("wtf")
	}
}

func Nil() Value {
	return Value{}
}

func (v Value) IsNil() bool {
	return v == Value{}
}

func Int64(v int64) Value {
	return Value{a: uint64(v), p: typeXInt}
}

func (v Value) IsInt64() bool {
	return v.p == typeXInt
}

func (v Value) Int64() (int64, bool) {
	return int64(v.a), v.p == typeXInt
}

func Float64(v float64) Value {
	return Value{a: math.Float64bits(v), p: typeXFloat}
}

func (v Value) IsFloat64() bool {
	return v.p == typeXFloat
}

func (v Value) Float64() (float64, bool) {
	return math.Float64frombits(v.a), v.p == typeXFloat
}

func String(s string) Value {
	r := *(*reflect.StringHeader)(unsafe.Pointer(&s))
	v := Value{}
	v.p = unsafe.Pointer(r.Data)
	v.a = uint64(typeString)<<62 | uint64(r.Len)
	return v
}

func (v Value) IsString() bool {
	return valueType(v.a>>62) == typeString
}

func (v Value) String() (string, bool) {
	if valueType(v.a>>62) != typeString {
		return "", false
	}
	r := reflect.StringHeader{}
	r.Data = uintptr(v.p)
	r.Len = int(v.a << 2 >> 2)
	return *(*string)(unsafe.Pointer(&r)), true
}

func Bytes(p []byte) Value {
	r := *(*reflect.SliceHeader)(unsafe.Pointer(&p))
	if r.Cap >= 1<<31 || r.Len >= 1<<31 {
		panic("Value can't support bytes more than 1<<31 - 1")
	}
	v := Value{}
	v.p = unsafe.Pointer(r.Data)
	v.a = uint64(typeBytes)<<62 | uint64(r.Len)<<31 | uint64(r.Cap)
	return v
}

func (v Value) IsBytes() bool {
	return valueType(v.a>>62) == typeBytes
}

func (v Value) Bytes() ([]byte, bool) {
	if valueType(v.a>>62) != typeBytes {
		return nil, false
	}
	r := reflect.SliceHeader{}
	r.Data = uintptr(v.p)
	r.Len = int(v.a << 2 >> 33)
	r.Cap = int(v.a << 33 >> 33)
	return *(*[]byte)(unsafe.Pointer(&r)), true
}

func Interface(i interface{}) Value {
	if strconv.IntSize == 64 {
		v := *(*Value)(unsafe.Pointer(&i))
		v.a |= uint64(typeInterface) << 62
		return v
	}

	tmp := *(*[2]uintptr)(unsafe.Pointer(&i))
	v := Value{}
	v.p = unsafe.Pointer(tmp[1])
	v.a = uint64(typeInterface)<<62 | uint64(tmp[0])
	return v
}

func (v Value) IsInterface() bool {
	return valueType(v.a>>62) == typeInterface
}

func (v Value) Interface() (interface{}, bool) {
	if valueType(v.a>>62) != typeInterface {
		return nil, false
	}

	v.a = v.a << 2 >> 2
	if strconv.IntSize == 64 {
		return *(*interface{})(unsafe.Pointer(&v)), true
	}

	tmp := [2]uintptr{uintptr(v.a), uintptr(v.p)}
	return *(*interface{})(unsafe.Pointer(&tmp)), true

}

func (v Value) Value() interface{} {
	var a interface{}
	switch true {
	case v.IsInt64():
		a, _ = v.Int64()
	case v.IsFloat64():
		a, _ = v.Float64()
	case v.IsString():
		a, _ = v.String()
	case v.IsBytes():
		a, _ = v.Bytes()
	case v.IsInterface():
		a, _ = v.Interface()
	}
	return a
}
