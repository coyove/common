package slice

import (
	"bytes"
	"reflect"
	"strconv"
	"unsafe"
)

func slicepointerinterface(slice interface{}) *reflect.SliceHeader {
	slice = reflect.ValueOf(slice).Interface()
	sliceaddr := (uintptr)(len(*(*[]byte)(unsafe.Pointer(&slice))))
	return (*reflect.SliceHeader)(unsafe.Pointer(sliceaddr))
}

func sizeof(slice interface{}) int {
	var sizeof int
	switch slice.(type) {
	case *[]byte, *[]int8:
		sizeof = 1
	case *[]int, *[]uint:
		sizeof = strconv.IntSize / 8
	case *[]complex128:
		sizeof = 16
	case *[]int64, *[]uint64, *[]float64, *[]complex64:
		sizeof = 8
	case *[]int32, *[]uint32, *[]float32:
		sizeof = 4
	case *[]int16, *[]uint16:
		sizeof = 2
	case *[]string:
		sizeof = strconv.IntSize / 8 * 2
	case *[]bool:
		sizeof = 1
	default:
		sizeof = (int)(reflect.TypeOf(slice).Elem().Elem().Size())
	}
	return sizeof
}

// Remove removes the element of index from slice
// slice must be a pointer
func Remove(slice interface{}, index int) {
	elemsize := sizeof(slice)
	sliceheader := slicepointerinterface(slice)
	sliceheader.Cap *= elemsize
	sliceheader.Len *= elemsize
	bytes := (*[]byte)(unsafe.Pointer(sliceheader))

	copy((*bytes)[index*elemsize:], (*bytes)[(index+1)*elemsize:])
	sliceheader.Len -= elemsize
	sliceheader.Cap /= elemsize
	sliceheader.Len /= elemsize
}

// Equal compares slice1 and slice2
// slices must be both pointers
func Equal(slice1, slice2 interface{}) bool {
	elemsize1, elemsize2 := sizeof(slice1), sizeof(slice2)
	if elemsize1 != elemsize2 {
		return false
	}

	sliceheader1, sliceheader2 := slicepointerinterface(slice1), slicepointerinterface(slice2)
	if sliceheader1.Len != sliceheader2.Len {
		return false
	}

	sliceheader1.Cap *= elemsize1
	sliceheader1.Len *= elemsize1
	sliceheader2.Cap *= elemsize2
	sliceheader2.Len *= elemsize2

	bytes1, bytes2 := (*[]byte)(unsafe.Pointer(sliceheader1)), (*[]byte)(unsafe.Pointer(sliceheader2))
	e := bytes.Equal(*bytes1, *bytes2)

	sliceheader1.Cap /= elemsize1
	sliceheader1.Len /= elemsize1
	sliceheader2.Cap /= elemsize2
	sliceheader2.Len /= elemsize2

	return e
}

// Clone shallow clones the slice
// slice must be a pointer
func Clone(slice interface{}) interface{} {
	elemsize := sizeof(slice)
	sliceheader1 := slicepointerinterface(slice)
	newslice := reflect.MakeSlice(reflect.TypeOf(slice).Elem(), sliceheader1.Len, sliceheader1.Cap)

	i := newslice.Interface()
	sliceaddr := (uintptr)(len(*(*[]byte)(unsafe.Pointer(&i))))
	sliceheader2 := (*reflect.SliceHeader)(unsafe.Pointer(sliceaddr))

	sliceheader1.Cap *= elemsize
	sliceheader1.Len *= elemsize
	sliceheader2.Cap *= elemsize
	sliceheader2.Len *= elemsize

	bytes1, bytes2 := (*[]byte)(unsafe.Pointer(sliceheader1)), (*[]byte)(unsafe.Pointer(sliceheader2))
	copy(*bytes2, *bytes1)

	sliceheader1.Cap /= elemsize
	sliceheader1.Len /= elemsize
	sliceheader2.Cap /= elemsize
	sliceheader2.Len /= elemsize

	return i
}
