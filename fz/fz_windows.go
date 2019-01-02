package fz

import (
	"log"
	"syscall"
	"unsafe"
)

func init() {
	// for windows, VitualAlloc will fail if process working set size is too small
	testSetMem = func() {
		phandle, err := syscall.GetCurrentProcess()
		if err != nil {
			panic(err)
		}

		kernel32 := syscall.NewLazyDLL("kernel32.dll")
		getwss := kernel32.NewProc("GetProcessWorkingSetSize")
		setwss := kernel32.NewProc("SetProcessWorkingSetSize")

		var wsmin, wsmax uintptr
		var wsmin2, wsmax2 uintptr

		getwss.Call(uintptr(phandle), uintptr(unsafe.Pointer(&wsmin)), uintptr(unsafe.Pointer(&wsmax)))
		setwss.Call(uintptr(phandle), uintptr(defaultMMapSize*2), uintptr(defaultMMapSize*2))
		getwss.Call(uintptr(phandle), uintptr(unsafe.Pointer(&wsmin2)), uintptr(unsafe.Pointer(&wsmax2)))

		log.Println("init: Windows process working set size")
		log.Printf("  before: % 8d - % 8d\n", wsmin, wsmax)
		log.Printf("  after : % 8d - % 8d\n", wsmin2, wsmax2)
	}
}
