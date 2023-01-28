package common

import (
	"fmt"
	"unsafe"
)

// Assert will panic with a given formatted message if the given condition is false.
func Assert(condition bool, msg string, v ...interface{}) {
	if !condition {
		panic(fmt.Sprintf("assertion failed: "+msg, v...))
	}
}

func LoadBucket(buf []byte) *InBucket {
	return (*InBucket)(unsafe.Pointer(&buf[0]))
}

func LoadPage(buf []byte) *Page {
	return (*Page)(unsafe.Pointer(&buf[0]))
}

func LoadPageMeta(buf []byte) *Meta {
	return (*Meta)(unsafe.Pointer(&buf[PageHeaderSize]))
}
