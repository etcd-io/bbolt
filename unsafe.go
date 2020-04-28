package bbolt

import "unsafe"

func unsafeAdd(base unsafe.Pointer, offset uintptr) unsafe.Pointer {
	return unsafe.Pointer(uintptr(base) + offset)
}

func unsafeIndex(base unsafe.Pointer, offset uintptr, elemsz uintptr, n int) unsafe.Pointer {
	return unsafe.Pointer(uintptr(base) + offset + uintptr(n)*elemsz)
}

func unsafeByteSlice(base unsafe.Pointer, offset uintptr, i, j int) []byte {
	return (*[maxAllocSize]byte)(unsafeAdd(base, offset))[i:j:j]
}
