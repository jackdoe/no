package main

import "unsafe"

const uint32size = 4
const dequeItemSize = 1024
const dequeItemSizeBytes = dequeItemSize * uint32size

type deque struct {
	head, current    *dequeItem
	size, partitions int
}

type dequeItem struct {
	idx       int
	partition int
	vals      [dequeItemSize]uint32
	next      *dequeItem
}

func newDeque(partition int) *deque {
	head := newDequeItem(partition)
	return &deque{head, head, 0, 1}
}

func newDequeItem(partition int) *dequeItem {
	return &dequeItem{partition: partition}
}

func (d *deque) Append(v uint32) {
	if d.current.idx >= len(d.current.vals) {
		next := newDequeItem(d.current.partition)
		d.current.next = next
		d.current = next
	}

	d.current.vals[d.current.idx] = v
	d.current.idx++
	d.size++
}

func (d *deque) AppendAll(da *deque) {
	if da.current.partition != d.current.partition {
		d.partitions++
	}

	d.size += da.size
	d.current.next = da.head
	d.current = da.current
}

func (di *dequeItem) valsAsByteSlice() []byte {
	vals := *(*[dequeItemSizeBytes]byte)(unsafe.Pointer(&di.vals[0]))
	return vals[:di.idx*uint32size]
}

func (di *dequeItem) partitionSize() (size int) {
	for p := di.partition; di != nil && p == di.partition; di = di.next {
		size += di.idx
	}
	return size
}
