package main

const dequeItemSize = 1024

type deque struct {
	head    *dequeItem
	current *dequeItem
	size    int
}

type dequeItem struct {
	cnt  int
	vals [dequeItemSize]int64
	next *dequeItem
}

func newDeque() *deque {
	head := &dequeItem{}
	return &deque{head, head, 0}
}

func (d *deque) Append(v int64) {
	if d.current.cnt >= len(d.current.vals) {
		next := &dequeItem{}
		d.current.next = next
		d.current = next
	}

	d.current.vals[d.current.cnt] = v
	d.current.cnt++
	d.size++
}
