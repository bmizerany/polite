// Package queues implements FIFO and LIFO queues.
package queues

import "slices"

type Fifo[E any] struct {
	a []E
}

func (q *Fifo[E]) Unshift(v E) {
	q.a = append(q.a, v)
}

func (q *Fifo[E]) Shift() (v E, ok bool) {
	var zero E
	if len(q.a) == 0 {
		return zero, false
	}
	v = q.a[0]
	q.a = append(q.a[:0], q.a[1:]...)
	return v, true
}

func (q *Fifo[E]) DeleteFunc(f func(E) bool) {
	q.a = slices.DeleteFunc(q.a, f)
}

func (q *Fifo[E]) Len() int {
	return len(q.a)
}

type Lifo[E any] struct {
	a []E
}

func (q *Lifo[E]) Push(v E) {
	q.a = append(q.a, v)
}

func (q *Lifo[E]) Pop() (v E, ok bool) {
	var zero E
	if len(q.a) == 0 {
		return zero, false
	}
	v = q.a[len(q.a)-1]
	q.a[len(q.a)-1] = zero
	q.a = q.a[:len(q.a)-1]
	return v, true
}

func (q *Lifo[E]) DeleteFunc(f func(E) bool) {
	q.a = slices.DeleteFunc(q.a, f)
}

func (q *Lifo[T]) Len() int {
	return len(q.a)
}

// Handle is an element of an Intrusive queue.
// It holds a value of type T and pointers to the next and previous elements.
// Users must embed it in their own structs to use it with Intrusive.
//
// It is not safe for use across multiple goroutines.
type Handle[T any] struct {
	next *Handle[T]
	prev *Handle[T]
	p    T
}

// makeHandle initializes a Handle with the given value.
// If h is nil, it allocates a new Handle,
// otherwise it reuses the existing one.
func makeHandle[T any](h *Handle[T], p T) *Handle[T] {
	if h == nil {
		h = new(Handle[T])
	}
	*h = Handle[T]{p: p}
	return h
}

// Value returns the value stored in the Handle.
func (h *Handle[T]) Value() T {
	return h.p
}

// List implements a double-ended queue using intrusive linked list nodes.
// It implements both Fifo and Lifo interfaces.
//
// It supports O(1) time complexity for pushing and poping elements from both ends of the list,
// as well as O(1) time complexity for removing an element from any position in the list.
//
// Users must embed Handle in their own structs to use it with List.
type List[T any] struct {
	head *Handle[T]
	tail *Handle[T]
	len  int64
}

// PushBack adds w to the tail of the queue in O(1) time.
// If w is nil, it allocates a new Handle to hold v.
// The returned Handle is the one that holds v.
func (q *List[T]) PushBack(w *Handle[T], p T) *Handle[T] {
	w = makeHandle(w, p)
	if q.tail == nil {
		q.head = w
		q.tail = w
	} else {
		w.prev = q.tail
		q.tail.next = w
		q.tail = w
	}
	q.len++
	return w
}

// PopBack removes and returns the tail of the queue in O(1) time.
// If the queue is empty, it returns nil.
func (q *List[T]) PopBack() *Handle[T] {
	w := q.tail
	if w != nil {
		q.tail = w.prev
		if q.tail == nil {
			q.head = nil
		} else {
			q.tail.next = nil
		}
		w.next = nil
		w.prev = nil
		q.len--
	}
	return w
}

// PushFront adds w to the head of the queue in O(1) time.
// If w is nil, it allocates a new Handle to hold v.
// The returned Handle is the one that holds v.
func (q *List[T]) PushFront(w *Handle[T], p T) *Handle[T] {
	w = makeHandle(w, p)
	if q.head == nil {
		q.head = w
		q.tail = w
	} else {
		w.next = q.head
		q.head.prev = w
		q.head = w
	}
	q.len++
	return w
}

// PopFront removes and returns the head of the queue in O(1) time.
// If the queue is empty, it returns nil.
func (q *List[T]) PopFront() *Handle[T] {
	w := q.head
	if w != nil {
		q.head = w.next
		if q.head == nil {
			q.tail = nil
		} else {
			q.head.prev = nil
		}
		w.next = nil
		w.prev = nil
		q.len--
	}
	return w
}

// Linked reports whether w is in the queue in worst-case O(n) time.
func (q *List[T]) Linked(w *Handle[T]) bool {
	if w == nil || q.len == 0 {
		return false
	}
	if w == q.head || w == q.tail {
		return true
	}
	for f, b := q.head.next, q.tail.prev; f != nil && b != nil; f, b = f.next, b.prev {
		if f == w || b == w {
			return true
		}
		if f == b || f.next == b {
			break
		}
	}
	return false
}

// Unlink removes w from the queue in O(1) time.
func (q *List[T]) Unlink(w *Handle[T]) bool {
	if w == nil || (w.prev == nil && w.next == nil && q.head != w) {
		return false
	}
	if w.prev != nil {
		w.prev.next = w.next
	} else {
		q.head = w.next
	}
	if w.next != nil {
		w.next.prev = w.prev
	} else {
		q.tail = w.prev
	}
	w.next = nil
	w.prev = nil
	q.len--
	return true
}

// Len returns the number of elements in the queue in O(1) time.
func (q *List[T]) Len() int {
	return int(q.len)
}
