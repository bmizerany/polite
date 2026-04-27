// Package lru implements a fixed-capacity cache keyed by K with values of
// type V.
//
// Entries are evicted using the SIEVE policy from Zhang et al., NSDI 2024
// ("SIEVE is Simpler than LRU"). Each entry carries a visited bit set on
// hit. When the cache is full, an internal hand pointer walks the queue
// toward the head, clearing set bits, until it finds an entry whose bit
// is clear; that entry is evicted. The hand keeps its position across
// evictions.
package lru

import "iter"

// A Cache holds up to a fixed number of key/value pairs.
//
// The zero value is not usable; use [New] to create one.
// A Cache is not safe for concurrent use by multiple goroutines.
type Cache[K comparable, V any] struct {
	max        int
	m          map[K]*entry[K, V]
	head, tail *entry[K, V]
	hand       *entry[K, V]
}

type entry[K comparable, V any] struct {
	key        K
	value      V
	visited    bool
	prev, next *entry[K, V] // prev toward head (newer); next toward tail (older)
}

// New returns an empty Cache that holds at most n entries.
// It panics if n is not positive.
func New[K comparable, V any](n int) *Cache[K, V] {
	if n <= 0 {
		panic("lru: capacity must be positive")
	}
	return &Cache[K, V]{
		max: n,
		m:   make(map[K]*entry[K, V], n),
	}
}

// Get returns the value for key. On a hit, Get marks the entry visited
// and returns the stored value; load is not called. On a miss, Get calls
// load, stores the returned value under key, evicting an older entry if
// the cache is full, and returns it.
//
// load must not panic. If it does, the cache may be left in an
// inconsistent state (an eviction may already have been performed) and
// further behavior is undefined. Callers should recover from panics
// inside load and return a sentinel value instead of re-panicking.
func (c *Cache[K, V]) Get(key K, load func() V) V {
	if e, ok := c.m[key]; ok {
		e.visited = true
		return e.value
	}
	v := load()
	var e *entry[K, V]
	if len(c.m) == c.max {
		e = c.evict()
		e.key = key
		e.value = v
		e.visited = false
	} else {
		e = &entry[K, V]{key: key, value: v}
	}
	e.prev = nil
	e.next = c.head
	if c.head != nil {
		c.head.prev = e
	}
	c.head = e
	if c.tail == nil {
		c.tail = e
	}
	c.m[key] = e
	return v
}

// evict removes one entry per the SIEVE policy and returns the unlinked
// struct so the caller can reuse it.
func (c *Cache[K, V]) evict() *entry[K, V] {
	h := c.hand
	if h == nil {
		h = c.tail
	}
	for h.visited {
		h.visited = false
		h = h.prev
		if h == nil {
			h = c.tail
		}
	}
	next := h.prev
	if h.prev != nil {
		h.prev.next = h.next
	} else {
		c.head = h.next
	}
	if h.next != nil {
		h.next.prev = h.prev
	} else {
		c.tail = h.prev
	}
	delete(c.m, h.key)
	if next == nil {
		next = c.tail // wrap from head to (new) tail
	}
	c.hand = next
	return h
}

// Delete removes the entry for key, if any. It is a no-op if key is not
// present.
func (c *Cache[K, V]) Delete(key K) {
	e, ok := c.m[key]
	if !ok {
		return
	}
	handOnE := c.hand == e
	if e.prev != nil {
		e.prev.next = e.next
	} else {
		c.head = e.next
	}
	if e.next != nil {
		e.next.prev = e.prev
	} else {
		c.tail = e.prev
	}
	delete(c.m, key)
	if handOnE {
		if e.prev != nil {
			c.hand = e.prev
		} else {
			c.hand = c.tail // wrap; nil if cache is now empty
		}
	}
}

// Items returns an iterator that yields each cached entry from the most
// recently inserted to the least recently inserted. The cache must not be
// mutated while the iterator is live: [Cache.Get] (which may evict on miss),
// [Cache.Delete], and any other mutating call invalidate the iteration.
func (c *Cache[K, V]) Items() iter.Seq2[K, V] {
	return func(yield func(K, V) bool) {
		for e := c.head; e != nil; e = e.next {
			if !yield(e.key, e.value) {
				return
			}
		}
	}
}
