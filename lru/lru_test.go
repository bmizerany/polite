package lru

import (
	mrand "math/rand"
	"math/rand/v2"
	"slices"
	"strconv"
	"strings"
	"testing"
)

// runTape executes a tape — a multiline script of cache operations and the
// expected cache state after each one — against a fresh Cache. The format:
//
//	cap=N            sets capacity (must appear first; required)
//	get K → STATE    calls c.Get(K, …), then asserts state matches STATE
//	# anything       comment (whole line or trailing on a get line)
//	(blank line)     ignored
//
// STATE is rendered head→tail, space-separated, in square brackets:
//
//	[d c* b^]
//
// where '*' marks a visited bit and '^' marks the eviction hand. The loader
// passed to Get returns the key as the value.
func runTape(t *testing.T, tape string) {
	t.Helper()
	var c *Cache[string, string]
	for i, raw := range strings.Split(strings.TrimSpace(tape), "\n") {
		line, _, _ := strings.Cut(raw, "#")
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		if rest, ok := strings.CutPrefix(line, "cap="); ok {
			n, err := strconv.Atoi(rest)
			if err != nil {
				t.Fatalf("line %d: bad cap %q: %v", i+1, rest, err)
			}
			c = New[string, string](n)
			continue
		}
		rest, ok := strings.CutPrefix(line, "get ")
		if !ok {
			t.Fatalf("line %d: unrecognized %q", i+1, raw)
		}
		key, want, ok := strings.Cut(rest, "→")
		if !ok {
			t.Fatalf("line %d: missing '→' in %q", i+1, raw)
		}
		key = strings.TrimSpace(key)
		want = strings.TrimSpace(want)
		if c == nil {
			t.Fatalf("line %d: cap= must come first", i+1)
		}
		c.Get(key, func() string { return key })
		if got := render(c); got != want {
			t.Errorf("line %d: %s\n  got:  %s\n  want: %s", i+1, raw, got, want)
		}
	}
}

func render(c *Cache[string, string]) string {
	var b strings.Builder
	b.WriteByte('[')
	for e := c.head; e != nil; e = e.next {
		if e != c.head {
			b.WriteByte(' ')
		}
		b.WriteString(e.key)
		if e.visited {
			b.WriteByte('*')
		}
		if e == c.hand {
			b.WriteByte('^')
		}
	}
	b.WriteByte(']')
	return b.String()
}

func TestTape_basicFill(t *testing.T) {
	runTape(t, `
		cap=3
		get a → [a]
		get b → [b a]
		get c → [c b a]
	`)
}

func TestTape_evictsOldestWhenNoneVisited(t *testing.T) {
	runTape(t, `
		cap=3
		get a → [a]
		get b → [b a]
		get c → [c b a]
		get d → [d c b^]   # evicts a; hand parks on b
		get e → [e d c^]   # evicts b; hand drifts to c
	`)
}

func TestTape_evictionSkipsVisited(t *testing.T) {
	runTape(t, `
		cap=3
		get a → [a]
		get b → [b a]
		get c → [c b a]
		get a → [c b a*]   # mark a hot
		get d → [d c^ a]   # walk clears a*, evicts b; a is spared
	`)
}

func TestTape_handPersistsAcrossEvictions(t *testing.T) {
	// The defining SIEVE property: the second eviction does NOT restart
	// from the tail; it continues from where the first one left off.
	runTape(t, `
		cap=3
		get a → [a]
		get b → [b a]
		get c → [c b a]
		get a → [c b a*]
		get d → [d c^ a]   # evicts b, hand on c
		get e → [e d^ a]   # if hand reset to tail, a would die; instead c does
	`)
}

func TestTape_handWrapsAtHead(t *testing.T) {
	// All bits set: the walk clears them all on the first pass, wraps from
	// the head back to the (current) tail, and evicts there.
	runTape(t, `
		cap=3
		get a → [a]
		get b → [b a]
		get c → [c b a]
		get a → [c b a*]
		get b → [c b* a*]
		get c → [c* b* a*]
		get d → [d c b^]   # walk clears all, wraps, evicts a; hand on b
	`)
}

func TestTape_hitMarksVisited(t *testing.T) {
	runTape(t, `
		cap=3
		get a → [a]
		get b → [b a]
		get c → [c b a]
		get a → [c b a*]    # hit on tail: bit set, no reorder
		get b → [c b* a*]   # hit on middle
		get c → [c* b* a*]  # hit on head
		get a → [c* b* a*]  # already visited; idempotent
	`)
}

func collect(c *Cache[string, string]) []string {
	var out []string
	for k, v := range c.Items() {
		out = append(out, k+"="+v)
	}
	return out
}

func TestItems_orderHeadToTail(t *testing.T) {
	c := New[string, string](3)
	for _, k := range []string{"a", "b", "c"} {
		c.Get(k, func() string { return strings.ToUpper(k) })
	}
	got := collect(c)
	want := []string{"c=C", "b=B", "a=A"}
	if !slices.Equal(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}
}

func TestItems_breakStops(t *testing.T) {
	c := New[string, string](3)
	for _, k := range []string{"a", "b", "c"} {
		c.Get(k, func() string { return k })
	}
	var got []string
	for k, v := range c.Items() {
		got = append(got, k+"="+v)
		if k == "b" {
			break
		}
	}
	want := []string{"c=c", "b=b"}
	if !slices.Equal(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}
}

func TestItems_emptyCache(t *testing.T) {
	c := New[string, string](3)
	if got := collect(c); got != nil {
		t.Errorf("got %v, want nil", got)
	}
}

func TestNew_panicsOnNonPositive(t *testing.T) {
	for _, n := range []int{0, -1, -1000} {
		t.Run(strconv.Itoa(n), func(t *testing.T) {
			defer func() {
				if recover() == nil {
					t.Errorf("New(%d) did not panic", n)
				}
			}()
			New[int, int](n)
		})
	}
}

func TestGet_loadNotCalledOnHit(t *testing.T) {
	c := New[string, string](2)
	calls := 0
	load := func() string { calls++; return "v" }
	c.Get("a", load)
	c.Get("a", load)
	c.Get("a", load)
	if calls != 1 {
		t.Errorf("load called %d times, want 1", calls)
	}
}

// naiveSIEVE is an obviously-correct reference implementation: a slice of
// keys, a parallel slice of visited bits, and a hand index. Used as an
// oracle in TestOracle.
type naiveSIEVE struct {
	max     int
	keys    []string
	vals    []string
	visited []bool
	hand    int // -1 when empty
}

func newNaive(n int) *naiveSIEVE { return &naiveSIEVE{max: n, hand: -1} }

func (s *naiveSIEVE) get(k string, load func() string) {
	for i, kk := range s.keys {
		if kk == k {
			s.visited[i] = true
			return
		}
	}
	v := load()
	if len(s.keys) == s.max {
		h := s.hand
		if h == -1 {
			h = 0
		}
		for s.visited[h] {
			s.visited[h] = false
			h++
			if h == len(s.keys) {
				h = 0
			}
		}
		s.keys = slices.Delete(s.keys, h, h+1)
		s.vals = slices.Delete(s.vals, h, h+1)
		s.visited = slices.Delete(s.visited, h, h+1)
		if h >= len(s.keys) {
			h = 0
		}
		if len(s.keys) == 0 {
			h = -1
		}
		s.hand = h
	}
	s.keys = append(s.keys, k)
	s.vals = append(s.vals, v)
	s.visited = append(s.visited, false)
}

func (s *naiveSIEVE) items() []string {
	out := make([]string, 0, len(s.keys))
	for i := len(s.keys) - 1; i >= 0; i-- {
		out = append(out, s.keys[i]+"="+s.vals[i])
	}
	return out
}

func TestOracle(t *testing.T) {
	cases := []struct {
		name                    string
		capacity, keyspace, ops int
		seed1, seed2            uint64
	}{
		{"small_high_churn", 4, 16, 10000, 1, 2},
		{"medium", 32, 64, 10000, 3, 4},
		{"cap_eq_keyspace_no_evict", 16, 16, 5000, 5, 6},
		{"cap_one_pathological", 1, 8, 5000, 7, 8},
		{"cap_huge_relative", 256, 32, 5000, 9, 10},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rng := rand.New(rand.NewPCG(tc.seed1, tc.seed2))
			c := New[string, string](tc.capacity)
			n := newNaive(tc.capacity)
			for i := range tc.ops {
				k := strconv.Itoa(rng.IntN(tc.keyspace))
				load := func() string { return "v" + k }
				c.Get(k, load)
				n.get(k, load)
				if got, want := collect(c), n.items(); !slices.Equal(got, want) {
					t.Fatalf("step %d (key=%s) diverged:\n  got:  %v\n  want: %v", i, k, got, want)
				}
			}
		})
	}
}

func BenchmarkGet_hit(b *testing.B) {
	const cap = 256
	c := New[int, int](cap)
	load := func() int { return 0 }
	for i := range cap {
		c.Get(i, load)
	}
	for b.Loop() {
		c.Get(cap/2, load)
	}
}

func BenchmarkGet_miss(b *testing.B) {
	const cap = 256
	c := New[int, int](cap)
	load := func() int { return 0 }
	for i := range cap {
		c.Get(i, load)
	}
	i := 0
	for b.Loop() {
		c.Get(cap+i, load) // unique key each iter; cache stays full
		i++
	}
}

func BenchmarkGet_zipf(b *testing.B) {
	const cap = 1024
	c := New[int, int](cap)
	rng := mrand.New(mrand.NewSource(1))
	z := mrand.NewZipf(rng, 1.07, 1, 1<<16)
	calls := 0
	load := func() int { calls++; return 0 }
	for range cap {
		c.Get(int(z.Uint64()), load)
	}
	calls = 0
	ops := 0
	for b.Loop() {
		c.Get(int(z.Uint64()), load)
		ops++
	}
	b.ReportMetric(1.0-float64(calls)/float64(ops), "hits/op")
}

func BenchmarkItems_full(b *testing.B) {
	const cap = 1024
	c := New[int, int](cap)
	load := func() int { return 0 }
	for i := range cap {
		c.Get(i, load)
	}
	for b.Loop() {
		for range c.Items() {
		}
	}
}
