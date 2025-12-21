# Plan: Simplify wait.List API

## Goal
Remove `ShouldLoad()`, `CancelLoad()`, and `Wait()` from the public API. Pool should only use the core `wait.List` interface: `Take()`, `TryTake()`, `Put()`, and `Close()`.

## Current Problem

Pool.Get() currently does this dance:
```go
// Fast path
cn, ok := p.waitlist.TryTake()
if ok {
    return cn, nil
}

// Slow path - manual orchestration
if p.waitlist.ShouldLoad() {        // ← exposed helper
    go p.openAndPut()                 // ← spawns goroutine manually
}
cn, err := p.waitlist.Wait(ctx)       // ← exposed helper

// In openAndPut():
cn, err := p.open(p.cancelCtx)
if err == nil {
    p.waitlist.Put(cn)
    return
}
p.waitlist.CancelLoad()               // ← exposed helper
```

This exposes internal orchestration details (ShouldLoad/CancelLoad/Wait) that should be hidden.

## Solution

### 1. Change Take's Load Function Signature

**Current:**
```go
func (p *List[T]) Take(ctx context.Context, load func() T) (T, error)
```

**New:**
```go
func (p *List[T]) Take(ctx context.Context, load func() (T, error)) (T, error)
```

### 2. Update Take's Internal Handling

Take's goroutine currently:
```go
go func() {
    defer func() { /* panic handling */ }()
    ready := load()      // always calls Put
    p.Put(ready)
}()
```

Change to:
```go
go func() {
    defer func() { /* panic handling */ }()
    ready, err := load()
    if err != nil {
        p.cancelLoad()   // internal method - don't expose
        return
    }
    p.Put(ready)
}()
```

### 3. Make Internal Methods Private

- `ShouldLoad()` → `shouldLoad()` (internal only)
- `CancelLoad()` → `cancelLoad()` (internal only)
- `Wait()` → remove entirely (not needed - use Take)

### 4. Simplify Pool.Get()

**Current (complex):**
```go
func (p *Pool) Get(ctx context.Context) (*Conn, error) {
    // Early checks...

    cn, ok := p.waitlist.TryTake()
    if ok {
        return cn, nil
    }

    // Manual orchestration
    if p.waitlist.ShouldLoad() {
        go p.openAndPut()
    }
    p.waitlist.TestHookWaiterCanceled = p.testHookWaitCanceled
    cn, err := p.waitlist.Wait(ctx)

    // Error mapping...
}
```

**New (simple):**
```go
func (p *Pool) Get(ctx context.Context) (*Conn, error) {
    // Early checks...

    cn, err := p.waitlist.Take(ctx, p.openConn)

    // Error mapping...
}
```

### 5. Create openConn Method

Pool needs a method that matches the new signature:
```go
func (p *Pool) openConn() (*Conn, error) {
    for i := range backoff.Loop(p.cancelCtx, 5*time.Second) {
        cn, err := p.open(p.cancelCtx)
        if err == nil {
            cn.created = time.Now()
            return cn, nil
        }
        if p.cancelCtx.Err() != nil {
            return nil, p.cancelCtx.Err()
        }
        if i > 3 {
            p.log().Info("pqlite: pool: error opening new connection; retrying",
                "err", cmp.Or(context.Cause(p.cancelCtx), err))
        }
    }
    return nil, errors.New("failed to open connection after retries")
}
```

## Changes Required

### wait/list.go
1. Change Take signature: `load func() (T, error)`
2. Update Take's goroutine to handle error from load
3. Rename ShouldLoad → shouldLoad (unexport)
4. Rename CancelLoad → cancelLoad (unexport)
5. Remove Wait method entirely

### pool.go
1. Remove openAndPut() method
2. Remove loadConn() method
3. Add openConn() method with new signature
4. Simplify Get() to just call Take()
5. Remove Wait/ShouldLoad/CancelLoad calls

### wait/list_test.go
1. Update all Take() calls to use `func() (T, error)` signature
2. Update load functions to return (value, nil) for success
3. Can test error handling by returning errors from load

## Benefits

✅ **Simpler API** - Only 4 public methods: Take, TryTake, Put, Close
✅ **Better encapsulation** - Internal orchestration hidden
✅ **Cleaner Pool.Get()** - One call to Take instead of manual coordination
✅ **Error handling** - Load function can return errors naturally
✅ **Same performance** - No additional allocations

## Migration Path

1. Update wait.List implementation first
2. Update wait.List tests
3. Update Pool to use new API
4. Update Pool tests
5. Verify zero allocations maintained

## Test Coverage Needed

- Load function returns error → waiter gets item from another loader
- Load function returns success → waiter gets item
- Load function panics → loads decremented, panic propagated
- Multiple loaders racing with errors
- Close during loading

## Expected Final API

```go
package wait

type List[T any] struct {
    MaxItems   int
    MaxWaiters int
    // ... internal fields
}

// Public API (only these 4 methods)
func (p *List[T]) Take(ctx context.Context, load func() (T, error)) (T, error)
func (p *List[T]) TryTake() (T, bool)
func (p *List[T]) Put(v T)
func (p *List[T]) Close()

// Exported for testing hooks only
var (
    ErrMaxWaiters = errors.New("too many waiters")
    ErrClosed     = errors.New("closed")
)
```
