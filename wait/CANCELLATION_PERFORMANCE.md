# Cancellation Performance Analysis

## Question
Does the O(n) `DeleteFunc` in `handleCancel` cause contention under high waiter load with frequent cancellations?

## Scenario
- MaxWaiters reached (e.g., 1000 waiters blocked)
- High cancellation rate (90%+ waiters timing out)
- 1:100 ratio of items to waiters

## Theoretical Concern
Each cancellation calls `DeleteFunc` which scans the waiters queue O(n):
```go
p.waitersMu.Lock()
p.waiters.DeleteFunc(func(wch chan T) bool {
    return ch == wch  // O(n) scan
})
p.waitersMu.Unlock()
```

With 1000 concurrent cancellations:
- Worst case: O(n²) total work if all scan full queue
- Lock serializes access, amplifying contention

## Measured Results

### Scaling Test (concurrent cancellations)
```
100 waiters:  7434 ns/cancel
500 waiters:  3327 ns/cancel
1000 waiters: 2081 ns/cancel
2000 waiters: 1661 ns/cancel
```

**Result:** ns/cancel *decreases* with more waiters (opposite of O(n²))

### CPU Profiling (1000 waiters, 90% cancellation)
```
Total runtime: 30.79s
Time in DeleteFunc: 40ms (0.13%)
Time in mutex lock/unlock: 18s (58%)
```

**Result:** DeleteFunc is <1% of CPU time, mutex contention is 58%

### Breakdown of handleCancel
```
p.waitersMu.Lock():        470ms  (24%)  - acquiring mutex
p.waiters.DeleteFunc():     40ms  (2%)   - O(n) scan
p.waitersMu.Unlock():     1430ms  (73%)  - releasing + waking waiters
```

**Result:** Lock contention is 35x more expensive than the O(n) scan

## Why This Works

1. **Natural desynchronization**: Context timeouts don't all fire simultaneously due to timer resolution and scheduler variance

2. **Queue shrinks**: By the time goroutine N acquires the lock, goroutines 1..N-1 have already removed themselves

3. **Cache-friendly scan**: Sequential slice scan is extremely fast (~40ms for 1000 items)

4. **Lock is the bottleneck**: Mutex contention dominates over scan cost

## Conclusion

**The O(n) DeleteFunc is NOT a performance problem in practice.**

- Real bottleneck: mutex contention (58% of time)
- DeleteFunc cost: 0.13% of time
- Scales linearly (or better) due to natural cancellation spread

## Future Considerations

If we see DeleteFunc becoming a bottleneck in production:
- Option 1: Use intrusive linked list (O(1) removal, but allocates Handle)
- Option 2: Mark-and-sweep (periodic cleanup instead of immediate removal)
- Option 3: Accept it (current cost is negligible)

Current recommendation: **No optimization needed.** Profile in production first.
