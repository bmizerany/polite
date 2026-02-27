# Waitlist Cancellation Performance (Pool Waiters)

This note explains the waiter-cancellation path behind `Pool` handoff
semantics documented in `doc.go`. It is intended for maintainers evaluating
whether `wait.List.handleCancel` needs optimization.

## Scenario

- Waiter queue is saturated (`MaxWaiters` reached, e.g. 1000 blocked waiters).
- Cancellation rate is high (about 90% of waiters time out).
- Item-to-waiter ratio is low (about 1:100).
- Concern: `DeleteFunc` in `handleCancel` is O(n) per cancellation.

```go
p.waitersMu.Lock()
p.waiters.DeleteFunc(func(wch chan T) bool {
	return ch == wch // O(n) scan
})
p.waitersMu.Unlock()
```

## Measurement Setup

- Scaling stress: concurrent waiter cancellations at queue sizes 100, 500,
  1000, and 2000.
- CPU profile sample: 1000 waiters with about 90% cancellation.
- Benchmark/test sources:
  `wait/list_cancellation_bench_test.go` and
  `wait/list_cancellation_stress_test.go`.

## Observed Results

### Scaling Stress (ns/cancel)

```text
100 waiters:  7434 ns/cancel
500 waiters:  3327 ns/cancel
1000 waiters: 2081 ns/cancel
2000 waiters: 1661 ns/cancel
```

### CPU Profile (1000 waiters, about 90% cancellation)

```text
Total runtime: 30.79s
Time in DeleteFunc: 40ms (0.13%)
Time in mutex lock/unlock: 18s (58%)
```

### `handleCancel` Breakdown

```text
p.waitersMu.Lock():        470ms  (24%)  - acquiring mutex
p.waiters.DeleteFunc():     40ms  (2%)   - O(n) scan
p.waitersMu.Unlock():     1430ms  (73%)  - releasing + waking waiters
```

## Interpretation

- The measured `ns/cancel` decreases as waiter count increases in this setup,
  which does not match a practical O(n^2) collapse.
- `DeleteFunc` is a small share of runtime relative to mutex contention.
- Cancellation timing naturally desynchronizes and the queue shrinks while
  contenders serialize on the same mutex.
- In this workload, lock contention dominates scan cost.

## Conclusion

For the measured workloads, `DeleteFunc` in `handleCancel` is not the
performance bottleneck. Current behavior is acceptable, and no immediate
optimization is required.

## Future Considerations

If production profiles show `DeleteFunc` becoming material:

- Option 1: intrusive linked list (O(1) removal, adds handle allocations).
- Option 2: mark-and-sweep cancellation cleanup.
- Option 3: keep current design if profile impact remains negligible.

Default stance: profile in production first, then optimize only if measured
cost justifies additional complexity.
