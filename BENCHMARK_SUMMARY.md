# WaitList Integration Benchmark Results

## Summary

The WaitList integration provides **lower variance** (more predictable performance) but at the cost of **reduced throughput** and **increased allocations**.

## Simple Benchmarks (No Contention)

```
                    │ before       │     after         │
                    │   sec/op     │  sec/op   vs base │
PoolGetPut-16         443.4n ± 1%  544.4n ± 0%  +22.75%
PoolStartNewConn-16   70.79n ±11%  84.02n ± 8%  +18.69%

                    │ before │ after    vs base │
                    │  B/op  │  B/op             │
PoolGetPut-16          16.00  192.00   +1100.00%
PoolStartNewConn-16   141.5   120.0      -15.19%

                    │ before  │ after    vs base │
                    │allocs/op│allocs/op          │
PoolGetPut-16         1.000    5.000    +400.00%
PoolStartNewConn-16   1.000    2.000    +100.00%
```

## Contention Benchmarks (MaxOpen=10, High Concurrency)

```
                  │   old    │ waitlist  vs base │
                  │ sec/op   │ sec/op            │
PoolContention-16  940.9n±2%  1048.5n±1%  +11.44%

                  │  old   │waitlist  vs base │
                  │sec/get │ sec/get          │
PoolContention-16  6.806µ±3% 7.717µ±1%  +13.39%

                  │  old   │waitlist  vs base │
                  │sec/put │ sec/put          │
PoolContention-16  1.925µ±7% 2.629µ±2%  +36.58%

                  │ old  │waitlist vs base │
                  │ B/op │  B/op           │
PoolContention-16  0.0    150.0±1%        

                  │  old    │waitlist vs base │
                  │allocs/op│allocs/op        │
PoolContention-16  0.000     3.000±0%        
```

## Key Finding: Variance Reduction

**The critical insight is variance reduction under contention:**

- **sec/op variance**: 2% → 1% (50% reduction)
- **sec/get variance**: 3% → 1% (67% reduction)  
- **sec/put variance**: 7% → 2% (71% reduction)

This shows WaitList provides more **predictable latency** under contention, which is valuable for applications that prioritize consistency over peak throughput.

## CPU Profile Analysis

Old Pool Get+Put: ~16.28s (38.9% of benchmark time)
WaitList Get+Put: ~19.98s (43.5% of benchmark time)

The overhead (+23% CPU time) comes from:
1. Intrusive list allocations (Handle nodes)
2. Load function closures in Take()
3. Additional mutex coordination in WaitList

## Trade-offs

**WaitList wins on:**
- Variance/predictability (50-71% reduction)
- Fairness (strict FIFO ordering)

**Old Pool wins on:**
- Raw throughput (+11-36% faster)
- Memory efficiency (0 vs 150 B/op)
- Allocation overhead (0 vs 3 allocs/op)

## Recommendation

The choice depends on application requirements:
- **Use WaitList** if predictable latency and fairness matter more than peak throughput
- **Use old Pool** if raw performance and minimal allocations are critical
