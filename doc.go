// Package pqlite implements PostgreSQL Conn and Pool types with a modern Go API.
//
// Use the [Conn] API for direct session control and streaming access to query
// results, COPY, and notifications.
//
// Use the [Pool] API for sharing a pool of connections across concurrent
// workloads with strong fairness guarantees. See its documentation for more
// details.
//
// Use the [Listener] API for PostgreSQL LISTEN/NOTIFY support over a dedicated connection.
//
// # Performance
//
// Pqlite is designed for high performance and low latency. It minimizes
// allocations, avoids unnecessary buffering, and uses efficient data
// structures and algorithms for protocol handling. It is suitable for
// high-throughput, high-concurrency applications that want low-latency query
// execution. Performance may vary based on workload and environment, so
// benchmarking with realistic scenarios is recommended.
//
// # Prepared Statement Caching
//
// [Conn] automatically prepares SQL statements on first use and caches them
// per connection by exact SQL text. Later calls with the same SQL reuse the
// prepared statement automatically.
//
// This applies to [Conn.Query], [Conn.Exec], [Conn.CopyTo], and
// [Conn.CopyFrom].
//
// # Fairness and Concurrency
//
// [Pool] provides strong fairness guarantees for concurrent workloads to
// prevent starving clients and ensure predictable performance. When multiple
// goroutines call [Pool.Get] concurrently, they are served in FIFO order. Each
// gets the most recently returned connection from the pool, so they benefit
// from any warm-up or caching effects on that connection.
//
// # Contexts and Cancellation
//
// All [Conn] and [Pool] APIs that interact with PostgreSQL accept a context for
// cancellation and timeouts.
//
// A context cancellation or timeout during an inflight operation with
// PostgreSQL causes a [Conn] to send a cancel request to the backend and
// return an error that wraps [context.Canceled] or [context.DeadlineExceeded].
// The connection is returned to ReadyForQuery and can be reused for subsequent
// operations.
//
// A context cancellation or timeout while waiting for a connection from a
// [Pool] causes the [Pool.Get] call to return an error that wraps
// [context.Canceled] or [context.DeadlineExceeded]. No cancel request is sent
// to PostgreSQL since no operation has started on a connection yet. The
// cancellation does not affect any inflight connection attempt that may have
// started as a result of the [Pool.Get] call, so that the next caller to
// [Pool.Get] may still receive a connection if the attempt succeeds, avoid
// having to start and wait for another connection attempt.
//
// # Read-Only Connection Guards
//
// The [WithReadOnly] function returns a copy of a context that marks any
// connection obtained from a call to [Pool.Get] with that context as configured for
// read-only access. This is implemented by setting `default_transaction_read_only=on`
// on the connection before returning it from [Pool.Get], and resetting it to
// `default_transaction_read_only=off` when the connection is returned to the pool.
//
// This provides a guard against accidental writes to the database by ensuring any
// connection obtained from a call to [Pool.Get] is configured for read-only
// access, and preventing the use of any [Conn] without `default_transaction_read_only=on` set.
// This is useful for enforcing read-only access in parts of an application,
// such as HTTP request handlers for GET requests:
//
//	func readOnlyMiddleware(next http.Handler) http.Handler {
//		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
//			if r.Method == "GET" {
//				// Downstream uses of the request context with a [Pool.Get] will
//				// yield a read-only connection, preventing
//				// accidental side-effects on the database from
//				// GET requests.
//				r = r.WithContext(pqlite.WithReadOnly(r.Context()))
//			}
//
//			next.ServeHTTP(w, r)
//		})
//	}
//
// # Safety and Compatibility
//
// [Conn.CopyFrom] does not send iterator error text to PostgreSQL by default.
// This avoids accidental exposure of frontend details in PostgreSQL-visible
// errors and logs. Wrap errors with [CopyError] to send a sanitized message
// through COPY FAIL.
//
// pqlite keeps public APIs and documented semantics stable. Internal protocol
// handling, buffering strategy, and performance tuning may change across
// releases.
package pqlite
