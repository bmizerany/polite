// Package pqlite is a PostgreSQL wire-protocol client for streaming workloads
// that need explicit connection control and low abstraction overhead without
// database/sql.
//
// # Purpose
//
// pqlite exposes backend sessions directly so callers can control query,
// COPY, and notification flows with predictable lifecycle behavior.
//
// # Core Types
//
// [Conn] is one backend session. Use it for [Conn.Query], [Conn.Exec],
// [Conn.CopyTo], and [Conn.CopyFrom].
//
// [Pool] shares [Conn] values across goroutines.
//
// [Listener] is a dedicated LISTEN/NOTIFY session. If a notification consumer
// can stall, keep that stream on a dedicated [Conn].
//
// # Auto Prepared Statements
//
// [Conn] automatically prepares SQL statements on first use and caches them
// per connection by exact SQL text. Later calls with the same SQL reuse the
// prepared statement automatically.
//
// This applies to [Conn.Query], [Conn.Exec], [Conn.CopyTo], and
// [Conn.CopyFrom]. Statement caches are per-[Conn] and are not shared across a
// [Pool].
//
// # Read-Only Contexts
//
// [WithReadOnly] marks a context as requiring a read-only backend session.
// It does not run SET commands or change backend state.
//
// When a read-only context is used, statement preparation checks the current
// backend value of default_transaction_read_only. If it is not "on",
// operations fail with [ErrInvalidReadOnlyConn] before query execution.
//
// This guard applies uniformly to [Conn.Query], [Conn.Exec], [Conn.CopyTo],
// and [Conn.CopyFrom].
//
// # Query, COPY, and Row Draining
//
// Query and COPY readers yield iter.Seq2[*Row, error].
//
// [Row] exposes columns as a stream. Use [Row.View] for raw bytes,
// [Row.ParseString] for text values, [Row.ParseInt] for integer encodings,
// and [Row.ParseTime] for timestamp-without-time-zone values.
//
// Unread columns are drained automatically when iteration advances to the next
// row. If iteration stops early (for example break or return), pqlite drains
// the current row and remaining protocol messages before returning the
// connection to ReadyForQuery.
//
// [Row.Close] is available when callers want to force draining immediately and
// observe the close error at that call site.
//
// COPY APIs use PostgreSQL binary format. [Conn.CopyTo] yields binary column
// values. [Conn.CopyFrom] expects binary column values.
//
// Streaming query pattern:
//
//	for row, err := range cn.Query(ctx, "SELECT ...") {
//		if err != nil {
//			break
//		}
//		// consume row values
//	}
//
// # Pool and Listener Behavior
//
// [Pool] waiters are FIFO for predictable handoff. [Pool.Close] wakes blocked
// waiters, and subsequent [Pool.Get] calls fail with [ErrPoolClosed].
//
// For waiter cancellation and contention measurements, see
// wait/CANCELLATION_PERFORMANCE.md.
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
