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
// # Query and COPY Contract
//
// Query and COPY readers yield iter.Seq2[*Row, error].
//
// Each yielded [Row] must be fully consumed before iteration advances. If a
// loop stops early, call [Row.Close] on the current row.
//
// COPY APIs use PostgreSQL binary format. [Conn.CopyTo] yields binary column
// values. [Conn.CopyFrom] expects binary column values.
//
// Use [Row.View] for raw bytes, [Row.ParseString] for text values,
// [Row.ParseInt] for integer encodings, and [Row.ParseTime] for
// timestamp-without-time-zone values.
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
