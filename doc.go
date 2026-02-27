// Package pqlite is a PostgreSQL client for direct wire-protocol access.
//
// pqlite targets streaming workloads that need explicit connection control and
// low abstraction overhead without database/sql.
//
// # Core Model
//
// [Conn] is one backend session. Use it for [Conn.Query], [Conn.Exec],
// [Conn.CopyTo], and [Conn.CopyFrom].
//
// [Pool] shares [Conn] values across goroutines.
//
// [Listener] is a dedicated LISTEN/NOTIFY session. Keep listener streams on
// dedicated connections when consumers can block.
//
// # Core Guarantees
//
// Query and COPY readers yield iter.Seq2[*Row, error].
//
// Each yielded [Row] must be fully consumed before iteration advances. If a
// loop stops early, call [Row.Close] on the current row.
//
// COPY APIs use PostgreSQL binary format. [Conn.CopyTo] yields binary column
// values. [Conn.CopyFrom] expects binary column values.
//
// # Streaming Query Pattern
//
//	for row, err := range cn.Query(ctx, "SELECT ...") {
//		if err != nil {
//			break
//		}
//		// consume row values
//	}
//
// # Exceptions and Safety Defaults
//
// [Conn.CopyFrom] does not send iterator error text to PostgreSQL by default.
// This avoids accidental exposure of frontend details in PostgreSQL-visible
// errors and logs.
//
// Wrap errors with [CopyError] to send a sanitized message through COPY FAIL.
//
// # Pool and Listener Behavior
//
// [Pool] uses FIFO waiters for predictable handoff and no starvation.
//
// Listener streams are consumer-driven. If a consumer can stall, keep that
// stream on a dedicated [Conn].
//
// # Compatibility
//
// pqlite keeps public APIs and documented semantics stable.
//
// Internal protocol handling, buffering strategy, and performance tuning may
// change across releases.
//
// # Parser Helpers
//
// Use [Row.View] for raw bytes, [Row.ParseString] for text values,
// [Row.ParseInt] for integer encodings, and [Row.ParseTime] for
// timestamp-without-time-zone values.
package pqlite
