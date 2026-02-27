// Package pqlite is a small PostgreSQL client that works directly at the
// protocol level, without database/sql.
//
// # Performance and Streaming
//
// APIs are designed for low-allocation operation and immediate row processing.
// [Conn.Query] returns rows as an iterator sequence, so callers can consume
// results as they arrive instead of buffering all rows first.
//
// Typical query flow:
//
//	for row, err := range cn.Query(ctx, "SELECT ...") {
//		if err != nil {
//			// handle error
//			break
//		}
//		// read row values
//	}
//
// # Listener and Pool Connection Types
//
// [Conn] represents a single backend session for executing [Conn.Query],
// [Conn.Exec], and COPY operations such as [Conn.CopyTo] and [Conn.CopyFrom].
// [Pool] shares connections across goroutines for general database work.
// [Listener] is a separate, dedicated LISTEN/NOTIFY connection. Sharing a
// listener connection through [Pool] can break fairness after checkout: one
// client may receive a connection first, then wedge because its
// [Listener.Notifications] consumer is slow (which [Pool] cannot observe), while
// clients that were queued later receive connections without slow consumers and
// complete first. Internal buffering would still need a cap and eventually
// reintroduce the same contention, so buffering is left to callers that need it
// and pool fairness is preserved.
//
// # Pool Design
//
// [Pool] is implemented around an explicit waiter queue and ready queue.
// Waiters are enqueued FIFO, which keeps hand-off latency predictable and avoids
// starving earlier callers. Connections are returned with [Pool.Put], so active
// sessions are reused and short bursts of traffic can avoid repeated
// authentication and startup.
//
// # Iterators and Iteration Control
//
// Query and COPY APIs use a two-value iterator sequence:
// first value is a row ([Row]), second is an error.
// This shape supports testing by feeding a sequence and collecting concrete
// values.
//
//	for row, err := range cn.Query(ctx, "SELECT 1, 'two'") {
//		if err != nil {
//			t.Fatal(err)
//		}
//		gotInt := must(row.ParseInt())
//		gotString := must(row.ParseString())
//		_ = gotInt
//		_ = gotString
//	}
//
// If you break a range loop over [Conn.Query] or [Conn.CopyTo], the iterator
// closes the active [Row] before it returns.
//
// # Transactions and One-Shot Batch Execution
//
// A single connection can send a batch of statements to the backend in one
// round-trip. SQL is powerful enough to combine multiple operations when
// appropriate, which is often better than opening separate execution cycles for
// each statement. The connection-level helpers ([Conn.Exec] and [Conn.Query])
// are intentionally simple so callers can group work while keeping control in
// their own SQL.
//
// # API Design
//
// The package intentionally stays close to PostgreSQL capabilities:
// small, composable primitives; explicit iteration control; and no heavy
// abstraction layer. The goal is a straightforward API that is ergonomic for
// common use and flexible enough for advanced query patterns.
//
// # Parser Helpers
//
// Row parsers are intentionally minimal and fast:
//
//	for row, err := range cn.Query(ctx, "SELECT 1, 2::bigint, now()::timestamp") {
//		if err != nil {
//			t.Fatal(err)
//		}
//		id, _ := row.ParseInt()
//		count, _ := row.ParseInt()
//		ts, _ := row.ParseTime()
//		_ = id
//		_ = count
//		_ = ts
//	}
//
// Use [Row.View] when you need raw bytes, [Row.ParseString] for text-like
// values, [Row.ParseInt] for integer encodings, and [Row.ParseTime] for
// PostgreSQL timestamp-without-timezone values.
package pqlite
