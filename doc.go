// Package polite provides PostgreSQL client primitives for Go.
//
// polite is not a database/sql driver and does not implement
// database/sql/driver.
//
// [Conn] represents one PostgreSQL session for queries, COPY, and
// LISTEN/NOTIFY.
//
// [Pool] manages reusable [Conn] values for concurrent callers.
//
// [Listener] provides LISTEN/NOTIFY over a dedicated connection.
//
// # Concurrency
//
// [Conn] is not safe for concurrent use by multiple goroutines.
//
// [Pool] is safe for concurrent use by multiple goroutines.
//
// # Contexts
//
// [Conn] and [Pool] methods that interact with PostgreSQL accept
// [context.Context].
//
// If a [Conn] operation is canceled in flight, Conn sends a cancel request to
// PostgreSQL. The returned error wraps [context.Canceled] or
// [context.DeadlineExceeded].
//
// If [Pool.Get] is canceled while waiting for a connection, it returns an
// error that wraps [context.Canceled] or [context.DeadlineExceeded]. No cancel
// request is sent to PostgreSQL because no operation has started.
//
// # Row Iteration
//
// [Conn.Query] returns [iter.Seq2[*Row, error]]. Each iteration is either
// (row, nil) or (nil, err). A non-nil err ends iteration.
//
// # Row Draining
//
// [Row] values are forward-only. Read columns in order with [Row.View] or
// [Row.ParseInt], [Row.ParseTime], and [Row.ParseString].
//
// Unread columns are drained automatically when iteration advances or ends.
//
// If iteration stops early, Query drains unread row data and backend messages
// until ReadyForQuery before the connection is reused.
//
// Call [Row.Close] to force draining of unread columns immediately and observe
// drain errors at that call site.
//
// # COPY TO
//
// Use [Conn.Query] with a COPY ... TO STDOUT statement:
//
//	// Error handling elided for brevity.
//	for row, err := range cn.Query(ctx, "COPY foo TO STDOUT WITH (FORMAT binary)") {
//		id, ... := row.ParseInt()
//		name, ... := row.ParseString()
//		fmt.Println(id, name)
//	}
//
// # COPY FROM
//
// Use [Conn.Query] to start a COPY ... FROM STDIN, then [Conn.Exec] with
// an empty sql string to send each data row:
//
//	for row, err := range cn.Query(ctx, "COPY foo FROM STDIN WITH (FORMAT binary)") {
//		if err != nil { return err }
//		// row.View() returns ErrCopyFrom to signal COPY FROM mode.
//		for _, item := range items {
//			if err := cn.Exec(ctx, "", item.ID, item.Name); err != nil {
//				return err
//			}
//		}
//	}
//
// The iterator handles the COPY protocol: it sends the binary trailer,
// CopyDone, and reads CommandComplete + ReadyForQuery when the loop exits.
//
// The public API and documented semantics are stable across minor releases.
// Internal protocol handling, buffering strategy, and performance tuning may
// change across releases.
package polite
