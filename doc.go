// Package polite provides PostgreSQL client primitives for Go.
//
// polite is not a database/sql driver and does not implement
// database/sql/driver. It exposes three types — [Conn], [Pool], and [Row] —
// that cover connections, queries, COPY, and pooling directly against the
// PostgreSQL wire protocol.
//
// # Opening Connections
//
// [Dial] dials a PostgreSQL server, performs startup and authentication, and
// waits for ReadyForQuery before returning a [Conn]. The dsnOrURL parameter
// accepts DSN key=value pairs or a postgres:// URL; see the [dsn] subpackage
// for the full grammar. [Dialer] wraps Dial into a func suitable for
// [Pool.Dial], so that the pool can create connections on demand.
//
// # Queries and Results
//
// [Conn.Query] returns [iter.Seq][*Row]. Each iteration yields a [Row].
// Call [Row.View] to retrieve the first value and observe any error for that
// iteration.
//
//	for row := range cn.Query(ctx, "SELECT id, name FROM users WHERE active") {
//		if _, err := row.View(); err != nil { return err }
//		id, _ := row.ParseInt()
//		name, _ := row.ParseString()
//		fmt.Println(id, name)
//	}
//
// [Conn.Exec] executes a command that returns no rows, such as an INSERT,
// UPDATE, or DELETE. Call [Conn.LastResult] afterward for the number of rows
// affected.
//
// # Prepared Statement Cache
//
// Statements are prepared on first use and cached per connection by exact SQL
// text. Subsequent calls with the same SQL skip the Parse and Describe round
// trips, avoiding re-parsing and re-planning on every execution. The cache is
// per-connection because prepared statements in PostgreSQL are scoped to the
// session.
//
// # Row Access
//
// Rows are forward-only because they read directly from the connection's wire
// buffer — there is no intermediate materialization. [Row.View] returns the
// raw bytes for the next column and advances the cursor; the caller decides
// how to interpret the data. [Row.ParseInt], [Row.ParseString], and
// [Row.ParseTime] are convenience methods built on View for common types.
// [Row.NumColumns] reports how many columns the row contains.
//
// # Row Draining
//
// The PostgreSQL wire protocol requires the client to consume all row data
// before the connection returns to ReadyForQuery. The iterator handles this
// automatically: when iteration advances past a row, any unread columns are
// drained, and when iteration stops early (via break or return), the iterator
// drains remaining rows and backend messages until ReadyForQuery so the
// connection can be reused.
//
// Call [Row.Close] to force draining of unread columns immediately and
// observe drain errors at that call site.
//
// # Bulk Import/Export (COPY FROM/TO)
//
// Bulk operations use PostgreSQL's COPY protocol. There are no special
// helpers — callers write standard COPY SQL and use [Conn.Query] and
// [Conn.Exec] directly, because the wire protocol's COPY messages map
// naturally onto the same iteration pattern used for ordinary queries.
//
// To export rows in bulk, pass a COPY ... TO STDOUT statement to Query.
// Each iteration yields a row whose columns can be read the same way as a
// SELECT result:
//
//	for row := range cn.Query(ctx, "COPY foo TO STDOUT WITH (FORMAT binary)") {
//		if _, err := row.View(); err != nil { return err }
//		id, _ := row.ParseInt()
//		name, _ := row.ParseString()
//		fmt.Println(id, name)
//	}
//
// To import rows in bulk, pass a COPY ... FROM STDIN statement to Query to
// begin the COPY, then call Exec with an empty SQL string to send each data
// row. The iterator yields a single sentinel row whose [Row.View] returns
// [ErrCopyFrom] to signal that the connection is in COPY FROM mode:
//
//	for row := range cn.Query(ctx, "COPY foo FROM STDIN WITH (FORMAT binary)") {
//		if _, err := row.View(); err != nil { return err }
//		// row.View() returns ErrCopyFrom to signal COPY FROM mode.
//		for _, item := range items {
//			if err := cn.Exec(ctx, "", item.ID, item.Name); err != nil {
//				return err
//			}
//		}
//	}
//
// When the loop exits, the iterator sends the binary trailer and CopyDone,
// then reads CommandComplete and ReadyForQuery. COPY requires FORMAT binary.
//
// # Errors
//
// PostgreSQL errors arrive as [*febe.Error], which embeds [*febe.Notice].
// The structured fields — Code, Constraint, Detail, Hint, Schema, Table, and
// others — are exactly what PostgreSQL sends in an ErrorResponse message.
// Use [ConstraintViolated] for a concise unique-violation check:
//
//	if ConstraintViolated(err, "users_email_key") {
//		// handle duplicate
//	}
//
// After a non-fatal error, the connection discards backend messages until it
// reaches ReadyForQuery, then returns to a usable state. Fatal and Panic
// errors mean the backend has closed the connection; no ReadyForQuery will
// follow and the [Conn] cannot be reused.
//
// # Connection Pool
//
// [Pool] manages a set of [Conn] values for concurrent callers. The zero
// value is ready to use.
//
// Waiters are served in FIFO order to prevent starvation, and each waiter
// receives the most recently returned connection so that per-connection state
// stays warm. In particular, the prepared statement cache is per-connection, so
// reusing the same connection avoids re-preparing statements that have already
// been cached. When [Pool.Dial] fails, the pool retries with exponential
// backoff and jitter rather than returning an error to a single caller.
//
// # Concurrency
//
// A [Conn] corresponds to one PostgreSQL backend process communicating over a
// single connection. Sending multiple queries concurrently on one Conn would
// interleave wire-protocol messages, corrupting the session. Conn is therefore
// not safe for concurrent use by multiple goroutines.
//
// [Pool] is safe for concurrent use. It coordinates access so that each caller
// receives its own Conn for the duration of its work.
//
// # Contexts
//
// Canceling a [Conn] operation sends a cancel request to PostgreSQL because
// there is an in-flight query that must be interrupted on the server side.
// The returned error wraps [context.Canceled] or [context.DeadlineExceeded],
// and the connection recovers to ReadyForQuery so it can be reused.
//
// Canceling a [Pool.Get] while waiting for a connection removes the caller
// from the wait queue. No cancel request is sent to PostgreSQL because no
// connection has been acquired yet and there is nothing to cancel on the
// server.
//
// # Tracing and Notifications
//
// [febe.WithTrace] attaches a [febe.Trace] to a context. The trace callbacks
// fire for message headers, notices, errors, notifications, and parameter
// status changes as they are decoded from the wire. Rather than a dedicated
// listener type, LISTEN/NOTIFY notifications arrive through the
// [febe.Trace.ReceivedNotification] callback on whichever connection happens
// to receive them. This keeps the API small: any Conn carrying a traced
// context will deliver notifications without additional setup.
//
// # Deterministic Time
//
// Setting [Config].UseGoTime overrides PostgreSQL's now() and related
// timestamp functions with Go's [time.Now]. It works by installing SQL
// functions that read from a per-session configuration variable, so queries
// that call now() return the value of Go's clock at the start of each
// operation. This means test clocks — including [testing/synctest] — control
// database timestamps, making time-dependent queries deterministic in tests.
//
// The public API and documented semantics are stable across minor releases.
// Internal protocol handling, buffering strategy, and performance tuning may
// change across releases.
package polite
