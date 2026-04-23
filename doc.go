// Package polite is a small, robust PostgreSQL client for Go. The API
// surface is narrow by design: PostgreSQL and its wire protocol are built
// for the heavy lifting, and polite is the Go handle that exposes them. It
// does this by providing scalar row decoders for common PostgreSQL types
// and raw wire bytes for the rest. Custom decoders are plain Go functions
// over those bytes.
//
// The package centers on [Conn]. Dial a connection, call [Conn.Query] to range
// over rows, call [Conn.Exec] for commands whose rows you do not need, and use
// [Pool] when multiple goroutines need to share connections. A [Conn] is not
// safe for concurrent use. A [Pool] is safe and preserves warm per-connection
// state such as prepared statements.
//
// # Queries
//
// Query is the main operation. It executes SQL and yields a sequence of rows
// and errors. Exec is a thin convenience wrapper for commands such as INSERT,
// UPDATE, and DELETE.
//
// polite prepares SQL on first use and caches prepared statements per
// connection by exact SQL text. Reusing the same SQL on the same connection
// reuses the prepared statement as well.
//
// From there, the row API defines how results are read.
//
// # Rows and Columns
//
// A [Row] is a view of the current result row. polite requests every column
// in PostgreSQL's binary format, so [Column] values arrive as raw wire bytes
// rather than decimal strings. Each Column reports its name, width,
// PostgreSQL type OID, and those bytes. [Column.DecodeInt],
// [Column.DecodeFloat], and [Column.DecodeTime] read the binary format
// directly; for types without a built-in decoder, call [Column.Bytes] and
// decode the wire bytes yourself. Raw column bytes refer to storage owned by
// the current row and remain valid only until the next row is determined.
//
// Rows reuse storage across iterations. In steady-state query paths, that lets
// polite avoid per-row allocation. The same column metadata also makes it
// straightforward to fill Go structs by matching column names in a single
// pass, without reflection or per-row lookup tables.
//
// # COPY
//
// Query supports COPY TO and COPY FROM in PostgreSQL's binary
// format. See [Column] for details on reading that format and
// [Conn.Query] for examples.
//
// # Errors
//
// *[febe.Error] is the decoded form of a PostgreSQL ErrorResponse.
// [Conn.Query] and [Conn.Exec] surface one whenever the backend replies
// with an error. An *Error carries the SQLSTATE Code, Message, Severity,
// Detail, and Hint, along with Schema, Table, Column, DataType, and
// Constraint when the backend attaches them.
//
// To match a specific code, use [errors.Is] with [febe.ErrorCode]:
//
//	if errors.Is(err, febe.ErrorCode("23505")) {
//	    // unique violation
//	}
//
// Use [febe.Error.IsFatal] to tell a recoverable backend error — one where
// a ReadyForQuery will follow — from a FATAL or PANIC that has closed the
// connection.
//
// polite's own `Err*` sentinels, such as [ErrBusy], are produced
// by polite or [Pool] when they detect a local condition — not decoded
// from a backend message.
//
// # Pooling
//
// A [Conn] is intentionally single-user. Use [Pool] when callers need shared
// access. The zero value of Pool is ready to use. Waiters are served in FIFO
// order, and the most recently returned connection is handed out first so
// prepared statements and other per-connection state stay warm.
//
// # Testing and Time
//
// When a [Config]'s Now function is set, unqualified SQL calls to now(),
// clock_timestamp(), statement_timestamp(), and transaction_timestamp() use
// the returned clock on that connection. If Now is nil, polite leaves
// PostgreSQL's default time behavior unchanged. This is useful in tests,
// including tests run under testing/synctest, because SQL that calls those
// functions can observe the same clock as the Go test.
package polite
