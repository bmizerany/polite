// Package polite provides a small PostgreSQL client for Go built around
// binary queries, row views, and a narrow API surface.
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
// A [Row] is a view of the current result row. Each [Column] reports its name,
// width, PostgreSQL type OID, and raw bytes. [Column.DecodeInt],
// [Column.DecodeFloat], and [Column.DecodeTime] decode common PostgreSQL
// binary values directly. Raw column bytes refer to storage owned by the
// current row and remain valid only until the next row is determined.
//
// Rows reuse storage across iterations. In steady-state query paths, that lets
// polite avoid per-row allocation. The same column metadata also makes it
// straightforward to fill Go structs by matching column names in a single
// pass, without reflection or per-row lookup tables.
//
// # Binary Format
//
// polite uses PostgreSQL's extended query path and requests result columns in
// binary format. Arguments are encoded in PostgreSQL wire form when possible.
// That keeps the hot path short and predictable: integers, floats, timestamps,
// byte slices, and strings can move between Go and PostgreSQL without text
// scanning. Unsigned values that do not fit in signed 64 bits are sent as text
// NUMERIC when needed.
//
// Binary results lead naturally to bulk transfer.
//
// # COPY
//
// Query supports COPY TO and COPY FROM in PostgreSQL's binary format. COPY TO
// yields rows. COPY FROM reports [ErrCopyFrom] to enter copy mode; while copy
// mode is active, calling [Conn.Exec] with an empty SQL string sends one COPY
// data row encoded from the arguments. When the range ends, polite finalizes
// the COPY and records the command tag for [Conn.LastResult].
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
// Setting [Config.Now] makes unqualified calls to now(), clock_timestamp(),
// statement_timestamp(), and transaction_timestamp() use the returned clock on
// that connection. If it is nil, polite leaves PostgreSQL's default time
// behavior unchanged. This is useful in tests, including tests run under
// testing/synctest, because SQL that calls those functions can observe the
// same clock as the Go test.
package polite
