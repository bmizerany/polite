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
// Polite prepares SQL on first use and caches prepared statements per
// connection by exact SQL text. Reusing the same SQL on the same connection
// reuses the prepared statement as well.
//
// # Arguments
//
// Query and Exec bind variadic arguments to placeholders ($1, $2, ...) in
// the SQL. Polite encodes arguments in PostgreSQL's binary wire format;
// the format is chosen at prepare time to match the placeholder type.
//
// The encoding rules are:
//
//   - nil, and a nil []byte, become NULL.
//   - []byte and string pass through as raw parameter bytes; the binary
//     format for text and bytea is the bytes themselves.
//   - Fixed-width integers and floats use their natural width: 2 bytes
//     for int8, int16, and uint8; 4 bytes for int32, uint16, and float32;
//     8 bytes for int, int64, uint32, and float64. PostgreSQL widens
//     narrower integers to fit the column type.
//   - uint and uint64 encode as int8 when they fit a signed 64-bit
//     value; values that overflow encode as NUMERIC text.
//   - time.Time encodes as a microsecond-precision timestamp or
//     timestamptz; any other placeholder type returns an error, because
//     polite will not invent a destination type for a Go time.
//
// Every other Go type, including bool, is rejected with an "unsupported
// argument type" error. To pass a boolean, use a SQL literal (TRUE /
// FALSE) or a string parameter with an explicit cast such as $1::bool.
// The same trick works for any type polite does not encode directly:
// pass the value as text and let PostgreSQL parse it.
//
// From there, the row API defines how results are read.
//
// # Rows and Columns
//
// A [Row] is a view of the current result row. Polite requests every column
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
// # NULL
//
// [Column.Bytes] reports SQL NULL as a nil slice with a nil error;
// a non-null empty value returns a non-nil zero-length slice. The
// typed decoders treat NULL as an error, because their return types
// cannot represent "missing."
//
// NULL often goes away in the query. COALESCE supplies a default,
// NULLIF reverses it, and IS NOT NULL filters at the source.
// Pushing the question to PostgreSQL is usually clearer than a
// client-side branch, and reuses the database's NULL semantics
// instead of re-deriving them in Go.
//
// # COPY
//
// Query supports COPY TO and COPY FROM in PostgreSQL's binary format;
// text COPY returns [ErrCopyFormat]. COPY FROM is entered through
// Query: while the iterator yields rows whose columns report
// [ErrCopyFrom], send each data row with [Conn.Exec] using an empty SQL
// string and the row's column values as arguments. A nil argument is
// sent as SQL NULL.
//
// See [Column] for details on reading binary values and [Conn.Query]
// for examples.
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
// Polite's own `Err*` sentinels, such as [ErrBusy], are produced
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
// Failed dials retry with exponential backoff until the pool is closed; see
// [Pool.Dial] for the retry contract and [Pool.Logger] for redirecting or
// silencing the retry warnings.
//
// # Testing and Time
//
// When a [Config]'s Now function is set, unqualified SQL calls to now(),
// clock_timestamp(), statement_timestamp(), and transaction_timestamp() use
// the returned clock on that connection. If Now is nil, polite leaves
// PostgreSQL's default time behavior unchanged. This is useful in tests,
// including tests run under testing/synctest, because SQL that calls those
// functions can observe the same clock as the Go test.
//
// The override is implemented by creating SQL functions in a "polite"
// schema and prepending that schema to the connection's search_path.
// Tests that read pg_proc or inspect the search_path will see those
// functions; the original PostgreSQL versions remain reachable via their
// fully qualified names (e.g. pg_catalog.now()).
package polite
