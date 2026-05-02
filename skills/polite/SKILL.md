---
name: polite
description: PostgreSQL-first usage guide for blake.io/polite. Use when writing or reviewing Go code that queries PostgreSQL with polite — schema design, queries, error handling, notifications, COPY, pooling, and tests.
---

# Polite

Polite is a small Go client for PostgreSQL. It is the Go handle;
PostgreSQL does the work. The package surface is narrow on purpose —
`Conn`, `Pool`, `Query`, `Exec`, a handful of column decoders, and the
`febe` low-level wire package.

## Operating posture

The first reflex on any database problem is "what does PostgreSQL
already do?" Push the work to the server. Reach for Go only when the
answer is "nothing useful."

- NULL → `COALESCE` / `NULLIF` / `IS NOT NULL`. Not a Go branch.
- Type coercion → server-side cast (`$1::uuid`, `$1::bool`,
  `$1::numeric`, `$1::jsonb`). Not a Go conversion layer.
- Filtering, counting, aggregation → SQL.
- Validation, defaults → CHECK constraints, generated columns,
  triggers.
- Schema → pure `.sql` migrations against a golden schema.

Be skeptical of version-sensitive behavior. Verify against PostgreSQL
release notes for 15 through current/devel.

## Connecting

- `polite.Dial(ctx, dsn)` returns a single-user `*Conn`.
- `polite.Pool` (zero value works) when multiple goroutines share
  database access. The pool serves waiters FIFO and idle connections
  LIFO so prepared-statement caches stay warm on the most-used
  connections.
- A `Conn` is not safe for concurrent use. A `Pool` is.

## Queries and rows

- `Conn.Query(ctx, sql, args...)` returns `iter.Seq[*Row]`. Range over
  it. **The iterator is single-use** — call `Query` again to
  re-execute.
- `Conn.Exec(ctx, sql, args...)` for commands without rows.
- Row errors surface through any column method on the `*Row`, e.g.
  `id, err := row.Column(0).DecodeInt()`. Idiomatic form is one line.
- `Column` decoders: `DecodeText`, `DecodeInt`, `DecodeFloat`,
  `DecodeTime`, `Bytes`. PostgreSQL's bool is one byte (0x00 / 0x01),
  so `DecodeInt` reads a bool column directly: 0 is false, 1 is true.
- For types polite does not decode, write a plain Go function over
  `Column.Bytes`. The wire format is the contract.
- Row column bytes are valid only until the next iteration. Copy
  anything you need to keep.

## SQL rules

- **SQL text is constant.** Never build SQL from user input or
  application data. Pass values through parameters.
- **Cast on the server** when polite's pinned OID needs coercion:
  `$1::uuid`, `$1::jsonb`, `$1::numeric`, `$1::bool`.
- **`LISTEN` is the lone exception** to constant SQL — PostgreSQL takes
  a channel identifier, not a parameter. Use a hard-coded constant
  channel owned by the application; never derive it from data, never
  accept it from a user.
- **For notifications, prefer `pg_notify($1, $2)`** with a static
  channel by policy. Keep payloads small; for large or durable data,
  store rows and notify with a key.
- **Multi-line backtick SQL** puts the closing backtick on its own
  line, aligned with the `const` (or `var`) keyword. The SQL body is
  indented one level deeper than the backtick:

  ```go
  const q = `
  	SELECT id, name
  	FROM accounts
  	WHERE active
  `
  ```

## Arguments

Polite pins each parameter's PostgreSQL OID from the Go type:

- `nil` and a nil `[]byte` → SQL NULL.
- `[]byte` → bytea. `string` → text.
- Fixed-width integers pin int2/int4/int8 by Go width.
- `float32` → float4. `float64` → float8.
- `time.Time` → timestamptz; cast for other temporal types
  (`$1::timestamp`, `$1::date`).
- **Go `bool` is rejected on purpose.** Pass a SQL literal
  `TRUE`/`FALSE`, or a string with `$1::bool`. The same trick covers
  any type polite does not encode directly: pass a string and cast in
  SQL.
- Unsigned values above `int64` max wrap; pass them as a string for
  the full unsigned range.

## Errors

`*febe.Error` is the decoded ErrorResponse — SQLSTATE Code, Message,
Severity, Detail, Hint, plus Schema/Table/Column/DataType/Constraint
when the backend attaches them.

- Match SQLSTATE with `errors.Is`, not type assertions:

  ```go
  if errors.Is(err, febe.ErrorCode("23505")) {
      // unique violation
  }
  ```

- `febe.Error.IsFatal` distinguishes a recoverable backend error (a
  ReadyForQuery follows) from a FATAL/PANIC that closed the
  connection. A fatal error is sticky and returned by every subsequent
  call on that connection.
- Polite's own `Err*` (e.g. `ErrBusy`, `ErrCopyFormat`, `ErrCopyFrom`)
  are local conditions polite produces, not decoded from the backend.

## Prepared statement cache

Polite caches prepared statements per connection, keyed on (SQL text,
pinned parameter OIDs). The same SQL with different Go arg types
becomes distinct cache entries and distinct server-side prepared
statements.

DDL that invalidates a cached plan surfaces as SQLSTATE `0A000`
("cached plan must not change result type"). Polite drops the matching
cache entry on that error, so the caller's next call re-Parses
cleanly. The orphaned server-side prepared statement is left until the
connection closes — the same hygiene cost polite already accepts on
ordinary LRU eviction. Cycle connections (PgBouncer, app restart) if
zombies accumulate in long-lived processes.

**Do not issue `DEALLOCATE ALL` on a polite-managed connection.** The
LRU still holds the now-stale statement-name mappings; the next bind
will fail.

## Notifications

- `LISTEN <constant>` followed by `polite.Notifications` for the
  receive loop.
- While the notification iterator is active on a connection, do not
  issue `Query` or `Exec` on that same connection.
- `SELECT pg_notify($1, $2)` to send.

## COPY

Polite supports COPY TO and COPY FROM in PostgreSQL's binary format
only; text COPY returns `ErrCopyFormat`. COPY FROM is entered through
`Query`: while the iterator yields rows whose columns report
`ErrCopyFrom`, send each data row with `Conn.Exec("", values...)`. A
nil argument is sent as SQL NULL.

## Schema and migrations

Iterate against the golden schema first. Make queries, constraints,
and indexes right; tests should create databases from only that
schema. Once tests pass, write the migration as a pure `.sql` file. Do
not run migrations — leave that to the operator.

At the top of each migration file, add concise SQL comments for
operational considerations: lock behavior, backfill strategy, index
concurrency, validation steps, rollback limits, whether the migration
should run outside a transaction.

## Testing

- Run tests against real PostgreSQL. Don't mock.
- For time-sensitive code, set `Config.Now` and run under
  `testing/synctest`. Polite installs SQL functions in a `polite`
  schema on `search_path` so SQL `now()`, `clock_timestamp()`,
  `statement_timestamp()`, and `transaction_timestamp()` see the same
  clock as the Go test.
- Verify behavior through PostgreSQL's catalogs —
  `pg_prepared_statements`, `pg_stat_activity`, `pg_proc`,
  `pg_settings` — rather than internal polite state.

## Reference

```sh
go doc blake.io/polite
go doc blake.io/polite.Conn.Query
go doc blake.io/polite.Conn.Exec
go doc blake.io/polite.Pool
go doc blake.io/polite.Column
go doc blake.io/polite.Notifications
go doc blake.io/polite/febe.Error
```
