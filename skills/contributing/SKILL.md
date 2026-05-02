---
name: polite
description: The polite ethos — PostgreSQL does the work, Go is the handle. Use when designing API, adding decoders or errors, writing godoc, reviewing changes, or testing.
---

# Polite

Polite is the Go handle on PostgreSQL. PostgreSQL does the work. The package
surface stays narrow on purpose; if a problem can be pushed to the server, it
is.

## Just use PostgreSQL

The first reflex on any database problem is "what does PostgreSQL already do?"
Only after that answer is "nothing useful" does Go-side code earn its keep.

- NULL: COALESCE, NULLIF, `IS NOT NULL`. Not a Go branch.
- Infinity, ±∞: filter or clamp at the source (`WHERE ts <> 'infinity'`,
  `LEAST(ts, '9999-12-31')`). Not a Go decoder special case.
- Type coercion: server-side cast (`$1::uuid`, `$1::bool`, `$1::numeric`,
  `$1::jsonb`). Not a Go conversion layer.
- Truthiness, defaults, validation: SQL has constraints, CHECK, generated
  columns, triggers. Not a Go validator.
- Counting, filtering, aggregation: SQL. Don't pull rows back to count
  them in Go.
- Schema work: pure `.sql` migrations, edited against the golden schema
  first, run by the user.

## Don't

- **Don't build SQL from data.** SQL text is constant; values go through
  parameters. The lone exception is `LISTEN`, and even there the channel
  name is a hard-coded constant owned by the application — never derived,
  never user-supplied. For notifications use `pg_notify($1, $2)` with a
  static channel by policy.
- **Don't pass a Go `bool` as a query argument.** Polite rejects it on
  purpose. Use `$1::bool` with a string, or a SQL literal `TRUE`/`FALSE`.
  The same trick covers any type polite does not encode directly: pass a
  string and cast in SQL.
- **Don't add ORMs, query builders, or wrapper layers above polite.** Keep
  the API surface small and let PostgreSQL carry the complexity.
- **Don't introduce typed sentinels for errors callers shouldn't branch
  on.** Inline `errors.New` is the right shape for "you misused this API."
  Reserve exported `Err*` for local conditions polite itself produces
  (e.g. `ErrBusy`, `ErrCopyFormat`, `ErrCopyFrom`).
- **Don't read SQLSTATE via type assertion + field comparison.** Use
  `errors.Is(err, febe.ErrorCode("23505"))`.
- **Don't issue `DEALLOCATE ALL` on a polite-managed connection.** The
  prepared-statement cache still holds the now-stale name mappings; the
  next bind will fail. Cycle the connection instead (PgBouncer, app
  restart).
- **Don't share a `Conn` across goroutines.** `Conn` is single-user. Use
  `Pool` for sharing.
- **Don't issue `Query` or `Exec` on a connection while its
  `Notifications` iterator is active.**
- **Don't capture row column values past the next iteration.** Row
  storage is reused; raw bytes belong to the current row only.
- **Don't reuse a `Query` iterator.** It is single-use; a second range
  yields an error row. Call `Query` again to re-execute.
- **Don't mock the database.** Tests run against real PostgreSQL.
- **Don't verify behavior by reading polite's internal state.** Verify
  with PostgreSQL — `pg_prepared_statements`, `pg_stat_activity`,
  `pg_proc`, `pg_settings`. The test pins a server-observable contract,
  not an implementation detail.
- **Don't simulate impossible states just to hit a branch.** A test
  should model something a real client, server, or transport could
  actually produce.
- **Don't add a decoder, error, or knob until a real call site needs
  it.** Speculative API is dead weight.
- **Don't speculate on version-sensitive PostgreSQL behavior.** Verify
  against official docs and release notes for 15 through current/devel.
- **Don't add client-side heuristics for unknown OIDs based on payload
  width or shape.** Keep parameter typing and binary encodings aligned
  with libpq.
- **Don't use `_ = foo()` to ignore a return value.** Just call `foo()`.

## Do (sparingly)

- `Conn.Query` for rows; range over `iter.Seq[*Row]`. Errors arrive via
  a column method on the `*Row`.
- `Conn.Exec` for commands without rows.
- `Pool` for shared access. FIFO on waiters bounds latency; LIFO on
  connections concentrates prepared-statement reuse.
- Decoders: `Column.DecodeText`, `DecodeInt`, `DecodeFloat`, `DecodeTime`,
  `Bytes`. PostgreSQL's bool is one byte (0x00/0x01); `DecodeInt` reads
  it directly: 0 is false, 1 is true.
- For types polite does not decode, write a plain Go function over
  `Column.Bytes`. The wire format is the contract; that is enough.
- `errors.Is(err, febe.ErrorCode("..."))` for SQLSTATE matching.
  `febe.Error.IsFatal` for recoverable-vs-sticky.
- `Config.Now` plus `testing/synctest` for time-sensitive tests.

## Voice

Documentation and code follow Golang maintainer energy.

- Declarative, not imperative at the reader. State what something *is*
  and what it *does*, not what the reader should do. `"Compare the
  result to 0 yourself"` is wrong; `"0 is false, 1 is true"` is right.
- Terse. A short clear sentence beats a clear paragraph.
- No editorializing about why an API was shaped a particular way. The
  shape is the answer.
- No meta-commentary on design choices in godoc. Don't write things like
  "intentionally inline-only" or "callers should not branch on this."
  Trust the reader.
- Default to no comments. Add one only when the *why* is non-obvious — a
  hidden constraint, a subtle invariant, a workaround for a specific
  bug. Comments are load-bearing or absent.
- Don't explain what well-named code already says. Don't reference the
  current task, fix, or call sites; that belongs in commit messages and
  rots in source.

## Testing

- Real PostgreSQL, not mocks.
- Use the database to assert behavior. `pg_prepared_statements` for
  cache invariants, `pg_proc` for schema overrides, `pg_stat_activity`
  for connection state.
- Prefer exported setup (`Dial`, `Pool`) over unexported field
  construction.
- `iter.Pull` when a test needs to drive an `iter.Seq` step by step.
- Each test proves one real thing with the fewest moving parts. If a
  helper hides essential setup, inline it and delete the fiction.

## Commits

- Stage relevant files first, then write the message. Don't ask the
  user about unstaged files; just stage and proceed.
- Subject prefix `polite:` for root-package commits. Lowercase after
  the colon. Imperative verb. Under ~72 characters. No trailing period.
  No `.:` prefix.
- Body explains the *why* — the motivation, the constraint, the
  surprising design decision. Not a file-by-file enumeration.
- New commits over `--amend`. No `--no-verify`. No skipping hooks
  unless the user explicitly asks.

## Nomenclature

- "SQL" means PostgreSQL SQL unless otherwise noted, not ANSI or
  another dialect.
- "Phoebe" means `febe`, the PostgreSQL Front End / Back End protocol.
