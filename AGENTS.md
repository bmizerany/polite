# AGENTS Instructions

Read [skills/contributing/SKILL.md](skills/contributing/SKILL.md) before
editing code, godoc, or tests in this repo. It carries the package ethos
and the rules that don't fit in `doc.go`.

`skills/polite/SKILL.md` is the *user-facing* skill, distributed to
consumers of the polite package. Keep it consumer-focused; contributor
rules belong in `skills/contributing/SKILL.md`.

## Overview

This codebase is name `polite` because it is for `postgres` and `lite` on API
surface area, so we must be diligent in keeping its API surface area small and
focused on one thing: Providing the simplest possible API for executing SQL
queries and handling results, without any additional features or abstractions
that could complicate its usage.

- Keep parameter typing and binary encodings aligned with libpq; when a
  parameter OID is unknown, do not add client-side heuristics based on payload
  width or shape.

## Tests

- Prefer exported setup over unexported field construction. If the package has a
  real entry point such as `Dial`, use it unless the test is specifically about
  malformed internal state that cannot be reached otherwise. There is almost
  certainly a way to "just use postgres" to test the client using well crafted
  postgres SQL.
- Do not simulate impossible states just to hit a branch. A test should model a
  situation a real client, server, or network transport could actually produce.
- Each test should prove one real thing with the fewest moving parts possible.
  If a helper starts hiding too much setup or behavior, inline the essential
  path and delete the fiction instead.

## Commit message rules for this repository

- For commits that touch the root package (`package polite`), use the
  commit subject prefix `polite:` (for example, `polite: handle row draining
  correctly`).
- Do not use `.:` as a package prefix in commit messages.
- If a commit message touches the root package, avoid using any other prefix.

## Commit skill (inlined)

Follow Go commit style when writing commit messages:

- Subject format: `polite: short summary` (lowercase after `:`).
- Keep the subject under 72 characters when practical and avoid trailing periods.
- Use imperative verbs (`add`, `fix`, `remove`, `implement`, etc.).
- Use prose in the body only when needed to explain rationale not obvious from
  the diff.
- Include issue references at the end only (e.g. `Fixes #12345`) if applicable.
- Do not use GitHub aliases like `Close` or `Resolves`.
- Do not include `Signed-off-by` lines or markdown in the body.
- Avoid the `.:` prefix and prefer the root package prefix above.

## Commit workflow

- Inspect staged changes first:
  - `git diff --staged`
  - Identify the primary package; use other prefixes only when ownership is equal.
- Draft and validate the message:

- Commit with heredoc message:

```sh
git commit -m "$(cat <<'EOF'
polite: short summary

optional body

Fixes #12345
EOF
)"
```

## Documentation

- Always include white space between fields of a struct and preceding fields.
- When referencing columns by index, it is idiomatic to keep the read on one
  line, for example `id, err := row.Column(0).DecodeInt()`. Exceptions exist,
  but this is usually the preferred form.
- Do not use `_ = foo()` or similar blank-identifier assignments to signal that
  a function is being called while ignoring its error or result. If the call is
  intentionally ignored, just invoke the function directly.

Good:

```go
type Foo struct {
    // Bar is the bar field.
    Bar string

    // Foo is the foo field.
    Foo string
}
```

Bad:

```go
type Foo struct {
    // Bar is the bar field.
    Bar string
    // Foo is the foo field.
    Foo string
}
```

## Nomenclature

When the user uses "SQL" casually in a sentence, unless otherwise noted, it
should be interpreted to mean specifically SQL as defined by Postgres, not ANSI
SQL or some other dialect of SQL.

When the user says "Phoebe", it should be interpreted as "Febe", the Postgres
Front End Back End protocol.
