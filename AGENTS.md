# AGENTS Instructions

## Overview

This codebase is name `polite` because it is for `postgres` and `lite` on API
surface area, so we must be diligent in keeping its API surface area small and
focused on one thing: Providing the simplest possible API for executing SQL
queries and handling results, without any additional features or abstractions
that could complicate its usage.

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
