# crdb-dump v0.4.0 — Schema-Aware Overhaul (Design)

Date: 2026-06-26
Status: Approved-pending-review

## Problem

`crdb-dump` assumes two-part `database.table` object names throughout. CockroachDB
objects are three-part `database.schema.table`. Any object not in the `public`
schema is mishandled: it is silently re-qualified to the wrong name, fails its
`SHOW CREATE`, or crashes the data exporter (`db, tbl = table.split('.')` raises on
three parts). A user (Fabio) hit this when exporting tables in a non-`public`
schema and patched it locally.

Beyond the reported bug, the codebase has correctness, safety, and packaging gaps
that should be addressed in the same pass.

## Goals

1. Make export **and** restore fully correct for objects in any schema (not just `public`).
2. Bring the project up to current best practices (safety, restore ordering, packaging, tests, docs).
3. Require Python >= 3.10.

## Decisions (from brainstorming)

- **Scope:** Comprehensive overhaul, implemented in phases under one spec.
- **DDL strategy:** Use CockroachDB-native `SHOW CREATE ALL TABLES` / `SHOW CREATE ALL TYPES`
  for full-database export (dependency-ordered, FK constraints split out). Per-object
  `SHOW CREATE` for selective/filtered export.
- **Compatibility:** Clean break to three-part `database.schema.table` naming for
  filenames, manifests, and resume-log keys. No backward compatibility with old
  two-part manifests.
- **Version:** bump to `0.4.0` (breaking).
- **New dependency:** `sqlparse` for safe SQL statement splitting on load.

## Architecture

### 1. Identifier model — `crdb_dump/utils/identifiers.py` (new, foundational)

Single source of truth for object identity and quoting. Everything else uses it.

- `ObjectName(database, schema, table)` dataclass.
- `parse_object_name(s, default_db, default_schema="public") -> ObjectName`
  - Accepts `table`, `schema.table`, or `db.schema.table`.
  - For CLI `--tables` input, the value is interpreted as `db.schema.table` or
    `schema.table` (database defaults to `--db`). Two-part input is treated as
    `schema.table`, **never** the legacy `db.table`. This is documented in `--help`
    and README as a breaking change.
- `quote_ident(name) -> str` — double-quote, escape embedded quotes. Handles
  mixed-case and reserved words.
- `ObjectName.fq_quoted() -> '"db"."schema"."table"'` — for SQL.
- `ObjectName.fq_plain() -> 'db.schema.table'` — for filenames/manifests/logs/resume keys.
- `ObjectName.file_base() -> 'db.schema.table'` — filename stem.

This eliminates, in one place: schema-dropping, SQL injection via interpolated
identifiers, and mixed-case/reserved-word breakage.

### 2. Schema export — `crdb_dump/export/schema.py`

- **Full-database** (no `--tables`): emit `SHOW CREATE ALL TYPES` (enum filter) first,
  then `SHOW CREATE ALL TABLES` (sequences → tables → views in dependency order,
  with FK constraints emitted as trailing `ALTER ... ADD CONSTRAINT ... ; ... VALIDATE`).
- **Selective** (`--tables` / `--exclude-tables` / `--region`): schema-aware object-type
  resolver (no hardcoded `public`); per-object `SHOW CREATE <type> <fq_quoted>`.
- Fixes folded in:
  - `import click` (currently missing — `click.UsageError` at line ~153 raises `NameError`).
  - Truncate the aggregate `<db>_schema.sql` before writing (currently appended, so
    re-runs duplicate DDL).
  - Replace deprecated `datetime.utcnow()` with `datetime.now(timezone.utc)`.
  - Permissions export (`dump_permissions`) becomes schema-aware in object names.
- JSON/YAML schema output preserved, keyed by three-part names.

### 3. Data export — `crdb_dump/export/data.py`

- Parse each table with `ObjectName`.
- Column discovery query filters on **both** `table_name` and `table_schema`.
- `SELECT` / `INSERT` use `fq_quoted()`.
- Output naming: `db.schema.table_001.sql`, `db.schema.table_001.csv[.gz]`,
  manifest `db.schema.table.manifest.json`, manifest `"table"` field = `db.schema.table`.
- Keep OFFSET/LIMIT chunking. (Keyset pagination noted as a future perf item — out of scope.)

### 4. Loader — `crdb_dump/loader/loader.py`, `crdb_dump/utils/db_connection.py`

- **Connection bug fix:** `get_psycopg_connection` must honor the same connection
  inputs as the engine (`CRDB_URL`, else `--host/--port/--db/--certs-dir`). Today it
  ignores opts and only reads `CRDB_URL`/localhost, so `load --host ...` silently
  targets the wrong cluster.
- `COPY` targets `fq_quoted()`; `validate_csv_header` filters by schema too.
- **Schema apply:** replace naive `sql.split(';')` with `sqlparse.split()` to handle
  semicolons inside string literals, UDF bodies, and dollar-quoting.
- Manifests carry three-part names; resume-log keys derived from `fq_plain()`.

### 5. Packaging & hygiene — `pyproject.toml`, `.gitignore`

- `requires-python = ">=3.10"`; classifiers for 3.10–3.13; drop 3.7–3.9.
- Version → `0.4.0`.
- Add `sqlparse` dependency; add `[project.optional-dependencies].dev` (pytest, etc.).
- Stop tracking `build/` and `dist/`; add them plus `logs/`, `*.egg-info/`, output
  dirs to `.gitignore`.
- Add `CHANGELOG.md` with the breaking-change entry.

## Testing strategy (REQUIRED before any push)

Per user preference, every change must be verified at three levels and pass before a
push is proposed; commit messages carry no AI/bot attribution.

- **Unit** (`tests/test_unit.py` + new files, no DB):
  - `identifiers`: parse/quote/fq for public, non-public, mixed-case, reserved-word,
    embedded-quote, and 2- vs 3-part inputs.
  - Filename/manifest stem generation.
  - Schema-qualification logic with mocked SQLAlchemy connections.
  - Existing literal/type tests retained.
- **Integration** (`tests/test_integration.py`, gated on `CRDB_URL`):
  - Round-trip test that creates a **non-`public`** schema + table, exports, drops,
    loads, and asserts row/DDL fidelity — the exact gap reported.
  - Mixed-case identifier round-trip.
- **E2E** (`test-local.sh`): extend with a non-`public`-schema scenario across
  export → verify → load → resume → region → S3. Must run green against a live
  CockroachDB before any push.
- **CI** (`.github/workflows/python-ci.yml`): unit matrix on 3.10–3.13; integration
  job against a CockroachDB service container.

## Docs & memory

- README: three-part naming, schema-aware examples, breaking-change note, `--tables`
  input semantics.
- New repo-root `CLAUDE.md`: architecture map, the identifier-model convention,
  module responsibilities, how to run unit/integration/e2e, and the
  test-before-push / no-bot-commit rules.

## Implementation phases

1. `identifiers` module + unit tests.
2. Schema export rewrite (full + selective) + tests.
3. Data export rewrite (naming, qualification) + tests.
4. Loader + connection fix + sqlparse + tests.
5. Packaging, `.gitignore`, CHANGELOG, Python 3.10 bump.
6. Integration tests + `test-local.sh` non-public scenario + CI.
7. README + `CLAUDE.md`.
8. Full unit + integration + e2e green, then propose push.

## Out of scope

- Keyset/cursor pagination for very large tables (future perf work).
- Backward compatibility with pre-0.4.0 two-part manifests.
- Cross-database (multi-db) single-invocation export.
