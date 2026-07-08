# Changelog

## Unreleased

_No changes yet._

## 0.6.1 — 2026-07-08

### Fixed
- JSONB values are now exported as valid JSON. CSV previously emitted Python
  dict repr (single quotes), which `COPY`/psql rejected with
  `invalid input syntax for type json`; SQL format emitted them unquoted.
  Encoding is type-aware (via `information_schema.columns.data_type`), so a
  JSONB array is JSON-encoded while a SQL `ARRAY` keeps the array literal.
- SQL-format data exports now quote `TIMESTAMP`/`TIMESTAMPTZ`/`DATE`/`TIME`
  values. They were previously emitted bare, producing `syntax error at or
  near ...` for timestamps and silently wrong arithmetic for dates.
- Sequences (and views) returned by `SHOW TABLES` are no longer data-exported.
  Previously they produced invalid files like
  `INSERT INTO "...seq" () VALUES (0, 0, True)` and bogus CSV chunks that had
  to be filtered out by hand.

## 0.6.0 — 2026-06-26

### Added
- `--as-of-system-time=follower`: pin `follower_read_timestamp()` so exports are
  served by the nearest replica (follower reads), keeping the consistent-snapshot
  guarantee. Reads use default priority to stay low-impact. Fails fast with a clear
  message if the cluster lacks the follower-reads entitlement.

## 0.5.0 — 2026-06-26

### Added
- `--as-of-system-time` on `export`: read all table data at one pinned cluster
  timestamp for a consistent point-in-time snapshot. The bare flag pins
  `cluster_logical_timestamp()`; an explicit value (interval, timestamp, or
  decimal) is used verbatim. The pinned timestamp is recorded in each manifest as
  `as_of_system_time`.

## 0.4.0 — 2026-06-26

### Breaking
- Object naming is now three-part `database.schema.table` everywhere
  (filenames, manifests, resume-log keys, and `--tables` input). Pre-0.4.0
  two-part dumps are not compatible with the 0.4.0 loader.
- `--tables` two-part input now means `schema.table` (database taken from
  `--db`), not the legacy `database.table`.
- Minimum supported Python is now 3.10.

### Fixed
- Export and restore now work for objects in non-`public` schemas
  (the data exporter previously crashed on three-part names, and schema
  export silently dropped the schema component).
- Mixed-case and reserved-word identifiers are now correctly quoted.
- `load` honors `--host`/`--port`/`--certs-dir` (the psycopg2 path previously
  ignored them and only read `CRDB_URL`/localhost).
- Schema export no longer duplicates DDL when the aggregate file already exists.
- Schema load splitting handles semicolons inside string literals and
  function bodies (via `sqlparse`).
- Selective data export (`--tables`) resolves the database from `--db` instead
  of misreading the first name segment as the database.
- Replaced deprecated `datetime.utcnow()`.

### Added
- Native `SHOW CREATE ALL TYPES` / `SHOW CREATE ALL TABLES` for full-database
  dumps (dependency-ordered, with foreign-key constraints validated post-load).
- Centralized identifier model (`crdb_dump/utils/identifiers.py`) used across
  all modules for naming and safe SQL quoting.
- Regression tests for CockroachDB `VECTOR` data round-trips (SQL and CSV).

### Changed
- Dependencies upgraded to current GA releases (SQLAlchemy 2.0.51,
  sqlalchemy-cockroachdb 2.0.4, click 8.4.2, PyYAML 6.0.3, psycopg2-binary
  2.9.12, boto3 1.43.36, sqlparse 0.5.5).
- Data chunk files renamed from `<table>_chunk_NNN.*` to
  `<db.schema.table>_NNN.*`; manifests to `<db.schema.table>.manifest.json`.
