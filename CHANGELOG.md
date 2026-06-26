# Changelog

## Unreleased

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
