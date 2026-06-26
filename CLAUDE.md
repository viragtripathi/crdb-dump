# CLAUDE.md — crdb-dump

Guidance for AI assistants (and humans) working in this repository.

## What this is

A Click-based CLI to export and import CockroachDB schemas and data. Export uses
SQLAlchemy (`cockroachdb://`); data import uses psycopg2 `COPY`.

Commands: `crdb-dump export`, `crdb-dump load`, `crdb-dump version`.

## Layout

- `crdb_dump/cli.py` — Click entrypoint and option definitions.
- `crdb_dump/export/schema.py` — DDL export (tables/views/sequences/types/permissions).
- `crdb_dump/export/data.py` — chunked data export (CSV/SQL, gzip, ordering, S3, manifests).
- `crdb_dump/loader/loader.py` — schema apply + resumable chunk `COPY` (parallel, validate, S3).
- `crdb_dump/utils/` — `identifiers.py` (object naming/quoting), `db_connection.py`,
  `common.py` (retry, literal encoding, localities), `s3.py`, `io.py`, `logging.py`,
  `type_constants.py`.
- `crdb_dump/verify/` — checksum + diff.
- `tests/` — `test_unit.py` (no DB), `test_integration.py` (needs `CRDB_URL`).
- `test-local.sh` — full end-to-end run against a live cluster + MinIO.

## Core convention: three-part identifiers

CockroachDB objects are `database.schema.table`. **Never** assume two-part
`database.table`. All object naming and SQL identifier quoting goes through
`crdb_dump/utils/identifiers.py`:

- `parse_object_name(...)` to get an `ObjectName(database, schema, table)`.
- `.fq_quoted()` → `"db"."schema"."table"` for SQL.
- `.fq_plain()` → `db.schema.table` for filenames, manifests, resume-log keys, logs.

Do not interpolate raw identifiers into SQL strings. Always quote via the helper —
this is both a correctness fix (mixed-case/reserved/non-public names) and a safety fix.

`--tables` input is interpreted as `db.schema.table` or `schema.table` (db defaults to
`--db`). Two-part input means `schema.table`, not legacy `db.table`.

## Schema DDL

- Full-database export: `CREATE SCHEMA IF NOT EXISTS` for user schemas, then
  `SHOW CREATE ALL TYPES`, then `SHOW CREATE ALL TABLES` (dependency-ordered, FK
  constraints split into trailing `ALTER ... VALIDATE`).
- Selective export: per-object `SHOW CREATE <type> <fq_quoted>`.
- BYTES are encoded as bytea hex (`\x...`) in CSV so `COPY` restores them
  correctly; in SQL they use `decode('...','hex')`.

## Working rules (user preferences)

- **Test before proposing a push.** Every change must have passing **unit +
  integration + e2e** coverage. Run `pytest -m "not integration"`,
  `pytest -m integration` (with `CRDB_URL`), and `./test-local.sh` before
  asking to push.
- **No bot commit messages.** Plain, human-style messages. No `Co-Authored-By`,
  no "Generated with …" trailers.

## Running

```bash
export CRDB_URL="cockroachdb://root@localhost:26257/defaultdb?sslmode=disable"
pip install -e ".[dev]"

pytest -m "not integration"
pytest -m integration   # requires CRDB_URL + reachable cluster
./test-local.sh         # full e2e (needs cockroach + docker/MinIO)
```

## Requirements

Python >= 3.10.
