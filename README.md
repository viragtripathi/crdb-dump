[![PyPI version](https://img.shields.io/pypi/v/crdb-dump)](https://pypi.org/project/crdb-dump/)
[![Python versions](https://img.shields.io/pypi/pyversions/crdb-dump)](https://pypi.org/project/crdb-dump/)
[![License](https://img.shields.io/pypi/l/crdb-dump)](https://pypi.org/project/crdb-dump/)
[![Build status](https://github.com/viragtripathi/crdb-dump/actions/workflows/python-ci.yml/badge.svg)](https://github.com/viragtripathi/crdb-dump/actions)

# crdb-dump

A feature-rich CLI for exporting and importing CockroachDB schemas and data. Includes support for parallel chunked exports, manifest checksums, BYTES/UUID/ARRAY/VECTOR types, multi-schema (non-`public`) objects, permission introspection, secure resumable imports, S3-compatible storage (MinIO, Cohesity), region-aware filtering, and automatic retry logic.

> **Requires Python 3.10+.**

> ### ⚠️ Breaking changes in 0.4.0
> - All object names are now three-part `database.schema.table` (filenames,
>   manifests, resume-log keys, and `--tables` input). Objects in non-`public`
>   schemas are now exported and restored correctly.
> - `--tables` two-part input means `schema.table` (database taken from `--db`),
>   not the old `database.table`. Use `db.schema.table` to be explicit, or a bare
>   `table` for the `public` schema.
> - Data chunk files are now `db.schema.table_NNN.csv|sql`; manifests are
>   `db.schema.table.manifest.json`. Pre-0.4.0 dumps are not compatible.
>
> See [CHANGELOG.md](CHANGELOG.md) for the full list.

---

## 🚀 Features

* ✅ Schema export: tables, views, sequences, enums (objects in any schema, not just `public`)
* ✅ Full-database dumps use native `SHOW CREATE ALL TABLES`/`ALL TYPES` (dependency-ordered, FK constraints validated post-load)
* ✅ Data export: CSV or SQL with chunking, gzip, and ordering
* ✅ Types: handles BYTES, UUIDs, STRING\[], TIMESTAMP, enums, VECTOR
* ✅ Schema output formats: `sql`, `json`, `yaml`
* ✅ Resumable `COPY`-based imports with chunk-level tracking
* ✅ Permission exports: roles, grants, role memberships
* ✅ Parallel loading (`--parallel-load`) and manifest verification
* ✅ Dry-run for schema or chunk loading
* ✅ TLS and insecure auth supported
* ✅ Schema diff support (`--diff`)
* ✅ Full logging via `logs/crdb_dump.log`
* ✅ Automatic retry logic with exponential backoff for transient failures
* ✅ Fault-tolerant, resumable imports with `--resume-log` or `--resume-log-dir`
* ✅ Region-aware export/import via `--region`
* ✅ S3-compatible support (`--use-s3`) with MinIO, Cohesity, or AWS
* ✅ CSV header validation (`--validate-csv`)
* ✅ Python-based S3 bucket creation (via `boto3`) for MinIO

---

## 📦 Installation

```bash
pip install crdb-dump
```

---

## 🧪 Local Testing

```bash
./test-local.sh
```

This script will:

* Start a multi-region demo CockroachDB cluster
* Create test schema + data
* Export schema and chunked data (CSV)
* Verify chunk checksums
* Dry-run and real import with retry/resume
* Upload chunks to MinIO (S3-compatible)
* Download and verify import from S3
* Use Python (`boto3`) to create S3 buckets

---

## 🔧 CLI Overview

```bash
crdb-dump --help
crdb-dump export --help
crdb-dump load --help
```

Example usage:

```bash
crdb-dump export --db=mydb --data --per-table
crdb-dump load --db=mydb --schema=... --data-dir=... --resume-log=resume.json
```

---

## 🔐 Connection

```bash
export CRDB_URL="cockroachdb://root@localhost:26257/defaultdb?sslmode=disable"
# or
export CRDB_URL="postgresql://root@localhost:26257/defaultdb?sslmode=disable"
```

Alternatively:

```bash
--db mydb --host localhost --certs-dir ~/certs
```

Use `--print-connection` to verify resolved URL.

---

## 🏗 Export Options

```bash
crdb-dump export \
  --db=mydb \
  --per-table \
  --data \
  --data-format=csv \
  --chunk-size=1000 \
  --data-order=id \
  --data-compress \
  --data-parallel \
  --verify \
  --include-permissions \
  --archive
```

### Schema Output

| Option                  | Description                                   |
| ----------------------- | --------------------------------------------- |
| `--per-table`           | One file per object (e.g., `table_mydb.public.users.sql`) |
| `--format`              | Output format: `sql`, `json`, `yaml`          |
| `--diff`                | Show schema diff vs previous `.sql` file      |
| `--tables`              | Comma-separated names to include: `table`, `schema.table`, or `db.schema.table` |
| `--exclude-tables`      | Skip specific table names (same forms as `--tables`) |
| `--include-permissions` | Export roles, grants, and memberships         |
| `--region`              | Only export tables matching this region       |

### Data Export

| Option              | Description                            |
| ------------------- | -------------------------------------- |
| `--data`            | Enable data export                     |
| `--data-format`     | Format: `csv` or `sql`                 |
| `--chunk-size`      | Number of rows per chunk               |
| `--data-split`      | Output one file per table              |
| `--data-compress`   | Output `.csv.gz`                       |
| `--data-order`      | Order rows by column(s)                |
| `--data-order-desc` | Use descending order                   |
| `--data-parallel`   | Parallel export across tables          |
| `--verify`          | Verify chunk checksums                 |
| `--region`          | Filter tables by region in manifests   |
| `--use-s3`          | Upload exported chunks to S3           |
| `--s3-bucket`       | S3 bucket name                         |
| `--s3-prefix`       | Key prefix under which to store chunks |
| `--s3-endpoint`     | S3-compatible endpoint URL             |
| `--s3-access-key`   | S3 access key (can use env)            |
| `--s3-secret-key`   | S3 secret key (can use env)            |

---

## ⛓ Import Options

```bash
crdb-dump load \
  --db=mydb \
  --schema=crdb_dump_output/mydb/mydb_schema.sql \
  --data-dir=crdb_dump_output/mydb \
  --resume-log=resume.json \
  --validate-csv \
  --parallel-load \
  --print-connection
```

| Option             | Description                                      |
| ------------------ | ------------------------------------------------ |
| `--schema`         | `.sql` file to apply                             |
| `--data-dir`       | Folder containing chunked CSV + manifests        |
| `--resume-log`     | Track loaded chunks in a single JSON file        |
| `--resume-log-dir` | Per-table resume logs (e.g. `resume/users.json`) |
| `--validate-csv`   | Ensure chunk headers match DB schema             |
| `--parallel-load`  | Load chunks in parallel                          |
| `--region`         | Only import chunks from matching region          |
| `--dry-run`        | Print actions but don't execute                  |
| `--use-s3`         | Download chunks from S3                          |
| `--s3-bucket`      | S3 bucket name                                   |
| `--s3-prefix`      | Path prefix inside the bucket                    |
| `--s3-endpoint`    | S3-compatible endpoint (MinIO, Cohesity)         |
| `--s3-access-key`  | S3 access key                                    |
| `--s3-secret-key`  | S3 secret key                                    |

---

## 🔄 Fault Tolerance & Resume Support

* ✅ Retries failed operations with exponential backoff
* ✅ Resumable imports:

  * `--resume-log` (single file)
  * `--resume-log-dir` (per-table)
  * `--resume-strict` (abort on failure)

Writes resume state after each successful chunk. Restarts are safe and idempotent.

---

## ☁️ S3 / MinIO / Cohesity Example

```bash
crdb-dump export \
  --db=mydb \
  --per-table \
  --data \
  --chunk-size=1000 \
  --data-format=csv \
  --use-s3 \
  --s3-bucket=crdb-test-bucket \
  --s3-endpoint=http://localhost:9000 \
  --s3-access-key=minioadmin \
  --s3-secret-key=minioadmin \
  --s3-prefix=test1/ \
  --out-dir=crdb_dump_output

crdb-dump load \
  --db=mydb \
  --data-dir=crdb_dump_output/mydb \
  --resume-log-dir=resume/ \
  --parallel-load \
  --validate-csv \
  --use-s3 \
  --s3-bucket=crdb-test-bucket \
  --s3-endpoint=http://localhost:9000 \
  --s3-access-key=minioadmin \
  --s3-secret-key=minioadmin \
  --s3-prefix=test1/
```

---

## 🔍 Schema Diff Example

```bash
crdb-dump export --db=mydb --diff=old_schema.sql
```

Output:

```
crdb_dump_output/mydb/mydb_schema.diff
```

---

## 🧪 Testing

Requires Python 3.10+.

```bash
python -m venv .venv && source .venv/bin/activate
pip install -e ".[dev]"

# Unit tests (no database needed)
pytest -m "not integration"

# Integration tests (need a reachable CockroachDB)
export CRDB_URL="cockroachdb://root@localhost:26257/defaultdb?sslmode=disable"
pytest -m integration

# Full end-to-end (needs cockroach + Docker/MinIO)
./test-local.sh
```

---

## ❤️ Contributing

Pull requests welcome! Star ⭐ the repo, file issues, or request features at:

👉 [https://github.com/viragtripathi/crdb-dump/issues](https://github.com/viragtripathi/crdb-dump/issues)
