[![PyPI version](https://img.shields.io/pypi/v/crdb-dump)](https://pypi.org/project/crdb-dump/)
[![Python versions](https://img.shields.io/pypi/pyversions/crdb-dump)](https://pypi.org/project/crdb-dump/)
[![License](https://img.shields.io/pypi/l/crdb-dump)](https://pypi.org/project/crdb-dump/)
[![Build status](https://github.com/viragtripathi/crdb-dump/actions/workflows/python-ci.yml/badge.svg)](https://github.com/viragtripathi/crdb-dump/actions)

# crdb-dump

A feature-rich CLI for exporting and importing CockroachDB schemas and data. Includes support for parallel chunked exports, manifest checksums, BYTES/UUID/ARRAY types, permission introspection, and secure resumable imports.

---

## 🚀 Features

- ✅ Schema export: tables, views, sequences, enums
- ✅ Data export: CSV or SQL with chunking, gzip, and ordering
- ✅ Types: handles BYTES, UUIDs, STRING[], TIMESTAMP, enums
- ✅ Schema output formats: `sql`, `json`, `yaml`
- ✅ Resumable `COPY`-based imports with chunk-level tracking
- ✅ Permission exports: roles, grants, role memberships
- ✅ Parallel loading (`--parallel-load`) and manifest verification
- ✅ Dry-run for schema or chunk loading
- ✅ TLS and insecure auth supported
- ✅ Schema diff support (`--diff`)
- ✅ Full logging via `logs/crdb_dump.log`
- ✅ Automatic retry logic for transient connection errors (e.g., server restarts)
- ✅ Fault-tolerant, resumable imports using `--resume-log` with chunk-level tracking

---

## 📦 Installation

```bash
pip install crdb-dump
````

---

## 🧪 Local Testing

```bash
./test-local.sh
```

This script will:

* Create test schema + data
* Export schema and data (CSV)
* Verify checksums
* Dry-run re-import
* Perform real import with `--validate-csv` and `--parallel-load`

---

## 🔧 CLI Overview

```bash
crdb-dump --help
crdb-dump export --help
crdb-dump load --help
```

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

Alternatively, specify connection parts via flags:

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
| `--per-table`           | One file per object (e.g., `table_users.sql`) |
| `--format`              | Output format: `sql`, `json`, `yaml`          |
| `--diff`                | Show schema diff vs previous `.sql` file      |
| `--tables`              | Comma-separated FQ names to include           |
| `--exclude-tables`      | Skip specific FQ table names                  |
| `--include-permissions` | Export roles, grants, and memberships         |

### Data Export

| Option              | Description                    |
| ------------------- | ------------------------------ |
| `--data`            | Enable data export             |
| `--data-format`     | Format: `csv` or `sql`         |
| `--chunk-size`      | Number of rows per chunk       |
| `--data-split`      | Output one file per table      |
| `--data-compress`   | Output `.csv.gz`               |
| `--data-order`      | Order rows by column(s)        |
| `--data-order-desc` | Use descending order           |
| `--data-parallel`   | Parallel export across tables  |
| `--verify`          | Verify chunk checksums         |
| `--archive`         | Compress output into `.tar.gz` |

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

| Option               | Description                                 |
| -------------------- | ------------------------------------------- |
| `--schema`           | `.sql` file to apply                        |
| `--data-dir`         | Folder containing chunked CSV and manifests |
| `--resume-log`       | JSON file to resume partial loads           |
| `--validate-csv`     | Fail early if column mismatch is detected   |
| `--parallel-load`    | Load chunks using multiple threads          |
| `--dry-run`          | Print what would be loaded, but skip action |
| `--include-tables`   | Restrict to specific FQ table names         |
| `--exclude-tables`   | Skip specific FQ table names                |
| `--print-connection` | Echo resolved DB connection                 |

---

## 📂 Output Structure

```bash
crdb_dump_output/mydb/
├── mydb_schema.sql
├── mydb_schema.json
├── mydb_schema.yaml
├── mydb_schema.diff
├── table_users.sql
├── users_chunk_001.csv
├── users.manifest.json
├── roles.sql
├── grants.sql
├── role_memberships.sql
├── permissions.sql
```

---

## 🔍 Schema Diff Example

```bash
crdb-dump export --db=mydb --diff=old_schema.sql
```

Result written to:

```
crdb_dump_output/mydb/mydb_schema.diff
```

---

## 🧪 Testing

```bash
pytest -m unit
pytest -m integration
./test-local.sh
```

---

## 🧑‍💻 Developer Notes

* Based on `click` + `sqlalchemy` + `psycopg2`
* PEP 621 pyproject-based project layout
* Supports TLS via `--certs-dir` or insecure fallback
* Uses CockroachDB `SHOW CREATE`, `COPY`, and `GRANTS`
* Tested with CockroachDB v25.2
* CI runs via GitHub Actions + Docker

---

## ❤️ Contributing

Pull requests welcome! Star ⭐ the repo, file issues, or request features at:

👉 [https://github.com/viragtripathi/crdb-dump/issues](https://github.com/viragtripathi/crdb-dump/issues)
