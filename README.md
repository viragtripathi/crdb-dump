[![PyPI version](https://img.shields.io/pypi/v/crdb-dump)](https://pypi.org/project/crdb-dump/)
[![Python versions](https://img.shields.io/pypi/pyversions/crdb-dump)](https://pypi.org/project/crdb-dump/)
[![License](https://img.shields.io/pypi/l/crdb-dump)](https://pypi.org/project/crdb-dump/)
[![Build status](https://github.com/viragtripathi/crdb-dump/actions/workflows/python-ci.yml/badge.svg)](https://github.com/viragtripathi/crdb-dump/actions)

# crdb-dump

A CLI tool to export and import schema definitions and data from CockroachDB in SQL, JSON, YAML, or chunked CSV formats.

Supports chunking, parallelism, resumability, diffing, manifest checksums, BYTES and UUID types, TLS auth, and dry-run safety.

---

## 🚀 Features

* Export tables, views, sequences, and user-defined types
* Output formats: SQL, JSON, YAML, CSV (with optional gzip)
* Export BYTES as `decode('<hex>', 'hex')`
* Handles UUIDs, TIMESTAMPS, arrays
* Create per-table schema files or a unified schema file
* Parallel + chunked data export with manifest and row tracking
* Resumable `COPY`-based data import
* Schema + data `dry-run` mode
* Schema diffing against previous .sql
* CLI output + logging to `logs/`
* TLS certs or insecure connection supported
* `--print-connection` shows full resolved DB URL (safe)

---

## 🔧 Installation

```bash
pip install crdb-dump
```

---

## 🥺 Local Testing

Run an integration test:

```bash
./test-local.sh
```

This performs:

* Schema + data export (with BYTES, UUID)
* Chunked CSV manifest creation
* Dry-run import
* Full schema and data reload

---

## 🗋 Usage

```bash
crdb-dump export --db=mydb [options]
crdb-dump load --db=mydb --schema=<.sql> --data-dir=...
```

### 🔐 Connection Options

```bash
export CRDB_URL="cockroachdb://root@localhost:26257/defaultdb?sslmode=disable"
```

or use flags:

```bash
--db mydb --host localhost --certs-dir ~/certs
```

---

## 🏠 Export Options

```bash
crdb-dump export --db=mydb --data --data-format=csv --chunk-size=1000
```

| Option               | Description                           |
|----------------------|---------------------------------------|
| `--data`             | Enable data export                    |
| `--data-format`      | `csv` or `sql` output                 |
| `--data-compress`    | Output `.csv.gz` instead              |
| `--chunk-size`       | Split into fixed-row chunks           |
| `--per-table`        | Write per-table files                 |
| `--data-order`       | Order rows (e.g., by `id`)            |
| `--data-order-desc`  | Order descending                      |
| `--data-parallel`    | Export tables in parallel             |
| `--verify`           | Check manifest SHA256s                |
| `--print-connection` | Show resolved DB connection URL       |
| `--archive`          | Create `.tar.gz` from exported folder |

---

## 🛬 Load Options

```bash
crdb-dump load \
  --db=mydb \
  --schema=defaultdb_schema.sql \
  --data-dir=export/defaultdb \
  --resume-log=resume.json \
  --print-connection \
  --dry-run
```

| Option               | Description                                |
|----------------------|--------------------------------------------|
| `--schema`           | Load schema from `.sql` file               |
| `--data-dir`         | Path containing chunked CSVs and manifests |
| `--resume-log`       | Resume tracking file for chunked load      |
| `--dry-run`          | Don't execute, just print plan             |
| `--include-tables`   | Restrict to specific table names           |
| `--exclude-tables`   | Skip specific table names                  |
| `--print-connection` | Print resolved CockroachDB connection      |

---

## 📂 Output Structure

By default, output is stored under:

```
crdb_dump_output/<db_name>/
├── defaultdb_schema.sql
├── table_users.sql
├── users_chunk_001.csv
├── users.manifest.json
├── logins_chunk_001.csv
├── logins.manifest.json
```

All logs go to:

```
logs/crdb_dump.log
```

---

## 📄 Example: Full Export + Verify + Import

```bash
crdb-dump export \
  --db=defaultdb \
  --data \
  --data-format=csv \
  --chunk-size=1000 \
  --per-table \
  --verify \
  --archive \
  --print-connection

crdb-dump load \
  --db=defaultdb \
  --schema=crdb_dump_output/defaultdb/defaultdb_schema.sql \
  --data-dir=crdb_dump_output/defaultdb \
  --resume-log=resume.json \
  --print-connection
```

---

## 🔍 Schema Diffing

```bash
crdb-dump export \
  --db=defaultdb \
  --diff=previous_schema.sql
```

This prints a unified diff and writes to:

```
crdb_dump_output/<db_name>/<db_name>_schema.diff
```

---

## 🤖 Test Coverage

* `pytest -m unit`  – runs fast unit tests
* `pytest -m integration` – full Docker-based test
* `./test-local.sh` – end-to-end data roundtrip

---

## 🛠️ Developer Notes

* Configured via `pyproject.toml` (PEP 621)
* Click-based CLI
* Tested with CRDB v25.2
* CI runs all tests via GitHub Actions and Docker

---

## 👤 Author

Created by [Virag Tripathi](https://github.com/viragtripathi)
MIT License
