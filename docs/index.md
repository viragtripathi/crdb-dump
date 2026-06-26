# crdb-dump

A feature-rich CLI for exporting and importing CockroachDB schemas and data:
parallel chunked exports, manifest checksums, `BYTES`/`UUID`/`ARRAY`/`VECTOR`
types, multi-schema (non-`public`) objects, permissions, resumable imports,
S3-compatible storage (MinIO, Cohesity), region-aware filtering, and retry logic.

!!! info "What crdb-dump is — and isn't"
    crdb-dump is a **logical, point-in-time dump/restore** tool. It is **not** a
    live-migration tool. For minimal-downtime or continuous migration, use
    CockroachDB **MOLT** or **Logical Data Replication (LDR)**. See
    [Migration & Limitations](guides/migration-limitations.md).

## Install

```bash
pip install crdb-dump
```

Requires Python 3.10+.

## Quick example

```bash
export CRDB_URL="cockroachdb://root@localhost:26257/mydb?sslmode=disable"

# Export full schema + chunked CSV data
crdb-dump export --db=mydb --data --data-format=csv --chunk-size=1000

# Restore into a fresh database
crdb-dump load --db=mydb \
  --schema=crdb_dump_output/mydb/mydb_schema.sql \
  --data-dir=crdb_dump_output/mydb --validate-csv
```

See [Getting Started](getting-started/installation.md) to go further.
