# crdb-dump

[![Tests](https://github.com/viragtripathi/crdb-dump/actions/workflows/python-ci.yml/badge.svg)](https://github.com/viragtripathi/crdb-dump/actions/workflows/python-ci.yml)
[![PyPI version](https://badge.fury.io/py/crdb-dump.svg)](https://badge.fury.io/py/crdb-dump)
[![Python 3.10+](https://img.shields.io/badge/python-3.10+-blue.svg)](https://www.python.org/downloads/)
[![PyPI Downloads](https://static.pepy.tech/personalized-badge/crdb-dump?period=total&units=INTERNATIONAL_SYSTEM&left_color=BLACK&right_color=GREEN&left_text=downloads)](https://pepy.tech/projects/crdb-dump)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

**Export and import CockroachDB schemas and data — SQL, JSON, YAML, or chunked CSV.**

A feature-rich CLI for dumping and restoring CockroachDB: parallel chunked
exports, manifest checksums, multi-schema (non-`public`) objects, rich type
support (`BYTES`/`UUID`/`ARRAY`/`VECTOR`), permissions, resumable imports,
S3-compatible storage, region-aware filtering, and automatic retries.

!!! info "What crdb-dump is — and isn't"
    crdb-dump is a **logical, point-in-time dump/restore** tool. It is **not** a
    live-migration tool. For minimal-downtime or continuous migration, use
    CockroachDB **MOLT** or **Logical Data Replication (LDR)**. See
    [Migration & Limitations](guides/migration-limitations.md).

---

## Key features

### :material-table-cog: Schema export
Tables, views, sequences, and enum types in **any schema** (not just `public`).
Full-database dumps use native `SHOW CREATE ALL TYPES` / `SHOW CREATE ALL TABLES`
— dependency-ordered, with foreign keys validated after load.

### :material-database-arrow-down: Data export
CSV or SQL, with chunking, gzip, ordering, parallelism, and per-chunk SHA-256
manifests.

### :material-shape: Rich types
`BYTES`, `UUID`, `STRING[]`/arrays, `TIMESTAMP`, enums, and **`VECTOR`** all
round-trip in both SQL and CSV.

### :material-backup-restore: Resumable imports
`COPY`-based loads with per-chunk resume logs, parallel loading, CSV validation,
and dry runs — restarts are safe and idempotent.

### :material-cloud-upload: S3-compatible storage
Stream chunks to and from AWS S3, MinIO, or Cohesity with custom endpoints.

### :material-earth: Region-aware
Filter export and import by table locality in multi-region databases.

---

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

---

## Get started

<div class="grid cards" markdown>

-   :material-lightning-bolt:{ .lg .middle } **Getting Started**

    ---

    Install crdb-dump and run your first export and restore.

    [:octicons-arrow-right-24: Quick Start](getting-started/quickstart.md)

-   :material-book-open-variant:{ .lg .middle } **Guides**

    ---

    Learn every feature: schema, data, types, S3, regions, and more.

    [:octicons-arrow-right-24: Guides](guides/index.md)

-   :material-chef-hat:{ .lg .middle } **Recipes**

    ---

    Task-oriented, copy-paste workflows for common jobs.

    [:octicons-arrow-right-24: Recipes](recipes/index.md)

-   :material-console:{ .lg .middle } **CLI Reference**

    ---

    Every command and option, generated from the CLI.

    [:octicons-arrow-right-24: CLI Reference](reference/index.md)

</div>

---

## Install

```bash
pip install crdb-dump
```

Requires Python 3.10+.

## Community & support

- Issues and feature requests: <https://github.com/viragtripathi/crdb-dump/issues>
- [Contributing](development/contributing.md)
- Released under the [MIT License](about/license.md)
