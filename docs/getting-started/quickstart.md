# Quick Start

## 1. Point at a cluster

```bash
export CRDB_URL="cockroachdb://root@localhost:26257/mydb?sslmode=disable"
```

## 2. Export schema + data

```bash
crdb-dump export --db=mydb --data --data-format=csv --chunk-size=1000
```

Output lands under `crdb_dump_output/mydb/`:

- `mydb_schema.sql` — full DDL (dependency-ordered, with `CREATE SCHEMA` for
  non-`public` schemas)
- `mydb.<schema>.<table>_NNN.csv` — chunked data
- `mydb.<schema>.<table>.manifest.json` — checksummed chunk manifest

## 3. Restore

```bash
crdb-dump load --db=mydb \
  --schema=crdb_dump_output/mydb/mydb_schema.sql \
  --data-dir=crdb_dump_output/mydb \
  --validate-csv
```

## 4. Verify checksums

```bash
crdb-dump export --db=mydb --verify
```

Next: read the [Guides](../guides/export-schema.md) for the full feature set, or
the [CLI Reference](../reference/cli.md) for every option.
