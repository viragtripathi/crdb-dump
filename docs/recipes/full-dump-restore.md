# Full Database Dump & Restore

Export an entire database (schema + data), then restore it into a fresh database.

## Export

```bash
export CRDB_URL="cockroachdb://root@localhost:26257/mydb?sslmode=disable"
crdb-dump export --db=mydb --data --data-format=csv --chunk-size=1000
```

Produces `crdb_dump_output/mydb/mydb_schema.sql` plus per-table chunk files and
manifests.

## Restore into a fresh database

```bash
cockroach sql --insecure -e "DROP DATABASE IF EXISTS mydb CASCADE; CREATE DATABASE mydb;"

crdb-dump load --db=mydb \
  --schema=crdb_dump_output/mydb/mydb_schema.sql \
  --data-dir=crdb_dump_output/mydb \
  --validate-csv --parallel-load --resume-log-dir=resume/
```

The schema file recreates non-`public` schemas, enum types, sequences, tables,
and validates foreign keys.

## Verify

```bash
cockroach sql --insecure -d mydb -e "SELECT count(*) FROM users;"
crdb-dump export --db=mydb --verify
```
