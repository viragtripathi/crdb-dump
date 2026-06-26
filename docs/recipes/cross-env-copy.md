# Cross-Environment Copy

Copy a database from one cluster to another (e.g. staging → local).

!!! warning "Consistency"
    crdb-dump takes a logical snapshot without `AS OF SYSTEM TIME`. Quiesce writes
    on the source for a consistent copy. See
    [Migration & Limitations](../guides/migration-limitations.md).

## Export from the source

```bash
export CRDB_URL="cockroachdb://user@source-host:26257/mydb?sslmode=verify-full&..."
crdb-dump export --db=mydb --data --data-format=csv --chunk-size=1000 \
  --out-dir=copy_out
```

## Load into the target

```bash
export CRDB_URL="cockroachdb://root@localhost:26257/mydb?sslmode=disable"
cockroach sql --insecure -e "CREATE DATABASE IF NOT EXISTS mydb;"

crdb-dump load --db=mydb \
  --schema=copy_out/mydb/mydb_schema.sql \
  --data-dir=copy_out/mydb \
  --validate-csv --parallel-load --resume-log-dir=resume/
```
