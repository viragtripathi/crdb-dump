# Importing & Restoring

`crdb-dump load` applies schema and loads data via CockroachDB `COPY`.

## Schema + data

```bash
crdb-dump load --db=mydb \
  --schema=crdb_dump_output/mydb/mydb_schema.sql \
  --data-dir=crdb_dump_output/mydb \
  --validate-csv
```

- `--schema` applies a `.sql` DDL file (statements are split safely, respecting
  string literals and function bodies).
- `--data-dir` loads every `*.manifest.json` found in the directory.
- `--validate-csv` checks each chunk's header against the live table columns
  before loading.

## Parallel loading

```bash
crdb-dump load --db=mydb --data-dir=crdb_dump_output/mydb --parallel-load
```

## Dry run

```bash
crdb-dump load --db=mydb --data-dir=crdb_dump_output/mydb --dry-run
```

## Resumable loads

Loads record progress so re-runs skip already-loaded chunks (idempotent):

```bash
# single shared log file
crdb-dump load --db=mydb --data-dir=... --resume-log=resume.json

# one log per table
crdb-dump load --db=mydb --data-dir=... --resume-log-dir=resume/

# abort on the first failed chunk
crdb-dump load --db=mydb --data-dir=... --resume-log-dir=resume/ --resume-strict
```

## Selecting tables on load

```bash
crdb-dump load --db=mydb --data-dir=... --include-tables=mydb.public.users
crdb-dump load --db=mydb --data-dir=... --exclude-tables=mydb.public.audit_log
```
