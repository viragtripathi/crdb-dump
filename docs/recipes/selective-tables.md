# Selective Tables / Schemas

Export and restore a subset of objects.

## Export specific objects

```bash
# table / schema.table / db.schema.table forms all work
crdb-dump export --db=mydb \
  --tables=public.users,cpkit.tasks \
  --data --data-format=csv
```

For schema DDL of the selected objects, add `--per-table` to get one file each.

## Restore only some tables

`--data-dir` loads every manifest it finds; restrict with `--include-tables` (or
exclude with `--exclude-tables`), using fully-qualified names:

```bash
crdb-dump load --db=mydb \
  --data-dir=crdb_dump_output/mydb \
  --include-tables=mydb.cpkit.tasks \
  --validate-csv --resume-log-dir=resume/
```
