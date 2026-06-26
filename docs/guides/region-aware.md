# Region-Aware Export & Import

For multi-region databases, `--region` filters by table locality on export and by
manifest region on import.

## Export a single region

```bash
crdb-dump export --db=mydb --data --data-format=csv --region=us-east1
```

Only tables whose locality matches `us-east1` are exported. Each manifest records
the table's region.

## Import a single region

```bash
crdb-dump load --db=mydb --data-dir=crdb_dump_output/mydb --region=us-east1
```

Chunks whose manifest region does not match are skipped.

!!! note
    Region metadata comes from `SHOW TABLES` locality. On a single-region (or
    non-multi-region) cluster there are no regions to filter on, and `--region`
    selects nothing.
