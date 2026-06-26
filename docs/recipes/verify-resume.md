# Verify & Resume

## Verify chunk checksums

After an export, validate every chunk against its manifest SHA-256:

```bash
crdb-dump export --db=mydb --verify
```

## Resume an interrupted load

Loads record progress per chunk. If a load is interrupted, re-running with the
same resume log skips chunks that already succeeded:

```bash
# first attempt (interrupted partway)
crdb-dump load --db=mydb --data-dir=crdb_dump_output/mydb \
  --resume-log-dir=resume/ --validate-csv --parallel-load

# re-run — already-loaded chunks are skipped
crdb-dump load --db=mydb --data-dir=crdb_dump_output/mydb \
  --resume-log-dir=resume/ --validate-csv --parallel-load --resume-strict
```

The second run reports `Skipped` for chunks already present, making restarts safe
and idempotent.
