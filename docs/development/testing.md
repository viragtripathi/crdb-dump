# Testing

crdb-dump is tested at three levels. Requires Python 3.10+.

```bash
python -m venv .venv && source .venv/bin/activate
pip install -e ".[dev]"
```

## Unit tests (no database)

```bash
pytest -m "not integration"
```

## Integration tests (need a reachable CockroachDB)

```bash
export CRDB_URL="cockroachdb://root@localhost:26257/defaultdb?sslmode=disable"
pytest -m integration
```

Integration tests cover non-`public` schema, `VECTOR`, and `BYTES` round-trips.

## End-to-end

```bash
./test-local.sh
```

The script auto-detects a single- or multi-region cluster, exercises export →
verify → restore → resume, and an S3 (MinIO) round-trip. It resolves the CLI from
the repo `.venv` automatically.
