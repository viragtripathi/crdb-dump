# Exporting Data

Add `--data` to export table data alongside (or instead of) schema.

## Formats

```bash
crdb-dump export --db=mydb --data --data-format=csv    # chunked CSV (default)
crdb-dump export --db=mydb --data --data-format=sql    # INSERT statements
```

## Chunking

Rows are written in chunks; each chunk is a separate file plus an entry in the
table's manifest.

```bash
crdb-dump export --db=mydb --data --data-format=csv --chunk-size=1000
```

Files are named `mydb.<schema>.<table>_NNN.csv`. Each table also gets a
`mydb.<schema>.<table>.manifest.json` recording every chunk's row count and
SHA-256 checksum.

## Compression

```bash
crdb-dump export --db=mydb --data --data-format=csv --data-compress   # .csv.gz
```

## Ordering

```bash
crdb-dump export --db=mydb --data --data-order=id
crdb-dump export --db=mydb --data --data-order=id --data-order-desc
# Fail (instead of warn) if an ordered column is missing:
crdb-dump export --db=mydb --data --data-order=id --data-order-strict
```

## Parallelism and limits

```bash
crdb-dump export --db=mydb --data --data-parallel      # export tables concurrently
crdb-dump export --db=mydb --data --data-limit=100000  # cap rows per table
```

## Consistent snapshots (`--as-of-system-time`)

By default each table is read independently, so a dump of a live database is not
consistent across tables. Use `--as-of-system-time` to read every table (and every
chunk) at one pinned cluster timestamp:

```bash
# pin one cluster_logical_timestamp() for the whole export
crdb-dump export --db=mydb --data --as-of-system-time

# or pass an explicit interval / timestamp / decimal
crdb-dump export --db=mydb --data --as-of-system-time='-30s'
```

The pinned timestamp is recorded in each manifest as `as_of_system_time`.

!!! warning
    The timestamp must be within the table's garbage-collection window
    ([`gc.ttlseconds`](https://www.cockroachlabs.com/docs/stable/configure-replication-zones#gc-ttlseconds)).
    A very long export against an old timestamp can fail once the snapshot ages out
    of GC.

### Follower reads

Use `follower` to read from the **nearest replica** instead of the leaseholder,
reducing impact on the live workload (and cross-region latency). It pins
`follower_read_timestamp()` once and reuses it for the whole export:

```bash
crdb-dump export --db=mydb --data --as-of-system-time=follower
```

This still produces a consistent snapshot (one pinned timestamp, recorded in each
manifest). Reads use default priority to stay low-impact, so a read may
occasionally wait briefly on an unresolved write intent.

!!! note "Requirements & caveats"
    - Requires a CockroachDB entitlement that enables follower reads. Without it,
      the export fails fast with a clear message rather than silently reading from
      the leaseholder.
    - `follower_read_timestamp()` returns a timestamp **slightly in the past**
      (CockroachDB's guidance is to use exact-staleness reads when you can tolerate
      data at least a few seconds old). Objects created in the last few seconds may
      not be visible at that timestamp — a non-issue for normal exports of existing
      data. See the [CockroachDB follower-reads docs](https://www.cockroachlabs.com/docs/stable/follower-reads)
      for current details.
    - Actual follower routing requires a multi-node cluster; on a single node the
      read is served locally. Verify with
      `EXPLAIN ANALYZE SELECT … AS OF SYSTEM TIME follower_read_timestamp()`
      (look for `used follower read`).

## Verifying

Re-run with `--verify` to validate each chunk against its manifest checksum:

```bash
crdb-dump export --db=mydb --verify
```
