# `--as-of-system-time=follower` (Follower Reads) Design

Date: 2026-06-26
Status: Approved-pending-review

## Problem

Exporting a large database from a live cluster reads from the leaseholder
(now unified with the Raft leader under **Leader Leases**, v25.2), competing with
the foreground workload. CockroachDB's **follower reads** let sufficiently old
reads be served by the nearest replica instead, reducing impact on the live
workload and cross-region latency. crdb-dump already supports pinned
`--as-of-system-time`; this adds a convenient way to read at a
follower-read-eligible timestamp.

## Goal

Add `--as-of-system-time=follower`, which pins `follower_read_timestamp()` once and
reuses it for the whole export, so data is read from followers while keeping the
existing cross-table consistency guarantee.

## Decisions (from brainstorming, approved)

- **Surface:** reuse the existing option as a resolved keyword
  `--as-of-system-time=follower` (mirrors `auto`). No separate flag.
- **Transaction model:** keep the existing **AUTOCOMMIT** reads at the pinned
  timestamp (do NOT switch to explicit `PRIORITY HIGH` transactions).
  - Consistency comes from the pinned timestamp, not transaction grouping, so
    AUTOCOMMIT statements at one fixed `T` are fully consistent.
  - AUTOCOMMIT avoids long-running transactions, which a chunked export would
    otherwise create (a read txn held open across all chunks + file I/O).
  - Follower routing depends only on the read timestamp ≤ the range's closed
    timestamp, not on transaction structure, so AUTOCOMMIT reads are still served
    by followers.
  - Accepted tradeoff: an exact-staleness read may occasionally wait on an
    unresolved write intent. `PRIORITY HIGH` would avoid that but can push/abort
    foreground writers — counter to a low-impact export — so it is intentionally
    not used. A future opt-in `--priority` could add it.

## Architecture

### Resolution (`crdb_dump/export/data.py`, `export_data`)

Extend the existing pin-once block with one branch:

```python
aost = opts.get("aost")
if aost == "auto":
    with engine.connect() as conn:
        aost = str(conn.execute(text("SELECT cluster_logical_timestamp()")).scalar())
elif aost == "follower":
    try:
        with engine.connect() as conn:
            aost = str(conn.execute(text("SELECT follower_read_timestamp()")).scalar())
    except Exception as e:
        raise click.UsageError(
            "Follower reads are not available on this cluster "
            "(requires a CockroachDB entitlement that enables follower reads): "
            f"{e}")
if aost is not None:
    logger.info(f"🕒 Pinned AS OF SYSTEM TIME {aost}")
opts["aost_resolved"] = aost
```

`click` is imported in `data.py` for `UsageError` (add the import).
The resolved decimal flows through the existing `aost_clause()` + AUTOCOMMIT path
unchanged; the manifest's `as_of_system_time` records it as before.

### CLI help (`crdb_dump/cli.py`)

Update the `--as-of-system-time` help to mention `follower`:

> "Read data at a consistent snapshot. Use `auto` (or the bare flag) to pin
> `cluster_logical_timestamp()`, `follower` to pin `follower_read_timestamp()`
> for follower reads, or pass an explicit interval/timestamp/decimal."

### Error handling

`follower_read_timestamp()` raises if the cluster lacks the follower-reads
entitlement. We catch this at resolution time (before exporting any table) and
raise a `click.UsageError` with a clear message — never a raw traceback, and never
a silent fall-back to leaseholder reads.

## Verification of follower routing

Because "it's served by a follower" is the whole value proposition, the spec
requires empirical confirmation (not just that the export succeeds):

- Run `EXPLAIN ANALYZE SELECT ... AS OF SYSTEM TIME '<pinned>'` against a table
  and confirm the plan reports `used follower read`. This is done during
  implementation against a live cluster and captured as an integration check
  where the cluster supports it.

## Testing (all three levels, entitlement-tolerant)

- **Unit** (`tests/test_data_export.py` or `tests/test_aost.py`): with a mocked
  engine whose `SELECT follower_read_timestamp()` returns a value, calling
  `export_data` with `opts["aost"]="follower"` sets `opts["aost_resolved"]` to that
  value; and when the mock raises, a `click.UsageError` is raised.
- **Integration** (`tests/test_integration.py`, gated on `CRDB_URL`): run
  `export --as-of-system-time=follower`; assert **either** success with a recorded
  `as_of_system_time` **or** a clean `UsageError`-style message (exit_code != 0,
  no traceback) if the cluster lacks the entitlement. Where follower reads are
  available, also assert `EXPLAIN ANALYZE` shows `used follower read`.
- **E2E** (`test-local.sh`): add a follower step that runs the export and accepts
  either outcome (success with manifest timestamp, or the clean "not available"
  message), so the script passes on an insecure single-node cluster regardless of
  entitlement.

Empirical pre-check during implementation: determine whether
`follower_read_timestamp()` works on a local insecure single-node v25.4 (the 2024
licensing changes may allow it); shape the integration/e2e assertions accordingly.

## Docs

- `docs/guides/export-data.md`: document `--as-of-system-time=follower` under the
  consistent-snapshots section, including the entitlement requirement and the
  low-impact (default priority) tradeoff.
- `docs/guides/region-aware.md` (or migration-limitations): cross-link follower
  reads as a low-impact export option for multi-region clusters.
- `CHANGELOG.md`: under `Unreleased` → Added.

## Out of scope

- Bounded-staleness reads (`with_max_staleness(...)`): use a dynamic per-statement
  timestamp, incompatible with the pin-once consistency model.
- `PRIORITY HIGH` / explicit-transaction reads (possible future `--priority` opt-in).
- Strong follower reads for `GLOBAL` tables (automatic; nothing to add).
