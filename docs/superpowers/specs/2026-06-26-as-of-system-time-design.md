# `--as-of-system-time` for Consistent Data Exports (Design)

Date: 2026-06-26
Status: Approved

## Problem

crdb-dump reads each table independently with `OFFSET`/`LIMIT` and no
`AS OF SYSTEM TIME`. A dump taken while the database is being written is therefore
not a transactionally consistent snapshot across (or even within) tables. This
limits its usefulness for clones and with-downtime migrations.

## Goal

Add an option to read all table data at a single, pinned cluster timestamp so a
dump is a consistent point-in-time snapshot.

## Decisions (from brainstorming, approved)

- **Flag:** `--as-of-system-time` on `export`, with an optional value.
  - Bare `--as-of-system-time` → `aost="auto"`: capture one
    `cluster_logical_timestamp()` at export start and reuse it for every table and
    every chunk.
  - `--as-of-system-time=<value>` → used verbatim (e.g. `-30s`, a timestamp, a
    decimal).
  - Omitted → `None` (current behavior, no AOST).
- **Consistency:** the value is resolved/pinned **once** in `export_data` and
  applied to all queries.
- **Scope:** applied to the row `SELECT`s and the column-discovery query, so the
  column list matches the snapshot. Schema DDL (`SHOW CREATE`) reads at current
  time — CockroachDB does not support `AS OF SYSTEM TIME` there; this is a
  **data**-consistency guarantee.
- **Traceability:** the pinned timestamp is recorded in each manifest as
  `as_of_system_time`.

## Architecture

### CLI (`crdb_dump/cli.py`)

Add to the `export` command:

```python
@click.option('--as-of-system-time', 'aost', is_flag=False, flag_value='auto',
              default=None,
              help="Read data at a consistent snapshot. The bare flag pins one "
                   "cluster_logical_timestamp(); or pass a value like '-30s', a "
                   "timestamp, or a decimal.")
```

`flag_value='auto'` makes the bare flag yield `"auto"`; `--as-of-system-time=-30s`
yields `"-30s"`; omission yields `None`. The value lands in `kwargs`/`opts` as
`aost`.

### Clause helper (`crdb_dump/utils/common.py`)

A pure formatter, unit-testable without a database:

```python
def aost_clause(resolved_value):
    """Return an ' AS OF SYSTEM TIME ...' SQL fragment, or '' when no AOST."""
    if not resolved_value:
        return ""
    escaped = str(resolved_value).replace("'", "''")
    return f" AS OF SYSTEM TIME '{escaped}'"
```

Uniform single-quoting works for decimals, intervals (`-30s`), and timestamps.

### Resolution + pinning (`crdb_dump/export/data.py`, `export_data`)

Before exporting any table, resolve the value exactly once:

```python
aost = opts.get("aost")
if aost == "auto":
    with engine.connect() as conn:
        aost = str(conn.execute(text("SELECT cluster_logical_timestamp()")).scalar())
opts["aost_resolved"] = aost            # None or a fixed string, reused everywhere
```

`export_table_data` already receives `opts`, so it reads `opts["aost_resolved"]`
and computes `clause = aost_clause(opts.get("aost_resolved"))` once.

### Applying the clause (`export_table_data`)

- Column query:
  ```sql
  SELECT column_name FROM information_schema.columns
  WHERE table_name = :t AND table_schema = :s ORDER BY ordinal_position
  ```
  becomes `... ORDER BY ordinal_position{clause}` (clause appended).
- Row query:
  ```sql
  SELECT * FROM <fq>{clause} {order_clause} OFFSET <o> LIMIT <n>
  ```
  The `AS OF SYSTEM TIME` fragment goes immediately after the table reference and
  before `ORDER BY`/`OFFSET`/`LIMIT`.

**Implementation check:** verify empirically that `AS OF SYSTEM TIME` is accepted
on the `information_schema.columns` query (a virtual table). If CockroachDB rejects
it, fall back to running the column query at current time and document the minor
skew risk; the row-level snapshot guarantee is unaffected.

### Manifest (`export_table_data`)

Add the pinned timestamp:

```json
{
  "table": "mydb.public.users",
  "as_of_system_time": "1750000000.0000000000",
  "region": "N/A",
  "chunks": [ ... ]
}
```

`as_of_system_time` is `null` when AOST is not used.

## Error handling

- An AOST timestamp older than the table's GC TTL fails the query with a clear
  CockroachDB error; this surfaces through the existing per-table error handling
  and is documented as a caveat (keep exports shorter than the GC window, or raise
  `gc.ttlseconds`).
- Function-expression values (e.g. `follower_read_timestamp()`) are out of scope —
  the quoted-value model treats inputs as AOST string literals.

## Testing (all three levels)

- **Unit** (`tests/test_aost.py`): `aost_clause(None)` → `""`;
  `aost_clause("1750.0")` → `" AS OF SYSTEM TIME '1750.0'"`;
  `aost_clause("-30s")` → `" AS OF SYSTEM TIME '-30s'"`; quote escaping.
- **Integration** (`tests/test_integration.py`, gated on `CRDB_URL`):
  capture `cluster_logical_timestamp()`, insert a new row *after* it, export the
  table with `--as-of-system-time=<captured>` → the new row is **excluded**, and
  the manifest records `as_of_system_time`. Also a bare-flag `--as-of-system-time`
  smoke test (export succeeds, manifest timestamp populated).
- **E2E** (`test-local.sh`): add a step that exports with `--as-of-system-time`
  and asserts the manifest contains `as_of_system_time`.

## Docs

- `docs/guides/export-data.md`: add an "As-of-system-time (consistent snapshots)"
  section.
- `docs/guides/migration-limitations.md`: flip the consistency caveat from "planned
  enhancement" to "available via `--as-of-system-time`", keeping the GC-window note.
- `CHANGELOG.md`: add under `Unreleased` → Added.

## Out of scope

- AOST on schema DDL (`SHOW CREATE`) — unsupported by CockroachDB.
- Function-expression AOST values.
- Applying AOST to `collect_objects`/locality discovery (object set is not part of
  the data-snapshot guarantee).
