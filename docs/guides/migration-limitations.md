# Migration & Limitations

## What crdb-dump is

A **logical, point-in-time** dump/restore tool: it exports DDL (`SHOW CREATE`) and
table data (chunked `SELECT`), and restores via `CREATE` + `COPY`. It is ideal for
clones, dev/test seeding, selective extracts, and backups of static or quiesced
data.

## What crdb-dump is not

It is **not** a live-migration tool. It has no change-data-capture, no continuous
replication, and no cutover orchestration. For minimal-downtime or continuous
migration, use CockroachDB's purpose-built tooling:

- **MOLT** (Migrate Off Legacy Technology) — Fetch (bulk load), **Replicator**
  (continuous replication), and Verify. The right choice for minimal-downtime
  migrations.
- **Logical Data Replication (LDR)** — continuous, table-level CockroachDB↔CockroachDB
  replication (self-hosted clusters).

## CockroachDB Cloud tier moves (Basic → Standard → Advanced)

The documented tier-to-tier path is an **export/import via cloud storage**
workflow. crdb-dump can automate that pattern — **with a write-freeze window** on
the source (stop writes → dump → restore). For a live/near-zero-downtime tier
move, use MOLT, and for CockroachDB-to-CockroachDB migrations contact your
Cockroach Labs account team.

## Consistency caveat

By default crdb-dump reads each table independently with `OFFSET`/`LIMIT`, so a
dump taken while the database is being written is **not** a transactionally
consistent snapshot across tables.

!!! tip "Use `--as-of-system-time` for a consistent snapshot"
    Pass [`--as-of-system-time`](export-data.md#consistent-snapshots-as-of-system-time)
    to read every table at one pinned cluster timestamp:

    ```bash
    crdb-dump export --db=mydb --data --as-of-system-time
    ```

    The timestamp must stay within the garbage-collection window
    ([`gc.ttlseconds`](https://www.cockroachlabs.com/docs/stable/configure-replication-zones#gc-ttlseconds)),
    so keep exports shorter than that window or raise the TTL.

## Further reading

- [CockroachDB MOLT](https://www.cockroachlabs.com/docs/molt/migration-strategy)
- [Logical Data Replication](https://www.cockroachlabs.com/docs/stable/logical-data-replication-overview)
- [Migrate from Standard or Basic to Advanced](https://www.cockroachlabs.com/docs/cockroachcloud/migrate-from-standard-to-advanced)
