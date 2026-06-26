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

crdb-dump reads each table independently with `OFFSET`/`LIMIT` and **does not use
`AS OF SYSTEM TIME`**. A dump taken while the database is being written is
therefore **not** a transactionally consistent snapshot across tables. For a
consistent dump, quiesce writes for the duration of the export.

!!! note "Planned enhancement"
    Adding an `AS OF SYSTEM TIME` option (read all tables at a single cluster
    timestamp) is tracked as future work to make consistent online snapshots
    possible.

## Further reading

- [CockroachDB MOLT](https://www.cockroachlabs.com/docs/molt/migration-strategy)
- [Logical Data Replication](https://www.cockroachlabs.com/docs/stable/logical-data-replication-overview)
- [Migrate from Standard or Basic to Advanced](https://www.cockroachlabs.com/docs/cockroachcloud/migrate-from-standard-to-advanced)
