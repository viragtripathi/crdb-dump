# Multi-Schema (non-`public`) Objects

CockroachDB objects are three-part: `database.schema.table`. crdb-dump is
schema-aware everywhere — objects in non-`public` schemas are exported and
restored correctly (filenames, manifests, DDL, and `COPY` targets are all fully
qualified).

## `--tables` input forms

| Input | Interpreted as |
| --- | --- |
| `users` | `<db>.public.users` |
| `cpkit.tasks` | `<db>.cpkit.tasks` |
| `mydb.cpkit.tasks` | `mydb.cpkit.tasks` |

A two-part name is `schema.table` (the database comes from `--db`), **not** the
legacy `database.table`.

## Example

```bash
# Export a table in the cpkit schema
crdb-dump export --db=mydb --tables=cpkit.tasks --data --data-format=csv
```

This produces `mydb.cpkit.tasks_001.csv` and
`mydb.cpkit.tasks.manifest.json`. A full-database export additionally emits
`CREATE SCHEMA IF NOT EXISTS "cpkit";` in the schema file so a fresh-database
restore works.

See the [Naming Model](../reference/naming-model.md) reference for details.
