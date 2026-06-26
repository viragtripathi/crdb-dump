# Naming Model

CockroachDB objects are three-part: `database.schema.table`. crdb-dump uses this
fully-qualified form consistently across SQL, filenames, manifests, and resume
logs.

## `--tables` / `--exclude-tables` input

| Input | Interpreted as |
| --- | --- |
| `users` | `<db>.public.users` |
| `cpkit.tasks` | `<db>.cpkit.tasks` |
| `mydb.cpkit.tasks` | `mydb.cpkit.tasks` |

A two-part name means `schema.table` (database from `--db`), **not** the legacy
`database.table`. An explicit database prefix must match `--db`.

## Identifier quoting

All identifiers (database, schema, table, column) are double-quoted when used in
SQL, so mixed-case and reserved-word names work correctly and safely.

## File and manifest naming

Data files and manifests use the `database.schema.table` stem:

```
mydb.public.users_001.csv
mydb.public.users.manifest.json
mydb.cpkit.tasks_001.csv
mydb.cpkit.tasks.manifest.json
```

Resume-log keys are derived from the same `database.schema.table` name.
