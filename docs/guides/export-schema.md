# Exporting Schema

By default, `crdb-dump export` writes the full database DDL to
`crdb_dump_output/<db>/<db>_schema.sql`.

## Full-database DDL

For a whole-database export, crdb-dump uses CockroachDB's native bulk DDL:

```bash
crdb-dump export --db=mydb
```

It emits, in dependency order:

1. `CREATE SCHEMA IF NOT EXISTS` for every non-`public` schema,
2. `SHOW CREATE ALL TYPES` (enums),
3. `SHOW CREATE ALL TABLES` (sequences → tables → views, with foreign-key
   constraints split into trailing `ALTER TABLE ... ADD CONSTRAINT ... VALIDATE`).

This makes the resulting `mydb_schema.sql` safe to replay into a fresh database.

## Output formats

```bash
crdb-dump export --db=mydb --format sql     # default
crdb-dump export --db=mydb --format json
crdb-dump export --db=mydb --format yaml
```

## One file per object

```bash
crdb-dump export --db=mydb --per-table
# e.g. table_mydb.public.users.sql, type_mydb.public.status.sql
```

## Selecting objects

```bash
# Include specific objects (table / schema.table / db.schema.table)
crdb-dump export --db=mydb --tables=public.users,cpkit.tasks

# Exclude specific objects
crdb-dump export --db=mydb --exclude-tables=public.audit_log
```

See the [Naming Model](../reference/naming-model.md) for how names are
interpreted.

## Permissions

Add roles, grants, and role memberships with `--include-permissions` — see
[Permissions](permissions.md).
