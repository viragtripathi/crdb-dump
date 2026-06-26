# Permissions

Add `--include-permissions` to export roles, grants, and role memberships
alongside the schema:

```bash
crdb-dump export --db=mydb --include-permissions
```

This writes, under `crdb_dump_output/mydb/`:

- `roles.sql` — `CREATE ROLE` statements
- `grants.sql` — object `GRANT` statements
- `role_memberships.sql` — role membership grants (with `WITH ADMIN OPTION` where
  applicable)
- `permissions.sql` — an aggregate of all of the above, with an export timestamp

Apply them during restore with `crdb-dump load --schema=...` (or run the SQL
directly), after the schema objects exist.
