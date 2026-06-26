# Type Handling

crdb-dump preserves CockroachDB data types across export and restore in both SQL
and CSV formats.

| Type | SQL export | CSV export |
| --- | --- | --- |
| `BYTES` | `decode('0102', 'hex')` | bytea hex `\x0102` |
| `UUID` | string literal | string |
| `STRING[]` / arrays | `'{a,b}'` array literal | `{a,b}` |
| `VECTOR(n)` | `'[1.5,2,3]'` string literal | `[1.5,2,3]` (quoted) |
| enums (`CREATE TYPE`) | via `SHOW CREATE ALL TYPES` | value as string |

## BYTES

BYTES round-trip safely: SQL exports use `decode(..., 'hex')`, and CSV exports use
the PostgreSQL bytea hex format (`\x...`) so `COPY ... WITH CSV` decodes them back
to bytes rather than storing the literal characters.

## VECTOR

CockroachDB returns `VECTOR` values as strings like `[1.5,2,3.25]`. crdb-dump
preserves them verbatim — in CSV the comma-bearing value is quoted, and `COPY`
restores it unchanged. The `VECTOR(n)` column type itself is emitted by
`SHOW CREATE`.

## Enums

User-defined enum types are exported via `SHOW CREATE ALL TYPES` and recreated
before the tables that use them.
