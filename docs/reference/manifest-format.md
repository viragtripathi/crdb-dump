# Manifest Format

Each exported table gets a `database.schema.table.manifest.json` describing its
chunks. Example:

```json
{
  "table": "mydb.public.users",
  "region": "N/A",
  "chunks": [
    { "file": "mydb.public.users_001.csv", "rows": 1000, "sha256": "<hex>" },
    { "file": "mydb.public.users_002.csv", "rows": 1000, "sha256": "<hex>" }
  ]
}
```

| Field | Description |
| --- | --- |
| `table` | Fully-qualified `database.schema.table` name |
| `region` | Table locality (or `N/A` on single-region clusters) |
| `chunks[].file` | Chunk filename (relative to the data directory) |
| `chunks[].rows` | Row count in the chunk |
| `chunks[].sha256` | SHA-256 checksum of the chunk file |

The loader reads every `*.manifest.json` in `--data-dir`, loads each chunk via
`COPY`, and records progress under a resume-log key derived from the manifest's
`table` value.
