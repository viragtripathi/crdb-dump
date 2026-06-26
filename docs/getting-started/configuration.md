# Configuration

## Connection

crdb-dump connects via the `CRDB_URL` environment variable (preferred), otherwise
it defaults to `localhost:26257`:

```bash
export CRDB_URL="cockroachdb://root@localhost:26257/mydb?sslmode=disable"
# postgresql:// URLs are also accepted and normalized automatically
export CRDB_URL="postgresql://root@host:26257/mydb?sslmode=disable"
```

### TLS / secure clusters

When `CRDB_URL` is not set, you can point at a certificates directory:

```bash
crdb-dump export --db=mydb --certs-dir ~/certs
```

This connects with `sslmode=verify-full` using `ca.crt`, `client.root.crt`, and
`client.root.key` from that directory.

Use `--print-connection` to print the resolved connection URL (credentials
redacted) and exit.

## S3 / object storage

S3 credentials can be supplied via flags or the standard AWS environment
variables `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY`. See
[S3-Compatible Storage](../guides/s3-storage.md) for the full set of options
(custom endpoints for MinIO and Cohesity, bucket, prefix).
