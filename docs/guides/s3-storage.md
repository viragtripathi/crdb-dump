# S3-Compatible Storage

crdb-dump can write data chunks to and read them from any S3-compatible store
(AWS S3, MinIO, Cohesity) with `--use-s3`.

## Options

| Option | Description |
| --- | --- |
| `--use-s3` | Enable S3 upload (export) / download (load) of data chunks |
| `--s3-bucket` | Bucket name |
| `--s3-prefix` | Key prefix under which chunks are stored |
| `--s3-endpoint` | Custom endpoint (e.g. MinIO/Cohesity) |
| `--s3-access-key` | Access key (or `AWS_ACCESS_KEY_ID`) |
| `--s3-secret-key` | Secret key (or `AWS_SECRET_ACCESS_KEY`) |

## Export to MinIO

```bash
crdb-dump export --db=mydb --data --data-format=csv --chunk-size=1000 \
  --use-s3 \
  --s3-bucket=crdb-test-bucket \
  --s3-endpoint=http://localhost:9000 \
  --s3-access-key=minioadmin \
  --s3-secret-key=minioadmin \
  --s3-prefix=backup1/
```

## Load from MinIO

```bash
crdb-dump load --db=mydb \
  --data-dir=crdb_dump_output/mydb \
  --use-s3 \
  --s3-bucket=crdb-test-bucket \
  --s3-endpoint=http://localhost:9000 \
  --s3-access-key=minioadmin \
  --s3-secret-key=minioadmin \
  --s3-prefix=backup1/ \
  --validate-csv --parallel-load --resume-log-dir=resume/
```

The schema file is written locally; only data chunks go to S3.
