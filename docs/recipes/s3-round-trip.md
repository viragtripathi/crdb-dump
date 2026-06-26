# S3 Round-Trip (MinIO)

Export data chunks to an S3-compatible store and restore from it.

## Export to MinIO

```bash
crdb-dump export --db=mydb --data --data-format=csv --chunk-size=1000 \
  --use-s3 \
  --s3-bucket=crdb-test-bucket \
  --s3-endpoint=http://localhost:9000 \
  --s3-access-key=minioadmin \
  --s3-secret-key=minioadmin \
  --s3-prefix=backup1/ \
  --out-dir=s3_out
```

## Restore from MinIO

```bash
cockroach sql --insecure -e "DROP DATABASE IF EXISTS mydb CASCADE; CREATE DATABASE mydb;"

# schema is local; data chunks are pulled from S3
crdb-dump load --db=mydb \
  --schema=s3_out/mydb/mydb_schema.sql \
  --data-dir=s3_out/mydb \
  --use-s3 \
  --s3-bucket=crdb-test-bucket \
  --s3-endpoint=http://localhost:9000 \
  --s3-access-key=minioadmin \
  --s3-secret-key=minioadmin \
  --s3-prefix=backup1/ \
  --validate-csv --parallel-load --resume-log-dir=resume/
```
