#!/bin/bash
set -euo pipefail

# run this in a separate shell first
# script -c $(cockroach demo --nodes=3 --demo-locality=region=us-east1:region=us-west1:region=us-central1 --no-example-database --empty) /dev/null >/dev/null
#

DB_NAME="defaultdb"
OUT_DIR="tmp/export-test"
BASE_OUT_DIR="$OUT_DIR/$DB_NAME"
SCHEMA_FILE="$BASE_OUT_DIR/defaultdb_schema.sql"
DATA_DIR="$BASE_OUT_DIR"
LOG_FILE="logs/crdb_dump.log"
RESUME_FILE="$OUT_DIR/resume.json"

echo "🚀 Ensuring MinIO is running..."

MINIO_CONTAINER="crdb-minio"
#MINIO_ENDPOINT="http://localhost:9000"
MINIO_ENDPOINT="http://127.0.0.1:9000"
MINIO_ACCESS_KEY="minioadmin"
MINIO_SECRET_KEY="minioadmin"
MINIO_BUCKET="crdb-test-bucket"

if ! docker ps --format '{{.Names}}' | grep -q "^$MINIO_CONTAINER$"; then
  docker run -d --rm \
    --name "$MINIO_CONTAINER" \
    -p 9000:9000 -p 9001:9001 \
    -e MINIO_ROOT_USER=$MINIO_ACCESS_KEY \
    -e MINIO_ROOT_PASSWORD=$MINIO_SECRET_KEY \
    minio/minio server /data --console-address ":9001"
  echo "🟢 MinIO started at $MINIO_ENDPOINT"
  sleep 5
else
  echo "ℹ️  MinIO already running"
fi

echo "⏳ Waiting for MinIO API to be ready..."
sleep 10

echo "🧹 Cleaning up old output..."
rm -rf "$OUT_DIR" logs/ "$RESUME_FILE"

echo "🌍 Creating multi-region database..."
cockroach sql --insecure --host=localhost -e "
  DROP DATABASE IF EXISTS $DB_NAME CASCADE;
  CREATE DATABASE $DB_NAME PRIMARY REGION 'us-east1';
  ALTER DATABASE $DB_NAME ADD REGION 'us-west1';
"

echo "📐 Creating tables..."
cockroach sql --insecure --host=localhost -d "$DB_NAME" -e "
  CREATE TABLE users (
    id INT PRIMARY KEY,
    username STRING,
    password_hash BYTES,
    salt BYTES,
    hash_algo STRING,
    iterations INT,
    attempts INT,
    groups STRING[]
  );

  CREATE TABLE logins (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    username STRING,
    login_at TIMESTAMP DEFAULT now()
  );
"

echo "🌍 Assigning table locality..."
cockroach sql --insecure --host=localhost -d "$DB_NAME" -e "
  ALTER TABLE users SET LOCALITY REGIONAL BY TABLE IN 'us-east1';
  ALTER TABLE logins SET LOCALITY REGIONAL BY TABLE IN 'us-west1';
"

echo "📊 Inserting test data..."
cockroach sql --insecure --host=localhost -d "$DB_NAME" -e "
  INSERT INTO users
  SELECT
    i,
    'user_' || i,
    gen_random_bytes(16),
    gen_random_bytes(8),
    'sha256',
    100000,
    i % 5,
    ARRAY['group_' || (i % 3), 'group_' || (i % 5)]
  FROM generate_series(1, 10000) AS g(i);

  INSERT INTO logins (username)
  VALUES ('alice'), ('bob'), ('carol');
"

echo "📦 Exporting schema and data..."
crdb-dump --verbose export \
  --db="$DB_NAME" \
  --per-table \
  --data \
  --data-format=csv \
  --chunk-size=1000 \
  --out-dir="$OUT_DIR"

echo "🧪 Verifying chunks..."
crdb-dump --verbose export \
  --db="$DB_NAME" \
  --verify \
  --out-dir="$OUT_DIR"

echo "🔍 Dry run import (should not write to DB)..."
crdb-dump --verbose load \
  --db="$DB_NAME" \
  --schema="$SCHEMA_FILE" \
  --data-dir="$DATA_DIR" \
  --resume-log="$RESUME_FILE" \
  --dry-run \
  --print-connection

echo "❌ Dropping tables to prep for reload..."
cockroach sql --insecure --host=localhost -d "$DB_NAME" -e "
  DROP TABLE IF EXISTS users;
  DROP TABLE IF EXISTS logins;
"
echo "✅ Tables dropped"

echo "🧪 Full import with --validate-csv and --parallel-load"
crdb-dump --verbose load \
  --db="$DB_NAME" \
  --schema="$SCHEMA_FILE" \
  --data-dir="$DATA_DIR" \
  --resume-log="$RESUME_FILE" \
  --validate-csv \
  --parallel-load

echo "🔁 Testing data export variations..."
for ORDER_FLAG in "" "--data-order=id" "--data-order=id --data-order-desc"; do
  for PARALLEL in "" "--data-parallel"; do
    for LIMIT in 100 1000 ""; do
      echo "▶️ Exporting with $ORDER_FLAG $PARALLEL --data-limit=$LIMIT"
      OUT_SUBDIR="$OUT_DIR/variant_$(date +%s%N)"
      crdb-dump --verbose export \
        --db="$DB_NAME" \
        --per-table \
        --data \
        --data-format=csv \
        --data-split \
        --chunk-size=500 \
        $ORDER_FLAG $PARALLEL \
        ${LIMIT:+--data-limit=$LIMIT} \
        --out-dir="$OUT_SUBDIR"

      crdb-dump --verbose export --db="$DB_NAME" --verify --out-dir="$OUT_SUBDIR"
    done
  done
done

echo "🔍 Verifying loaded users..."
cockroach sql --insecure --host=localhost -d "$DB_NAME" -e "SELECT COUNT(*) FROM users"

echo "🔍 Verifying loaded logins..."
cockroach sql --insecure --host=localhost -d "$DB_NAME" -e "SELECT COUNT(*) FROM logins"

echo "🧪 Simulating failure and resuming import..."
echo "❌ Dropping one table for partial import..."
cockroach sql --insecure --host=localhost -d "$DB_NAME" -e "
  DROP TABLE IF EXISTS users;
"

echo "▶️ Running partial import to test resume..."
crdb-dump --verbose load \
  --db="$DB_NAME" \
  --data-dir="$DATA_DIR" \
  --resume-log="$RESUME_FILE" \
  --validate-csv \
  --parallel-load

echo "▶️ Resuming again — should skip already imported chunks..."
crdb-dump --verbose load \
  --db="$DB_NAME" \
  --data-dir="$DATA_DIR" \
  --resume-log="$RESUME_FILE" \
  --validate-csv \
  --parallel-load \
  --resume-strict

echo "🌍 Exporting us-east1 only..."
crdb-dump --verbose export \
  --db="$DB_NAME" \
  --data \
  --per-table \
  --region="us-east1" \
  --data-format=csv \
  --chunk-size=1000 \
  --out-dir="$OUT_DIR/us-east1"

echo "🌍 Exporting us-west1 only..."
crdb-dump --verbose export \
  --db="$DB_NAME" \
  --data \
  --per-table \
  --region="us-west1" \
  --data-format=csv \
  --chunk-size=1000 \
  --out-dir="$OUT_DIR/us-west1"

#echo "🪣 Creating test bucket in MinIO..."

#docker run --rm \
#  --network container:$MINIO_CONTAINER \
#  minio/mc alias set localminio http://localhost:9000 $MINIO_ACCESS_KEY $MINIO_SECRET_KEY

#docker run --rm \
#  --network container:$MINIO_CONTAINER \
#  minio/mc mb --ignore-existing localminio/$MINIO_BUCKET

echo "🪣 Creating test bucket in MinIO via boto3..."
python3 - <<EOF
import boto3
from botocore.exceptions import ClientError

s3 = boto3.client(
    's3',
    endpoint_url='$MINIO_ENDPOINT',
    aws_access_key_id='$MINIO_ACCESS_KEY',
    aws_secret_access_key='$MINIO_SECRET_KEY',
    region_name='us-east-1'
)

try:
    s3.head_bucket(Bucket='$MINIO_BUCKET')
    print("✔️ Bucket '$MINIO_BUCKET' already exists.")
except ClientError as e:
    if e.response['Error']['Code'] == '404':
        print("⚙️ Bucket does not exist. Creating it...")
        s3.create_bucket(Bucket='$MINIO_BUCKET')
        print("✅ Bucket '$MINIO_BUCKET' created.")
    else:
        print(f"❌ Error checking/creating bucket: {e}")
        exit(1)
EOF

echo "📤 Testing export to MinIO S3..."
crdb-dump --verbose export \
  --db="$DB_NAME" \
  --per-table \
  --data \
  --data-format=csv \
  --chunk-size=1000 \
  --use-s3 \
  --s3-bucket="$MINIO_BUCKET" \
  --s3-endpoint="$MINIO_ENDPOINT" \
  --s3-access-key="$MINIO_ACCESS_KEY" \
  --s3-secret-key="$MINIO_SECRET_KEY" \
  --s3-prefix="test1/" \
  --out-dir="$OUT_DIR/s3-test"

echo "🧹 Dropping logins table before S3 import..."
cockroach sql --insecure --host=localhost -d "$DB_NAME" -e "DROP TABLE IF EXISTS logins;"

echo "📐 Loading schema before S3 data import..."
crdb-dump --verbose load \
  --db="$DB_NAME" \
  --schema="$SCHEMA_FILE"

echo "📥 Testing import from MinIO S3..."
crdb-dump --verbose load \
  --db="$DB_NAME" \
  --data-dir="$OUT_DIR/s3-test/$DB_NAME" \
  --resume-log-dir="$OUT_DIR/s3-test-resume" \
  --use-s3 \
  --s3-bucket="$MINIO_BUCKET" \
  --s3-endpoint="$MINIO_ENDPOINT" \
  --s3-access-key="$MINIO_ACCESS_KEY" \
  --s3-secret-key="$MINIO_SECRET_KEY" \
  --s3-prefix="test1/" \
  --validate-csv \
  --parallel-load \
  --resume-strict

echo "📄 Log file written to: $LOG_FILE"
echo "✅ Done."

