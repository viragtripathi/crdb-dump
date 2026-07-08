#!/bin/bash
set -euo pipefail

# run this in a separate shell first
# script -c $(cockroach demo --nodes=3 --demo-locality=region=us-east1:region=us-west1:region=us-central1 --no-example-database --empty) /dev/null >/dev/null
#

# Resolve the crdb-dump CLI. We must verify the package imports *with its
# dependencies* (psycopg2, sqlalchemy, ...). Importing bare `crdb_dump` succeeds
# from the source tree even when nothing is installed, so we import
# `crdb_dump.cli`, which pulls the full dependency chain.
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
_can_run() { "$@" -c "import crdb_dump.cli" >/dev/null 2>&1; }

# Find a Python interpreter that can import the package WITH its dependencies
# (psycopg2, sqlalchemy, boto3, ...). boto3 is a dependency, so this interpreter
# is also used for the inline MinIO bucket-creation snippet below.
PYTHON=""
for cand in "$SCRIPT_DIR/.venv/bin/python" python python3; do
  if command -v "$cand" >/dev/null 2>&1 && _can_run "$cand"; then PYTHON="$cand"; break; fi
done

if command -v crdb-dump >/dev/null 2>&1; then
  CRDB_DUMP="crdb-dump"
elif [ -n "$PYTHON" ]; then
  CRDB_DUMP="$PYTHON -m crdb_dump.cli"
else
  echo "❌ crdb-dump is not installed with its dependencies."
  echo "   Install it (a virtualenv is recommended):"
  echo "     python -m venv .venv && source .venv/bin/activate && pip install -e ."
  echo "   Then re-run ./test-local.sh"
  exit 1
fi
# Ensure we have a deps-capable interpreter for the boto3 snippet even if the
# console script was found on PATH.
if [ -z "$PYTHON" ]; then PYTHON="python3"; fi
echo "🔧 Using CLI: $CRDB_DUMP"
echo "🔧 Using Python: $PYTHON"

DB_NAME="defaultdb"
OUT_DIR="tmp/export-test"
BASE_OUT_DIR="$OUT_DIR/$DB_NAME"
SCHEMA_FILE="$BASE_OUT_DIR/defaultdb_schema.sql"
DATA_DIR="$BASE_OUT_DIR"
LOG_FILE="logs/crdb_dump.log"
RESUME_FILE="$OUT_DIR/resume.json"

# This is a LOCAL end-to-end harness: setup uses `cockroach sql --host=localhost`,
# but crdb-dump honors $CRDB_URL. If an external CRDB_URL is inherited (e.g. a
# Cloud cluster), crdb-dump would target a DIFFERENT cluster than the setup — and
# could run destructive load/drop steps against it. Pin CRDB_URL to the local
# cluster so everything targets the same place. (Not the password — never echoed.)
if [ -n "${CRDB_URL:-}" ]; then
  echo "⚠️  An external CRDB_URL is set; overriding it to the local cluster for this run."
fi
export CRDB_URL="cockroachdb://root@localhost:26257/${DB_NAME}?sslmode=disable"

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

echo "🔌 Checking CockroachDB is reachable on localhost:26257..."
if ! cockroach sql --insecure --host=localhost -e "SELECT 1" >/dev/null 2>&1; then
  echo "❌ No CockroachDB reachable on localhost:26257."
  echo "   Start one of:"
  echo "     • single node:  cockroach start-single-node --insecure --store=type=mem,size=1GiB"
  echo "     • multi-region: cockroach demo --nodes=3 --demo-locality=region=us-east1:region=us-west1:region=us-central1 --no-example-database --empty"
  exit 1
fi

# Detect whether the cluster has regions; degrade gracefully on a single node.
CLUSTER_REGIONS=$(cockroach sql --insecure --host=localhost --format=csv \
  -e "SELECT region FROM [SHOW REGIONS FROM CLUSTER]" 2>/dev/null | tail -n +2 || true)
if [ -n "$CLUSTER_REGIONS" ]; then
  MULTIREGION=true
  PRIMARY_REGION=$(echo "$CLUSTER_REGIONS" | head -1)
  SECOND_REGION=$(echo "$CLUSTER_REGIONS" | sed -n '2p')
  [ -z "$SECOND_REGION" ] && SECOND_REGION="$PRIMARY_REGION"
  echo "🌍 Multi-region cluster detected (primary=$PRIMARY_REGION, second=$SECOND_REGION)"
else
  MULTIREGION=false
  echo "ℹ️  Single-region cluster — region-specific steps will be skipped"
fi

if [ "$MULTIREGION" = true ]; then
  echo "🌍 Creating multi-region database..."
  cockroach sql --insecure --host=localhost -e "
    DROP DATABASE IF EXISTS $DB_NAME CASCADE;
    CREATE DATABASE $DB_NAME PRIMARY REGION '$PRIMARY_REGION';
    ALTER DATABASE $DB_NAME ADD REGION '$SECOND_REGION';
  "
else
  echo "📦 Creating single-region database..."
  cockroach sql --insecure --host=localhost -e "
    DROP DATABASE IF EXISTS $DB_NAME CASCADE;
    CREATE DATABASE $DB_NAME;
  "
fi

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

if [ "$MULTIREGION" = true ]; then
  echo "🌍 Assigning table locality..."
  cockroach sql --insecure --host=localhost -d "$DB_NAME" -e "
    ALTER TABLE users SET LOCALITY REGIONAL BY TABLE IN '$PRIMARY_REGION';
    ALTER TABLE logins SET LOCALITY REGIONAL BY TABLE IN '$SECOND_REGION';
  "
fi

echo "📐 Creating non-public schema objects + VECTOR/JSONB columns + a sequence..."
cockroach sql --insecure --host=localhost -d "$DB_NAME" -e "
  CREATE SCHEMA IF NOT EXISTS cpkit;
  CREATE TABLE cpkit.tasks (id INT PRIMARY KEY, name STRING);
  INSERT INTO cpkit.tasks VALUES (1,'a'),(2,'b'),(3,'c');
  CREATE TABLE embeddings (id INT PRIMARY KEY, embd VECTOR(3));
  INSERT INTO embeddings VALUES (1,'[1.5,2,3.25]'),(2,'[0,0,0]');
  CREATE TABLE docs (id INT PRIMARY KEY, attrs JSONB, created_dtm TIMESTAMPTZ, created_d DATE);
  INSERT INTO docs VALUES (1, '{\"s3_keys\": {\"a\": []}}', '2021-08-02 15:39:18.5+00', '2021-08-02');
  CREATE SEQUENCE doc_key_seq;
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

echo "📦 Exporting schema (full-DB DDL) and data..."
$CRDB_DUMP --verbose export \
  --db="$DB_NAME" \
  --data \
  --data-format=csv \
  --chunk-size=1000 \
  --out-dir="$OUT_DIR"

echo "🔎 Asserting non-public schema + VECTOR were exported..."
test -f "$BASE_OUT_DIR/${DB_NAME}.cpkit.tasks.manifest.json" \
  && echo "✅ non-public schema (cpkit.tasks) exported" \
  || { echo "❌ non-public schema NOT exported"; exit 1; }
test -f "$BASE_OUT_DIR/${DB_NAME}.public.embeddings.manifest.json" \
  && echo "✅ VECTOR table (embeddings) exported" \
  || { echo "❌ VECTOR table NOT exported"; exit 1; }
if ls "$BASE_OUT_DIR"/*doc_key_seq* >/dev/null 2>&1; then
  echo "❌ sequence was data-exported (must not be)"; exit 1
else
  echo "✅ sequence not data-exported"
fi
grep -q '""s3_keys""' "$BASE_OUT_DIR/${DB_NAME}.public.docs_001.csv" \
  && echo "✅ JSONB exported as valid JSON (double-quoted)" \
  || { echo "❌ JSONB not valid JSON in CSV"; cat "$BASE_OUT_DIR/${DB_NAME}.public.docs_001.csv"; exit 1; }

echo "🕒 Testing --as-of-system-time (consistent snapshot)..."
$CRDB_DUMP --verbose export --db="$DB_NAME" --tables=public.users \
  --data --data-format=csv --as-of-system-time --out-dir="$OUT_DIR/aost"
$PYTHON - "$OUT_DIR/aost/$DB_NAME/${DB_NAME}.public.users.manifest.json" <<'PY'
import json, sys
m = json.load(open(sys.argv[1]))
assert m.get("as_of_system_time"), "manifest missing as_of_system_time"
print("✅ AOST timestamp recorded:", m["as_of_system_time"])
PY

echo "🛰️  Testing --as-of-system-time=follower (tolerant of entitlement)..."
if $CRDB_DUMP --verbose export --db="$DB_NAME" --tables=public.users \
     --data --data-format=csv --as-of-system-time=follower \
     --out-dir="$OUT_DIR/follower" 2>"$OUT_DIR/follower.err"; then
  echo "✅ follower-read export succeeded"
else
  if grep -q "Follower reads are not available" "$OUT_DIR/follower.err"; then
    echo "ℹ️  follower reads not entitled on this cluster — clean error, OK"
  else
    echo "❌ unexpected follower-read failure:"; cat "$OUT_DIR/follower.err"; exit 1
  fi
fi

echo "🧪 Verifying chunks..."
$CRDB_DUMP --verbose export \
  --db="$DB_NAME" \
  --verify \
  --out-dir="$OUT_DIR"

echo "🔍 Dry run import (should not write to DB)..."
$CRDB_DUMP --verbose load \
  --db="$DB_NAME" \
  --schema="$SCHEMA_FILE" \
  --data-dir="$DATA_DIR" \
  --resume-log="$RESUME_FILE" \
  --dry-run \
  --print-connection

echo "❌ Dropping entire database to prep for a clean full restore..."
cockroach sql --insecure --host=localhost -e "
  DROP DATABASE IF EXISTS $DB_NAME CASCADE;
  CREATE DATABASE $DB_NAME;
"
echo "✅ Database recreated empty"

echo "🧪 Full restore with --validate-csv and --parallel-load"
rm -rf "$OUT_DIR/resume-main"
$CRDB_DUMP --verbose load \
  --db="$DB_NAME" \
  --schema="$SCHEMA_FILE" \
  --data-dir="$DATA_DIR" \
  --resume-log-dir="$OUT_DIR/resume-main" \
  --validate-csv \
  --parallel-load

echo "🔎 Verifying restored row counts..."
for tbl in users logins cpkit.tasks embeddings docs; do
  CNT=$(cockroach sql --insecure --host=localhost -d "$DB_NAME" --format=csv -e "SELECT count(*) FROM $tbl" | tail -1)
  echo "  $tbl: $CNT rows"
  [ "$CNT" -gt 0 ] || { echo "❌ $tbl has no rows after restore"; exit 1; }
done
echo "✅ Full restore verified (all tables incl. non-public schema + VECTOR)"

echo "🔁 Testing data export variations..."
for ORDER_FLAG in "" "--data-order=id" "--data-order=id --data-order-desc"; do
  for PARALLEL in "" "--data-parallel"; do
    for LIMIT in 100 1000 ""; do
      echo "▶️ Exporting with $ORDER_FLAG $PARALLEL --data-limit=$LIMIT"
      OUT_SUBDIR="$OUT_DIR/variant_$(date +%s%N)"
      $CRDB_DUMP --verbose export \
        --db="$DB_NAME" \
        --per-table \
        --data \
        --data-format=csv \
        --data-split \
        --chunk-size=500 \
        $ORDER_FLAG $PARALLEL \
        ${LIMIT:+--data-limit=$LIMIT} \
        --out-dir="$OUT_SUBDIR"

      $CRDB_DUMP --verbose export --db="$DB_NAME" --verify --out-dir="$OUT_SUBDIR"
    done
  done
done

echo "🧪 Testing resume idempotency..."
echo "▶️ Re-running the load — every chunk should be SKIPPED (already loaded)..."
RESUME_OUT=$($CRDB_DUMP --verbose load \
  --db="$DB_NAME" \
  --data-dir="$DATA_DIR" \
  --resume-log-dir="$OUT_DIR/resume-main" \
  --validate-csv \
  --parallel-load \
  --resume-strict 2>&1)
echo "$RESUME_OUT" | grep -E "Loaded|Skipped|Failed" | tail -8
if echo "$RESUME_OUT" | grep -q "❌ Failed to load chunk"; then
  echo "❌ Resume pass unexpectedly reloaded/failed chunks"; exit 1
fi
echo "✅ Resume idempotency verified (no chunk reloaded)"

if [ "$MULTIREGION" = true ]; then
  echo "🌍 Exporting $PRIMARY_REGION only..."
  $CRDB_DUMP --verbose export \
    --db="$DB_NAME" \
    --data \
    --per-table \
    --region="$PRIMARY_REGION" \
    --data-format=csv \
    --chunk-size=1000 \
    --out-dir="$OUT_DIR/$PRIMARY_REGION"

  echo "🌍 Exporting $SECOND_REGION only..."
  $CRDB_DUMP --verbose export \
    --db="$DB_NAME" \
    --data \
    --per-table \
    --region="$SECOND_REGION" \
    --data-format=csv \
    --chunk-size=1000 \
    --out-dir="$OUT_DIR/$SECOND_REGION"
else
  echo "⏩ Skipping region-filtered exports (single-region cluster)"
fi

#echo "🪣 Creating test bucket in MinIO..."

#docker run --rm \
#  --network container:$MINIO_CONTAINER \
#  minio/mc alias set localminio http://localhost:9000 $MINIO_ACCESS_KEY $MINIO_SECRET_KEY

#docker run --rm \
#  --network container:$MINIO_CONTAINER \
#  minio/mc mb --ignore-existing localminio/$MINIO_BUCKET

echo "🪣 Creating test bucket in MinIO via boto3..."
$PYTHON - <<EOF
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
$CRDB_DUMP --verbose export \
  --db="$DB_NAME" \
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

echo "🧹 Recreating empty database before S3 restore..."
cockroach sql --insecure --host=localhost -e "
  DROP DATABASE IF EXISTS $DB_NAME CASCADE;
  CREATE DATABASE $DB_NAME;
"

echo "📐 Loading schema before S3 data import..."
$CRDB_DUMP --verbose load \
  --db="$DB_NAME" \
  --schema="$OUT_DIR/s3-test/$DB_NAME/${DB_NAME}_schema.sql"

echo "📥 Testing import from MinIO S3..."
$CRDB_DUMP --verbose load \
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

echo "🔎 Verifying S3-restored row counts..."
for tbl in users logins cpkit.tasks embeddings docs; do
  CNT=$(cockroach sql --insecure --host=localhost -d "$DB_NAME" --format=csv -e "SELECT count(*) FROM $tbl" | tail -1)
  echo "  $tbl: $CNT rows"
  [ "$CNT" -gt 0 ] || { echo "❌ $tbl has no rows after S3 restore"; exit 1; }
done

echo "📄 Log file written to: $LOG_FILE"
echo "✅ Done."

