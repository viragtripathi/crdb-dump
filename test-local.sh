#!/bin/bash
set -euo pipefail

DB_NAME="defaultdb"
OUT_DIR="tmp/export-test"
BASE_OUT_DIR="$OUT_DIR/$DB_NAME"
SCHEMA_FILE="$BASE_OUT_DIR/defaultdb_schema.sql"
DATA_DIR="$BASE_OUT_DIR"
LOG_FILE="logs/crdb_dump.log"
RESUME_FILE="$OUT_DIR/resume.json"

echo "🧹 Cleaning up old output..."
rm -rf "$OUT_DIR" logs/ "$RESUME_FILE"

echo "❌ Dropping and recreating tables..."
cockroach sql --insecure --host=localhost -d "$DB_NAME" -e "
  DROP TABLE IF EXISTS users;
  DROP TABLE IF EXISTS logins;

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

echo "📄 Log file written to: $LOG_FILE"
echo "✅ Done."

