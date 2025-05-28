#!/bin/bash
set -euo pipefail

DB_NAME="defaultdb"
OUT_DIR="tmp/export-test"
SCHEMA_FILE="$OUT_DIR/$DB_NAME/defaultdb_schema.sql"
DATA_DIR="$OUT_DIR/$DB_NAME"
LOG_FILE="logs/crdb_dump.log"
RESUME_FILE="$OUT_DIR/resume.json"

echo "🧹 Cleaning up old output..."
rm -rf "$OUT_DIR" logs/ "$RESUME_FILE"

echo "❌ Dropping and recreating tables..."
cockroach sql --insecure --host=localhost -d "$DB_NAME" -e "
  DROP TABLE IF EXISTS users;
  DROP TABLE IF EXISTS logins;

  CREATE TABLE users (
    username STRING PRIMARY KEY,
    password_hash BYTES,
    salt BYTES
  );

  CREATE TABLE logins (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    username STRING,
    login_at TIMESTAMP DEFAULT now()
  );
"

echo "🧪 Inserting test data..."
cockroach sql --insecure --host=localhost -d "$DB_NAME" -e "
  INSERT INTO users (username, password_hash, salt)
  VALUES
    ('alice', decode('a1b2c3', 'hex'), decode('01', 'hex')),
    ('bob',   decode('ddee', 'hex'),   decode('02', 'hex'));

  INSERT INTO logins (username)
  VALUES ('alice'), ('bob'), ('carol');
"

echo "📦 Exporting schema and data..."
crdb-dump export \
  --db="$DB_NAME" \
  --per-table \
  --data \
  --data-format=csv \
  --chunk-size=1000 \
  --out-dir="$OUT_DIR"

echo "🧪 Verifying chunks..."
crdb-dump export \
  --db="$DB_NAME" \
  --verify \
  --out-dir="$OUT_DIR"

echo "🔍 Dry run import (should not write to DB)..."
crdb-dump load \
  --db="$DB_NAME" \
  --schema="$SCHEMA_FILE" \
  --data-dir="$DATA_DIR" \
  --resume-log="$RESUME_FILE" \
  --dry-run \
  --print-connection

echo "❌ Dropping tables to prep for import..."
cockroach sql --insecure --host=localhost -d "$DB_NAME" -e "
  DROP TABLE IF EXISTS logins;
  DROP TABLE IF EXISTS users;
"

echo "🚀 Loading schema and data..."
crdb-dump load \
  --db="$DB_NAME" \
  --schema="$SCHEMA_FILE" \
  --data-dir="$DATA_DIR" \
  --resume-log="$RESUME_FILE"

echo "🔍 Verifying loaded users..."
cockroach sql --insecure --host=localhost -d "$DB_NAME" -e "SELECT * FROM users"

echo "🔍 Verifying loaded logins..."
cockroach sql --insecure --host=localhost -d "$DB_NAME" -e "SELECT * FROM logins"

echo "📄 Log file written to: $LOG_FILE"
echo "✅ Done."

