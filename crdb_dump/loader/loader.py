import csv
import psycopg2.extras
import json
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from crdb_dump.utils.common import retry
from sqlalchemy import text
from crdb_dump.utils.db_connection import get_psycopg_connection


def load_schema(schema_path, engine, logger):
    if not os.path.exists(schema_path):
        logger.error(f"Schema file not found: {schema_path}")
        return False

    with open(schema_path) as f:
        sql = f.read()
    statements = [stmt.strip() for stmt in sql.split(';') if stmt.strip()]

    try:
        with engine.begin() as conn:
            for stmt in statements:
                conn.execute(text(stmt))
        logger.info(f"✅ Loaded schema from {schema_path}")
        return True
    except Exception as e:
        logger.error(f"❌ Failed to load schema: {e}")
        return False

def validate_csv_header(table, filepath, logger):
    conn = get_psycopg_connection()
    with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
        cur.execute("SELECT column_name FROM information_schema.columns WHERE table_name = %s ORDER BY ordinal_position", (table.split('.')[-1],))
        db_columns = [row[0] for row in cur.fetchall()]

    with open(filepath, 'r') as f:
        reader = csv.reader(f)
        csv_header = next(reader)

    if db_columns != csv_header:
        logger.warning(f"Header mismatch for {table}:\nDB:   {db_columns}\nFile: {csv_header}")
        return False
    return True

def load_chunk(table, file_path, engine, logger, validate=False):
    try:
        if validate and not validate_csv_header(table, file_path, logger):
            logger.error(f"Skipping load for {file_path} due to header mismatch.")
            return False

        conn = get_psycopg_connection()
        with conn.cursor() as cur:
            with open(file_path, "r") as f:
                sql = f"COPY {table} FROM STDIN WITH CSV HEADER"
                cur.copy_expert(sql, f)
        conn.commit()
        conn.close()
        logger.info(f"✔️ Loaded chunk: {file_path}")
        return True
    except Exception as e:
        logger.error(f"❌ Failed to load chunk {file_path}: {e}")
        return False


def load_chunks_from_manifest(manifest_path, data_dir, engine, logger, resume_file=None, parallel=False, validate=False, retry_count=3, retry_delay=1.0):
    table_loaded = 0
    skipped = 0
    failed = 0

    wrapped_load_chunk = retry(retries=retry_count, delay=retry_delay)(load_chunk)

    with open(manifest_path) as mf:
        manifest = json.load(mf)
    table = manifest['table']
    log_key = table.replace('.', '_')

    loaded_chunks = set()
    if resume_file and os.path.exists(resume_file):
        with open(resume_file) as f:
            loaded_chunks = set(json.load(f).get(log_key, []))

    tasks = []
    for chunk in manifest['chunks']:
        chunk_file = os.path.join(data_dir, chunk['file'])
        if chunk['file'] in loaded_chunks:
            logger.info(f"⏩ Skipped already loaded: {chunk['file']}")
            skipped += 1
            continue
        tasks.append((table, chunk_file))

    def _load_task(table, path):
        success = wrapped_load_chunk(table, path, engine, logger, validate=validate)
        return path, success

    if parallel:
        with ThreadPoolExecutor() as executor:
            future_to_path = {executor.submit(_load_task, t, p): p for t, p in tasks}
            for future in as_completed(future_to_path):
                path, success = future.result()
                if success:
                    table_loaded += 1
                    loaded_chunks.add(os.path.basename(path))
                else:
                    failed += 1
    else:
        for table, path in tasks:
            success = wrapped_load_chunk(table, path, engine, logger, validate=validate)
            if success:
                table_loaded += 1
                loaded_chunks.add(os.path.basename(path))
            else:
                failed += 1

    if resume_file:
        _update_resume_log(resume_file, log_key, loaded_chunks)

    logger.info(f"✅ Loaded {table_loaded} chunks | ⏩ Skipped: {skipped} | ❌ Failed: {failed}")
    return table_loaded, skipped, failed


def _update_resume_log(resume_file, key, chunk_set):
    log_data = {}
    if os.path.exists(resume_file):
        with open(resume_file) as f:
            log_data = json.load(f)
    log_data[key] = list(chunk_set)
    with open(resume_file, 'w') as f:
        json.dump(log_data, f, indent=2)
