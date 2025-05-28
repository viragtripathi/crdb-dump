import json
import os

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


def load_chunk(table, file_path, engine, logger):

    try:
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


def load_chunks_from_manifest(manifest_path, data_dir, engine, logger, resume_file=None):
    table_loaded = 0
    skipped = 0
    failed = 0

    with open(manifest_path) as mf:
        manifest = json.load(mf)
    table = manifest['table']
    log_key = table.replace('.', '_')

    loaded_chunks = set()
    if resume_file and os.path.exists(resume_file):
        with open(resume_file) as f:
            loaded_chunks = set(json.load(f).get(log_key, []))

    for chunk in manifest['chunks']:
        chunk_file = os.path.join(data_dir, chunk['file'])
        if chunk['file'] in loaded_chunks:
            logger.info(f"⏩ Skipped already loaded: {chunk['file']}")
            skipped += 1
            continue

        success = load_chunk(table, chunk_file, engine, logger)
        if success:
            table_loaded += 1
            loaded_chunks.add(chunk['file'])
            if resume_file:
                _update_resume_log(resume_file, log_key, loaded_chunks)
        else:
            failed += 1

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
