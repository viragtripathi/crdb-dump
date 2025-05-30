import os
import json
import hashlib
from crdb_dump.export.schema import collect_objects
from crdb_dump.utils.db_connection import get_sqlalchemy_engine


def verify_checksums(opts, out_dir, logger):
    retry_count = opts.get("retry_count", 3)
    retry_delay = opts.get("retry_delay", 1000) / 1000.0  # Convert ms to seconds

    table_list = opts['tables'].split(',') if opts['tables'] else []
    if not table_list:
        engine = get_sqlalchemy_engine(opts)
        table_list = collect_objects(engine, opts['db'], 'table', logger, retry_count, retry_delay)

    failed = 0
    passed = 0
    missing = 0

    for table in table_list:
        base_name = table.split('.')[-1]
        manifest_path = os.path.join(out_dir, f"{base_name}.manifest.json")
        if not os.path.exists(manifest_path):
            logger.warning(f"No manifest found for {base_name}")
            missing += 1
            continue

        with open(manifest_path) as mf:
            manifest = json.load(mf)
            for chunk in manifest['chunks']:
                file_path = os.path.join(out_dir, chunk['file'])
                if not os.path.exists(file_path):
                    logger.error(f"Missing chunk: {file_path}")
                    missing += 1
                    continue

                h = hashlib.sha256()
                with open(file_path, 'rb') as f:
                    for part in iter(lambda: f.read(8192), b''):
                        h.update(part)
                actual = h.hexdigest()
                if actual != chunk['sha256']:
                    logger.error(f"Checksum mismatch for {file_path}")
                    failed += 1
                    if opts.get('verify_strict'):
                        raise ValueError(f"Checksum failed for {file_path}")
                else:
                    logger.info(f"✔️ Verified {file_path}")
                    passed += 1

    logger.info(f"✅ Checksum verification complete: {passed} passed, {failed} failed, {missing} missing")
