import os
import json
import hashlib


def verify_checksums(opts, out_dir, logger):
    table_list = opts['tables'].split(',') if opts['tables'] else []
    if not table_list:
        from crdb_dump.export.schema import collect_objects
        from sqlalchemy import create_engine

        engine = create_engine(
            f"cockroachdb://root@{opts['host']}:26257/{opts['db']}"
            + (f"?sslmode=verify-full&sslrootcert={opts['certs_dir']}/ca.crt"
               f"&sslcert={opts['certs_dir']}/client.root.crt"
               f"&sslkey={opts['certs_dir']}/client.root.key" if opts['certs_dir'] else "?sslmode=disable")
        )
        table_list = collect_objects(engine, opts['db'], 'table', logger)

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
