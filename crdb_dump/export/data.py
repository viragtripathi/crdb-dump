import csv
import gzip
import hashlib
import json
import os
import click
from crdb_dump.utils.common import retry, get_table_locality
from crdb_dump.utils.s3 import get_s3_client, upload_file_to_s3
from concurrent.futures import ThreadPoolExecutor, as_completed
from sqlalchemy import text
from crdb_dump.export.schema import collect_objects
from crdb_dump.utils.db_connection import get_sqlalchemy_engine
from crdb_dump.utils.common import to_sql_literal, to_csv_literal, aost_clause
from crdb_dump.utils.identifiers import parse_object_name, quote_ident
from crdb_dump.utils.io import validate_fq_table_names


def export_table_data(engine, table, out_dir, export_format, split, limit, compress, order, order_desc,
                      chunk_size, order_strict, logger, locality_map, retry_count, retry_delay, opts):
    try:
        obj = parse_object_name(table, default_db=table.split('.')[0])
        base_name = obj.file_base()
        clause = aost_clause(opts.get("aost_resolved"))
        with retry(retries=retry_count, delay=retry_delay)(engine.connect)() as conn:
            if clause:
                # Each AOST read runs in its own transaction at the SAME pinned
                # timestamp. CockroachDB allows only one AOST per transaction, so
                # AUTOCOMMIT avoids "inconsistent AS OF SYSTEM TIME" across the
                # column and chunk queries while still giving a consistent snapshot.
                conn = conn.execution_options(isolation_level="AUTOCOMMIT")
            cols_res = conn.execute(text(
                "SELECT column_name, data_type FROM information_schema.columns" + clause +
                " WHERE table_name = :t AND table_schema = :s ORDER BY ordinal_position"
            ), {"t": obj.table, "s": obj.schema})
            col_rows = list(cols_res)
            columns = [row[0] for row in col_rows]
            # Column types let the encoders distinguish e.g. a JSONB array
            # (JSON-encode) from a SQL ARRAY (array literal).
            col_types = [row[1] for row in col_rows]
            if order:
                for col in order.split(','):
                    if col not in columns:
                        msg = f"Column '{col}' not found in table {table}"
                        if order_strict:
                            raise ValueError(msg)
                        else:
                            logger.warning(f"Skipping order for {table} — {msg}.")
                            order = None
                            order_clause = ""
                            break
                else:
                    order_clause = f"ORDER BY {order} DESC" if order_desc else f"ORDER BY {order}"
            else:
                order_clause = ""

            offset = 0
            batch_size = chunk_size if chunk_size else 1000
            total_rows = 0
            chunk_index = 1
            manifest = []

            def file_checksum(path):
                h = hashlib.sha256()
                with open(path, 'rb') as f:
                    for chunk in iter(lambda: f.read(8192), b''):
                        h.update(chunk)
                return h.hexdigest()

            while True:
                query = f"SELECT * FROM {obj.fq_quoted()}{clause} {order_clause} OFFSET {offset} LIMIT {batch_size}"
                if limit and offset >= limit:
                    break
                rows = conn.execute(text(query)).fetchall()
                if not rows:
                    break
                total_rows += len(rows)
                offset += batch_size

                out_path = os.path.join(
                    out_dir,
                    f"{base_name}_{chunk_index:03d}.csv.gz" if compress
                    else f"{base_name}_{chunk_index:03d}.csv"
                ) if export_format == 'csv' else os.path.join(
                    out_dir, f"{base_name}_{chunk_index:03d}.sql")

                if export_format == 'csv':
                    open_func = gzip.open if compress else open
                    mode = 'wt' if compress else 'w'
                    with open_func(out_path, mode, newline='') as f:
                        writer = csv.writer(f, lineterminator='\n')
                        writer.writerow(columns)
                        writer.writerows(
                            [[to_csv_literal(v, t) for v, t in zip(row, col_types)]
                             for row in rows])
                elif export_format == 'sql':
                    col_list = ", ".join(quote_ident(c) for c in columns)
                    with open(out_path, 'w') as f:
                        for row in rows:
                            vals = ", ".join(
                                to_sql_literal(v, t) for v, t in zip(row, col_types))
                            f.write(f"INSERT INTO {obj.fq_quoted()} ({col_list}) VALUES ({vals});\n")

                checksum = file_checksum(out_path)
                manifest.append({
                    "file": os.path.basename(out_path),
                    "rows": len(rows),
                    "sha256": checksum
                })

                # ✅ S3 Upload
                if opts.get("use_s3"):
                    s3 = get_s3_client(
                        endpoint_url=opts.get("s3_endpoint"),
                        access_key=opts.get("s3_access_key"),
                        secret_key=opts.get("s3_secret_key")
                    )
                    s3_key = f"{opts['s3_prefix']}{os.path.basename(out_path)}"
                    upload_file_to_s3(s3, opts["s3_bucket"], s3_key, out_path)
                    logger.info(f"☁️ Uploaded to S3: s3://{opts['s3_bucket']}/{s3_key}")

                logger.info(f"Exported data for {table} chunk {chunk_index} to {out_path} ({len(rows)} rows)")
                chunk_index += 1

                if limit and total_rows >= limit:
                    break

            manifest_path = os.path.join(out_dir, f"{base_name}.manifest.json")
            region = locality_map.get(table, "N/A")
            with open(manifest_path, 'w') as mf:
                json.dump({
                    "table": obj.fq_plain(),
                    "as_of_system_time": opts.get("aost_resolved"),
                    "region": region,
                    "chunks": manifest
                }, mf, indent=2)

            logger.info(f"🌍 Exporting {table} (region: {region})")
            logger.info(f"Wrote manifest for {table} to {manifest_path}")

            return total_rows

    except Exception as e:
        logger.error(f"Failed to export data for {table}: {e}")
        return 0


def export_data(opts, out_dir, logger):
    engine = get_sqlalchemy_engine(opts)

    retry_count = opts.get("retry_count", 3)
    retry_delay = opts.get("retry_delay", 1000) / 1000.0

    region_filter = opts.get("region")
    locality_map = get_table_locality(engine, opts["db"], logger)

    # Pin the AS OF SYSTEM TIME value ONCE so every table and chunk reads the same
    # consistent snapshot. "auto" captures a single cluster_logical_timestamp().
    aost = opts.get("aost")
    if aost == "auto":
        with engine.connect() as conn:
            aost = str(conn.execute(text("SELECT cluster_logical_timestamp()")).scalar())
    elif aost == "follower":
        try:
            # Cast to CRDB's native timestamp string (e.g. '2026-06-26 19:20:03.25+00')
            # which AOST accepts. The raw timestamptz would be reformatted by the
            # driver to '+00:00' (rejected), and ::DECIMAL is in seconds, not the
            # nanosecond scale AOST decimals use.
            with engine.connect() as conn:
                aost = str(conn.execute(
                    text("SELECT follower_read_timestamp()::STRING")).scalar())
        except Exception as e:
            raise click.UsageError(
                "Follower reads are not available on this cluster "
                "(requires a CockroachDB entitlement that enables follower reads): "
                f"{e}")
    if aost is not None:
        logger.info(f"🕒 Pinned AS OF SYSTEM TIME {aost}")
    opts["aost_resolved"] = aost

    if opts.get("print_connection"):
        print("🔗 Using CockroachDB URL:")
        print(str(engine.url))
        return

    if opts['tables']:
        # Normalize user-provided names (table / schema.table / db.schema.table)
        # to three-part db.schema.table using --db as the default database.
        table_list = validate_fq_table_names(opts['tables'].split(','), opts['db'])
    else:
        table_list = collect_objects(engine, opts['db'], 'table', logger, retry_count, retry_delay)

    if region_filter:
        def region_matches(fqname):
            loc = locality_map.get(fqname, "").upper()
            return region_filter.upper() in loc

        before = len(table_list)
        table_list = [t for t in table_list if region_matches(t)]
        logger.info(f"📍 Region filter: {region_filter} — selected {len(table_list)}/{before} tables")

    wrapped_export = lambda *args: export_table_data(*args, locality_map, retry_count, retry_delay, opts)

    data_tasks = [
        (engine, table, out_dir, opts['data_format'], opts['data_split'], opts['data_limit'],
         opts['data_compress'], opts['data_order'], opts['data_order_desc'],
         opts['chunk_size'], opts['data_order_strict'], logger)
        for table in table_list
    ]

    if opts['data_parallel']:
        results = []
        with ThreadPoolExecutor() as executor:
            futures = [executor.submit(wrapped_export, *args) for args in data_tasks]
            for future in as_completed(futures):
                results.append(future.result())
    else:
        results = [wrapped_export(*args) for args in data_tasks]

    table_row_counts = {t[1]: count for t, count in zip(data_tasks, results)}
    total_rows = sum(table_row_counts.values())

    for table, count in table_row_counts.items():
        logger.info(f" - {table}: {count} rows")
    logger.info(f"✅ Total rows exported: {total_rows}")
