import csv
import gzip
import hashlib
import json
import os
from crdb_dump.utils.common import retry
from concurrent.futures import ThreadPoolExecutor, as_completed
from sqlalchemy import text
from crdb_dump.export.schema import collect_objects
from crdb_dump.utils.db_connection import get_sqlalchemy_engine
from crdb_dump.utils.common import to_sql_literal, to_csv_literal


def export_table_data(engine, table, out_dir, export_format, split, limit, compress, order, order_desc, chunk_size, order_strict, logger, retry_count, retry_delay):
    try:
        db, tbl = table.split('.')
        base_name = tbl.replace('.', '_')
        with retry(retries=retry_count, delay=retry_delay)(engine.connect)() as conn:
            cols_res = conn.execute(text(f"SELECT column_name FROM information_schema.columns WHERE table_name='{tbl}'"))
            columns = [row[0] for row in cols_res]
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
                query = f"SELECT * FROM {table} {order_clause} OFFSET {offset} LIMIT {batch_size}"
                if limit and offset >= limit:
                    break
                rows = conn.execute(text(query)).fetchall()
                if not rows:
                    break
                total_rows += len(rows)
                offset += batch_size

                out_path = os.path.join(
                    out_dir,
                    f"{base_name}_chunk_{chunk_index:03d}.csv.gz" if compress else f"{base_name}_chunk_{chunk_index:03d}.csv"
                ) if export_format == 'csv' else os.path.join(out_dir, f"{base_name}_chunk_{chunk_index:03d}_data.sql")

                if export_format == 'csv':
                    open_func = gzip.open if compress else open
                    mode = 'wt' if compress else 'w'
                    with open_func(out_path, mode, newline='') as f:
                        writer = csv.writer(f, lineterminator='\n')
                        writer.writerow(columns)
                        writer.writerows([[to_csv_literal(v) for v in row] for row in rows])
                elif export_format == 'sql':
                    with open(out_path, 'w') as f:
                        for row in rows:
                            vals = ", ".join(to_sql_literal(v) for v in row)
                            f.write(f"INSERT INTO {tbl} ({', '.join(columns)}) VALUES ({vals});\n")

                checksum = file_checksum(out_path)
                manifest.append({
                    "file": os.path.basename(out_path),
                    "rows": len(rows),
                    "sha256": checksum
                })
                logger.info(f"Exported data for {table} chunk {chunk_index} to {out_path} ({len(rows)} rows)")
                chunk_index += 1

                if limit and total_rows >= limit:
                    break

            manifest_path = os.path.join(out_dir, f"{base_name}.manifest.json")
            with open(manifest_path, 'w') as mf:
                json.dump({"table": table, "chunks": manifest}, mf, indent=2)
            logger.info(f"Wrote manifest for {table} to {manifest_path}")

            return total_rows

    except Exception as e:
        logger.error(f"Failed to export data for {table}: {e}")
        return 0


def export_data(opts, out_dir, logger):
    engine = get_sqlalchemy_engine(opts)

    retry_count = opts.get("retry_count", 3)
    retry_delay = opts.get("retry_delay", 1000) / 1000.0  # Convert ms to seconds

    if opts.get("print_connection"):
        print("🔗 Using CockroachDB URL:")
        print(str(engine.url))
        return

    table_list = opts['tables'].split(',') if opts['tables'] else []
    if not table_list:
        table_list = collect_objects(engine, opts['db'], 'table', logger, retry_count, retry_delay)

    wrapped_export = lambda *args: export_table_data(*args, retry_count, retry_delay)

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

    table_row_counts = {tbl.split('.')[-1]: count for tbl, count in zip([t[1] for t in data_tasks], results)}
    total_rows = sum(table_row_counts.values())

    for table, count in table_row_counts.items():
        logger.info(f" - {table}: {count} rows")
    logger.info(f"✅ Total rows exported: {total_rows}")
