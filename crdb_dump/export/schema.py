import json
from concurrent.futures import ThreadPoolExecutor

import yaml
from sqlalchemy import text

from crdb_dump.utils.db_connection import get_sqlalchemy_engine
from crdb_dump.utils.io import write_file

def dump_create_statement(engine, obj_type, full_name, logger):
    try:
        db = full_name.split('.')[0]
        short_name = full_name.split('.')[-1]
        with engine.connect() as conn:
            conn.execute(text(f"USE {db}"))
            if obj_type == "TYPE":
                result = conn.execute(text("SHOW CREATE ALL TYPES"))
                matches = [
                    row[0] for row in result
                    if row[0].startswith("CREATE TYPE") and f".{short_name} " in row[0]
                ]
                if matches:
                    return matches[0] + ";\n"
                logger.warning(f"Type {short_name} not found in SHOW CREATE ALL TYPES output")
                return None
            else:
                result = conn.execute(text(f"SHOW CREATE {obj_type} {short_name}"))
                rows = list(result)
                if rows and len(rows[0]) > 1:
                    return rows[0][1] + ";\n"
                else:
                    logger.warning(f"No DDL returned for {obj_type} {full_name}")
                    return None
    except Exception as e:
        logger.error(f"Failed to get DDL for {obj_type} {full_name}: {e}")
        return None

def collect_objects(engine, db, obj_type, logger):
    query_map = {
        'table': "SHOW TABLES",
        'view': "SELECT table_name FROM information_schema.views WHERE table_schema NOT IN ('pg_catalog', 'information_schema')",
        'sequence': "SHOW SEQUENCES",
        'type': "SHOW TYPES"
    }
    objs = []
    try:
        with engine.connect() as conn:
            conn.execute(text(f"USE {db}"))
            result = conn.execute(text(query_map[obj_type]))
            for row in result:
                name = row[0] if obj_type == 'view' else row[1] if obj_type in ['table', 'sequence', 'type'] else None
                if name:
                    if obj_type == 'type':
                        enum_check = conn.execute(
                            text(f"SELECT * FROM pg_type WHERE typname = '{name}' AND typtype = 'e'")
                        ).fetchall()
                        if not enum_check:
                            logger.warning(f"Skipping non-enum type: {name}")
                            continue
                    objs.append(f"{db}.{name}")
    except Exception as e:
        logger.error(f"Error fetching {obj_type}s: {e}")
    return objs

def export_schema(opts, out_dir, logger):

    engine = get_sqlalchemy_engine(opts)

    db = opts['db']
    parallel = opts['parallel']
    per_table = opts['per_table']
    out_format = opts['out_format']

    tables = collect_objects(engine, db, 'table', logger)
    views = collect_objects(engine, db, 'view', logger)
    sequences = collect_objects(engine, db, 'sequence', logger)
    types = collect_objects(engine, db, 'type', logger)

    all_objects = [("TABLE", name) for name in tables] + \
                  [("VIEW", name) for name in views] + \
                  [("SEQUENCE", name) for name in sequences] + \
                  [("TYPE", name) for name in types]

    dump_data = []

    def process(obj_type, full_name):
        ddl = dump_create_statement(engine, obj_type, full_name, logger)
        if ddl:
            if out_format in ["json", "yaml"]:
                dump_data.append({"name": full_name, "type": obj_type, "ddl": ddl.strip()})
            elif per_table:
                filename = f"{out_dir}/{obj_type.lower()}_{full_name.split('.')[-1]}.sql"
                write_file(filename, f"-- {obj_type}: {full_name}\n{ddl}\n")
            else:
                with open(f"{out_dir}/{db}_schema.sql", "a") as f:
                    f.write(f"-- {obj_type}: {full_name}\n{ddl}\n\n")

    if parallel:
        with ThreadPoolExecutor() as executor:
            list(executor.map(lambda args: process(*args), all_objects))
    else:
        for obj in all_objects:
            process(*obj)

    if out_format == "json":
        write_file(f"{out_dir}/{db}_schema.json", json.dumps(dump_data, indent=2))
    elif out_format == "yaml":
        write_file(f"{out_dir}/{db}_schema.yaml", yaml.dump(dump_data))
