import json
import os
import yaml
from crdb_dump.utils.common import retry
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
from sqlalchemy import text
from crdb_dump.utils.db_connection import get_sqlalchemy_engine
from crdb_dump.utils.common import to_json_literal
from crdb_dump.utils.io import write_file, archive_output, normalize_filename, validate_fq_table_names

def dump_create_statement(engine, obj_type, full_name, logger, retry_count, retry_delay):
    try:
        db = full_name.split('.')[0]
        short_name = full_name.split('.')[-1]
        with retry(retries=retry_count, delay=retry_delay)(engine.connect)() as conn:
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

def collect_objects(engine, db, obj_type, logger, retry_count, retry_delay):
    query_map = {
        'table': "SHOW TABLES",
        'view': "SELECT table_name FROM information_schema.views WHERE table_schema NOT IN ('pg_catalog', 'information_schema')",
        'sequence': "SHOW SEQUENCES",
        'type': "SHOW TYPES"
    }
    objs = []
    try:
        with retry(retries=retry_count, delay=retry_delay)(engine.connect)() as conn:
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

def resolve_object_types(engine, object_names, logger, retry_count, retry_delay):
    mapping = {}
    with retry(retries=retry_count, delay=retry_delay)(engine.connect)() as conn:
        for obj in object_names:
            db, name = obj.split('.')
            kind = None

            try:
                # Check TABLE
                res = conn.execute(text(
                    f"SELECT table_name FROM information_schema.tables "
                    f"WHERE table_schema = 'public' AND table_name = :name"
                ), {"name": name}).fetchone()
                if res:
                    kind = "TABLE"
            except Exception as e:
                logger.debug(f"Error checking table for {obj}: {e}")

            if not kind:
                try:
                    # Check VIEW
                    res = conn.execute(text(
                        f"SELECT table_name FROM information_schema.views "
                        f"WHERE table_schema = 'public' AND table_name = :name"
                    ), {"name": name}).fetchone()
                    if res:
                        kind = "VIEW"
                except:
                    pass

            if not kind:
                try:
                    # Check TYPE
                    res = conn.execute(text(
                        f"SELECT typname FROM pg_type WHERE typname = :name"
                    ), {"name": name}).fetchone()
                    if res:
                        kind = "TYPE"
                except:
                    pass

            if not kind:
                try:
                    # Check SEQUENCE
                    res = conn.execute(text(f"SHOW SEQUENCES")).fetchall()
                    seqs = [row[1] for row in res]  # db.schema.name format
                    if name in [s.split('.')[-1] for s in seqs]:
                        kind = "SEQUENCE"
                except:
                    pass

            if kind:
                mapping[obj] = kind
            else:
                logger.warning(f"⚠️ Could not determine type of object: {obj}")

    return mapping

def export_schema(opts, out_dir, logger):
    engine = get_sqlalchemy_engine(opts)
    db = opts["db"]
    parallel = opts.get("parallel", False)
    per_table = opts.get("per_table", False)
    out_format = opts.get("out_format", "sql")

    os.makedirs(out_dir, exist_ok=True)

    include = opts.get("tables")
    exclude = opts.get("exclude_tables")

    retry_count = opts.get("retry_count", 3)
    retry_delay = opts.get("retry_delay", 1000) / 1000.0

    # Wrap retry around critical functions
    wrapped_dump_create = lambda *args: dump_create_statement(*args, retry_count, retry_delay)
    wrapped_collect_objects = lambda *args: collect_objects(*args, retry_count, retry_delay)
    wrapped_resolve_types = lambda *args: resolve_object_types(*args, retry_count, retry_delay)
    wrapped_dump_permissions = lambda *args: dump_permissions(*args, retry_count, retry_delay)

    if include and exclude:
        raise click.UsageError("You cannot use --tables and --exclude-tables at the same time.")

    if include:
        tables_fq = validate_fq_table_names(include.split(','), db)
        object_map = wrapped_resolve_types(engine, tables_fq, logger)
        all_objects = [(typ, fqname) for fqname, typ in object_map.items()]
    else:
        tables = wrapped_collect_objects(engine, db, 'table', logger)
        views = wrapped_collect_objects(engine, db, 'view', logger)
        sequences = wrapped_collect_objects(engine, db, 'sequence', logger)
        types = wrapped_collect_objects(engine, db, 'type', logger)

        all_objects = [("TABLE", name) for name in tables] + \
                      [("VIEW", name) for name in views] + \
                      [("SEQUENCE", name) for name in sequences] + \
                      [("TYPE", name) for name in types]

        if exclude:
            exclude_set = set(validate_fq_table_names(exclude.split(','), db))
            all_objects = [obj for obj in all_objects if obj[1] not in exclude_set]

    if opts.get("include_permissions"):
        wrapped_dump_permissions(engine, out_dir, logger)

    dump_data = []

    def process(obj_type, full_name):
        ddl = wrapped_dump_create(engine, obj_type, full_name, logger)
        if ddl:
            entry = {"name": full_name, "type": obj_type, "ddl": ddl.strip()}
            dump_data.append(entry)

        if per_table and out_format == "sql":
            filename = f"{out_dir}/{obj_type.lower()}_{full_name.split('.')[-1]}.sql"
            write_file(filename, f"-- {obj_type}: {full_name}\n{ddl}\n")
            logger.info(f"Wrote: {filename}")
        elif not per_table and out_format == "sql":
            filename = f"{out_dir}/{db}_schema.sql"
            with open(filename, "a") as f:
                f.write(f"-- {obj_type}: {full_name}\n{ddl}\n\n")
            logger.info(f"Wrote: {filename}")

    if parallel:
        with ThreadPoolExecutor() as executor:
            list(executor.map(lambda args: process(*args), all_objects))
    else:
        for obj in all_objects:
            process(*obj)

    if out_format == "json":
        write_file(f"{out_dir}/{db}_schema.json", json.dumps(to_json_literal(dump_data), indent=2))
    elif out_format == "yaml":
        write_file(f"{out_dir}/{db}_schema.yaml", yaml.dump(to_json_literal(dump_data), sort_keys=False))

def dump_permissions(engine, out_dir, logger, retry_count, retry_delay):
    try:
        roles = []
        grants = []
        memberships = []

        roles_file = f"{out_dir}/roles.sql"
        grants_file = f"{out_dir}/grants.sql"
        memberships_file = f"{out_dir}/role_memberships.sql"

        with retry(retries=retry_count, delay=retry_delay)(engine.connect)() as conn:
            # -- ROLES
            roles_result = conn.execute(text("SHOW ROLES"))
            for row in roles_result:
                role_name = row[0]
                roles.append(f"CREATE ROLE {role_name};")

            # -- OBJECT GRANTS
            grants_result = conn.execute(text("SHOW GRANTS"))
            for row in grants_result:
                grantee = row[0]
                object_type = row[1]
                object_name = row[2]
                privileges = row[3]
                grants.append(f"GRANT {privileges} ON {object_type} {object_name} TO {grantee};")

            # -- ROLE MEMBERSHIPS
            memberships_result = conn.execute(text("SHOW GRANTS ON ROLE"))
            for row in memberships_result:
                role = row[0]
                member = row[1]
                is_admin = row[2]
                stmt = f"GRANT {role} TO {member}"
                if is_admin:
                    stmt += " WITH ADMIN OPTION"
                stmt += ";"
                memberships.append(stmt)

        # Write individual files
        if roles:
            write_file(roles_file, "\n".join(roles) + "\n")
            logger.info(f"Wrote: {roles_file}")

        if grants:
            write_file(grants_file, "\n".join(grants) + "\n")
            logger.info(f"Wrote: {grants_file}")

        if memberships:
            write_file(memberships_file, "\n".join(memberships) + "\n")
            logger.info(f"Wrote: {memberships_file}")

        # Aggregate file
        all_lines = [
            f"-- Exported at: {datetime.utcnow().isoformat()} UTC",
            "-- ROLES --",
            *roles,
            "",
            "-- GRANTS --",
            *grants,
            "",
            "-- ROLE MEMBERSHIPS --",
            *memberships,
            ""
        ]
        write_file(f"{out_dir}/permissions.sql", "\n".join(all_lines))
        logger.info("✅ Wrote permissions to permissions.sql (and supporting files)")

    except Exception as e:
        logger.warning(f"⚠️ Failed to export permissions: {e}")
