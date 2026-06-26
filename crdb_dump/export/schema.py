import json
import os
import click
import yaml
from crdb_dump.utils.common import retry
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor
from sqlalchemy import text
from crdb_dump.utils.db_connection import get_sqlalchemy_engine
from crdb_dump.utils.common import to_json_literal, get_table_locality
from crdb_dump.utils.io import write_file, archive_output, normalize_filename, validate_fq_table_names
from crdb_dump.utils.identifiers import ObjectName, parse_object_name, quote_ident

def dump_all_ddl(engine, db, logger, retry_count, retry_delay):
    """Return full-database DDL via native bulk SHOW CREATE statements.

    Emits ``SHOW CREATE ALL TYPES`` first (enums), then ``SHOW CREATE ALL TABLES``
    which CockroachDB returns dependency-ordered (sequences -> tables -> views) with
    foreign-key constraints split into trailing ALTER ... ADD CONSTRAINT ... VALIDATE.
    """
    parts = []
    with retry(retries=retry_count, delay=retry_delay)(engine.connect)() as conn:
        conn.execute(text(f"USE {quote_ident(db)}"))
        try:
            types = conn.execute(text("SHOW CREATE ALL TYPES"))
            for row in types:
                stmt = row[0]
                if stmt and stmt.strip():
                    parts.append(stmt.rstrip().rstrip(";") + ";")
        except Exception as e:
            logger.warning(f"⚠️ SHOW CREATE ALL TYPES failed: {e}")
        tables = conn.execute(text("SHOW CREATE ALL TABLES"))
        for row in tables:
            stmt = row[0]
            if stmt and stmt.strip():
                parts.append(stmt.rstrip().rstrip(";") + ";")
    return "\n".join(parts) + ("\n" if parts else "")


def dump_create_statement(engine, obj_type, full_name, logger, retry_count, retry_delay):
    obj = parse_object_name(full_name, default_db=full_name.split('.')[0])
    try:
        with retry(retries=retry_count, delay=retry_delay)(engine.connect)() as conn:
            conn.execute(text(f"USE {quote_ident(obj.database)}"))
            if obj_type == "TYPE":
                result = conn.execute(text("SHOW CREATE ALL TYPES"))
                matches = [
                    row[0] for row in result
                    if row[0] and row[0].startswith("CREATE TYPE") and f".{obj.table} " in row[0]
                ]
                if matches:
                    return matches[0].rstrip().rstrip(";") + ";\n"
                if not obj.table.startswith("crdb_internal"):
                    logger.warning(f"Type {obj.fq_plain()} not found in SHOW CREATE ALL TYPES output")
                else:
                    logger.info(f"Skipping internal type: {obj.fq_plain()}")
                return None
            else:
                result = conn.execute(text(f"SHOW CREATE {obj_type} {obj.fq_quoted()}"))
                rows = list(result)
                if rows and len(rows[0]) > 1:
                    return rows[0][1].rstrip().rstrip(";") + ";\n"
                else:
                    logger.warning(f"No DDL returned for {obj_type} {obj.fq_plain()}")
                    return None
    except Exception as e:
        logger.error(f"Failed to get DDL for {obj_type} {full_name}: {e}")
        return None

def collect_objects(engine, db, obj_type, logger, retry_count, retry_delay):
    # SHOW TABLES/SEQUENCES/TYPES expose (schema, name, ...) in columns 0 and 1.
    query_map = {
        'table': "SHOW TABLES",
        'view': ("SELECT table_schema, table_name FROM information_schema.views "
                 "WHERE table_schema NOT IN ('pg_catalog', 'information_schema', 'crdb_internal')"),
        'sequence': "SHOW SEQUENCES",
        'type': "SHOW TYPES",
    }
    objs = []
    try:
        with retry(retries=retry_count, delay=retry_delay)(engine.connect)() as conn:
            conn.execute(text(f"USE {quote_ident(db)}"))
            result = conn.execute(text(query_map[obj_type]))
            for row in result:
                if obj_type in ('view', 'table', 'sequence', 'type'):
                    schema, name = row[0], row[1]
                else:
                    continue
                if not name:
                    continue
                if obj_type == 'type':
                    enum_check = conn.execute(
                        text("SELECT 1 FROM pg_type WHERE typname = :n AND typtype = 'e'"),
                        {"n": name},
                    ).fetchall()
                    if not enum_check:
                        logger.warning(f"Skipping non-enum type: {schema}.{name}")
                        continue
                objs.append(f"{db}.{schema}.{name}")
    except Exception as e:
        logger.error(f"Error fetching {obj_type}s: {e}")
    return objs

def resolve_object_types(engine, object_names, logger, retry_count, retry_delay):
    mapping = {}
    with retry(retries=retry_count, delay=retry_delay)(engine.connect)() as conn:
        for obj_str in object_names:
            obj = parse_object_name(obj_str, default_db=obj_str.split('.')[0])
            conn.execute(text(f"USE {quote_ident(obj.database)}"))
            kind = None

            try:
                res = conn.execute(text(
                    "SELECT 1 FROM information_schema.tables "
                    "WHERE table_schema = :s AND table_name = :n"
                ), {"s": obj.schema, "n": obj.table}).fetchone()
                if res:
                    kind = "TABLE"
            except Exception as e:
                logger.debug(f"Error checking table for {obj.fq_plain()}: {e}")

            if not kind:
                try:
                    res = conn.execute(text(
                        "SELECT 1 FROM information_schema.views "
                        "WHERE table_schema = :s AND table_name = :n"
                    ), {"s": obj.schema, "n": obj.table}).fetchone()
                    if res:
                        kind = "VIEW"
                except Exception:
                    pass

            if not kind:
                try:
                    res = conn.execute(text(
                        "SELECT 1 FROM pg_type WHERE typname = :n"
                    ), {"n": obj.table}).fetchone()
                    if res:
                        kind = "TYPE"
                except Exception:
                    pass

            if not kind:
                try:
                    res = conn.execute(text("SHOW SEQUENCES")).fetchall()
                    if obj.table in [row[1] for row in res]:
                        kind = "SEQUENCE"
                except Exception:
                    pass

            if kind:
                mapping[obj.fq_plain()] = kind
            else:
                logger.warning(f"⚠️ Could not determine type of object: {obj.fq_plain()}")

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

    region_filter = opts.get("region")
    locality_map = get_table_locality(engine, db, logger)

    if include and exclude:
        raise click.UsageError("You cannot use --tables and --exclude-tables at the same time.")

    aggregate_file = f"{out_dir}/{db}_schema.sql"

    # Full-database, unfiltered, single-file SQL output -> native bulk DDL,
    # which CockroachDB returns in dependency order with FK constraints validated.
    if not include and not exclude and not region_filter and out_format == "sql" and not per_table:
        ddl = dump_all_ddl(engine, db, logger, retry_count, retry_delay)
        write_file(aggregate_file, ddl)
        logger.info(f"Wrote: {aggregate_file}")
        if opts.get("include_permissions"):
            dump_permissions(engine, out_dir, logger, retry_count, retry_delay)
        return

    # Selective / per-table / json / yaml path (per-object SHOW CREATE).
    if include:
        tables_fq = validate_fq_table_names(include.split(','), db)
        object_map = resolve_object_types(engine, tables_fq, logger, retry_count, retry_delay)
        all_objects = [(typ, name) for name, typ in object_map.items()]
    else:
        tables = collect_objects(engine, db, 'table', logger, retry_count, retry_delay)
        views = collect_objects(engine, db, 'view', logger, retry_count, retry_delay)
        sequences = collect_objects(engine, db, 'sequence', logger, retry_count, retry_delay)
        types = collect_objects(engine, db, 'type', logger, retry_count, retry_delay)

        # Dependency-friendly order: types -> sequences -> tables -> views.
        all_objects = [("TYPE", name) for name in types] + \
                      [("SEQUENCE", name) for name in sequences] + \
                      [("TABLE", name) for name in tables] + \
                      [("VIEW", name) for name in views]

        if region_filter:
            before = len(all_objects)
            all_objects = [obj for obj in all_objects
                           if region_filter.upper() in locality_map.get(obj[1], "").upper()]
            logger.info(f"📍 Region filter: {region_filter} — selected {len(all_objects)}/{before} objects")

        if exclude:
            exclude_set = set(validate_fq_table_names(exclude.split(','), db))
            all_objects = [obj for obj in all_objects if obj[1] not in exclude_set]

    if opts.get("include_permissions"):
        dump_permissions(engine, out_dir, logger, retry_count, retry_delay)

    # Truncate the aggregate file once before appending per-object DDL.
    if not per_table and out_format == "sql":
        write_file(aggregate_file, "")

    dump_data = []

    def process(obj_type, full_name):
        ddl = dump_create_statement(engine, obj_type, full_name, logger, retry_count, retry_delay)
        if not ddl:
            return  # Skip entirely if no DDL returned

        dump_data.append({"name": full_name, "type": obj_type, "ddl": ddl.strip()})

        if per_table and out_format == "sql":
            filename = f"{out_dir}/{obj_type.lower()}_{full_name}.sql"
            write_file(filename, f"-- {obj_type}: {full_name}\n{ddl}\n")
            logger.info(f"Wrote: {filename}")
        elif not per_table and out_format == "sql":
            with open(aggregate_file, "a") as f:
                f.write(f"-- {obj_type}: {full_name}\n{ddl}\n\n")
            logger.info(f"Appended {full_name} to {aggregate_file}")

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
            f"-- Exported at: {datetime.now(timezone.utc).isoformat()} UTC",
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
