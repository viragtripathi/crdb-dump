import os
import tarfile
import logging

from crdb_dump.utils.identifiers import parse_object_name


logger = logging.getLogger(__name__)

def write_file(path, content):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, 'w') as f:
        f.write(content)

def archive_output(directory):
    archive_name = f"{directory}.tar.gz"
    with tarfile.open(archive_name, "w:gz") as tar:
        tar.add(directory, arcname=os.path.basename(directory))
    logger.info(f"Archived output to {archive_name}")

def validate_fq_table_names(tables, db):
    """Normalize table names to three-part ``db.schema.table`` strings.

    Accepts ``table``, ``schema.table``, or ``db.schema.table`` input. A bare
    table defaults to the ``public`` schema; a two-part name is ``schema.table``.
    Raises if an explicit database prefix does not match ``db``.
    """
    out = []
    for t in tables:
        obj = parse_object_name(t, default_db=db)
        if obj.database != db:
            raise ValueError(
                f"❌ Invalid table name '{t}': database '{obj.database}' does not match --db '{db}'")
        out.append(obj.fq_plain())
    return out

def normalize_filename(obj_type, full_name):
    return f"{obj_type.lower()}_{full_name}.sql"