import os
import tarfile
import os
import re
import logging


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
    for t in tables:
        if '.' not in t:
            raise ValueError(f"❌ Invalid table name '{t}': must be fully-qualified like '{db}.users'")
        parts = t.split('.')
        if len(parts) != 2 or parts[0] != db:
            raise ValueError(f"❌ Invalid table name '{t}': expected database prefix '{db}.'")
    return tables

def normalize_filename(obj_type, full_name):
    _, name = full_name.split('.', 1)
    safe_name = re.sub(r'[^\w]+', '_', name).lower()
    return f"{obj_type.lower()}_{safe_name}.sql"