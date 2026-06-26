# Schema-Aware Overhaul Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make `crdb-dump` export and restore correct for objects in any CockroachDB schema (not just `public`), and bring the project to current best practices.

**Architecture:** Introduce one canonical identifier model (`ObjectName` + quoting) that every module uses for naming/qualification/quoting. Rewrite schema export to use native `SHOW CREATE ALL TYPES`/`SHOW CREATE ALL TABLES` for full dumps and schema-aware per-object `SHOW CREATE` for selective dumps. Make data export, the loader, and the connection layer fully three-part aware. Clean break to `database.schema.table` naming for files/manifests.

**Tech Stack:** Python ≥3.10, Click, SQLAlchemy + sqlalchemy-cockroachdb, psycopg2, sqlparse, boto3, pytest.

## Global Constraints

- Python requirement: `requires-python = ">=3.10"`; classifiers 3.10–3.13 only.
- Package version: `0.4.0`.
- All SQL identifiers MUST be produced via `crdb_dump/utils/identifiers.py` — never raw f-string interpolation of names.
- Object naming everywhere is three-part `database.schema.table`. No legacy two-part `database.table`.
- New dependency allowed: `sqlparse`. No other new runtime deps without noting it.
- Tests: every change needs passing unit + integration; full e2e (`test-local.sh`) must be green before proposing a push.
- Commit messages: plain, human-style. No AI/bot attribution or trailers.

---

## Task 0: Dev environment + branch baseline

**Files:**
- Modify: `pyproject.toml` (dev extras only in this task)

- [ ] **Step 1: Create venv and install editable with dev extras**

Run:
```bash
python3 -m venv .venv && source .venv/bin/activate
python -m pip install -U pip
pip install -e . pytest sqlparse
```
Expected: install succeeds. (We formalize `[dev]` extras in Task 9.)

- [ ] **Step 2: Run existing unit tests as a baseline**

Run: `python -m pytest -m unit -q`
Expected: existing tests pass (or note pre-existing failures).

- [ ] **Step 3: Commit nothing yet** — baseline only.

---

## Task 1: Identifier model (`identifiers.py`)

**Files:**
- Create: `crdb_dump/utils/identifiers.py`
- Test: `tests/test_identifiers.py`

**Interfaces:**
- Produces:
  - `class ObjectName` with fields `database: str, schema: str, table: str`
    and methods `fq_quoted() -> str`, `fq_plain() -> str`, `file_base() -> str`.
  - `quote_ident(name: str) -> str`
  - `parse_object_name(s: str, default_db: str, default_schema: str = "public") -> ObjectName`

- [ ] **Step 1: Write the failing tests**

```python
# tests/test_identifiers.py
import pytest
from crdb_dump.utils.identifiers import ObjectName, quote_ident, parse_object_name


def test_quote_ident_simple():
    assert quote_ident("users") == '"users"'

def test_quote_ident_escapes_embedded_quote():
    assert quote_ident('we"ird') == '"we""ird"'

def test_quote_ident_mixed_case_preserved():
    assert quote_ident("MyTable") == '"MyTable"'

def test_parse_three_part():
    o = parse_object_name("cp.cpkit.tasks", default_db="ignored")
    assert (o.database, o.schema, o.table) == ("cp", "cpkit", "tasks")

def test_parse_two_part_is_schema_table():
    o = parse_object_name("cpkit.tasks", default_db="cp")
    assert (o.database, o.schema, o.table) == ("cp", "cpkit", "tasks")

def test_parse_one_part_defaults_public():
    o = parse_object_name("tasks", default_db="cp")
    assert (o.database, o.schema, o.table) == ("cp", "public", "tasks")

def test_fq_quoted():
    o = ObjectName("cp", "cpkit", "tasks")
    assert o.fq_quoted() == '"cp"."cpkit"."tasks"'

def test_fq_plain_and_file_base():
    o = ObjectName("cp", "cpkit", "tasks")
    assert o.fq_plain() == "cp.cpkit.tasks"
    assert o.file_base() == "cp.cpkit.tasks"

def test_parse_rejects_four_parts():
    with pytest.raises(ValueError):
        parse_object_name("a.b.c.d", default_db="cp")
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `python -m pytest tests/test_identifiers.py -q`
Expected: FAIL (module not found).

- [ ] **Step 3: Implement `identifiers.py`**

```python
# crdb_dump/utils/identifiers.py
from dataclasses import dataclass


def quote_ident(name: str) -> str:
    """Quote a single SQL identifier for CockroachDB/Postgres."""
    return '"' + name.replace('"', '""') + '"'


@dataclass(frozen=True)
class ObjectName:
    database: str
    schema: str
    table: str

    def fq_quoted(self) -> str:
        return ".".join(quote_ident(p) for p in (self.database, self.schema, self.table))

    def fq_plain(self) -> str:
        return f"{self.database}.{self.schema}.{self.table}"

    def file_base(self) -> str:
        return self.fq_plain()


def parse_object_name(s: str, default_db: str, default_schema: str = "public") -> ObjectName:
    """Parse a user/DB-supplied object name into a three-part ObjectName.

    Accepts ``table`` (schema defaults to ``default_schema``), ``schema.table``
    (database defaults to ``default_db``), or ``database.schema.table``.
    Two-part input is treated as ``schema.table``, never legacy ``database.table``.
    """
    parts = s.split(".")
    if len(parts) == 1:
        return ObjectName(default_db, default_schema, parts[0])
    if len(parts) == 2:
        return ObjectName(default_db, parts[0], parts[1])
    if len(parts) == 3:
        return ObjectName(parts[0], parts[1], parts[2])
    raise ValueError(f"Invalid object name '{s}': expected 1, 2, or 3 dot-separated parts")
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `python -m pytest tests/test_identifiers.py -q`
Expected: PASS (all).

- [ ] **Step 5: Commit**

```bash
git add crdb_dump/utils/identifiers.py tests/test_identifiers.py
git commit -m "Add three-part ObjectName identifier model with quoting"
```

---

## Task 2: Schema-aware object collection + locality

**Files:**
- Modify: `crdb_dump/export/schema.py` (`collect_objects`)
- Modify: `crdb_dump/utils/common.py` (`get_table_locality`)
- Test: `tests/test_collect_objects.py`

**Interfaces:**
- Consumes: `ObjectName`, `parse_object_name`, `quote_ident` from Task 1.
- Produces:
  - `collect_objects(engine, db, obj_type, logger, retry_count, retry_delay) -> list[str]`
    now returns three-part `db.schema.name` strings.
  - `get_table_locality(engine, db, logger) -> dict[str, str]` keyed by `db.schema.table`.

- [ ] **Step 1: Write the failing test (mocked connection)**

```python
# tests/test_collect_objects.py
import logging
from unittest.mock import MagicMock
from crdb_dump.export.schema import collect_objects


class FakeConn:
    def __init__(self, rows):
        self._rows = rows
    def execute(self, *_a, **_k):
        return iter(self._rows)
    def __enter__(self): return self
    def __exit__(self, *a): return False


def _engine_returning(rows):
    eng = MagicMock()
    eng.connect.return_value = FakeConn(rows)
    return eng


def test_collect_tables_includes_non_public_schema():
    # SHOW TABLES rows: (schema_name, table_name, type, owner, est_rows, locality)
    rows = [
        ("public", "users", "table", "root", 0, None),
        ("cpkit", "tasks", "table", "root", 0, None),
    ]
    # USE <db> runs first via execute; collect_objects calls execute twice.
    eng = MagicMock()
    conn = MagicMock()
    conn.__enter__.return_value = conn
    conn.__exit__.return_value = False
    conn.execute.side_effect = [None, iter(rows)]
    eng.connect.return_value = conn
    result = collect_objects(eng, "cp", "table", logging.getLogger("t"), 1, 0.0)
    assert "cp.public.users" in result
    assert "cp.cpkit.tasks" in result
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest tests/test_collect_objects.py -q`
Expected: FAIL (returns `cp.users`, `cp.tasks` — schema dropped).

- [ ] **Step 3: Update `collect_objects` to keep schema**

Replace the `collect_objects` body's row handling in `crdb_dump/export/schema.py`:

```python
def collect_objects(engine, db, obj_type, logger, retry_count, retry_delay):
    # SHOW results expose schema in column 0 for tables/sequences/types.
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
                if obj_type == 'view':
                    schema, name = row[0], row[1]
                elif obj_type in ('table', 'sequence'):
                    schema, name = row[0], row[1]      # SHOW TABLES/SEQUENCES: (schema, name, ...)
                elif obj_type == 'type':
                    schema, name = row[0], row[1]       # SHOW TYPES: (schema, name)
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
```

Add `from crdb_dump.utils.identifiers import quote_ident` to the imports at the top of `schema.py`.

- [ ] **Step 4: Update `get_table_locality` to key by three parts**

In `crdb_dump/utils/common.py`, replace the loop body:

```python
def get_table_locality(engine, db, logger):
    """Returns a dict mapping db.schema.table => locality string (or 'N/A')."""
    from crdb_dump.utils.identifiers import quote_ident
    mapping = {}
    try:
        with engine.connect() as conn:
            conn.execute(text(f"USE {quote_ident(db)}"))
            result = conn.execute(text("SHOW TABLES"))
            for row in result:
                schema_name = row[0]
                table_name = row[1]
                locality = row[5] if len(row) > 5 else "N/A"
                fqname = f"{db}.{schema_name}.{table_name}"
                mapping[fqname] = locality or "N/A"
    except Exception as e:
        logger.warning(f"⚠️ Failed to retrieve table localities: {e}")
    return mapping
```

- [ ] **Step 5: Run tests**

Run: `python -m pytest tests/test_collect_objects.py -q`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add crdb_dump/export/schema.py crdb_dump/utils/common.py tests/test_collect_objects.py
git commit -m "Make object collection and locality map schema-aware (three-part names)"
```

---

## Task 3: Schema-aware DDL export (full + selective)

**Files:**
- Modify: `crdb_dump/export/schema.py` (`dump_create_statement`, `resolve_object_types`, `export_schema`, `dump_permissions`)
- Test: `tests/test_schema_export.py`

**Interfaces:**
- Consumes: `ObjectName`, `parse_object_name`, `quote_ident`; `collect_objects` (Task 2).
- Produces:
  - `dump_all_ddl(engine, db, logger, retry_count, retry_delay) -> str` — returns full
    DDL from `SHOW CREATE ALL TYPES` then `SHOW CREATE ALL TABLES`.
  - `dump_create_statement(engine, obj_type, full_name, logger, retry_count, retry_delay) -> str | None`
    where `full_name` is three-part and SQL uses `ObjectName.fq_quoted()`.

- [ ] **Step 1: Write the failing tests**

```python
# tests/test_schema_export.py
import logging
from unittest.mock import MagicMock
from crdb_dump.export.schema import dump_all_ddl, dump_create_statement


def _conn_with(execute_results):
    conn = MagicMock()
    conn.__enter__.return_value = conn
    conn.__exit__.return_value = False
    conn.execute.side_effect = execute_results
    return conn


def test_dump_all_ddl_types_then_tables():
    conn = _conn_with([
        None,                                   # USE db
        iter([("CREATE TYPE cpkit.status AS ENUM ('a');",)]),  # SHOW CREATE ALL TYPES
        iter([("CREATE TABLE cpkit.tasks (id INT8 PRIMARY KEY);",)]),  # SHOW CREATE ALL TABLES
    ])
    eng = MagicMock()
    eng.connect.return_value = conn
    out = dump_all_ddl(eng, "cp", logging.getLogger("t"), 1, 0.0)
    assert "CREATE TYPE cpkit.status" in out
    assert out.index("CREATE TYPE") < out.index("CREATE TABLE cpkit.tasks")


def test_dump_create_statement_uses_quoted_fq_name():
    captured = {}
    conn = MagicMock()
    conn.__enter__.return_value = conn
    conn.__exit__.return_value = False
    def execute(stmt, *a, **k):
        captured.setdefault("stmts", []).append(str(stmt))
        if "USE" in str(stmt):
            return None
        return iter([("tasks", "CREATE TABLE cpkit.tasks (id INT8 PRIMARY KEY)")])
    conn.execute.side_effect = execute
    eng = MagicMock()
    eng.connect.return_value = conn
    ddl = dump_create_statement(eng, "TABLE", "cp.cpkit.tasks", logging.getLogger("t"), 1, 0.0)
    assert ddl.strip().endswith(";")
    assert any('"cp"."cpkit"."tasks"' in s for s in captured["stmts"])
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `python -m pytest tests/test_schema_export.py -q`
Expected: FAIL (`dump_all_ddl` missing; `dump_create_statement` uses unqualified name).

- [ ] **Step 3: Implement `dump_all_ddl` and rewrite `dump_create_statement`**

Add at top of `schema.py`:
```python
import click
from datetime import datetime, timezone
from crdb_dump.utils.identifiers import ObjectName, parse_object_name, quote_ident
```
Remove the old `from datetime import datetime` line.

Add:
```python
def dump_all_ddl(engine, db, logger, retry_count, retry_delay):
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
```

Rewrite `dump_create_statement`:
```python
def dump_create_statement(engine, obj_type, full_name, logger, retry_count, retry_delay):
    obj = parse_object_name(full_name, default_db=full_name.split('.')[0])
    try:
        with retry(retries=retry_count, delay=retry_delay)(engine.connect)() as conn:
            conn.execute(text(f"USE {quote_ident(obj.database)}"))
            if obj_type == "TYPE":
                result = conn.execute(text("SHOW CREATE ALL TYPES"))
                matches = [r[0] for r in result
                           if r[0] and obj.table in r[0] and r[0].startswith("CREATE TYPE")]
                if matches:
                    return matches[0].rstrip().rstrip(";") + ";\n"
                logger.warning(f"Type {obj.fq_plain()} not found in SHOW CREATE ALL TYPES")
                return None
            result = conn.execute(text(f"SHOW CREATE {obj_type} {obj.fq_quoted()}"))
            rows = list(result)
            if rows and len(rows[0]) > 1:
                return rows[0][1].rstrip().rstrip(";") + ";\n"
            logger.warning(f"No DDL returned for {obj_type} {obj.fq_plain()}")
            return None
    except Exception as e:
        logger.error(f"Failed to get DDL for {obj_type} {full_name}: {e}")
        return None
```

- [ ] **Step 4: Make `resolve_object_types` schema-aware**

Replace `resolve_object_types` so it parses three-part names and queries by schema:
```python
def resolve_object_types(engine, object_names, logger, retry_count, retry_delay):
    mapping = {}
    with retry(retries=retry_count, delay=retry_delay)(engine.connect)() as conn:
        for obj_str in object_names:
            obj = parse_object_name(obj_str, default_db=obj_str.split('.')[0])
            conn.execute(text(f"USE {quote_ident(obj.database)}"))
            kind = None
            res = conn.execute(text(
                "SELECT 1 FROM information_schema.tables "
                "WHERE table_schema = :s AND table_name = :n"
            ), {"s": obj.schema, "n": obj.table}).fetchone()
            if res:
                kind = "TABLE"
            if not kind:
                res = conn.execute(text(
                    "SELECT 1 FROM information_schema.views "
                    "WHERE table_schema = :s AND table_name = :n"
                ), {"s": obj.schema, "n": obj.table}).fetchone()
                if res:
                    kind = "VIEW"
            if not kind:
                res = conn.execute(text(
                    "SELECT 1 FROM pg_type WHERE typname = :n"
                ), {"n": obj.table}).fetchone()
                if res:
                    kind = "TYPE"
            if not kind:
                seqs = conn.execute(text("SHOW SEQUENCES")).fetchall()
                if obj.table in [r[1] for r in seqs]:
                    kind = "SEQUENCE"
            if kind:
                mapping[obj.fq_plain()] = kind
            else:
                logger.warning(f"⚠️ Could not determine type of object: {obj.fq_plain()}")
    return mapping
```

- [ ] **Step 5: Rewrite `export_schema` to use full vs selective paths and truncate output**

Replace `export_schema` body's object-gathering and writing logic:
```python
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

    # Full-database, unfiltered, SQL output -> native bulk DDL (dependency-ordered).
    if not include and not exclude and not region_filter and out_format == "sql" and not per_table:
        ddl = dump_all_ddl(engine, db, logger, retry_count, retry_delay)
        write_file(aggregate_file, ddl)
        logger.info(f"Wrote: {aggregate_file}")
        if opts.get("include_permissions"):
            dump_permissions(engine, out_dir, logger, retry_count, retry_delay)
        return

    # Selective / per-table / json / yaml path.
    if include:
        tables_fq = validate_fq_table_names(include.split(','), db)
        object_map = resolve_object_types(engine, tables_fq, logger, retry_count, retry_delay)
        all_objects = [(typ, name) for name, typ in object_map.items()]
    else:
        tables = collect_objects(engine, db, 'table', logger, retry_count, retry_delay)
        views = collect_objects(engine, db, 'view', logger, retry_count, retry_delay)
        sequences = collect_objects(engine, db, 'sequence', logger, retry_count, retry_delay)
        types = collect_objects(engine, db, 'type', logger, retry_count, retry_delay)
        all_objects = ([("TYPE", n) for n in types] +
                       [("SEQUENCE", n) for n in sequences] +
                       [("TABLE", n) for n in tables] +
                       [("VIEW", n) for n in views])
        if region_filter:
            before = len(all_objects)
            all_objects = [o for o in all_objects
                           if region_filter.upper() in locality_map.get(o[1], "").upper()]
            logger.info(f"📍 Region filter: {region_filter} — selected {len(all_objects)}/{before}")
        if exclude:
            exclude_set = set(validate_fq_table_names(exclude.split(','), db))
            all_objects = [o for o in all_objects if o[1] not in exclude_set]

    if opts.get("include_permissions"):
        dump_permissions(engine, out_dir, logger, retry_count, retry_delay)

    # Truncate aggregate file once before appending per-object DDL.
    if not per_table and out_format == "sql":
        write_file(aggregate_file, "")

    dump_data = []

    def process(obj_type, full_name):
        ddl = dump_create_statement(engine, obj_type, full_name, logger, retry_count, retry_delay)
        if not ddl:
            return
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
            list(executor.map(lambda a: process(*a), all_objects))
    else:
        for obj in all_objects:
            process(*obj)

    if out_format == "json":
        write_file(f"{out_dir}/{db}_schema.json", json.dumps(to_json_literal(dump_data), indent=2))
    elif out_format == "yaml":
        write_file(f"{out_dir}/{db}_schema.yaml", yaml.dump(to_json_literal(dump_data), sort_keys=False))
```

Also update the `wrapped_*` lambda removals: delete the now-unused `wrapped_dump_create`,
`wrapped_collect_objects`, `wrapped_resolve_types`, `wrapped_dump_permissions` lines (calls
now pass retry args directly).

- [ ] **Step 6: Fix `datetime.utcnow()` in `dump_permissions`**

In `dump_permissions`, change:
```python
f"-- Exported at: {datetime.now(timezone.utc).isoformat()} UTC",
```

- [ ] **Step 7: Run tests**

Run: `python -m pytest tests/test_schema_export.py tests/test_identifiers.py tests/test_collect_objects.py -q`
Expected: PASS.

- [ ] **Step 8: Commit**

```bash
git add crdb_dump/export/schema.py tests/test_schema_export.py
git commit -m "Schema export: native bulk DDL for full dumps, schema-aware selective dumps"
```

---

## Task 4: Update `validate_fq_table_names` + `normalize_filename`

**Files:**
- Modify: `crdb_dump/utils/io.py`
- Test: `tests/test_io_names.py`

**Interfaces:**
- Produces: `validate_fq_table_names(tables, db) -> list[str]` accepts 2- or 3-part names,
  returns normalized three-part `db.schema.table` strings.

- [ ] **Step 1: Write the failing tests**

```python
# tests/test_io_names.py
import pytest
from crdb_dump.utils.io import validate_fq_table_names


def test_accepts_schema_table_and_normalizes():
    assert validate_fq_table_names(["cpkit.tasks"], "cp") == ["cp.cpkit.tasks"]

def test_accepts_full_three_part():
    assert validate_fq_table_names(["cp.cpkit.tasks"], "cp") == ["cp.cpkit.tasks"]

def test_bare_table_defaults_public():
    assert validate_fq_table_names(["tasks"], "cp") == ["cp.public.tasks"]

def test_rejects_wrong_db_prefix():
    with pytest.raises(ValueError):
        validate_fq_table_names(["other.cpkit.tasks"], "cp")
```

- [ ] **Step 2: Run to verify fail**

Run: `python -m pytest tests/test_io_names.py -q`
Expected: FAIL.

- [ ] **Step 3: Rewrite `validate_fq_table_names` and `normalize_filename`**

```python
# in crdb_dump/utils/io.py
from crdb_dump.utils.identifiers import parse_object_name

def validate_fq_table_names(tables, db):
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
```

- [ ] **Step 4: Run tests**

Run: `python -m pytest tests/test_io_names.py -q`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add crdb_dump/utils/io.py tests/test_io_names.py
git commit -m "Normalize table-name validation to three-part names"
```

---

## Task 5: Schema-aware data export + three-part filenames

**Files:**
- Modify: `crdb_dump/export/data.py` (`export_table_data`, `export_data`)
- Test: `tests/test_data_export.py`

**Interfaces:**
- Consumes: `ObjectName`, `parse_object_name` from Task 1.
- Produces: data files named `<db.schema.table>_NNN.csv[.gz]` / `_NNN.sql`,
  manifest `<db.schema.table>.manifest.json` with `"table": "db.schema.table"`.

- [ ] **Step 1: Write the failing test for filename/manifest naming**

```python
# tests/test_data_export.py
import json, os, logging
from unittest.mock import MagicMock
from crdb_dump.export import data as data_mod


def test_export_table_data_three_part_naming(tmp_path):
    # Two execute calls: columns query, then one page of rows, then empty page.
    cols = [("id",), ("name",)]
    page1 = [(1, "a"), (2, "b")]
    conn = MagicMock()
    conn.__enter__.return_value = conn
    conn.__exit__.return_value = False
    results = [iter(cols), MagicMock(fetchall=lambda: page1), MagicMock(fetchall=lambda: [])]
    conn.execute.side_effect = results
    engine = MagicMock()
    engine.connect.return_value = conn

    total = data_mod.export_table_data(
        engine, "cp.cpkit.tasks", str(tmp_path), "sql", False, None, False,
        None, False, 1000, False, logging.getLogger("t"), {}, 1, 0.0, {})
    assert total == 2
    files = os.listdir(tmp_path)
    assert any(f.startswith("cp.cpkit.tasks_001") and f.endswith(".sql") for f in files)
    manifest = json.load(open(tmp_path / "cp.cpkit.tasks.manifest.json"))
    assert manifest["table"] == "cp.cpkit.tasks"
```

- [ ] **Step 2: Run to verify fail**

Run: `python -m pytest tests/test_data_export.py -q`
Expected: FAIL (current code crashes on three-part split / uses old names).

- [ ] **Step 3: Rewrite `export_table_data` head + naming + SQL**

In `crdb_dump/export/data.py`, add import:
```python
from crdb_dump.utils.identifiers import parse_object_name
```
Replace the top of `export_table_data` (the `db, tbl = table.split('.')` block through column query):
```python
    try:
        obj = parse_object_name(table, default_db=table.split('.')[0])
        base_name = obj.file_base()
        with retry(retries=retry_count, delay=retry_delay)(engine.connect)() as conn:
            cols_res = conn.execute(text(
                "SELECT column_name FROM information_schema.columns "
                "WHERE table_name = :t AND table_schema = :s ORDER BY ordinal_position"
            ), {"t": obj.table, "s": obj.schema})
            columns = [row[0] for row in cols_res]
```
Replace the SELECT query line:
```python
                query = f"SELECT * FROM {obj.fq_quoted()} {order_clause} OFFSET {offset} LIMIT {batch_size}"
```
Replace the out_path computation:
```python
                out_path = os.path.join(
                    out_dir,
                    f"{base_name}_{chunk_index:03d}.csv.gz" if compress
                    else f"{base_name}_{chunk_index:03d}.csv"
                ) if export_format == 'csv' else os.path.join(
                    out_dir, f"{base_name}_{chunk_index:03d}.sql")
```
Replace the INSERT write to use quoted FQ name and quoted columns:
```python
                elif export_format == 'sql':
                    col_list = ", ".join(quote_ident(c) for c in columns)
                    with open(out_path, 'w') as f:
                        for row in rows:
                            vals = ", ".join(to_sql_literal(v) for v in row)
                            f.write(f"INSERT INTO {obj.fq_quoted()} ({col_list}) VALUES ({vals});\n")
```
Add `quote_ident` to the identifiers import:
```python
from crdb_dump.utils.identifiers import parse_object_name, quote_ident
```
Replace the manifest path + content:
```python
            manifest_path = os.path.join(out_dir, f"{base_name}.manifest.json")
            region = locality_map.get(table, "N/A")
            with open(manifest_path, 'w') as mf:
                json.dump({"table": obj.fq_plain(), "region": region, "chunks": manifest}, mf, indent=2)
```

- [ ] **Step 4: Fix the order-column membership check**

The `if col not in columns` block stays valid (columns now schema-filtered). No change needed.

- [ ] **Step 5: Fix `export_data` summary key collision**

Replace the summary aggregation (was keyed by `tbl.split('.')[-1]`, which collides across schemas):
```python
    table_row_counts = {t[1]: count for t, count in zip(data_tasks, results)}
```

- [ ] **Step 6: Run tests**

Run: `python -m pytest tests/test_data_export.py -q`
Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add crdb_dump/export/data.py tests/test_data_export.py
git commit -m "Data export: schema-aware queries, three-part file/manifest naming"
```

---

## Task 6: Connection layer honors opts on load

**Files:**
- Modify: `crdb_dump/utils/db_connection.py` (`get_psycopg_connection`)
- Modify: `crdb_dump/loader/loader.py` (pass opts through)
- Test: `tests/test_db_connection.py`

**Interfaces:**
- Produces: `get_psycopg_connection(opts=None) -> connection`. With `CRDB_URL` set, uses it;
  else builds from `opts` (`host`, `port`, `db`, `certs_dir`) mirroring `get_sqlalchemy_engine`.

- [ ] **Step 1: Write the failing test (URL building, no real connect)**

```python
# tests/test_db_connection.py
from unittest.mock import patch
from crdb_dump.utils import db_connection as dbc


def test_psycopg_uses_opts_when_no_env(monkeypatch):
    monkeypatch.delenv("CRDB_URL", raising=False)
    captured = {}
    def fake_connect(dsn):
        captured["dsn"] = dsn
        return "CONN"
    monkeypatch.setattr(dbc.psycopg2, "connect", fake_connect)
    conn = dbc.get_psycopg_connection({"host": "h1", "port": 5432, "db": "cp"})
    assert conn == "CONN"
    assert "h1" in captured["dsn"] and "cp" in captured["dsn"]


def test_psycopg_prefers_env(monkeypatch):
    monkeypatch.setenv("CRDB_URL", "cockroachdb://root@localhost:26257/defaultdb?sslmode=disable")
    captured = {}
    monkeypatch.setattr(dbc.psycopg2, "connect", lambda dsn: captured.setdefault("dsn", dsn) or "C")
    dbc.get_psycopg_connection({"host": "ignored"})
    assert captured["dsn"].startswith("postgresql://")
```

- [ ] **Step 2: Run to verify fail**

Run: `python -m pytest tests/test_db_connection.py -q`
Expected: FAIL (signature takes no opts).

- [ ] **Step 3: Rewrite `get_psycopg_connection`**

```python
def get_psycopg_connection(opts=None):
    url = os.getenv("CRDB_URL")
    if url:
        pg_url = url.replace("cockroachdb://", "postgresql://", 1)
        return psycopg2.connect(pg_url)
    opts = opts or {}
    host = opts.get("host", "localhost")
    port = opts.get("port", 26257)
    db = opts.get("db", "defaultdb")
    base = f"postgresql://root@{host}:{port}/{db}"
    if opts.get("certs_dir"):
        base += (f"?sslmode=verify-full"
                 f"&sslrootcert={opts['certs_dir']}/ca.crt"
                 f"&sslcert={opts['certs_dir']}/client.root.crt"
                 f"&sslkey={opts['certs_dir']}/client.root.key")
    else:
        base += "?sslmode=disable"
    return psycopg2.connect(base)
```

- [ ] **Step 4: Thread opts through the loader**

In `crdb_dump/loader/loader.py`, update `validate_csv_header` and `load_chunk` to use opts:
```python
def validate_csv_header(table, filepath, logger, opts=None):
    from crdb_dump.utils.identifiers import parse_object_name
    obj = parse_object_name(table, default_db=table.split('.')[0])
    conn = get_psycopg_connection(opts)
    with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
        cur.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_name = %s AND table_schema = %s ORDER BY ordinal_position",
            (obj.table, obj.schema))
        db_columns = [row[0] for row in cur.fetchall()]
    with open(filepath, 'r') as f:
        csv_header = next(__import__("csv").reader(f))
    if db_columns != csv_header:
        logger.warning(f"Header mismatch for {table}:\nDB:   {db_columns}\nFile: {csv_header}")
        return False
    return True
```
And in `load_chunk`, use opts for the connection and a quoted FQ COPY target:
```python
        if validate and not validate_csv_header(table, local_path, logger, opts):
            logger.error(f"Skipping load for {file_path} due to header mismatch.")
            return False
        from crdb_dump.utils.identifiers import parse_object_name
        obj = parse_object_name(table, default_db=table.split('.')[0])
        conn = get_psycopg_connection(opts)
        with conn.cursor() as cur:
            with open(local_path, "r") as f:
                sql = f"COPY {obj.fq_quoted()} FROM STDIN WITH CSV HEADER"
                cur.copy_expert(sql, f)
        conn.commit()
        conn.close()
```

- [ ] **Step 5: Run tests**

Run: `python -m pytest tests/test_db_connection.py -q`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add crdb_dump/utils/db_connection.py crdb_dump/loader/loader.py tests/test_db_connection.py
git commit -m "Loader honors connection opts; schema-aware COPY and CSV validation"
```

---

## Task 7: Robust schema-file splitting with sqlparse

**Files:**
- Modify: `crdb_dump/loader/loader.py` (`load_schema`)
- Test: `tests/test_load_schema_split.py`

**Interfaces:**
- Produces: `load_schema(schema_path, engine, logger)` splits statements via `sqlparse.split`.

- [ ] **Step 1: Write the failing test**

```python
# tests/test_load_schema_split.py
from unittest.mock import MagicMock
from crdb_dump.loader.loader import _split_sql_statements


def test_split_ignores_semicolons_in_string_literals():
    sql = "INSERT INTO t VALUES ('a;b'); CREATE TABLE x (id INT);"
    stmts = _split_sql_statements(sql)
    assert len(stmts) == 2
    assert "a;b" in stmts[0]
```

- [ ] **Step 2: Run to verify fail**

Run: `python -m pytest tests/test_load_schema_split.py -q`
Expected: FAIL (`_split_sql_statements` missing).

- [ ] **Step 3: Implement splitter and use it**

In `crdb_dump/loader/loader.py` add `import sqlparse` and:
```python
def _split_sql_statements(sql):
    return [s.strip().rstrip(";").strip()
            for s in sqlparse.split(sql) if s.strip().rstrip(";").strip()]
```
In `load_schema`, replace the `statements = [...]` line:
```python
    statements = _split_sql_statements(sql)
```

- [ ] **Step 4: Run tests**

Run: `python -m pytest tests/test_load_schema_split.py -q`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add crdb_dump/loader/loader.py tests/test_load_schema_split.py
git commit -m "Use sqlparse for safe schema statement splitting"
```

---

## Task 8: Verify-checksum + CLI consistency for three-part names

**Files:**
- Modify: `crdb_dump/verify/checksum.py`
- Test: `tests/test_checksum.py`

**Interfaces:**
- Produces: `verify_checksums` resolves manifests by `db.schema.table` base names.

- [ ] **Step 1: Write the failing test**

```python
# tests/test_checksum.py
import json, logging
from crdb_dump.verify.checksum import verify_checksums


def test_verify_uses_three_part_manifest(tmp_path):
    base = "cp.cpkit.tasks"
    data_file = tmp_path / f"{base}_001.sql"
    data_file.write_text("INSERT INTO x VALUES (1);\n")
    import hashlib
    h = hashlib.sha256(data_file.read_bytes()).hexdigest()
    (tmp_path / f"{base}.manifest.json").write_text(json.dumps(
        {"table": base, "region": "N/A", "chunks": [{"file": f"{base}_001.sql", "rows": 1, "sha256": h}]}))
    opts = {"tables": "cp.cpkit.tasks", "db": "cp", "retry_count": 1, "retry_delay": 0}
    # Should not raise and should find the manifest.
    verify_checksums(opts, str(tmp_path), logging.getLogger("t"))
```

- [ ] **Step 2: Run to verify fail/inspect**

Run: `python -m pytest tests/test_checksum.py -q`
Expected: FAIL (base name uses `split('.')[-1]` → `tasks`, manifest not found).

- [ ] **Step 3: Fix base name resolution**

In `crdb_dump/verify/checksum.py`, replace:
```python
        base_name = table.split('.')[-1]
```
with:
```python
        from crdb_dump.utils.identifiers import parse_object_name
        base_name = parse_object_name(table, default_db=table.split('.')[0]).file_base()
```

- [ ] **Step 4: Run tests**

Run: `python -m pytest tests/test_checksum.py -q`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add crdb_dump/verify/checksum.py tests/test_checksum.py
git commit -m "Verify checksums against three-part manifest names"
```

---

## Task 9: Packaging, Python 3.10, hygiene

**Files:**
- Modify: `pyproject.toml`
- Modify: `.gitignore`
- Create: `CHANGELOG.md`
- Remove from tracking: `build/`, `dist/`

- [ ] **Step 1: Update `pyproject.toml`**

Set:
```toml
version = "0.4.0"
requires-python = ">=3.10"
```
Replace classifiers Python lines with 3.10–3.13 only. Add `sqlparse` to `dependencies`. Add:
```toml
[project.optional-dependencies]
dev = ["pytest"]
```

- [ ] **Step 2: Update `.gitignore`**

Append:
```
build/
dist/
*.egg-info/
logs/
.venv/
crdb_dump_output/
tmp/
```

- [ ] **Step 3: Untrack build artifacts**

Run:
```bash
git rm -r --cached build dist crdb_dump.egg-info 2>/dev/null || true
```

- [ ] **Step 4: Create `CHANGELOG.md`**

```markdown
# Changelog

## 0.4.0 (unreleased)

### Breaking
- Object naming is now three-part `database.schema.table` everywhere
  (filenames, manifests, resume-log keys, `--tables` input). Pre-0.4.0
  two-part dumps are not compatible.
- `--tables` two-part input now means `schema.table` (db from `--db`),
  not `database.table`.
- Minimum Python is now 3.10.

### Fixed
- Export and restore now work for objects in non-`public` schemas.
- Mixed-case / reserved-word identifiers are correctly quoted.
- `load` honors `--host/--port/--certs-dir` (previously ignored).
- Schema export no longer duplicates DDL on re-run.
- `load` schema splitting handles semicolons inside string/UDF bodies.

### Added
- Native `SHOW CREATE ALL TYPES` / `SHOW CREATE ALL TABLES` for full dumps
  (dependency-ordered, FK constraints validated post-load).
```

- [ ] **Step 5: Verify build + unit tests**

Run:
```bash
pip install -e ".[dev]"
python -m pytest -m unit -q
```
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add pyproject.toml .gitignore CHANGELOG.md
git commit -m "Require Python 3.10+, bump to 0.4.0, add sqlparse, repo hygiene"
```

---

## Task 10: Integration tests for non-public schema round-trip

**Files:**
- Modify: `tests/test_integration.py`

**Interfaces:**
- Consumes: live CockroachDB via `CRDB_URL`.

- [ ] **Step 1: Add the failing integration test**

```python
@pytest.mark.integration
@pytest.mark.skipif("CRDB_URL" not in os.environ, reason="CRDB_URL must be set")
def test_non_public_schema_roundtrip(tmp_path):
    conn = get_psycopg_connection()
    cur = conn.cursor()
    cur.execute("CREATE SCHEMA IF NOT EXISTS cpkit")
    cur.execute("DROP TABLE IF EXISTS cpkit.tasks")
    cur.execute("CREATE TABLE cpkit.tasks (id INT PRIMARY KEY, name STRING)")
    cur.execute("INSERT INTO cpkit.tasks VALUES (1, 'a'), (2, 'b')")
    conn.commit(); cur.close(); conn.close()

    out = tmp_path / "out"
    runner = CliRunner()
    r = runner.invoke(main, ["export", "--db=defaultdb", "--data",
                             "--data-format=csv", f"--out-dir={out}"])
    assert r.exit_code == 0, r.output
    db_dir = out / "defaultdb"
    assert (db_dir / "defaultdb.cpkit.tasks.manifest.json").exists()

    conn = get_psycopg_connection(); cur = conn.cursor()
    cur.execute("DROP TABLE cpkit.tasks"); conn.commit(); cur.close(); conn.close()

    r = runner.invoke(main, ["load", "--db=defaultdb",
                             f"--schema={db_dir}/defaultdb_schema.sql",
                             f"--data-dir={db_dir}", "--validate-csv"])
    assert r.exit_code == 0, r.output
    conn = get_psycopg_connection(); cur = conn.cursor()
    cur.execute("SELECT count(*) FROM cpkit.tasks")
    assert cur.fetchone()[0] == 2
    cur.close(); conn.close()
```

- [ ] **Step 2: Start a local cluster and run integration**

Run:
```bash
cockroach start-single-node --insecure --background --store=type=mem,size=1GiB --listen-addr=localhost:26257
export CRDB_URL="cockroachdb://root@localhost:26257/defaultdb?sslmode=disable"
python -m pytest -m integration -q
```
Expected: PASS (this is the exact bug Fabio reported — must be green).

- [ ] **Step 3: Commit**

```bash
git add tests/test_integration.py
git commit -m "Integration test: non-public schema export/load round-trip"
```

---

## Task 11: E2E scenario + docs + CLAUDE.md finalization

**Files:**
- Modify: `test-local.sh` (add non-public schema)
- Modify: `README.md`
- Modify: `CLAUDE.md` (already created; confirm accuracy)
- Modify: `.github/workflows/python-ci.yml`

- [ ] **Step 1: Add a non-public-schema block to `test-local.sh`**

After the table-creation block, add:
```bash
echo "📐 Creating non-public schema objects..."
cockroach sql --insecure --host=localhost -d "$DB_NAME" -e "
  CREATE SCHEMA IF NOT EXISTS cpkit;
  CREATE TABLE cpkit.tasks (id INT PRIMARY KEY, name STRING);
  INSERT INTO cpkit.tasks VALUES (1,'a'),(2,'b'),(3,'c');
"
```
And after the main export, assert the schema-qualified manifest exists:
```bash
test -f "$BASE_OUT_DIR/${DB_NAME}.cpkit.tasks.manifest.json" \
  && echo "✅ non-public schema exported" \
  || { echo "❌ non-public schema NOT exported"; exit 1; }
```

- [ ] **Step 2: Update README**

Update naming examples to `db.schema.table`, document `--tables` two-part semantics,
add a "Breaking changes in 0.4.0" note pointing at `CHANGELOG.md`, and update the
Python badge/requirement text to 3.10+.

- [ ] **Step 3: Update CI matrix**

In `.github/workflows/python-ci.yml`, set the unit-test matrix to
`python-version: ["3.10", "3.11", "3.12", "3.13"]` and add an integration job
that runs a `cockroachdb/cockroach` service/container, sets `CRDB_URL`, and runs
`pytest -m integration`.

- [ ] **Step 4: Run the full e2e**

Run: `./test-local.sh`
Expected: all steps green, including the new non-public schema assertion.

- [ ] **Step 5: Final full test pass**

Run:
```bash
python -m pytest -q
CRDB_URL=... python -m pytest -m integration -q
```
Expected: all green.

- [ ] **Step 6: Commit**

```bash
git add test-local.sh README.md CLAUDE.md .github/workflows/python-ci.yml
git commit -m "E2E non-public schema scenario, docs, and CI matrix for 3.10+"
```

---

## Self-Review notes

- **Spec coverage:** identifier model (T1), schema export full+selective (T2,T3),
  validation/normalization (T4), data export naming (T5), loader connection+COPY (T6),
  schema splitting (T7), checksum (T8), packaging/Python 3.10/hygiene (T9),
  integration non-public (T10), e2e+docs+CI+CLAUDE.md (T11). All spec sections covered.
- **Placeholders:** none — every code step shows code; every run step shows command + expected.
- **Type consistency:** `ObjectName.fq_quoted/fq_plain/file_base`, `parse_object_name`,
  `quote_ident` used with consistent signatures across all tasks.
- **Out of scope** (keyset pagination, 2-part back-compat, multi-db) intentionally excluded.
```