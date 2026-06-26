# `--as-of-system-time` Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans. Steps use checkbox (`- [ ]`).

**Goal:** Add `--as-of-system-time` to `crdb-dump export` for consistent, pinned point-in-time data dumps.

**Architecture:** A pure `aost_clause()` formatter; resolve/pin the timestamp once in `export_data`; apply the clause to the row and column-discovery queries in `export_table_data`; record it in the manifest.

**Tech Stack:** Python, Click, SQLAlchemy, pytest, CockroachDB.

## Global Constraints

- Flag: `--as-of-system-time` (Click `flag_value='auto'`, `default=None`). Bare → pin one `cluster_logical_timestamp()`; value → verbatim; omitted → off.
- Pin once; reuse for every table and chunk.
- Apply to row `SELECT` and column-discovery query; NOT schema DDL.
- Record pinned timestamp in each manifest as `as_of_system_time` (null when off).
- Tests: unit + integration + e2e green before push. Plain commit messages.
- Build/verify in repo `.venv`; live CRDB at `localhost:26257`.

---

## Task 1: `aost_clause()` formatter

**Files:** Modify `crdb_dump/utils/common.py`; Test `tests/test_aost.py`

- [ ] **Step 1: Failing test**

```python
# tests/test_aost.py
from crdb_dump.utils.common import aost_clause

def test_none_returns_empty():
    assert aost_clause(None) == ""

def test_empty_returns_empty():
    assert aost_clause("") == ""

def test_decimal_value():
    assert aost_clause("1750.0") == " AS OF SYSTEM TIME '1750.0'"

def test_interval_value():
    assert aost_clause("-30s") == " AS OF SYSTEM TIME '-30s'"

def test_quote_escaping():
    assert aost_clause("a'b") == " AS OF SYSTEM TIME 'a''b'"
```

- [ ] **Step 2: Run → fail** (`pytest tests/test_aost.py -q`).

- [ ] **Step 3: Implement** in `crdb_dump/utils/common.py`:

```python
def aost_clause(resolved_value):
    """Return an ' AS OF SYSTEM TIME ...' SQL fragment, or '' when no AOST."""
    if not resolved_value:
        return ""
    escaped = str(resolved_value).replace("'", "''")
    return f" AS OF SYSTEM TIME '{escaped}'"
```

- [ ] **Step 4: Run → pass.**

- [ ] **Step 5: Commit** `git commit -m "Add aost_clause() AS OF SYSTEM TIME formatter"`.

---

## Task 2: CLI option

**Files:** Modify `crdb_dump/cli.py`

- [ ] **Step 1:** Add the option to the `export` command (near `--data-*` options):

```python
@click.option('--as-of-system-time', 'aost', is_flag=False, flag_value='auto',
              default=None,
              help="Read data at a consistent snapshot. The bare flag pins one "
                   "cluster_logical_timestamp(); or pass a value like '-30s', a "
                   "timestamp, or a decimal.")
```

- [ ] **Step 2: Verify it parses**

Run: `python -m crdb_dump.cli export --help | grep -A2 as-of-system-time`
Expected: the option appears.

- [ ] **Step 3: Commit** `git commit -m "Add --as-of-system-time option to export"`.

---

## Task 3: Resolve/pin in `export_data` + apply in `export_table_data` + manifest

**Files:** Modify `crdb_dump/export/data.py`; Test `tests/test_data_export.py`

- [ ] **Step 1: Failing test** (manifest records AOST; clause used). Append to `tests/test_data_export.py`:

```python
def test_export_table_data_records_aost(tmp_path):
    cols = [("id",)]
    page1 = [(1,)]
    captured = {"stmts": []}
    conn = MagicMock()
    conn.__enter__.return_value = conn
    conn.__exit__.return_value = False
    def execute(stmt, *a, **k):
        captured["stmts"].append(str(stmt))
        s = str(stmt)
        if "information_schema.columns" in s:
            return iter(cols)
        return MagicMock(fetchall=lambda: page1 if "OFFSET 0 " in s else [])
    conn.execute.side_effect = execute
    engine = MagicMock()
    engine.connect.return_value = conn
    import json, os
    opts = {"aost_resolved": "1750.0"}
    data_mod.export_table_data(
        engine, "cp.cpkit.tasks", str(tmp_path), "sql", False, None, False,
        None, False, 1000, False, logging.getLogger("t"), {}, 1, 0.0, opts)
    manifest = json.load(open(tmp_path / "cp.cpkit.tasks.manifest.json"))
    assert manifest["as_of_system_time"] == "1750.0"
    assert any("AS OF SYSTEM TIME '1750.0'" in s for s in captured["stmts"])
```

- [ ] **Step 2: Run → fail.**

- [ ] **Step 3: Implement in `export_table_data`:**

Add near the top (after `obj = parse_object_name(...)`):
```python
from crdb_dump.utils.common import aost_clause   # add to imports at top of file
clause = aost_clause(opts.get("aost_resolved"))
```
Column query — append `clause`:
```python
cols_res = conn.execute(text(
    "SELECT column_name FROM information_schema.columns "
    "WHERE table_name = :t AND table_schema = :s ORDER BY ordinal_position" + clause
), {"t": obj.table, "s": obj.schema})
```
Row query — insert `clause` after the table:
```python
query = f"SELECT * FROM {obj.fq_quoted()}{clause} {order_clause} OFFSET {offset} LIMIT {batch_size}"
```
Manifest — add the field:
```python
json.dump({
    "table": obj.fq_plain(),
    "as_of_system_time": opts.get("aost_resolved"),
    "region": region,
    "chunks": manifest
}, mf, indent=2)
```

- [ ] **Step 4: Resolve/pin once in `export_data`** (before building `data_tasks`):

```python
aost = opts.get("aost")
if aost == "auto":
    with engine.connect() as conn:
        aost = str(conn.execute(text("SELECT cluster_logical_timestamp()")).scalar())
opts["aost_resolved"] = aost
```
(`text` is already imported in data.py.)

- [ ] **Step 5: Run → pass** (`pytest tests/test_data_export.py -q`).

- [ ] **Step 6: Verify AOST on information_schema empirically**

Run against the live cluster:
```bash
python - <<'PY'
import os
from sqlalchemy import create_engine, text
e = create_engine(os.environ["CRDB_URL"])
with e.connect() as c:
    ts = str(c.execute(text("SELECT cluster_logical_timestamp()")).scalar())
    try:
        c.execute(text("SELECT column_name FROM information_schema.columns "
                       "WHERE table_name='users' AND table_schema='public' "
                       f"ORDER BY ordinal_position AS OF SYSTEM TIME '{ts}'")).fetchall()
        print("AOST on information_schema: OK")
    except Exception as ex:
        print("AOST on information_schema: REJECTED:", str(ex)[:120])
PY
```
If REJECTED: change the column query to apply `clause` only to a real-table read,
or drop `clause` from the column query and document the skew. Re-run unit tests.

- [ ] **Step 7: Commit** `git commit -m "Pin AS OF SYSTEM TIME across data export; record in manifest"`.

---

## Task 4: Integration tests

**Files:** Modify `tests/test_integration.py`

- [ ] **Step 1: Add consistency + bare-flag tests:**

```python
@pytest.mark.integration
@pytest.mark.skipif("CRDB_URL" not in os.environ, reason="CRDB_URL must be set")
def test_aost_excludes_later_writes(tmp_path):
    conn = get_psycopg_connection(); cur = conn.cursor()
    cur.execute("DROP TABLE IF EXISTS aost_t")
    cur.execute("CREATE TABLE aost_t (id INT PRIMARY KEY)")
    cur.execute("INSERT INTO aost_t VALUES (1), (2)")
    conn.commit()
    cur.execute("SELECT cluster_logical_timestamp()")
    ts = str(cur.fetchone()[0])
    cur.execute("INSERT INTO aost_t VALUES (3)")  # after the snapshot
    conn.commit(); cur.close(); conn.close()

    out = tmp_path / "out"
    r = CliRunner().invoke(main, [
        "export", "--db=defaultdb", "--tables=public.aost_t",
        "--data", "--data-format=csv", f"--as-of-system-time={ts}",
        f"--out-dir={out}"])
    assert r.exit_code == 0, r.output
    import json
    db_dir = out / "defaultdb"
    manifest = json.load(open(db_dir / "defaultdb.public.aost_t.manifest.json"))
    assert manifest["as_of_system_time"] == ts
    rows = sum(c["rows"] for c in manifest["chunks"])
    assert rows == 2  # row id=3 (inserted after ts) excluded


@pytest.mark.integration
@pytest.mark.skipif("CRDB_URL" not in os.environ, reason="CRDB_URL must be set")
def test_aost_bare_flag_pins_timestamp(tmp_path):
    conn = get_psycopg_connection(); cur = conn.cursor()
    cur.execute("DROP TABLE IF EXISTS aost_b")
    cur.execute("CREATE TABLE aost_b (id INT PRIMARY KEY)")
    cur.execute("INSERT INTO aost_b VALUES (1)")
    conn.commit(); cur.close(); conn.close()

    out = tmp_path / "out"
    r = CliRunner().invoke(main, [
        "export", "--db=defaultdb", "--tables=public.aost_b",
        "--data", "--data-format=csv", "--as-of-system-time", f"--out-dir={out}"])
    assert r.exit_code == 0, r.output
    import json
    manifest = json.load(open(out / "defaultdb" / "defaultdb.public.aost_b.manifest.json"))
    assert manifest["as_of_system_time"]  # populated with a pinned timestamp
```

- [ ] **Step 2: Run integration**

```bash
export CRDB_URL="cockroachdb://root@localhost:26257/defaultdb?sslmode=disable"
pytest -m integration -q
```
Expected: pass.

- [ ] **Step 3: Commit** `git commit -m "Integration tests for --as-of-system-time"`.

---

## Task 5: E2E + docs + changelog

**Files:** Modify `test-local.sh`, `docs/guides/export-data.md`, `docs/guides/migration-limitations.md`, `CHANGELOG.md`

- [ ] **Step 1: Add an AOST step to `test-local.sh`** after the main export section:

```bash
echo "🕒 Testing --as-of-system-time (consistent snapshot)..."
$CRDB_DUMP --verbose export --db="$DB_NAME" --tables=public.users \
  --data --data-format=csv --as-of-system-time --out-dir="$OUT_DIR/aost"
python - "$OUT_DIR/aost/$DB_NAME/${DB_NAME}.public.users.manifest.json" <<'PY'
import json, sys
m = json.load(open(sys.argv[1]))
assert m.get("as_of_system_time"), "manifest missing as_of_system_time"
print("✅ AOST timestamp recorded:", m["as_of_system_time"])
PY
```

- [ ] **Step 2: Docs** — add an AOST section to `docs/guides/export-data.md`:

```markdown
## Consistent snapshots (`--as-of-system-time`)

By default each table is read independently, so a dump of a live database is not
consistent across tables. Use `--as-of-system-time` to read every table at one
pinned cluster timestamp:

```bash
# pin one cluster_logical_timestamp() for the whole export
crdb-dump export --db=mydb --data --as-of-system-time

# or pass an explicit interval / timestamp / decimal
crdb-dump export --db=mydb --data --as-of-system-time='-30s'
```

The pinned timestamp is recorded in each manifest as `as_of_system_time`.

!!! warning
    The timestamp must be within the table's garbage-collection window
    (`gc.ttlseconds`). Very long exports against an old timestamp can fail.
```

- [ ] **Step 3: Docs** — in `docs/guides/migration-limitations.md`, change the
"planned enhancement" note to state AOST is available via `--as-of-system-time`,
keeping the GC-window caveat.

- [ ] **Step 4: Changelog** — under `## Unreleased`, replace `_No changes yet._` with:

```markdown
### Added
- `--as-of-system-time` on `export`: read all table data at one pinned cluster
  timestamp for a consistent point-in-time snapshot. The bare flag pins
  `cluster_logical_timestamp()`; an explicit value is used verbatim. The pinned
  timestamp is recorded in each manifest.
```

- [ ] **Step 5: Run full suite + e2e**

```bash
pytest -q                      # unit + integration (CRDB_URL set)
./test-local.sh                # e2e incl. AOST assertion
mkdocs build --strict          # docs still build
```
Expected: all green.

- [ ] **Step 6: Commit** `git commit -m "E2E + docs + changelog for --as-of-system-time"`.

---

## Task 6: PR

- [ ] **Step 1:** `git push -u origin aost`
- [ ] **Step 2:** Open PR describing the option, consistency semantics, manifest field, and tests.

---

## Self-Review notes

- Spec coverage: formatter (T1), CLI (T2), pin+apply+manifest (T3), integration (T4), e2e+docs+changelog (T5), PR (T6).
- The information_schema AOST verification (T3 Step 6) gates whether the column
  query keeps the clause; fallback documented.
- Types/names consistent: `aost_clause`, `opts["aost"]`/`opts["aost_resolved"]`,
  manifest key `as_of_system_time`.
