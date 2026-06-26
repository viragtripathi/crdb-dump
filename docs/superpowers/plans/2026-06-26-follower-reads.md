# `--as-of-system-time=follower` Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans. Steps use checkbox (`- [ ]`).

**Goal:** Add `--as-of-system-time=follower`, which pins `follower_read_timestamp()` once so exports are served by the nearest replica (follower reads) while keeping the existing consistency guarantee.

**Architecture:** One new branch in the existing pin-once resolution in `export_data` resolves `follower` to `SELECT follower_read_timestamp()`, reusing the existing `aost_clause()` + AUTOCOMMIT read path. Fail fast with a clean message if the cluster lacks the entitlement.

**Tech Stack:** Python, Click, SQLAlchemy, pytest, CockroachDB v25.4.

## Global Constraints

- Keyword: `--as-of-system-time=follower` (reuse the existing option; mirrors `auto`).
- Keep AUTOCOMMIT reads at the pinned timestamp; default priority (no `PRIORITY HIGH`).
- Resolve/pin once; reuse for every table and chunk (consistency).
- On entitlement failure: raise `click.UsageError` with a clear message, no traceback, no silent leaseholder fallback.
- Verify follower routing empirically via `EXPLAIN ANALYZE` (`used follower read`).
- Tests at all three levels, entitlement-tolerant. Plain commit messages.
- Build/verify in repo `.venv`; live CRDB on `localhost:26257`.

---

## Task 0: Empirical pre-check (shapes the tests)

**Files:** none (investigation)

- [ ] **Step 1: Start a local cluster**

```bash
cockroach start-single-node --insecure --store=type=mem,size=1GiB \
  --listen-addr=localhost:26257 --http-addr=localhost:8081 --background
sleep 4
```

- [ ] **Step 2: Does `follower_read_timestamp()` work here, and does it route to a follower?**

```bash
cockroach sql --insecure --host=localhost -d defaultdb -e "
  CREATE TABLE IF NOT EXISTS fr_probe (id INT PRIMARY KEY);
  INSERT INTO fr_probe VALUES (1) ON CONFLICT DO NOTHING;
  SELECT follower_read_timestamp();
"
cockroach sql --insecure --host=localhost -d defaultdb -e "
  EXPLAIN ANALYZE SELECT * FROM fr_probe AS OF SYSTEM TIME follower_read_timestamp();
" | grep -i "follower read" || echo "no 'used follower read' line"
```
Record the outcome:
- If `follower_read_timestamp()` errors (entitlement) → the integration/e2e tests
  assert the clean-error path.
- If it succeeds → tests also assert success + (single node may NOT show
  `used follower read`, since there are no followers; note that and rely on the
  multi-node behavior being documented rather than asserted on one node).

---

## Task 1: Resolve `follower` + error handling + CLI help

**Files:**
- Modify: `crdb_dump/export/data.py` (resolution block; add `import click`)
- Modify: `crdb_dump/cli.py` (help text)
- Test: `tests/test_data_export.py`

**Interfaces:**
- Consumes: existing `opts["aost"]`, `opts["aost_resolved"]`, `aost_clause()`.
- Produces: `export_data` resolves `opts["aost"]=="follower"` to a pinned decimal via
  `SELECT follower_read_timestamp()`, or raises `click.UsageError`.

- [ ] **Step 1: Write failing unit tests**

```python
# append to tests/test_data_export.py
import click
import pytest
from sqlalchemy import text  # noqa: F401  (kept for clarity)


def _engine_returning_scalar(value=None, raises=None):
    conn = MagicMock()
    conn.__enter__.return_value = conn
    conn.__exit__.return_value = False
    if raises is not None:
        conn.execute.side_effect = raises
    else:
        conn.execute.return_value = MagicMock(scalar=lambda: value)
    engine = MagicMock()
    engine.connect.return_value = conn
    return engine


def test_export_data_resolves_follower(monkeypatch, tmp_path):
    engine = _engine_returning_scalar(value="1750.5")
    monkeypatch.setattr(data_mod, "get_sqlalchemy_engine", lambda opts: engine)
    monkeypatch.setattr(data_mod, "get_table_locality", lambda e, db, lg: {})
    # no tables -> collect_objects returns []; export loop is a no-op
    monkeypatch.setattr(data_mod, "collect_objects", lambda *a, **k: [])
    opts = {"db": "d", "tables": None, "aost": "follower", "region": None,
            "data_format": "csv", "data_split": False, "data_limit": None,
            "data_compress": False, "data_order": None, "data_order_desc": False,
            "chunk_size": 1000, "data_order_strict": False, "data_parallel": False,
            "retry_count": 1, "retry_delay": 0}
    data_mod.export_data(opts, str(tmp_path), logging.getLogger("t"))
    assert opts["aost_resolved"] == "1750.5"


def test_export_data_follower_unavailable_raises(monkeypatch, tmp_path):
    engine = _engine_returning_scalar(raises=Exception("requires enterprise"))
    monkeypatch.setattr(data_mod, "get_sqlalchemy_engine", lambda opts: engine)
    monkeypatch.setattr(data_mod, "get_table_locality", lambda e, db, lg: {})
    opts = {"db": "d", "tables": None, "aost": "follower", "region": None,
            "retry_count": 1, "retry_delay": 0}
    with pytest.raises(click.UsageError):
        data_mod.export_data(opts, str(tmp_path), logging.getLogger("t"))
```

- [ ] **Step 2: Run → fail** (`pytest tests/test_data_export.py -q`).

- [ ] **Step 3: Implement.** In `crdb_dump/export/data.py` add `import click` near the
top, and replace the existing resolution block:

```python
    aost = opts.get("aost")
    if aost == "auto":
        with engine.connect() as conn:
            aost = str(conn.execute(text("SELECT cluster_logical_timestamp()")).scalar())
    elif aost == "follower":
        try:
            with engine.connect() as conn:
                aost = str(conn.execute(text("SELECT follower_read_timestamp()")).scalar())
        except Exception as e:
            raise click.UsageError(
                "Follower reads are not available on this cluster "
                "(requires a CockroachDB entitlement that enables follower reads): "
                f"{e}")
    if aost is not None:
        logger.info(f"🕒 Pinned AS OF SYSTEM TIME {aost}")
    opts["aost_resolved"] = aost
```

(`get_table_locality` runs before this block, as today.)

- [ ] **Step 4: Update CLI help** in `crdb_dump/cli.py`:

```python
@click.option('--as-of-system-time', 'aost', is_flag=False, flag_value='auto', default=None,
              help="Read data at a consistent snapshot. Use 'auto' (or the bare flag) "
                   "to pin cluster_logical_timestamp(), 'follower' to pin "
                   "follower_read_timestamp() for follower reads, or pass a value "
                   "like '-30s', a timestamp, or a decimal.")
```

- [ ] **Step 5: Run → pass** (`pytest tests/test_data_export.py -q`).

- [ ] **Step 6: Commit**

```bash
git add crdb_dump/export/data.py crdb_dump/cli.py tests/test_data_export.py
git commit -m "Add --as-of-system-time=follower (pin follower_read_timestamp())"
```

---

## Task 2: Integration test (entitlement-tolerant) + routing check

**Files:** Modify `tests/test_integration.py`

- [ ] **Step 1: Add the test**

```python
@pytest.mark.integration
@pytest.mark.skipif("CRDB_URL" not in os.environ, reason="CRDB_URL must be set")
def test_aost_follower_keyword(tmp_path):
    conn = get_psycopg_connection()
    conn.autocommit = True
    cur = conn.cursor()
    cur.execute("DROP TABLE IF EXISTS fr_t")
    cur.execute("CREATE TABLE fr_t (id INT PRIMARY KEY)")
    cur.execute("INSERT INTO fr_t VALUES (1), (2)")
    cur.close()
    conn.close()

    out = tmp_path / "out"
    r = CliRunner().invoke(main, [
        "export", "--db=defaultdb", "--tables=public.fr_t",
        "--data", "--data-format=csv", "--as-of-system-time=follower",
        f"--out-dir={out}"])

    if r.exit_code == 0:
        import json
        manifest = json.load(open(out / "defaultdb" / "defaultdb.public.fr_t.manifest.json"))
        assert manifest["as_of_system_time"]  # a pinned follower timestamp
    else:
        # Cluster lacks the follower-reads entitlement: must be a clean message,
        # not a raw traceback.
        assert "Follower reads are not available" in r.output
        assert "Traceback" not in r.output
```

- [ ] **Step 2: Run**

```bash
export CRDB_URL="cockroachdb://root@localhost:26257/defaultdb?sslmode=disable"
pytest -m integration -q
```
Expected: pass (whichever branch the cluster takes).

- [ ] **Step 3: Manually confirm routing where supported**

```bash
cockroach sql --insecure --host=localhost -d defaultdb -e "
  EXPLAIN ANALYZE SELECT * FROM fr_t AS OF SYSTEM TIME follower_read_timestamp();" \
  | grep -i "follower read" || echo "(single node: no follower replica to route to)"
```
Record the result in the PR description.

- [ ] **Step 4: Commit**

```bash
git add tests/test_integration.py
git commit -m "Integration test for --as-of-system-time=follower"
```

---

## Task 3: E2E + docs + changelog

**Files:** Modify `test-local.sh`, `docs/guides/export-data.md`, `CHANGELOG.md`

- [ ] **Step 1: Add a tolerant follower step to `test-local.sh`** after the existing
AOST step:

```bash
echo "🛰️  Testing --as-of-system-time=follower (tolerant of entitlement)..."
if $CRDB_DUMP --verbose export --db="$DB_NAME" --tables=public.users \
     --data --data-format=csv --as-of-system-time=follower \
     --out-dir="$OUT_DIR/follower" 2>"$OUT_DIR/follower.err"; then
  echo "✅ follower-read export succeeded"
else
  if grep -q "Follower reads are not available" "$OUT_DIR/follower.err"; then
    echo "ℹ️  follower reads not entitled on this cluster — clean error, OK"
  else
    echo "❌ unexpected follower-read failure:"; cat "$OUT_DIR/follower.err"; exit 1
  fi
fi
```

- [ ] **Step 2: Docs** — in `docs/guides/export-data.md`, extend the
consistent-snapshots section:

```markdown
### Follower reads

Use `follower` to read from the nearest replica (lower impact on the live
workload) by pinning `follower_read_timestamp()`:

```bash
crdb-dump export --db=mydb --data --as-of-system-time=follower
```

This still produces a consistent snapshot (one pinned timestamp) and records it in
each manifest. Reads use default priority to stay low-impact, so an occasional read
may briefly wait on an unresolved write intent.

!!! note "Entitlement"
    `follower_read_timestamp()` requires a CockroachDB entitlement that enables
    follower reads. Without it, the export fails fast with a clear message rather
    than silently reading from the leaseholder.
```

- [ ] **Step 3: Changelog** — under `## Unreleased`, replace `_No changes yet._` with:

```markdown
### Added
- `--as-of-system-time=follower`: pin `follower_read_timestamp()` so exports are
  served by the nearest replica (follower reads), keeping the consistent-snapshot
  guarantee. Fails fast with a clear message if the cluster lacks the entitlement.
```

- [ ] **Step 4: Verify everything**

```bash
export CRDB_URL="cockroachdb://root@localhost:26257/defaultdb?sslmode=disable"
pytest -q
./test-local.sh
mkdocs build --strict
```
Expected: all green.

- [ ] **Step 5: Commit**

```bash
git add test-local.sh docs/guides/export-data.md CHANGELOG.md
git commit -m "E2E + docs + changelog for --as-of-system-time=follower"
```

---

## Task 4: PR

- [ ] **Step 1:** `git push -u origin follower-reads`
- [ ] **Step 2:** Open a PR describing the keyword, the AUTOCOMMIT/default-priority
rationale, the entitlement error handling, and the routing-verification result.

---

## Self-Review notes

- Spec coverage: resolution+error+help (T1), integration tolerant + routing (T2),
  e2e+docs+changelog (T3), PR (T4); pre-check (T0) shapes the assertions.
- Single-node caveat called out: `used follower read` may not appear with no
  follower replica; we assert success/clean-error, not routing, in automated tests.
- Names consistent: `opts["aost"]` value `"follower"`, `opts["aost_resolved"]`,
  `aost_clause`, manifest `as_of_system_time`, `click.UsageError`.
```