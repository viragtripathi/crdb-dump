import json
import os
import logging
from unittest.mock import MagicMock
from crdb_dump.export import data as data_mod


def test_export_table_data_three_part_naming(tmp_path):
    cols = [("id", "INT8"), ("name", "STRING")]
    page1 = [(1, "a"), (2, "b")]
    conn = MagicMock()
    conn.__enter__.return_value = conn
    conn.__exit__.return_value = False
    # 1) column query, 2) first page, 3) empty page
    conn.execute.side_effect = [
        iter(cols),
        MagicMock(fetchall=lambda: page1),
        MagicMock(fetchall=lambda: []),
    ]
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
    # The generated INSERT must use the fully-qualified quoted name.
    sql_file = [f for f in files if f.endswith(".sql")][0]
    content = (tmp_path / sql_file).read_text()
    assert 'INSERT INTO "cp"."cpkit"."tasks"' in content


def test_export_table_data_records_aost(tmp_path):
    cols = [("id", "INT8")]
    page1 = [(1,)]
    captured = {"stmts": []}
    conn = MagicMock()
    conn.__enter__.return_value = conn
    conn.__exit__.return_value = False

    def execute(stmt, *a, **k):
        s = str(stmt)
        captured["stmts"].append(s)
        if "information_schema.columns" in s:
            return iter(cols)
        return MagicMock(fetchall=lambda: page1 if "OFFSET 0 " in s else [])

    conn.execute.side_effect = execute
    conn.execution_options.return_value = conn  # AUTOCOMMIT branch returns same conn
    engine = MagicMock()
    engine.connect.return_value = conn

    opts = {"aost_resolved": "1750.0"}
    data_mod.export_table_data(
        engine, "cp.cpkit.tasks", str(tmp_path), "sql", False, None, False,
        None, False, 1000, False, logging.getLogger("t"), {}, 1, 0.0, opts)

    manifest = json.load(open(tmp_path / "cp.cpkit.tasks.manifest.json"))
    assert manifest["as_of_system_time"] == "1750.0"
    assert any("AS OF SYSTEM TIME '1750.0'" in s for s in captured["stmts"])


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
    monkeypatch.setattr(data_mod, "collect_objects", lambda *a, **k: [])
    opts = {"db": "d", "tables": None, "aost": "follower", "region": None,
            "data_parallel": False, "retry_count": 1, "retry_delay": 0}
    data_mod.export_data(opts, str(tmp_path), logging.getLogger("t"))
    assert opts["aost_resolved"] == "1750.5"


def test_export_data_follower_unavailable_raises(monkeypatch, tmp_path):
    import click
    import pytest
    engine = _engine_returning_scalar(raises=Exception("requires enterprise"))
    monkeypatch.setattr(data_mod, "get_sqlalchemy_engine", lambda opts: engine)
    monkeypatch.setattr(data_mod, "get_table_locality", lambda e, db, lg: {})
    opts = {"db": "d", "tables": None, "aost": "follower", "region": None,
            "data_parallel": False, "retry_count": 1, "retry_delay": 0}
    with pytest.raises(click.UsageError):
        data_mod.export_data(opts, str(tmp_path), logging.getLogger("t"))
