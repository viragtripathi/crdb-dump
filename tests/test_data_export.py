import json
import os
import logging
from unittest.mock import MagicMock
from crdb_dump.export import data as data_mod


def test_export_table_data_three_part_naming(tmp_path):
    cols = [("id",), ("name",)]
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
