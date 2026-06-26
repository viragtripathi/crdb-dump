import logging
from unittest.mock import MagicMock
from crdb_dump.export.schema import collect_objects
from crdb_dump.utils.common import get_table_locality


def test_collect_tables_includes_non_public_schema():
    # SHOW TABLES rows: (schema_name, table_name, type, owner, est_rows, locality)
    rows = [
        ("public", "users", "table", "root", 0, None),
        ("cpkit", "tasks", "table", "root", 0, None),
    ]
    eng = MagicMock()
    conn = MagicMock()
    conn.__enter__.return_value = conn
    conn.__exit__.return_value = False
    conn.execute.side_effect = [None, iter(rows)]  # USE db, then SHOW TABLES
    eng.connect.return_value = conn
    result = collect_objects(eng, "cp", "table", logging.getLogger("t"), 1, 0.0)
    assert "cp.public.users" in result
    assert "cp.cpkit.tasks" in result


def test_locality_map_keyed_by_three_part():
    rows = [
        ("public", "users", "table", "root", 0, "REGIONAL BY TABLE IN us-east1"),
        ("cpkit", "tasks", "table", "root", 0, None),
    ]
    eng = MagicMock()
    conn = MagicMock()
    conn.__enter__.return_value = conn
    conn.__exit__.return_value = False
    conn.execute.side_effect = [None, iter(rows)]
    eng.connect.return_value = conn
    m = get_table_locality(eng, "cp", logging.getLogger("t"))
    assert m["cp.public.users"] == "REGIONAL BY TABLE IN us-east1"
    assert m["cp.cpkit.tasks"] == "N/A"
