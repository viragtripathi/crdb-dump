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
        None,                                                       # USE db
        iter([("CREATE TYPE cpkit.status AS ENUM ('a');",)]),        # SHOW CREATE ALL TYPES
        iter([("CREATE TABLE cpkit.tasks (id INT8 PRIMARY KEY);",)]),  # SHOW CREATE ALL TABLES
    ])
    eng = MagicMock()
    eng.connect.return_value = conn
    out = dump_all_ddl(eng, "cp", logging.getLogger("t"), 1, 0.0)
    assert "CREATE TYPE cpkit.status" in out
    assert "CREATE TABLE cpkit.tasks" in out
    assert out.index("CREATE TYPE") < out.index("CREATE TABLE cpkit.tasks")


def test_dump_create_statement_uses_quoted_fq_name():
    captured = {"stmts": []}
    conn = MagicMock()
    conn.__enter__.return_value = conn
    conn.__exit__.return_value = False

    def execute(stmt, *a, **k):
        captured["stmts"].append(str(stmt))
        if "USE" in str(stmt):
            return None
        return iter([("tasks", "CREATE TABLE cpkit.tasks (id INT8 PRIMARY KEY)")])

    conn.execute.side_effect = execute
    eng = MagicMock()
    eng.connect.return_value = conn
    ddl = dump_create_statement(eng, "TABLE", "cp.cpkit.tasks", logging.getLogger("t"), 1, 0.0)
    assert ddl.strip().endswith(";")
    assert any('"cp"."cpkit"."tasks"' in s for s in captured["stmts"])
