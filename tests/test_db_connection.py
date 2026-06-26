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
    monkeypatch.setattr(dbc.psycopg2, "connect",
                        lambda dsn: captured.setdefault("dsn", dsn) or "C")
    dbc.get_psycopg_connection({"host": "ignored"})
    assert captured["dsn"].startswith("postgresql://")
