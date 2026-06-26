import os
import pytest
from click.testing import CliRunner
from crdb_dump.cli import main
from crdb_dump.utils.db_connection import get_psycopg_connection


@pytest.mark.integration
@pytest.mark.skipif("CRDB_URL" not in os.environ, reason="CRDB_URL must be set")
def test_export_users_schema_sql(tmp_path):
    conn = get_psycopg_connection()
    cur = conn.cursor()
    cur.execute("DROP TABLE IF EXISTS users")
    cur.execute("""
        CREATE TABLE users (
            username STRING PRIMARY KEY,
            password_hash BYTES,
            salt BYTES
        )
    """)
    conn.commit()
    cur.close()
    conn.close()

    dump_dir = tmp_path / "schema_sql"
    dump_dir.mkdir()

    runner = CliRunner()
    result = runner.invoke(main, [
        "export",
        "--db=defaultdb",
        "--per-table",
        f"--out-dir={dump_dir}"
    ])

    print("Schema export output:\n", result.output)
    assert result.exit_code == 0

    db_subdir = dump_dir / "defaultdb"
    ddl_files = list(db_subdir.glob("table_defaultdb.public.users*.sql"))
    assert ddl_files, f"No schema file found for users table in {db_subdir}"
    ddl_contents = ddl_files[0].read_text()
    assert "CREATE TABLE" in ddl_contents


@pytest.mark.integration
@pytest.mark.skipif("CRDB_URL" not in os.environ, reason="CRDB_URL must be set")
def test_export_users_bytes_data_sql(tmp_path):
    conn = get_psycopg_connection()
    cur = conn.cursor()
    cur.execute("DROP TABLE IF EXISTS users")
    cur.execute("""
        CREATE TABLE users (
            username STRING PRIMARY KEY,
            password_hash BYTES,
            salt BYTES
        )
    """)
    cur.execute("""
        INSERT INTO users (username, password_hash, salt)
        VALUES (%s, %s, %s)
    """, ("test", b'\x01\x02', b'\x03\x04'))
    conn.commit()
    cur.close()
    conn.close()

    dump_dir = tmp_path / "bytes_sql"
    dump_dir.mkdir()

    runner = CliRunner()
    result = runner.invoke(main, [
        "export",
        "--db=defaultdb",
        "--tables=public.users",
        "--data",
        "--data-format=sql",
        "--per-table",
        f"--out-dir={dump_dir}"
    ])

    print("Data export output:\n", result.output)
    assert result.exit_code == 0

    db_subdir = dump_dir / "defaultdb"
    data_files = list(db_subdir.glob("defaultdb.public.users_*.sql"))
    assert data_files, f"No data SQL file found for users table in {db_subdir}"
    contents = data_files[0].read_text()

    assert "decode('0102', 'hex')" in contents
    assert "decode('0304', 'hex')" in contents


@pytest.mark.integration
@pytest.mark.skipif("CRDB_URL" not in os.environ, reason="CRDB_URL must be set")
def test_non_public_schema_roundtrip(tmp_path):
    """Export and reload a table in a non-public schema (the reported bug)."""
    conn = get_psycopg_connection()
    cur = conn.cursor()
    cur.execute("CREATE SCHEMA IF NOT EXISTS cpkit")
    cur.execute("DROP TABLE IF EXISTS cpkit.tasks")
    cur.execute("CREATE TABLE cpkit.tasks (id INT PRIMARY KEY, name STRING)")
    cur.execute("INSERT INTO cpkit.tasks VALUES (1, 'a'), (2, 'b')")
    conn.commit()
    cur.close()
    conn.close()

    out = tmp_path / "out"
    runner = CliRunner()
    r = runner.invoke(main, [
        "export", "--db=defaultdb", "--tables=cpkit.tasks",
        "--per-table", "--data", "--data-format=csv", f"--out-dir={out}"])
    assert r.exit_code == 0, r.output

    db_dir = out / "defaultdb"
    assert (db_dir / "defaultdb.cpkit.tasks.manifest.json").exists(), \
        f"manifest missing; files: {list(db_dir.iterdir())}"
    schema_file = db_dir / "table_defaultdb.cpkit.tasks.sql"
    assert schema_file.exists(), f"schema file missing; files: {list(db_dir.iterdir())}"

    conn = get_psycopg_connection()
    cur = conn.cursor()
    cur.execute("DROP TABLE cpkit.tasks")
    conn.commit()
    cur.close()
    conn.close()

    r = runner.invoke(main, [
        "load", "--db=defaultdb",
        f"--schema={schema_file}",
        f"--data-dir={db_dir}", "--validate-csv",
        f"--resume-log-dir={tmp_path}/resume"])
    assert r.exit_code == 0, r.output

    conn = get_psycopg_connection()
    cur = conn.cursor()
    cur.execute("SELECT count(*) FROM cpkit.tasks")
    assert cur.fetchone()[0] == 2
    cur.close()
    conn.close()


@pytest.mark.integration
@pytest.mark.skipif("CRDB_URL" not in os.environ, reason="CRDB_URL must be set")
def test_bytes_csv_roundtrip(tmp_path):
    """BYTES values must survive a full CSV export/load round-trip (bytea hex)."""
    conn = get_psycopg_connection()
    cur = conn.cursor()
    cur.execute("DROP TABLE IF EXISTS bcsv")
    cur.execute("CREATE TABLE bcsv (id INT PRIMARY KEY, v BYTES)")
    cur.execute("INSERT INTO bcsv VALUES (1, %s), (2, %s)", (b'\x01\x02', b'\xff'))
    conn.commit()
    cur.close()
    conn.close()

    out = tmp_path / "bout"
    runner = CliRunner()
    r = runner.invoke(main, [
        "export", "--db=defaultdb", "--tables=public.bcsv",
        "--per-table", "--data", "--data-format=csv", f"--out-dir={out}"])
    assert r.exit_code == 0, r.output

    db_dir = out / "defaultdb"
    conn = get_psycopg_connection()
    cur = conn.cursor()
    cur.execute("DELETE FROM bcsv")
    conn.commit()
    cur.close()
    conn.close()

    r = runner.invoke(main, [
        "load", "--db=defaultdb", f"--data-dir={db_dir}", "--validate-csv",
        f"--resume-log-dir={tmp_path}/resume"])
    assert r.exit_code == 0, r.output

    conn = get_psycopg_connection()
    cur = conn.cursor()
    cur.execute("SELECT encode(v, 'hex') FROM bcsv ORDER BY id")
    vals = [row[0] for row in cur.fetchall()]
    assert vals == ["0102", "ff"]
    cur.close()
    conn.close()


@pytest.mark.integration
@pytest.mark.skipif("CRDB_URL" not in os.environ, reason="CRDB_URL must be set")
def test_vector_roundtrip(tmp_path):
    """VECTOR values must survive a full CSV export/load round-trip."""
    conn = get_psycopg_connection()
    cur = conn.cursor()
    cur.execute("DROP TABLE IF EXISTS vroundtrip")
    cur.execute("CREATE TABLE vroundtrip (id INT PRIMARY KEY, embd VECTOR(3))")
    cur.execute("INSERT INTO vroundtrip VALUES (1, '[1.5,2,3.25]'), (2, '[0,0,0]')")
    conn.commit()
    cur.close()
    conn.close()

    out = tmp_path / "vout"
    runner = CliRunner()
    r = runner.invoke(main, [
        "export", "--db=defaultdb", "--tables=public.vroundtrip",
        "--per-table", "--data", "--data-format=csv", f"--out-dir={out}"])
    assert r.exit_code == 0, r.output

    db_dir = out / "defaultdb"
    schema_file = db_dir / "table_defaultdb.public.vroundtrip.sql"
    assert "VECTOR(3)" in schema_file.read_text()

    conn = get_psycopg_connection()
    cur = conn.cursor()
    cur.execute("DELETE FROM vroundtrip")
    conn.commit()
    cur.close()
    conn.close()

    r = runner.invoke(main, [
        "load", "--db=defaultdb", f"--data-dir={db_dir}", "--validate-csv",
        f"--resume-log-dir={tmp_path}/resume"])
    assert r.exit_code == 0, r.output

    conn = get_psycopg_connection()
    cur = conn.cursor()
    cur.execute("SELECT embd::STRING FROM vroundtrip ORDER BY id")
    vals = [row[0] for row in cur.fetchall()]
    assert vals == ["[1.5,2,3.25]", "[0,0,0]"]
    cur.close()
    conn.close()


@pytest.mark.integration
@pytest.mark.skipif("CRDB_URL" not in os.environ, reason="CRDB_URL must be set")
def test_aost_excludes_later_writes(tmp_path):
    import time
    conn = get_psycopg_connection()
    conn.autocommit = True  # each statement its own txn so timestamps are ordered
    cur = conn.cursor()
    cur.execute("DROP TABLE IF EXISTS aost_t")
    cur.execute("CREATE TABLE aost_t (id INT PRIMARY KEY)")
    cur.execute("INSERT INTO aost_t VALUES (1), (2)")
    cur.execute("SELECT cluster_logical_timestamp()")
    ts = str(cur.fetchone()[0])
    time.sleep(0.5)
    cur.execute("INSERT INTO aost_t VALUES (3)")  # committed strictly after the snapshot
    cur.close()
    conn.close()

    out = tmp_path / "out"
    r = CliRunner().invoke(main, [
        "export", "--db=defaultdb", "--tables=public.aost_t",
        "--data", "--data-format=csv", f"--as-of-system-time={ts}",
        f"--out-dir={out}"])
    assert r.exit_code == 0, r.output

    import json
    manifest = json.load(open(out / "defaultdb" / "defaultdb.public.aost_t.manifest.json"))
    assert manifest["as_of_system_time"] == ts
    rows = sum(c["rows"] for c in manifest["chunks"])
    assert rows == 2  # row id=3 (inserted after ts) is excluded


@pytest.mark.integration
@pytest.mark.skipif("CRDB_URL" not in os.environ, reason="CRDB_URL must be set")
def test_aost_bare_flag_pins_timestamp(tmp_path):
    conn = get_psycopg_connection()
    cur = conn.cursor()
    cur.execute("DROP TABLE IF EXISTS aost_b")
    cur.execute("CREATE TABLE aost_b (id INT PRIMARY KEY)")
    cur.execute("INSERT INTO aost_b VALUES (1)")
    conn.commit()
    cur.close()
    conn.close()

    out = tmp_path / "out"
    r = CliRunner().invoke(main, [
        "export", "--db=defaultdb", "--tables=public.aost_b",
        "--data", "--data-format=csv", "--as-of-system-time", f"--out-dir={out}"])
    assert r.exit_code == 0, r.output

    import json
    manifest = json.load(open(out / "defaultdb" / "defaultdb.public.aost_b.manifest.json"))
    assert manifest["as_of_system_time"]  # populated with a pinned timestamp
