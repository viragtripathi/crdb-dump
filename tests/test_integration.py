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
    ddl_files = list(db_subdir.glob("table_users*.sql"))
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
        "--data",
        "--data-format=sql",
        "--per-table",
        f"--out-dir={dump_dir}"
    ])

    print("Data export output:\n", result.output)
    assert result.exit_code == 0

    db_subdir = dump_dir / "defaultdb"
    data_files = list(db_subdir.glob("users*data.sql"))
    assert data_files, f"No data SQL file found for users table in {db_subdir}"
    contents = data_files[0].read_text()

    assert "decode('0102', 'hex')" in contents
    assert "decode('0304', 'hex')" in contents
