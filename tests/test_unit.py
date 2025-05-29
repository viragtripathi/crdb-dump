import os
import pytest
from click.testing import CliRunner
from crdb_dump.cli import main
from crdb_dump.utils.io import write_file, archive_output
from crdb_dump.verify.diff_utils import diff_schemas
from crdb_dump.export.data import to_csv_literal, to_sql_literal
from crdb_dump.utils.common import get_type_and_args


@pytest.fixture
def sample_files(tmp_path):
    file1 = tmp_path / "file1.sql"
    file2 = tmp_path / "file2.sql"
    file1.write_text("CREATE TABLE test (id INT);")
    file2.write_text("CREATE TABLE test (id INT);")
    return str(file1), str(file2)

def test_write_file(tmp_path):
    filepath = tmp_path / "sample.txt"
    content = "This is a test."
    write_file(filepath, content)
    assert filepath.read_text() == content

def test_diff_schemas_no_diff(sample_files):
    file1, file2 = sample_files
    diff = diff_schemas(file1, file2)
    assert diff == ''

def test_archive_output(tmp_path):
    test_dir = tmp_path / "to_archive"
    test_dir.mkdir()
    test_file = test_dir / "file.txt"
    test_file.write_text("data")
    archive_output(str(test_dir))
    archive_path = str(test_dir) + ".tar.gz"
    assert os.path.exists(archive_path)

def test_cli_help():
    runner = CliRunner()
    result = runner.invoke(main, ['--help'])
    assert result.exit_code == 0
    assert "Usage:" in result.output

def test_cli_missing_db():
    runner = CliRunner()
    result = runner.invoke(main, ['export'])
    assert result.exit_code != 0
    assert "--db" in result.output

def test_csv_literal_bytes():
    assert to_csv_literal(b"\x01\x02") == "0102"

def test_csv_literal_memoryview():
    assert to_csv_literal(memoryview(b"\x03\x04")) == "0304"

def test_csv_literal_string():
    assert to_csv_literal("hello") == "hello"

def test_sql_literal_bytes():
    assert to_sql_literal(b"\xff") == "decode('ff', 'hex')"

def test_sql_literal_memoryview():
    mv = memoryview(b"\xaa\xbb")
    assert to_sql_literal(mv) == "decode('aabb', 'hex')"

def test_sql_literal_null():
    assert to_sql_literal(None) == "NULL"

def test_integer_type_basic():
    result = get_type_and_args(["int"])
    assert result["type"] == "integer"
    assert "min" in result["args"]
    assert "max" in result["args"]
    assert 0 <= result["args"]["null_pct"] <= 0.5

def test_integer_not_null():
    result = get_type_and_args(["int", "NOT", "NULL"])
    assert result["args"]["null_pct"] == 0.0, result

def test_varchar_with_length():
    result = get_type_and_args(["varchar", "100"])
    assert result["type"] == "string"
    assert result["args"]["max"] == 100

def test_boolean_array():
    result = get_type_and_args(["boolean", "array"])
    assert result["type"] == "bool"
    assert result["args"]["array"] > 0

def test_jsonb():
    result = get_type_and_args(["jsonb"])
    assert result["type"] == "json"
    assert "min" in result["args"]
    assert "max" in result["args"]

def test_timestamp_type():
    result = get_type_and_args(["timestamp"])
    assert result["type"] == "timestamp"
    assert "start" in result["args"]
    assert "end" in result["args"]

def test_uuid_type():
    result = get_type_and_args(["uuid"])
    assert result["type"] == "uuid"
    assert "seed" in result["args"]

def test_unknown_type_raises():
    with pytest.raises(ValueError):
        get_type_and_args(["xmlblob"])