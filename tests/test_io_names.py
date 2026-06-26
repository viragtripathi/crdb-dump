import pytest
from crdb_dump.utils.io import validate_fq_table_names, normalize_filename


def test_accepts_schema_table_and_normalizes():
    assert validate_fq_table_names(["cpkit.tasks"], "cp") == ["cp.cpkit.tasks"]


def test_accepts_full_three_part():
    assert validate_fq_table_names(["cp.cpkit.tasks"], "cp") == ["cp.cpkit.tasks"]


def test_bare_table_defaults_public():
    assert validate_fq_table_names(["tasks"], "cp") == ["cp.public.tasks"]


def test_rejects_wrong_db_prefix():
    with pytest.raises(ValueError):
        validate_fq_table_names(["other.cpkit.tasks"], "cp")


def test_normalize_filename_keeps_three_part():
    assert normalize_filename("TABLE", "cp.cpkit.tasks") == "table_cp.cpkit.tasks.sql"
