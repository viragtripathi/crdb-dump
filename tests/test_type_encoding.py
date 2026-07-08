"""Regression tests for the type-encoding bugs reported against 0.6.0:

1. JSONB exported to CSV as Python dict repr (single quotes) -> invalid JSON.
2. datetime/date exported to SQL unquoted -> syntax error (or wrong data).
3. Sequences (and views) data-exported because SHOW TABLES rows were not
   filtered by their type column.
"""
import datetime
import logging
from unittest.mock import MagicMock

from crdb_dump.export.schema import collect_objects
from crdb_dump.utils.common import to_csv_literal, to_sql_literal


# --- Bug 2 (+4): datetime/date/time and dicts in SQL INSERTs -----------------

def test_sql_literal_datetime_quoted():
    dt = datetime.datetime(2021, 8, 2, 15, 39, 18, 500000,
                           tzinfo=datetime.timezone.utc)
    assert to_sql_literal(dt) == "'2021-08-02 15:39:18.500000+00:00'"


def test_sql_literal_date_quoted():
    assert to_sql_literal(datetime.date(2021, 8, 2)) == "'2021-08-02'"


def test_sql_literal_time_quoted():
    assert to_sql_literal(datetime.time(15, 39, 18)) == "'15:39:18'"


def test_sql_literal_dict_is_json():
    val = {"05350fb5": [], "1aa83412": ["a", "b"]}
    out = to_sql_literal(val)
    assert out == '\'{"05350fb5": [], "1aa83412": ["a", "b"]}\''


def test_sql_literal_dict_escapes_single_quotes():
    assert to_sql_literal({"k": "o'brien"}) == "'{\"k\": \"o''brien\"}'"


# --- Bug 1: JSONB in CSV ------------------------------------------------------

def test_csv_literal_dict_is_json():
    val = {"05350fb5": [], "1aa83412": ["a", "b"]}
    assert to_csv_literal(val) == '{"05350fb5": [], "1aa83412": ["a", "b"]}'


def test_csv_literal_json_column_list_is_json():
    # A JSONB array arrives as a Python list; with the column type known it must
    # be JSON-encoded, not encoded as a Postgres array literal.
    assert to_csv_literal(["a", "b"], data_type="JSONB") == '["a", "b"]'


def test_csv_literal_array_column_list_stays_array():
    assert to_csv_literal(["a", "b"], data_type="ARRAY") == "{a,b}"
    # and without type info, lists keep the historical array encoding
    assert to_csv_literal(["a", "b"]) == "{a,b}"


def test_sql_literal_json_column_list_is_json():
    assert to_sql_literal(["a", "b"], data_type="JSONB") == "'[\"a\", \"b\"]'"


# --- Bug 3: SHOW TABLES rows must be filtered by type --------------------------

def _engine_with_rows(rows):
    conn = MagicMock()
    conn.__enter__.return_value = conn
    conn.__exit__.return_value = False
    conn.execute.side_effect = [None, iter(rows)]  # USE db, then SHOW TABLES
    eng = MagicMock()
    eng.connect.return_value = conn
    return eng


def test_collect_tables_excludes_sequences_and_views():
    rows = [
        ("public", "doc_attrs", "table", "root", 0, None),
        ("public", "comment_status_key_seq", "sequence", "root", 0, None),
        ("public", "some_view", "view", "root", 0, None),
    ]
    result = collect_objects(_engine_with_rows(rows), "cp", "table",
                             logging.getLogger("t"), 1, 0.0)
    assert result == ["cp.public.doc_attrs"]
