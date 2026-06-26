"""Regression tests for CockroachDB VECTOR support.

CockroachDB returns VECTOR values as strings like '[1.5,2,3.25]' from both
SQLAlchemy and psycopg2. Our literal encoders must preserve that string
verbatim (NOT treat it as a Postgres array). If a future driver starts
returning Python lists for VECTOR, the data-export integration test will
catch the regression.
"""
from crdb_dump.utils.common import to_sql_literal, to_csv_literal


def test_sql_literal_preserves_vector_string():
    assert to_sql_literal("[1.5,2,3.25]") == "'[1.5,2,3.25]'"


def test_csv_literal_preserves_vector_string():
    # Returned verbatim; csv.writer adds quoting for the embedded commas.
    assert to_csv_literal("[1.5,2,3.25]") == "[1.5,2,3.25]"
