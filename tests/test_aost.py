from crdb_dump.utils.common import aost_clause


def test_none_returns_empty():
    assert aost_clause(None) == ""


def test_empty_returns_empty():
    assert aost_clause("") == ""


def test_decimal_value():
    assert aost_clause("1750.0") == " AS OF SYSTEM TIME '1750.0'"


def test_interval_value():
    assert aost_clause("-30s") == " AS OF SYSTEM TIME '-30s'"


def test_quote_escaping():
    assert aost_clause("a'b") == " AS OF SYSTEM TIME 'a''b'"
