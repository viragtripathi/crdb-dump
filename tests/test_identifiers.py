import pytest
from crdb_dump.utils.identifiers import ObjectName, quote_ident, parse_object_name


def test_quote_ident_simple():
    assert quote_ident("users") == '"users"'


def test_quote_ident_escapes_embedded_quote():
    assert quote_ident('we"ird') == '"we""ird"'


def test_quote_ident_mixed_case_preserved():
    assert quote_ident("MyTable") == '"MyTable"'


def test_parse_three_part():
    o = parse_object_name("cp.cpkit.tasks", default_db="ignored")
    assert (o.database, o.schema, o.table) == ("cp", "cpkit", "tasks")


def test_parse_two_part_is_schema_table():
    o = parse_object_name("cpkit.tasks", default_db="cp")
    assert (o.database, o.schema, o.table) == ("cp", "cpkit", "tasks")


def test_parse_one_part_defaults_public():
    o = parse_object_name("tasks", default_db="cp")
    assert (o.database, o.schema, o.table) == ("cp", "public", "tasks")


def test_fq_quoted():
    o = ObjectName("cp", "cpkit", "tasks")
    assert o.fq_quoted() == '"cp"."cpkit"."tasks"'


def test_fq_plain_and_file_base():
    o = ObjectName("cp", "cpkit", "tasks")
    assert o.fq_plain() == "cp.cpkit.tasks"
    assert o.file_base() == "cp.cpkit.tasks"


def test_parse_rejects_four_parts():
    with pytest.raises(ValueError):
        parse_object_name("a.b.c.d", default_db="cp")
