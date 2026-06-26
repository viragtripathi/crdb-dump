from crdb_dump.loader.loader import _split_sql_statements


def test_split_ignores_semicolons_in_string_literals():
    sql = "INSERT INTO t VALUES ('a;b'); CREATE TABLE x (id INT);"
    stmts = _split_sql_statements(sql)
    assert len(stmts) == 2
    assert "a;b" in stmts[0]


def test_split_drops_trailing_semicolons_and_blanks():
    sql = "CREATE TABLE a (id INT);\n\n;  ; CREATE TABLE b (id INT);\n"
    stmts = _split_sql_statements(sql)
    assert stmts == ["CREATE TABLE a (id INT)", "CREATE TABLE b (id INT)"]
