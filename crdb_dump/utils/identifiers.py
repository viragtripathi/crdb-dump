from dataclasses import dataclass


def quote_ident(name: str) -> str:
    """Quote a single SQL identifier for CockroachDB/Postgres."""
    return '"' + name.replace('"', '""') + '"'


@dataclass(frozen=True)
class ObjectName:
    database: str
    schema: str
    table: str

    def fq_quoted(self) -> str:
        return ".".join(quote_ident(p) for p in (self.database, self.schema, self.table))

    def fq_plain(self) -> str:
        return f"{self.database}.{self.schema}.{self.table}"

    def file_base(self) -> str:
        return self.fq_plain()


def parse_object_name(s: str, default_db: str, default_schema: str = "public") -> ObjectName:
    """Parse a user/DB-supplied object name into a three-part ObjectName.

    Accepts ``table`` (schema defaults to ``default_schema``), ``schema.table``
    (database defaults to ``default_db``), or ``database.schema.table``.
    Two-part input is treated as ``schema.table``, never legacy ``database.table``.
    """
    parts = s.split(".")
    if len(parts) == 1:
        return ObjectName(default_db, default_schema, parts[0])
    if len(parts) == 2:
        return ObjectName(default_db, parts[0], parts[1])
    if len(parts) == 3:
        return ObjectName(parts[0], parts[1], parts[2])
    raise ValueError(f"Invalid object name '{s}': expected 1, 2, or 3 dot-separated parts")
