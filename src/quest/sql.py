"""SQL text helpers shared by the DDL generators and the checks."""


def sql_string(text: str) -> str:
    """Return ``text`` as a single-quoted Spark SQL string literal."""
    escaped = text.replace("\\", "\\\\").replace("'", "\\'")
    return f"'{escaped}'"
