from quest.sql import sql_string


def test_sql_string_quotes_and_escapes() -> None:
    assert sql_string("it's") == "'it\\'s'"
    assert sql_string("a\\b") == "'a\\\\b'"
    assert sql_string("plain") == "'plain'"
