import quest


def test_version_is_string() -> None:
    assert isinstance(quest.__version__, str)
    assert quest.__version__
