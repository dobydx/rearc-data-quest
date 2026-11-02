import json
from pathlib import Path

import pytest

from quest.contracts.loader import contract_by_role, contract_for_file
from quest.ingest.datausa import LOGICAL_FILE_NAME, DataUsaShapeError, validate_datausa_body

CONTRACT = contract_by_role("population")


def _body(fixtures_dir: Path) -> dict[str, object]:
    loaded: dict[str, object] = json.loads((fixtures_dir / "datausa_population.json").read_bytes())
    return loaded


def test_logical_name_routes_to_population_contract() -> None:
    contract = contract_for_file(LOGICAL_FILE_NAME)

    assert contract is not None
    assert contract.role == "population"


def test_real_body_is_valid(fixtures_dir: Path) -> None:
    shape = validate_datausa_body((fixtures_dir / "datausa_population.json").read_bytes(), CONTRACT)

    assert shape.record_count == 11
    assert shape.header.observed == ("nation id", "nation", "year", "population")


@pytest.mark.parametrize(
    ("mutate", "message"),
    [
        (lambda b: b.pop("data"), "data"),
        (lambda b: b.update(data=[]), "data"),
        (lambda b: b["page"].update(total=12), "page.total"),
        (lambda b: b.update(columns=["Nation ID", "Year", "Nation", "Population"]), "columns"),
    ],
)
def test_invalid_shapes_raise(fixtures_dir: Path, mutate: object, message: str) -> None:
    body = _body(fixtures_dir)
    mutate(body)  # type: ignore[operator]

    with pytest.raises(DataUsaShapeError, match=message):
        validate_datausa_body(json.dumps(body).encode(), CONTRACT)


def test_html_body_raises() -> None:
    with pytest.raises(DataUsaShapeError, match="JSON"):
        validate_datausa_body(b"<html>maintenance</html>", CONTRACT)


def test_json_array_body_raises() -> None:
    with pytest.raises(DataUsaShapeError, match="object"):
        validate_datausa_body(b"[1, 2]", CONTRACT)
