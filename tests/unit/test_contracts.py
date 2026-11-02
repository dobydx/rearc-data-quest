from pathlib import Path

import pytest

from quest.contracts.header import check_header
from quest.contracts.loader import (
    ContractError,
    _load_from,
    contract_by_role,
    contract_by_silver_table,
    contract_for_file,
    load_contracts,
    silver_contracts,
    silver_table_of,
)


def test_all_contract_headers_match_real_fixtures(fixtures_dir: Path) -> None:
    checked = []
    for path in sorted((fixtures_dir / "bls").iterdir()):
        contract = contract_for_file(path.name)
        if contract is None or contract.fmt != "tsv":
            continue
        result = check_header(contract, path.read_bytes())
        assert result.ok, f"{path.name}: {result.diff}"
        checked.append(path.name)

    assert "pr.seasonal" in checked
    assert len(checked) == 10


def test_header_mismatch_reports_diff(fixtures_dir: Path) -> None:
    body = (fixtures_dir / "bls" / "pr.data.1.AllData").read_bytes()
    renamed = body.replace(b"period", b"quarter", 1)

    result = check_header(contract_by_role("observations"), renamed)

    assert not result.ok
    assert result.observed[2] == "quarter"
    assert result.diff is not None
    assert "period" in result.diff
    assert "quarter" in result.diff


def test_header_with_missing_column_reports_count() -> None:
    result = check_header(contract_by_role("footnote"), b"footnote_code\r\nR\trevised\r\n")

    assert not result.ok
    assert result.diff is not None
    assert "2" in result.diff


@pytest.mark.parametrize(
    ("file_name", "role"),
    [
        ("pr.data.1.AllData", "observations"),
        ("pr.data.0.Current", "observations_current"),
        ("pr.data.2.Foo", None),
        ("pr.txt", "doc"),
        ("pr.contacts", "doc"),
        ("pr.series", "series"),
        ("pr.seasonal", "seasonal"),
        ("acs_population_nation.json", "population"),
    ],
)
def test_contract_for_file_routes_roles(file_name: str, role: str | None) -> None:
    contract = contract_for_file(file_name)

    assert (contract.role if contract else None) == role


def test_unit_by_duration_closed_domain() -> None:
    units = contract_by_role("series").unit_by_duration

    assert set(units) == {"1", "2", "3"}
    assert units["3"].unit_of_measure == "INDEX"
    assert units["3"].change_basis is None
    assert units["3"].value_decimals == 3
    assert units["2"].change_basis == "PREVIOUS_QUARTER"


def test_seasonal_label_canonical_map() -> None:
    seasonal = contract_by_role("seasonal")

    assert seasonal.label_column == "seasonal_text"
    assert seasonal.label_canonical == {"Seasonal Adjusted": "Seasonally Adjusted"}


def test_both_data_roles_share_bronze_table_and_glob() -> None:
    all_data = contract_by_role("observations")
    current = contract_by_role("observations_current")

    assert all_data.bronze_table == current.bronze_table == "bls_pr_data"
    assert all_data.landing_glob == current.landing_glob == "pr.data.*"
    assert all_data.silver_table == "bls_pr_observations"
    assert current.silver_table is None


def test_contracts_cover_every_role() -> None:
    roles = {contract.role for contract in load_contracts()}

    assert roles == {
        "observations",
        "observations_current",
        "series",
        "measure",
        "sector",
        "class",
        "duration",
        "seasonal",
        "period",
        "footnote",
        "population",
        "doc",
    }


def test_unknown_role_raises() -> None:
    with pytest.raises(ContractError, match="nope"):
        contract_by_role("nope")


VALID_CONTRACT = """
role: footnote
source: bls_pr
file_pattern: '^pr\\.footnote$'
landing_glob: pr.footnote
fmt: tsv
bronze_table: bls_pr_footnote
silver_table: bls_pr_footnote
grain: One row per footnote code.
key: [{key}]
label_column: footnote_text
columns:
  - {{name: footnote_code, source_aliases: [footnote_code], silver_type: STRING,
     nullable: false, description: Code.}}
  - {{name: footnote_text, source_aliases: [footnote_text], silver_type: STRING,
     nullable: false, description: Text.}}
"""


def test_valid_contract_loads_from_dir(tmp_path: Path) -> None:
    (tmp_path / "footnote.yml").write_text(VALID_CONTRACT.format(key="footnote_code"))

    (contract,) = _load_from(tmp_path)

    assert contract.key == ("footnote_code",)
    assert contract.columns[1].source_aliases == ("footnote_text",)


def test_invalid_contract_raises(tmp_path: Path) -> None:
    (tmp_path / "footnote.yml").write_text(VALID_CONTRACT.format(key="not_a_column"))

    with pytest.raises(ContractError, match="not_a_column"):
        _load_from(tmp_path)


def test_duplicate_role_raises(tmp_path: Path) -> None:
    for name in ("a.yml", "b.yml"):
        (tmp_path / name).write_text(VALID_CONTRACT.format(key="footnote_code"))

    with pytest.raises(ContractError, match="duplicate role"):
        _load_from(tmp_path)


def test_bad_fmt_raises(tmp_path: Path) -> None:
    text = VALID_CONTRACT.format(key="footnote_code").replace("fmt: tsv", "fmt: xml")
    (tmp_path / "footnote.yml").write_text(text)

    with pytest.raises(ContractError, match="xml"):
        _load_from(tmp_path)


def test_silver_contracts_and_silver_table_lookups() -> None:
    tables = {silver_table_of(c) for c in silver_contracts()}

    assert "bls_pr_observations" in tables
    assert len(tables) == 10
    assert contract_by_silver_table("bls_pr_series").role == "series"


def test_silver_table_of_a_bronze_only_role_raises() -> None:
    with pytest.raises(ContractError, match="observations_current"):
        silver_table_of(contract_by_role("observations_current"))
