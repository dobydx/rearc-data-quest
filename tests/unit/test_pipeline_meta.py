from pathlib import Path

import pytest

from quest.contracts.loader import contract_by_role, load_contracts
from quest.contracts.models import FileContract
from quest.pipeline_meta import (
    quarantine_table_properties,
    silver_column_names,
    silver_columns,
    silver_schema_ddl,
    table_comment,
    table_properties,
)

SNAPSHOT = Path(__file__).parent / "snapshots" / "silver_observations_ddl.sql"
SILVER_CONTRACTS = [c for c in load_contracts() if c.silver_table]


def test_silver_ddl_snapshot_observations() -> None:
    ddl = silver_schema_ddl(contract_by_role("observations"), "workspace")

    assert ddl + "\n" == SNAPSHOT.read_text()


def test_bronze_properties_block_reset() -> None:
    properties = table_properties("bronze", contract_by_role("observations"), "9.9.9")

    assert properties["pipelines.reset.allowed"] == "false"
    assert properties["quest.layer"] == "bronze"
    assert properties["quest.source"] == "bls_pr"
    assert properties["quest.code_version"] == "9.9.9"


def test_bronze_grain_covers_every_landed_version_of_every_matching_file() -> None:
    data = table_properties("bronze", contract_by_role("observations"), "9.9.9")
    population = table_properties("bronze", contract_by_role("population"), "9.9.9")

    assert data["quest.grain"] == (
        "One row per line of every landed version of every file matching pr.data.* "
        "(roles observations and observations_current)."
    )
    assert population["quest.grain"] == (
        "One row per data[] record of every landed version of every file matching "
        "acs_population_nation.json."
    )


def test_silver_properties_allow_reset() -> None:
    properties = table_properties("silver", contract_by_role("series"), "9.9.9")

    assert "pipelines.reset.allowed" not in properties
    assert properties["quest.grain"] == contract_by_role("series").grain


@pytest.mark.parametrize("contract", SILVER_CONTRACTS, ids=lambda c: c.role)
def test_every_silver_contract_has_ddl(contract: FileContract) -> None:
    ddl = silver_schema_ddl(contract, "workspace")

    assert f"CONSTRAINT {contract.silver_table}_pk PRIMARY KEY" in ddl
    for name in silver_column_names(contract):
        assert f"{name} " in ddl


def test_foreign_keys_use_three_part_names() -> None:
    ddl = silver_schema_ddl(contract_by_role("series"), "dev")

    assert (
        "FOREIGN KEY (sector_code) REFERENCES dev.quest_silver.bls_pr_sector (sector_code)" in ddl
    )


def test_key_columns_are_not_null() -> None:
    for contract in SILVER_CONTRACTS:
        columns = {c.name: c for c in silver_columns(contract)}
        assert not any(columns[k].nullable for k in contract.key), contract.role


def test_table_comment_is_plain_text() -> None:
    contract = contract_by_role("observations")

    assert table_comment(contract) == f"{contract.grain} Key: series_id, year, period."


def test_quarantine_properties_have_their_own_grain() -> None:
    contract = contract_by_role("series")

    properties = quarantine_table_properties(contract, "9.9.9")

    assert properties["quest.grain"] != contract.grain
    assert "bls_pr_series" in properties["quest.grain"]
    assert properties["quest.layer"] == "silver"
