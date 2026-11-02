"""Load and validate the contract and rule YAML files shipped with the package."""

import re
from collections.abc import Mapping
from functools import cache
from importlib.resources import files
from importlib.resources.abc import Traversable
from typing import Any

import yaml

from quest.contracts.models import (
    FILE_FORMATS,
    SEVERITIES,
    STAGES,
    Column,
    FileContract,
    Rule,
    UnitSpec,
)

RULES_FILE = "rules.yml"


class ContractError(ValueError):
    """Raised when a contract or rule file is invalid, or a lookup finds nothing."""


@cache
def load_contracts() -> tuple[FileContract, ...]:
    """Return every file contract, validated, in file-name order."""
    return _load_from(_yaml_dir())


@cache
def load_rules() -> tuple[Rule, ...]:
    """Return every data-quality rule from ``rules.yml``, validated."""
    raw = _read_yaml(_yaml_dir() / RULES_FILE)
    return tuple(_parse_rule(entry) for entry in raw["rules"])


def contract_for_file(file_name: str) -> FileContract | None:
    """Return the first contract whose ``file_pattern`` matches, or None if unmodelled."""
    return next(
        (c for c in load_contracts() if re.fullmatch(c.file_pattern, file_name)),
        None,
    )


def contract_by_role(role: str) -> FileContract:
    """Return the contract for a role."""
    for contract in load_contracts():
        if contract.role == role:
            return contract
    raise ContractError(f"no contract for role {role!r}")


def silver_contracts() -> tuple[FileContract, ...]:
    """Contracts whose files feed a silver table."""
    return tuple(c for c in load_contracts() if c.silver_table is not None)


def silver_table_of(contract: FileContract) -> str:
    """The contract's silver table name."""
    if contract.silver_table is None:
        raise ContractError(f"role {contract.role!r} has no silver table")
    return contract.silver_table


def contract_by_silver_table(table: str) -> FileContract:
    """Return the contract that feeds a silver table."""
    for contract in silver_contracts():
        if contract.silver_table == table:
            return contract
    raise ContractError(f"no contract feeds silver table {table!r}")


def rules_for(table: str, stage: str) -> tuple[Rule, ...]:
    """Return the rules of one silver (or gold, or ops) table at one stage."""
    return tuple(rule for rule in load_rules() if rule.table == table and rule.stage == stage)


def _yaml_dir() -> Traversable:
    return files("quest.contracts") / "yaml"


def _load_from(directory: Traversable) -> tuple[FileContract, ...]:
    """Load every contract file in ``directory``; a seam for tests."""
    paths = sorted(
        (p for p in directory.iterdir() if p.name.endswith(".yml") and p.name != RULES_FILE),
        key=lambda p: p.name,
    )
    contracts = tuple(_parse_contract(_read_yaml(path)) for path in paths)
    roles = [contract.role for contract in contracts]
    duplicates = sorted({role for role in roles if roles.count(role) > 1})
    if duplicates:
        raise ContractError(f"duplicate role(s): {duplicates}")
    return contracts


def _read_yaml(path: Traversable) -> dict[str, Any]:
    loaded = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(loaded, dict):
        raise ContractError(f"{path.name}: expected a mapping at the top level")
    return loaded


def _parse_contract(raw: Mapping[str, Any]) -> FileContract:
    role = raw["role"]
    columns = tuple(
        Column(
            name=c["name"],
            source_aliases=tuple(c["source_aliases"]),
            silver_type=c["silver_type"],
            nullable=c["nullable"],
            description=c["description"],
        )
        for c in raw.get("columns", [])
    )
    contract = FileContract(
        role=role,
        source=raw["source"],
        file_pattern=raw["file_pattern"],
        landing_glob=raw["landing_glob"],
        fmt=_checked(raw["fmt"], FILE_FORMATS, role),
        columns=columns,
        key=tuple(raw.get("key", [])),
        bronze_table=raw.get("bronze_table"),
        silver_table=raw.get("silver_table"),
        grain=raw["grain"],
        label_column=raw.get("label_column"),
        label_canonical=dict(raw.get("label_canonical", {})),
        unit_by_duration={
            code: UnitSpec(**spec) for code, spec in raw.get("unit_by_duration", {}).items()
        },
    )
    _validate_contract(contract)
    return contract


def _validate_contract(contract: FileContract) -> None:
    names = set(contract.column_names)
    unknown_key = [k for k in contract.key if k not in names]
    if unknown_key:
        raise ContractError(f"{contract.role}: key columns {unknown_key} are not columns")
    if contract.label_column is not None and contract.label_column not in names:
        raise ContractError(f"{contract.role}: label_column {contract.label_column!r} unknown")


def _parse_rule(raw: Mapping[str, Any]) -> Rule:
    code = raw["code"]
    rule = Rule(
        code=code,
        table=raw["table"],
        predicate=raw.get("predicate"),
        severity=_checked(raw["severity"], SEVERITIES, code),
        stage=_checked(raw["stage"], STAGES, code),
        description=raw["description"],
    )
    if (rule.stage == "validate") != (rule.predicate is None):
        raise ContractError(f"{code} on {rule.table}: only validate rules may omit a predicate")
    return rule


def _checked[T: str](value: str, allowed: tuple[T, ...], owner: str) -> T:
    """Return ``value`` typed as one of ``allowed``, or raise."""
    for option in allowed:
        if value == option:
            return option
    raise ContractError(f"{owner}: {value!r} is not one of {allowed}")
