"""Frozen dataclasses that the contract and rule YAML files load into."""

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Literal, get_args

FileFormat = Literal["tsv", "json", "text"]
Severity = Literal["quarantine", "warn", "fail"]
Stage = Literal["row", "pipeline", "validate"]

FILE_FORMATS: tuple[FileFormat, ...] = get_args(FileFormat)
SEVERITIES: tuple[Severity, ...] = get_args(Severity)
STAGES: tuple[Stage, ...] = get_args(Stage)


@dataclass(frozen=True)
class Column:
    """One source column: canonical name, accepted header tokens, silver type."""

    name: str
    # Normalized (stripped, lowercased) header tokens; the first is the expected one.
    source_aliases: tuple[str, ...]
    silver_type: str
    nullable: bool
    description: str


@dataclass(frozen=True)
class UnitSpec:
    """Units of a BLS series, decided by its duration code."""

    unit_of_measure: str
    change_basis: str | None
    value_decimals: int


@dataclass(frozen=True)
class FileContract:
    """Everything the pipeline needs to know about one source file role."""

    role: str
    source: str
    file_pattern: str
    landing_glob: str
    fmt: FileFormat
    columns: tuple[Column, ...]
    key: tuple[str, ...]
    bronze_table: str | None
    silver_table: str | None
    grain: str
    label_column: str | None
    label_canonical: Mapping[str, str]
    unit_by_duration: Mapping[str, UnitSpec]

    @property
    def column_names(self) -> tuple[str, ...]:
        """Canonical column names in file order."""
        return tuple(column.name for column in self.columns)


@dataclass(frozen=True)
class Rule:
    """One data-quality rule. A row or pipeline predicate is Spark SQL that is TRUE on pass."""

    code: str
    table: str
    predicate: str | None
    severity: Severity
    stage: Stage
    description: str
