"""Row data quality: a null-safe ``dq_reasons`` array, then silver and quarantine as complements."""

from collections.abc import Sequence

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from quest.contracts.loader import rules_for
from quest.contracts.models import FileContract, Rule
from quest.transforms.bronze import BRONZE_ADDED_COLUMNS, RESCUED_DATA

RAW_PREFIX = "_raw_"
# What a steward needs to trace a rejected row back to its file, plus any rescued fields.
QUARANTINE_LINEAGE_COLUMNS = ("_file_name", "_sha256", "_source_path", RESCUED_DATA)


def quarantine_rules(table: str) -> tuple[Rule, ...]:
    """Return the row rules of ``table`` that send a failing row to quarantine."""
    return tuple(rule for rule in rules_for(table, "row") if rule.severity == "quarantine")


def expectations(rules: Sequence[Rule]) -> dict[str, str]:
    """Map each rule code to its predicate, the shape ``@dp.expect_all`` takes."""
    return {rule.code: rule.predicate for rule in rules if rule.predicate is not None}


def trim_columns(df: DataFrame, contract: FileContract) -> DataFrame:
    """Trim every contract column, keeping the verbatim value as ``_raw_<name>`` for quarantine."""
    raw = {f"{RAW_PREFIX}{name}": F.col(name) for name in contract.column_names}
    trimmed = {name: F.trim(F.col(name)) for name in contract.column_names}
    return df.withColumns(raw).withColumns(trimmed)


def with_dq_reasons(df: DataFrame, rules: Sequence[Rule]) -> DataFrame:
    """Add ``dq_reasons``: the code of every rule whose predicate is not TRUE (NULL fails)."""
    failures = [
        F.when(~F.coalesce(F.expr(rule.predicate), F.lit(False)), F.lit(rule.code))
        for rule in rules
        if rule.predicate is not None
    ]
    reasons = F.filter(F.array(*failures), lambda code: code.isNotNull())
    return df.withColumn("dq_reasons", reasons.cast("array<string>"))


def valid_rows(flagged: DataFrame) -> DataFrame:
    """Rows with no reasons, without ``dq_reasons`` and helper columns; lineage stays."""
    return flagged.where(F.size("dq_reasons") == 0).select(*_payload_columns(flagged))


def quarantined_rows(flagged: DataFrame, contract: FileContract) -> DataFrame:
    """Rows with reasons: the verbatim contract columns, ``dq_reasons`` and lineage."""
    raw: list[Column] = [F.col(f"{RAW_PREFIX}{name}").alias(name) for name in contract.column_names]
    return flagged.where(F.size("dq_reasons") > 0).select(
        *raw, "dq_reasons", *QUARANTINE_LINEAGE_COLUMNS
    )


def _payload_columns(df: DataFrame) -> list[str]:
    # Helper columns (_key_count, _series_exists, _raw_*) start with "_" like lineage does.
    return [
        c
        for c in df.columns
        if c != "dq_reasons" and (not c.startswith("_") or c in BRONZE_ADDED_COLUMNS)
    ]
