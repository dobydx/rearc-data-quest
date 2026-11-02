from dataclasses import fields

from quest.contracts.loader import load_rules
from quest.contracts.models import Rule
from quest.gold.pyspark_gold import GOLD_BUILDERS
from quest.ops_ddl import OPS_TABLES
from quest.validate import checks
from quest.validate.checks import (
    CHECKS,
    CheckResult,
    DqCheck,
    TableVersion,
    ValidateContext,
    layer_of,
)


def test_registry_covers_all_validate_rules() -> None:
    validate_codes = {rule.code for rule in load_rules() if rule.stage == "validate"}

    assert set(CHECKS) == validate_codes


def test_every_gold_table_has_a_parity_rule() -> None:
    tables = {r.table for r in load_rules() if r.code == "PARITY_SQL_PYSPARK"}

    assert tables == set(GOLD_BUILDERS)


def test_dq_check_and_table_version_match_ops_columns() -> None:
    for record, table in ((DqCheck, "dq_checks"), (TableVersion, "table_versions")):
        columns = [column.name for column in OPS_TABLES[table].columns]
        assert sorted(columns) == sorted(f.name for f in fields(record)), table


def test_checksum_column_holds_a_sum_of_64_bit_hashes() -> None:
    (checksum,) = [c for c in OPS_TABLES["table_versions"].columns if c.name == "content_checksum"]

    assert checksum.sql_type == "DECIMAL(38,0)"


def test_layer_of_table_names() -> None:
    assert layer_of("bls_pr_observations") == "silver"
    assert layer_of("bls_pr_class_quarantine") == "silver"
    assert layer_of("series_best_year") == "gold"
    assert layer_of("file_state") == "ops"


def test_only_q1_parity_rule_mentions_a_tolerance() -> None:
    for rule in (r for r in load_rules() if r.code == "PARITY_SQL_PYSPARK"):
        assert ("1e-9" in rule.description) == (rule.table == "population_stats_2013_2018")


def test_a_crashing_check_becomes_an_error_result() -> None:
    def broken(ctx: ValidateContext, rule: Rule) -> CheckResult:
        raise TypeError("int() argument must not be NoneType")

    results = checks._for_each_rule("Q1_INCOMPLETE", broken)(None)  # type: ignore[arg-type]

    assert [(r.status, r.table_name) for r in results] == [("error", "population_stats_2013_2018")]
    assert results[0].details is not None
    assert "TypeError" in results[0].details
