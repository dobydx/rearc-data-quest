from collections import Counter

from quest.contracts.loader import load_contracts, load_rules, rules_for

SILVER_TABLES = {c.silver_table for c in load_contracts() if c.silver_table}
RATE_GATED_TABLES = {"bls_pr_observations", "bls_pr_series", "datausa_population"}


def test_rules_have_unique_codes_per_table_and_valid_stage() -> None:
    rules = load_rules()
    counts = Counter((rule.table, rule.stage, rule.code) for rule in rules)

    assert [key for key, n in counts.items() if n > 1] == []
    assert {rule.stage for rule in rules} <= {"row", "pipeline", "validate"}
    assert {rule.severity for rule in rules} <= {"quarantine", "warn", "fail"}


def test_row_rules_have_predicates_and_validate_rules_do_not() -> None:
    for rule in load_rules():
        if rule.stage == "validate":
            assert rule.predicate is None, rule.code
        else:
            assert rule.predicate, rule.code


def test_every_silver_table_has_row_rules() -> None:
    for table in SILVER_TABLES:
        assert rules_for(table, "row"), table


def test_observation_core_codes_present() -> None:
    codes = {rule.code for rule in rules_for("bls_pr_observations", "row")}

    assert {
        "NULL_KEY",
        "BAD_SERIES_ID",
        "BAD_YEAR",
        "BAD_PERIOD",
        "UNKNOWN_PERIOD",
        "BAD_VALUE",
        "ORPHAN_SERIES",
        "DUPLICATE_KEY",
        "INDEX_NOT_POSITIVE",
        "PCT_CHANGE_BELOW_MINUS_100",
        "UNKNOWN_FOOTNOTE",
    } <= codes


def test_lookup_quarantine_rate_is_warn_only() -> None:
    for table in SILVER_TABLES:
        (rate,) = [r for r in rules_for(table, "validate") if r.code == "QUARANTINE_RATE"]
        expected = "fail" if table in RATE_GATED_TABLES else "warn"
        assert rate.severity == expected, table


def test_rules_for_unknown_table_is_empty() -> None:
    assert rules_for("no_such_table", "row") == ()


def test_rescued_data_quarantines_every_bls_silver_table_but_not_population() -> None:
    bls_tables = {c.silver_table for c in load_contracts() if c.silver_table and c.fmt == "tsv"}

    for table in bls_tables:
        (rule,) = [r for r in rules_for(table, "row") if r.code == "RESCUED_DATA"]
        assert (rule.severity, rule.predicate) == ("quarantine", "_rescued_data IS NULL")
    assert not [r for r in rules_for("datausa_population", "row") if r.code == "RESCUED_DATA"]
