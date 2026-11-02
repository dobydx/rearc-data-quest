"""Silver table metadata for the pipeline: column model, schema DDL, comments, properties."""

from collections.abc import Mapping

from quest.contracts.loader import load_contracts, silver_table_of
from quest.contracts.models import Column, FileContract
from quest.sql import sql_string

SILVER_LINEAGE_COLUMNS = (
    Column("_file_name", (), "STRING", False, "Lineage: the landed file this row came from."),
    Column("_sha256", (), "STRING", False, "Lineage: SHA-256 of the file version (release id)."),
)
SILVER_LINEAGE_NAMES = tuple(column.name for column in SILVER_LINEAGE_COLUMNS)
SOURCE_SUFFIX = "_source"


def _derived(name: str, silver_type: str, description: str, nullable: bool = False) -> Column:
    return Column(name, (), silver_type, nullable, description)


def _label(name: str, what: str) -> tuple[Column, Column]:
    return (
        _derived(name, "STRING", f"{what}, canonical text."),
        _derived(name + SOURCE_SUFFIX, "STRING", f"{what}, exactly as BLS publishes it."),
    )


_QUARTER = _derived("quarter_number", "TINYINT", "1 to 4; NULL for Q05, the annual value.", True)
_ANNUAL = _derived("is_annual_average", "BOOLEAN", "True for Q05, never summed with quarters.")

# Columns each silver table adds after its contract columns, in output order.
DERIVED_COLUMNS: Mapping[str, tuple[Column, ...]] = {
    "bls_pr_observations": (
        _QUARTER,
        _ANNUAL,
        _derived("period_start_date", "DATE", "First day of the quarter; NULL for Q05.", True),
        _derived("is_revised", "BOOLEAN", "True when footnote R marks the value revised."),
    ),
    "bls_pr_series": (
        _derived("series_label", "STRING", "Measure | sector | class | duration | seasonal."),
        *_label("measure_text", "Measure name"),
        *_label("sector_name", "Sector name"),
        *_label("class_text", "Worker class"),
        *_label("duration_text", "Duration name"),
        *_label("seasonal_text", "Seasonal adjustment"),
        _derived("unit_of_measure", "STRING", "PERCENT_CHANGE or INDEX, from the duration code."),
        _derived(
            "change_basis",
            "STRING",
            "SAME_QUARTER_PRIOR_YEAR or PREVIOUS_QUARTER; NULL for an index.",
            nullable=True,
        ),
        _derived("value_decimals", "TINYINT", "Published decimals: 1 for % change, 3 for index."),
        _derived("frequency", "STRING", "QUARTERLY, or ANNUAL for Q05-only series."),
    ),
    "bls_pr_period": (_QUARTER, _ANNUAL),
    "datausa_population": (
        _derived("source_dataset", "STRING", "DataUSA dataset, for example ACS 1-year Estimate."),
        _derived("source_table_id", "STRING", "Census table id, for example B01003."),
    ),
}

# (column, referenced silver table) for each informational foreign key.
FOREIGN_KEYS: Mapping[str, tuple[tuple[str, str], ...]] = {
    "bls_pr_observations": (("series_id", "bls_pr_series"), ("period", "bls_pr_period")),
    "bls_pr_series": (
        ("sector_code", "bls_pr_sector"),
        ("class_code", "bls_pr_class"),
        ("measure_code", "bls_pr_measure"),
        ("duration_code", "bls_pr_duration"),
        ("seasonal_code", "bls_pr_seasonal"),
    ),
}


def silver_columns(contract: FileContract) -> tuple[Column, ...]:
    """Every column of a silver table, in output order: contract, derived, lineage."""
    table = silver_table_of(contract)
    label_source = tuple(
        _derived(c.name + SOURCE_SUFFIX, "STRING", f"{c.description} Exactly as published.")
        for c in contract.columns
        if c.name == contract.label_column
    )
    return (
        *contract.columns,
        *label_source,
        *DERIVED_COLUMNS.get(table, ()),
        *SILVER_LINEAGE_COLUMNS,
    )


def silver_column_names(contract: FileContract) -> list[str]:
    """Names of ``silver_columns``; every silver builder selects exactly these, in order."""
    return [column.name for column in silver_columns(contract)]


def silver_schema_ddl(contract: FileContract, catalog: str) -> str:
    """The ``schema=`` DDL of a silver table: comments, NOT NULL, a PK and FKs, all NORELY.

    FKs use three-part names, which pipeline table schemas require.
    """
    table = silver_table_of(contract)
    lines = [
        f"{c.name} {c.silver_type}{'' if c.nullable else ' NOT NULL'}"
        f" COMMENT {sql_string(c.description)}"
        for c in silver_columns(contract)
    ]
    lines.append(f"CONSTRAINT {table}_pk PRIMARY KEY ({', '.join(contract.key)}) NORELY")
    lines += [
        f"CONSTRAINT {table}_{column}_fk FOREIGN KEY ({column}) "
        f"REFERENCES {catalog}.quest_silver.{parent} ({column}) NORELY"
        for column, parent in FOREIGN_KEYS.get(table, ())
    ]
    return ",\n".join(lines)


def bronze_grain(contract: FileContract) -> str:
    """Bronze grain: every landed version of every file the contract's glob matches."""
    unit = "data[] record" if contract.fmt == "json" else "line"
    # pr.data.0.Current and pr.data.1.AllData share one bronze table, so name every role in it.
    roles = [c.role for c in load_contracts() if c.bronze_table == contract.bronze_table]
    shared = f" (roles {' and '.join(roles)})" if len(roles) > 1 else ""
    return (
        f"One row per {unit} of every landed version of every file matching "
        f"{contract.landing_glob}{shared}."
    )


def table_properties(layer: str, contract: FileContract, code_version: str) -> dict[str, str]:
    """``quest.*`` properties for a pipeline table; bronze also blocks a full refresh.

    ``code_version`` is the pipeline's ``quest.code_version`` setting, the value ingest's
    pipeline gate compares against.
    """
    properties = {
        "quest.layer": layer,
        "quest.source": contract.source,
        "quest.grain": bronze_grain(contract) if layer == "bronze" else contract.grain,
        "quest.code_version": code_version,
    }
    if layer == "bronze":
        # Bronze holds every landed version; a reset would re-read the Volume from scratch.
        properties["pipelines.reset.allowed"] = "false"
    return properties


def quarantine_table_properties(contract: FileContract, code_version: str) -> dict[str, str]:
    """Properties of a ``_quarantine`` twin: silver layer, with its own grain."""
    table = silver_table_of(contract)
    grain = f"One row per {table} source row that failed a rule, for the served versions."
    return {**table_properties("silver", contract, code_version), "quest.grain": grain}


def table_comment(contract: FileContract) -> str:
    """Table comment (plain text for the decorator): the grain sentence, then the key."""
    return f"{contract.grain} Key: {', '.join(contract.key)}."
