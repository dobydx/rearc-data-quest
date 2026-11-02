"""Silver transforms: serve the current versions, flag every row, then type the valid ones."""

from collections.abc import Callable, Mapping
from dataclasses import dataclass

from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F

from quest.contracts.loader import contract_by_role, silver_table_of
from quest.contracts.models import FileContract
from quest.ingest.models import SERVED_STATUSES
from quest.pipeline_meta import SILVER_LINEAGE_NAMES, SOURCE_SUFFIX, silver_column_names
from quest.transforms.dq import quarantine_rules, trim_columns, valid_rows, with_dq_reasons

# Lookup roles, in the order their labels appear in series_label.
SERIES_LABEL_ROLES = ("measure", "sector", "class", "duration", "seasonal")
LOOKUPS = (*SERIES_LABEL_ROLES, "period", "footnote")
# Each role reads only silver tables built before it.
SILVER_BUILD_ORDER = (*LOOKUPS, "series", "observations", "population")
FILE_STATE_TABLE = "quest_ops.file_state"
FILE_VERSIONS_TABLE = "quest_ops.file_versions"


def current_version_rows(
    bronze: DataFrame, file_state: DataFrame, file_versions: DataFrame, source: str
) -> DataFrame:
    """Keep bronze rows of the versions ``file_state`` points at, if served and header-clean.

    A ``missing`` file keeps serving its last version (first-absence debounce); only
    ``deleted`` and header-bad versions leave silver. ``source`` is the bronze table's source.
    """
    served = (
        file_state.where(
            (F.col("source") == source) & F.col("status").isin(*[s.value for s in SERVED_STATUSES])
        )
        .join(
            file_versions.where("header_ok"),
            (file_state["source"] == file_versions["source"])
            & (file_state["file_name"] == file_versions["file_name"])
            & (file_state["current_sha256"] == file_versions["sha256"]),
        )
        .select(
            file_state["file_name"].alias("_served_file_name"),
            file_state["current_sha256"].alias("_served_sha256"),
        )
    )
    on = (bronze["_file_name"] == served["_served_file_name"]) & (
        bronze["_sha256"] == served["_served_sha256"]
    )
    return bronze.join(served, on, "left_semi")


def max_year_by_sha(file_versions: DataFrame) -> DataFrame:
    """Return ``(_file_name, _sha256, _max_year)``: the latest year a version may hold.

    That is the year of its Last-Modified, or of its first fetch when the source sends none.
    """
    released = F.coalesce(F.col("http_last_modified"), F.col("first_seen_at"))
    return file_versions.select(
        F.col("file_name").alias("_file_name"),
        F.col("sha256").alias("_sha256"),
        F.year(released).alias("_max_year"),
    ).distinct()


def flag_lookup(current: DataFrame, contract: FileContract) -> DataFrame:
    """Trim a lookup and add ``dq_reasons``."""
    return _flagged(trim_columns(current, contract), contract, {})


def lookup_table(valid: DataFrame, contract: FileContract) -> DataFrame:
    """The lookup with its label canonicalized and the verbatim label kept as ``<label>_source``."""
    label = contract.label_column
    if label is None:
        return valid.select(*contract.column_names, *SILVER_LINEAGE_NAMES)
    labelled = valid.withColumns(
        {label: _canonical(F.col(label), contract), label + SOURCE_SUFFIX: F.col(label)}
    )
    return labelled.select(*contract.column_names, label + SOURCE_SUFFIX, *SILVER_LINEAGE_NAMES)


def period_dim(valid_period: DataFrame) -> DataFrame:
    """Add ``quarter_number`` and ``is_annual_average`` to the period lookup."""
    dim = valid_period.withColumns(
        {
            "quarter_number": _quarter_number(F.col("period")),
            "is_annual_average": F.col("period") == "Q05",
        }
    )
    return _ordered(dim, "period")


def flag_series(
    current: DataFrame, lookups: Mapping[str, DataFrame], contract: FileContract
) -> DataFrame:
    """Trim the series file and flag it against the lookups and the duration unit mapping."""
    flagged = trim_columns(current, contract)
    for role in SERIES_LABEL_ROLES:
        flagged = _with_exists(
            flagged, lookups[role], contract_by_role(role).key[0], f"_{role}_exists"
        )
    helpers = {"_unit_of_measure": _unit_map(contract, "unit_of_measure")[F.col("duration_code")]}
    return _flagged(flagged, contract, helpers)


def series_dim(
    valid: DataFrame, lookups: Mapping[str, DataFrame], contract: FileContract
) -> DataFrame:
    """The series dimension: labels folded in, units, base year, frequency and series_label."""
    dim = valid
    for role in SERIES_LABEL_ROLES:
        key, label = contract_by_role(role).key[0], _label_column(role)
        dim = dim.join(lookups[role].select(key, label, label + SOURCE_SUFFIX), key, "left")
    duration = F.col("duration_code")
    typed = dim.select(
        "series_id",
        F.concat_ws(" | ", *map(_label_column, SERIES_LABEL_ROLES)).alias("series_label"),
        *[c for role in SERIES_LABEL_ROLES for c in _code_and_labels(role)],
        _unit_map(contract, "unit_of_measure")[duration].alias("unit_of_measure"),
        _unit_map(contract, "change_basis")[duration].alias("change_basis"),
        F.when(F.col("base_year") != "-", F.col("base_year").cast("int")).alias("base_year"),
        _unit_map(contract, "value_decimals")[duration].cast("tinyint").alias("value_decimals"),
        F.when(F.col("begin_period") == "Q05", F.lit("ANNUAL"))
        .otherwise(F.lit("QUARTERLY"))
        .alias("frequency"),
        _footnote_array(F.col("footnote_codes")).alias("footnote_codes"),
        F.col("begin_year").cast("int").alias("begin_year"),
        "begin_period",
        F.col("end_year").cast("int").alias("end_year"),
        "end_period",
        *SILVER_LINEAGE_NAMES,
    )
    return _ordered(typed, "series")


def flag_observations(
    current: DataFrame,
    series: DataFrame,
    period: DataFrame,
    footnote: DataFrame,
    max_year_by_sha: DataFrame,
) -> DataFrame:
    """Flag AllData rows against the series and period dims, footnotes and the year bound."""
    contract = contract_by_role("observations")
    # bls_pr_data also holds pr.data.0.Current, which must never reach silver.
    rows = current.where(F.col("_file_name").rlike(contract.file_pattern))
    flagged = trim_columns(rows, contract)
    flagged = flagged.join(
        series.select(
            "series_id",
            F.lit(True).alias("_series_exists"),
            F.col("unit_of_measure").alias("_unit_of_measure"),
        ),
        "series_id",
        "left",
    )
    flagged = _with_exists(flagged, period, "period", "_period_exists")
    known = footnote.agg(F.collect_set("footnote_code").alias("_known_footnotes"))
    codes = _footnote_array(F.col("footnote_codes"))
    flagged = flagged.crossJoin(known).join(max_year_by_sha, ["_file_name", "_sha256"], "left")
    helpers = {
        "_series_exists": F.coalesce(F.col("_series_exists"), F.lit(False)),
        "_footnotes_known": F.forall(
            codes, lambda code: F.array_contains("_known_footnotes", code)
        ),
    }
    return _flagged(flagged, contract, helpers)


def observations(valid: DataFrame) -> DataFrame:
    """The typed observation fact (09 section 2.2.1)."""
    year = F.col("year").cast("int")
    quarter = _quarter_number(F.col("period"))
    footnote_codes = _footnote_array(F.col("footnote_codes"))
    typed = valid.select(
        "series_id",
        year.alias("year"),
        "period",
        quarter.alias("quarter_number"),
        (F.col("period") == "Q05").alias("is_annual_average"),
        F.make_date(year, (quarter - 1) * 3 + 1, F.lit(1)).alias("period_start_date"),
        F.col("value").cast("decimal(12,3)").alias("value"),
        footnote_codes.alias("footnote_codes"),
        F.array_contains(footnote_codes, "R").alias("is_revised"),
        *SILVER_LINEAGE_NAMES,
    )
    return _ordered(typed, "observations")


def flag_population(current: DataFrame, nation_id: str) -> DataFrame:
    """Flag exploded population rows; the year bound is the year the version was ingested."""
    contract = contract_by_role("population")
    helpers = {
        "_expected_nation_id": F.lit(nation_id),
        # DataUSA sends no Last-Modified; bronze _ingested_at is stored, so this stays stable.
        "_max_year": F.year("_ingested_at"),
    }
    return _flagged(trim_columns(current, contract), contract, helpers)


def population(valid: DataFrame) -> DataFrame:
    """The typed population fact, keyed on ``(nation_id, year)``."""
    typed = valid.select(
        "nation_id",
        "nation_name",
        F.col("year").cast("int").alias("year"),
        # Through the DECIMAL(38,18) that NON_INTEGRAL_POPULATION checked: the text is 316128839.0
        # (3.16128839E8 from the Databricks JSON reader), so it is whole and nothing rounds.
        F.col("population").cast("decimal(38,18)").cast("bigint").alias("population"),
        F.col("dataset_name").alias("source_dataset"),
        F.col("table_id").alias("source_table_id"),
        *SILVER_LINEAGE_NAMES,
    )
    return _ordered(typed, "population")


def _ordered(df: DataFrame, role: str) -> DataFrame:
    """Select a silver table's columns in the order its schema DDL declares them."""
    return df.select(*silver_column_names(contract_by_role(role)))


def _flagged(
    trimmed: DataFrame, contract: FileContract, helpers: Mapping[str, Column]
) -> DataFrame:
    key_count = F.count(F.lit(1)).over(Window.partitionBy(*contract.key))
    return with_dq_reasons(
        trimmed.withColumns({"_key_count": key_count, **helpers}),
        quarantine_rules(silver_table_of(contract)),
    )


def _with_exists(df: DataFrame, lookup: DataFrame, key: str, flag: str) -> DataFrame:
    """Add boolean ``flag``: whether ``key`` exists in ``lookup``; distinct keys, so no fan-out."""
    present = lookup.select(key).distinct().withColumn(flag, F.lit(True))
    return df.join(present, key, "left").fillna(False, subset=[flag])


def _canonical(label: Column, contract: FileContract) -> Column:
    """Map source label text to canonical text; unmapped text passes through."""
    if not contract.label_canonical:
        return label
    mapping = F.create_map(*[F.lit(v) for pair in contract.label_canonical.items() for v in pair])
    return F.coalesce(mapping[label], label)


def _unit_map(contract: FileContract, field: str) -> Column:
    """A literal map from duration code to one ``UnitSpec`` field."""
    pairs = [
        (F.lit(code), F.lit(getattr(spec, field)))
        for code, spec in contract.unit_by_duration.items()
    ]
    return F.create_map(*[c for pair in pairs for c in pair])


def _label_column(role: str) -> str:
    label = contract_by_role(role).label_column
    if label is None:
        raise ValueError(f"lookup {role!r} has no label_column")
    return label


def _code_and_labels(role: str) -> tuple[str, str, str]:
    label = _label_column(role)
    return contract_by_role(role).key[0], label, label + SOURCE_SUFFIX


def _quarter_number(period: Column) -> Column:
    """1 to 4 for Q01..Q04, NULL for Q05 (the annual value)."""
    return F.when(period != "Q05", F.substring(period, 3, 1).cast("tinyint"))


def _footnote_array(raw: Column) -> Column:
    """Comma-separated footnote codes as an array; NULL or blank becomes ``[]``."""
    text = F.coalesce(F.trim(raw), F.lit(""))
    return F.when(text == "", F.array().cast("array<string>")).otherwise(
        F.transform(F.split(text, ","), F.trim)
    )


def silver_table_name(role: str) -> str:
    """Two-part name of a role's published silver table."""
    return f"quest_silver.{silver_table_of(contract_by_role(role))}"


@dataclass(frozen=True)
class SilverWiring:
    """Which inputs feed each silver table. The pipeline and the tests both build silver here.

    ``read`` maps a two-part table name (bronze, ops or an earlier silver table) to a frame.
    """

    read: Callable[[str], DataFrame]
    nation_id: str

    def current(self, contract: FileContract) -> DataFrame:
        """Bronze rows of the versions currently served for a contract."""
        return current_version_rows(
            self.read(f"quest_bronze.{contract.bronze_table}"),
            self.read(FILE_STATE_TABLE),
            self.read(FILE_VERSIONS_TABLE),
            contract.source,
        )

    def flagged(self, role: str) -> DataFrame:
        """Every current row of a role with its ``dq_reasons``."""
        contract = contract_by_role(role)
        if role in LOOKUPS:
            return flag_lookup(self.current(contract), contract)
        if role == "series":
            return flag_series(self.current(contract), self._lookups(), contract)
        if role == "observations":
            return flag_observations(
                self.current(contract),
                self.read(silver_table_name("series")),
                self.read(silver_table_name("period")),
                self.read(silver_table_name("footnote")),
                max_year_by_sha(self.read(FILE_VERSIONS_TABLE)),
            )
        return flag_population(self.current(contract), self.nation_id)

    def published(self, role: str, flagged: DataFrame) -> DataFrame:
        """The typed silver table from a role's flagged rows."""
        contract = contract_by_role(role)
        valid = valid_rows(flagged)
        if role == "period":
            return period_dim(lookup_table(valid, contract))
        if role in LOOKUPS:
            return lookup_table(valid, contract)
        if role == "series":
            return series_dim(valid, self._lookups(), contract)
        if role == "observations":
            return observations(valid)
        return population(valid)

    def _lookups(self) -> dict[str, DataFrame]:
        return {role: self.read(silver_table_name(role)) for role in LOOKUPS}
