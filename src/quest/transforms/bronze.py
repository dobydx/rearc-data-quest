"""Bronze readers: explicit all-STRING schemas, verbatim values, lineage parsed from the path."""

from collections.abc import Mapping
from dataclasses import dataclass

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, StringType, StructField, StructType

from quest.contracts.models import FileContract

RESCUED_DATA = "_rescued_data"
# Auto Loader's rescuedDataColumn matches CSV columns by header name (exact, case-sensitive),
# and BLS headers are padded and renamed, so it would null every value. CSV uses Spark's
# corrupt-record column instead: columns map by position, and a line with the wrong number of
# fields keeps its raw text there.
CSV_OPTIONS: Mapping[str, str] = {
    "sep": "\t",
    "header": "true",
    "mode": "PERMISSIVE",
    "columnNameOfCorruptRecord": RESCUED_DATA,
}
# No rescued column for JSON: the DataUSA envelope always has keys the schema skips, so it
# would never be NULL. A renamed record key still leaves population NULL, which quarantines.
JSON_OPTIONS: Mapping[str, str] = {"multiLine": "true"}

# Exact DataUSA record keys, in contract column order; JSON keys are case-sensitive.
POPULATION_RECORD_KEYS = ("Nation ID", "Nation", "Year", "Population")
POPULATION_JSON_SCHEMA = StructType(
    [
        StructField(
            "annotations",
            StructType(
                [StructField("dataset_name", StringType()), StructField("table_id", StringType())]
            ),
        ),
        StructField("page", StructType([StructField("total", StringType())])),
        StructField(
            "data",
            ArrayType(
                StructType([StructField(key, StringType()) for key in POPULATION_RECORD_KEYS])
            ),
        ),
    ]
)

# Columns bronze adds to every row; they travel with the row through silver and quarantine.
BRONZE_ADDED_COLUMNS = ("_source_path", "_file_name", "_sha256", "_ingested_at", RESCUED_DATA)

_FILE_NAME_PATTERN = r"/([^/]+)/sha256=[0-9a-f]{64}/"
_SHA256_PATTERN = r"sha256=([0-9a-f]{64})"


@dataclass(frozen=True)
class ReadSpec:
    """How to read one contract's landed files: format, options and schema."""

    format: str
    options: Mapping[str, str]
    schema: StructType


def bronze_schema(contract: FileContract) -> StructType:
    """Every contract column as a nullable STRING, in contract order."""
    return StructType([StructField(name, StringType()) for name in contract.column_names])


def read_spec(contract: FileContract) -> ReadSpec:
    """Return the reader settings shared by Auto Loader and the local batch tests."""
    # The glob picks this contract's files; recursion reaches the sha256=<hex> folders and
    # turns off partition inference, so "sha256" never becomes a column.
    selection = {"pathGlobFilter": contract.landing_glob, "recursiveFileLookup": "true"}
    if contract.fmt == "json":
        return ReadSpec("json", {**JSON_OPTIONS, **selection}, POPULATION_JSON_SCHEMA)
    schema = StructType([*bronze_schema(contract).fields, StructField(RESCUED_DATA, StringType())])
    return ReadSpec("csv", {**CSV_OPTIONS, **selection}, schema)


def source_dir(landing_root: str, contract: FileContract) -> str:
    """Return the folder that holds every landed version of a contract's source."""
    return f"{landing_root}/{contract.source}"


def with_lineage(df: DataFrame) -> DataFrame:
    """Add ``_source_path``, ``_file_name``, ``_sha256`` (from the path) and ``_ingested_at``.

    Also adds ``_rescued_data`` as NULL when the reader has none (the JSON reader), so rules
    and quarantine see the same columns for every source.
    """
    if RESCUED_DATA not in df.columns:
        df = df.withColumn(RESCUED_DATA, F.lit(None).cast("string"))
    path = F.col("_metadata.file_path")
    return df.select(
        "*",
        path.alias("_source_path"),
        F.regexp_extract(path, _FILE_NAME_PATTERN, 1).alias("_file_name"),
        F.regexp_extract(path, _SHA256_PATTERN, 1).alias("_sha256"),
        F.current_timestamp().alias("_ingested_at"),
    )


def explode_population(df: DataFrame, contract: FileContract) -> DataFrame:
    """One row per ``data[]`` record, renamed to contract columns, with page and table metadata."""
    lineage = [c for c in df.columns if c.startswith("_")]
    records = df.select(
        F.explode("data").alias("record"),
        F.col("page.total").alias("page_total"),
        F.col("annotations.dataset_name").alias("dataset_name"),
        F.col("annotations.table_id").alias("table_id"),
        *lineage,
    )
    renamed = [
        F.col("record").getField(key).alias(name)
        for key, name in zip(POPULATION_RECORD_KEYS, contract.column_names, strict=True)
    ]
    return records.select(*renamed, "page_total", "dataset_name", "table_id", *lineage)
