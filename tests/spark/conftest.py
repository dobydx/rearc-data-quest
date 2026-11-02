import json
import os
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

import pytest
from pyspark.sql import DataFrame, SparkSession

from quest.contracts.loader import contract_by_role, load_contracts
from quest.contracts.models import FileContract
from quest.gold.sql_bodies import load_gold_selects
from quest.ingest.datausa import LOGICAL_FILE_NAME
from quest.ingest.landing import count_data_lines, land
from quest.spark import SPARK_CONF, pin_conf
from quest.transforms.bronze import explode_population, read_spec, source_dir, with_lineage
from quest.transforms.dq import quarantined_rows
from quest.transforms.silver import (
    LOOKUPS,
    SILVER_BUILD_ORDER,
    SilverWiring,
    silver_table_name,
)

# QUEST_SPARK=connect runs the connect_safe tests on serverless through databricks-connect.
# Those tests build every frame with createDataFrame. Two kinds of setup stay local-only and
# fail loudly if a connect_safe test reaches them: reading local files (serverless cannot see
# this machine) and saveAsTable (two-part names would write into the workspace catalog).
ON_CONNECT = os.environ.get("QUEST_SPARK") == "connect"


def require_local(what: str) -> None:
    """Fail the test if it reaches local-only setup while running on serverless."""
    if ON_CONNECT:
        pytest.fail(f"{what} is local-only; this test must not be marked connect_safe")


@pytest.fixture(scope="session")
def spark(tmp_path_factory: pytest.TempPathFactory) -> Iterator[SparkSession]:
    if ON_CONNECT:
        from databricks.connect import DatabricksSession  # type: ignore[import-not-found]

        yield pin_conf(DatabricksSession.builder.serverless().getOrCreate())
        return
    builder = (
        SparkSession.builder.master("local[1]")
        .appName("quest-tests")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.warehouse.dir", str(tmp_path_factory.mktemp("warehouse")))
    )
    for key, value in SPARK_CONF.items():
        builder = builder.config(key, value)
    session = builder.getOrCreate()
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


@pytest.fixture
def landed_dir(fixtures_dir: Path, tmp_path: Path) -> Path:
    """Every fixture file landed the way ingest lands it."""
    require_local("landed_dir (local files)")
    root = tmp_path / "landing"
    for path in (fixtures_dir / "bls").iterdir():
        land(root, "bls_pr", path.name, path.read_bytes())
    land(
        root, "datausa", LOGICAL_FILE_NAME, (fixtures_dir / "datausa_population.json").read_bytes()
    )
    return root


def read_landed(spark: SparkSession, root: Path, contract: FileContract) -> DataFrame:
    """Batch-read landed files with the same options the pipeline gives Auto Loader."""
    require_local("read_landed (local files)")
    spec = read_spec(contract)
    return (
        spark.read.format(spec.format)
        .options(**spec.options)
        .schema(spec.schema)
        .load(source_dir(str(root), contract))
    )


LAST_MODIFIED = datetime(2026, 9, 3, 12, 30, tzinfo=UTC)
FIRST_SEEN = datetime(2026, 11, 3, 15, 0, tzinfo=UTC)


@dataclass(frozen=True)
class ServedVersion:
    file_name: str
    sha256: str
    status: str = "active"
    header_ok: bool = True
    source: str = "bls_pr"
    line_count: int = 0
    last_modified: datetime = LAST_MODIFIED


def _line_count(path: Path) -> int:
    body = path.read_bytes()
    if path.name.endswith(".json"):
        return len(json.loads(body)["data"])
    return count_data_lines(body)


def landed_versions(root: Path) -> list[ServedVersion]:
    """One served version per landed file: ``<source>/<name>/sha256=<hex>/<name>``."""
    return [
        ServedVersion(
            file_name=path.name,
            sha256=path.parent.name.removeprefix("sha256="),
            source=path.parents[2].name,
            line_count=_line_count(path),
        )
        for path in sorted(root.glob("*/*/sha256=*/*"))
    ]


# The quest_ops.ingest_run_files columns validate reads: the pointer flips, in order.
RUN_FILES_DDL = (
    "run_id STRING, source STRING, file_name STRING, sha256_before STRING, sha256_after STRING, "
    "pointer_flipped BOOLEAN, recorded_at TIMESTAMP"
)


def ops_frames(spark: SparkSession, versions: list[ServedVersion]) -> tuple[DataFrame, DataFrame]:
    """Return ``(file_state, file_versions)`` frames with the columns silver reads."""
    file_state = spark.createDataFrame(
        [(v.source, v.file_name, v.sha256, v.status) for v in versions],
        "source STRING, file_name STRING, current_sha256 STRING, status STRING",
    )
    file_versions = spark.createDataFrame(
        [
            (
                v.source,
                v.file_name,
                v.sha256,
                v.header_ok,
                v.last_modified,
                FIRST_SEEN,
                v.line_count,
            )
            for v in versions
        ],
        "source STRING, file_name STRING, sha256 STRING, header_ok BOOLEAN, "
        "http_last_modified TIMESTAMP, first_seen_at TIMESTAMP, line_count BIGINT",
    )
    return file_state, file_versions


@dataclass(frozen=True)
class Silver:
    """Every silver frame built from one landing root, wired the way the pipeline wires them."""

    lookups: dict[str, DataFrame]
    lookup_flags: dict[str, DataFrame]
    series_flagged: DataFrame
    series: DataFrame
    observations_flagged: DataFrame
    observations: DataFrame
    population_current: DataFrame
    population_flagged: DataFrame
    population: DataFrame


def build_silver(
    spark: SparkSession, root: Path, versions: list[ServedVersion] | None = None
) -> Silver:
    """Build silver with the same SilverWiring the pipeline uses, over local frames."""
    file_state, file_versions = ops_frames(spark, versions or landed_versions(root))
    tables: dict[str, DataFrame] = {
        "quest_ops.file_state": file_state,
        "quest_ops.file_versions": file_versions,
    }
    for contract in load_contracts():
        if contract.bronze_table and f"quest_bronze.{contract.bronze_table}" not in tables:
            rows = with_lineage(read_landed(spark, root, contract))
            bronze = explode_population(rows, contract) if contract.fmt == "json" else rows
            tables[f"quest_bronze.{contract.bronze_table}"] = bronze
    wiring = SilverWiring(read=tables.__getitem__, nation_id="01000US")
    flagged: dict[str, DataFrame] = {}
    for role in SILVER_BUILD_ORDER:
        flagged[role] = wiring.flagged(role)
        tables[silver_table_name(role)] = wiring.published(role, flagged[role])
    return Silver(
        lookups={role: tables[silver_table_name(role)] for role in LOOKUPS},
        lookup_flags={role: flagged[role] for role in LOOKUPS},
        series_flagged=flagged["series"],
        series=tables[silver_table_name("series")],
        observations_flagged=flagged["observations"],
        observations=tables[silver_table_name("observations")],
        population_current=wiring.current(contract_by_role("population")),
        population_flagged=flagged["population"],
        population=tables[silver_table_name("population")],
    )


FIXTURES = Path(__file__).parent.parent / "fixtures"
SILVER_SCHEMA = "quest_silver"
TABLE_VERSIONS_DDL = (
    "run_id STRING, table_name STRING, layer STRING, delta_version BIGINT, row_count BIGINT, "
    "content_checksum DECIMAL(38,0), changed_since_previous BOOLEAN, captured_at TIMESTAMP"
)
GOLD_SQL = Path(__file__).parents[2] / "src" / "pipeline" / "transformations" / "gold.sql"


def save(spark: SparkSession, df: DataFrame, table: str) -> None:
    require_local("save (saveAsTable)")
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {table.split('.')[0]}")
    df.write.mode("overwrite").saveAsTable(table)


def publish_gold(spark: SparkSession, name: str) -> None:
    """(Re)build one gold table from its SQL body, as the pipeline would."""
    save(spark, spark.sql(load_gold_selects(GOLD_SQL)[name]), f"quest_gold.{name}")


@pytest.fixture(scope="session")
def silver_catalog(spark: SparkSession, tmp_path_factory: pytest.TempPathFactory) -> Silver:
    """Fixture data published as two-part bronze, silver, quarantine, gold and ops tables."""
    root = tmp_path_factory.mktemp("catalog_landing")
    for path in (FIXTURES / "bls").iterdir():
        land(root, "bls_pr", path.name, path.read_bytes())
    land(root, "datausa", LOGICAL_FILE_NAME, (FIXTURES / "datausa_population.json").read_bytes())
    silver = build_silver(spark, root)
    flagged = {
        "bls_pr_observations": (silver.observations_flagged, silver.observations, "observations"),
        "bls_pr_series": (silver.series_flagged, silver.series, "series"),
        "datausa_population": (silver.population_flagged, silver.population, "population"),
        **{
            contract_by_role(role).silver_table or role: (flags, silver.lookups[role], role)
            for role, flags in silver.lookup_flags.items()
        },
    }
    for table, (flags, valid, role) in flagged.items():
        save(spark, valid, f"{SILVER_SCHEMA}.{table}")
        save(
            spark,
            quarantined_rows(flags, contract_by_role(role)),
            f"{SILVER_SCHEMA}.{table}_quarantine",
        )
    observations_contract = contract_by_role("observations")
    population_contract = contract_by_role("population")
    save(
        spark,
        with_lineage(read_landed(spark, root, observations_contract)),
        "quest_bronze.bls_pr_data",
    )
    population_bronze = with_lineage(read_landed(spark, root, population_contract))
    save(
        spark,
        explode_population(population_bronze, population_contract),
        "quest_bronze.datausa_population",
    )
    file_state, file_versions = ops_frames(spark, landed_versions(root))
    save(spark, file_state, "quest_ops.file_state")
    save(spark, file_versions, "quest_ops.file_versions")
    save(spark, spark.createDataFrame([], TABLE_VERSIONS_DDL), "quest_ops.table_versions")
    save(spark, spark.createDataFrame([], RUN_FILES_DDL), "quest_ops.ingest_run_files")
    for name in load_gold_selects(GOLD_SQL):
        publish_gold(spark, name)
    return silver
