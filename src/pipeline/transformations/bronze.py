"""Bronze streaming tables: one Auto Loader table per contract bronze table, values verbatim.

Runs only inside a Databricks pipeline.
Configuration: ``quest.landing_root``, ``quest.code_version``.
"""

from pyspark import pipelines as dp
from pyspark.sql import DataFrame, SparkSession

from quest.contracts.loader import load_contracts
from quest.contracts.models import FileContract
from quest.pipeline_meta import table_properties
from quest.transforms.bronze import explode_population, read_spec, source_dir, with_lineage

spark = SparkSession.builder.getOrCreate()
LANDING_ROOT = spark.conf.get("quest.landing_root")
CODE_VERSION = spark.conf.get("quest.code_version")


def define_bronze_table(contract: FileContract) -> None:
    """Register ``quest_bronze.<bronze_table>`` reading every landed version of the contract."""
    spec = read_spec(contract)

    @dp.table(
        name=f"quest_bronze.{contract.bronze_table}",
        comment=f"Every landed version of {contract.landing_glob}, one row per line, verbatim.",
        table_properties=table_properties("bronze", contract, CODE_VERSION),
    )
    def bronze() -> DataFrame:
        stream = (
            spark.readStream.format("cloudFiles")
            .option("cloudFiles.format", spec.format)
            .options(**spec.options)
            .schema(spec.schema)
            .load(source_dir(LANDING_ROOT, contract))
        )
        rows = with_lineage(stream)
        return explode_population(rows, contract) if contract.fmt == "json" else rows


# Both pr.data roles share bls_pr_data and its glob, so the first contract defines it.
first_per_table: dict[str, FileContract] = {}
for c in load_contracts():
    if c.bronze_table is not None:
        first_per_table.setdefault(c.bronze_table, c)
for contract in first_per_table.values():
    define_bronze_table(contract)
