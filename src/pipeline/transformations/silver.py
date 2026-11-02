"""Silver materialized views and their quarantine twins. The wiring lives in quest.transforms.

Runs only inside a Databricks pipeline.
Configuration: ``quest.catalog``, ``quest.nation_id``, ``quest.code_version``.
"""

from pyspark import pipelines as dp
from pyspark.sql import DataFrame, SparkSession

from quest.contracts.loader import contract_by_role, rules_for, silver_table_of
from quest.pipeline_meta import (
    quarantine_table_properties,
    silver_schema_ddl,
    table_comment,
    table_properties,
)
from quest.transforms.dq import expectations, quarantined_rows
from quest.transforms.silver import LOOKUPS, SILVER_BUILD_ORDER, SilverWiring, silver_table_name

spark = SparkSession.builder.getOrCreate()
CATALOG = spark.conf.get("quest.catalog")
CODE_VERSION = spark.conf.get("quest.code_version")
WIRING = SilverWiring(read=spark.read.table, nation_id=spark.conf.get("quest.nation_id"))


def define_silver(role: str) -> None:
    """Register a role's private flagged view, its silver table and its quarantine twin."""
    contract = contract_by_role(role)
    table = silver_table_of(contract)
    flagged_name = f"_{table}_flagged"

    @dp.materialized_view(name=flagged_name, private=True)
    @dp.expect_all(expectations(rules_for(table, "row")))
    def flagged() -> DataFrame:
        return WIRING.flagged(role)

    @dp.materialized_view(
        name=silver_table_name(role),
        comment=table_comment(contract),
        table_properties=table_properties("silver", contract, CODE_VERSION),
        schema=silver_schema_ddl(contract, CATALOG),
    )
    def published() -> DataFrame:
        return WIRING.published(role, spark.read.table(flagged_name))

    @dp.materialized_view(
        name=f"{silver_table_name(role)}_quarantine",
        comment=f"Rows of {table} that failed a rule: verbatim values, dq_reasons, lineage.",
        table_properties=quarantine_table_properties(contract, CODE_VERSION),
    )
    def quarantine() -> DataFrame:
        return quarantined_rows(spark.read.table(flagged_name), contract)


def define_lookup_invariant(role: str) -> None:
    """Fail the update when a lookup has no valid row (LOOKUP_EMPTY)."""
    table = silver_table_of(contract_by_role(role))

    @dp.materialized_view(name=f"_{table}_row_count", private=True)
    @dp.expect_all_or_fail(expectations(rules_for(table, "pipeline")))
    def row_count() -> DataFrame:
        return spark.read.table(silver_table_name(role)).selectExpr("count(*) AS row_count")


for silver_role in SILVER_BUILD_ORDER:
    define_silver(silver_role)
for lookup_role in LOOKUPS:
    define_lookup_invariant(lookup_role)
