"""Scratch pipeline quest_scratch_verify: platform behaviour checked before the real pipeline.

Not part of the real pipeline (its libraries glob only covers transformations/). Each dataset
answers one question about the platform, so the real code relies only on observed behaviour.
Configuration: ``quest.landing_root`` (a scratch Volume holding copies of the BLS fixtures).
"""

from pyspark import pipelines as dp
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from quest.contracts.loader import contract_by_role
from quest.transforms.bronze import read_spec, source_dir, with_lineage

spark = SparkSession.builder.getOrCreate()
LANDING_ROOT = spark.conf.get("quest.landing_root")


def define_probe_bronze(role: str) -> None:
    """Auto Loader over a Volume, and the path format in ``_metadata``: the real bronze reader."""
    contract = contract_by_role(role)
    spec = read_spec(contract)

    @dp.table(name=f"quest_scratch.probe_bronze_{role}")
    def probe() -> DataFrame:
        stream = (
            spark.readStream.format("cloudFiles")
            .option("cloudFiles.format", spec.format)
            .options(**spec.options)
            .schema(spec.schema)
            .load(source_dir(LANDING_ROOT, contract))
        )
        return with_lineage(stream)


for probe_role in ("series", "seasonal", "observations"):
    define_probe_bronze(probe_role)


@dp.materialized_view(name="_probe_private", private=True)
def probe_private() -> DataFrame:
    """Private dataset, read back below by its unqualified name."""
    return spark.range(3).withColumnRenamed("id", "parent_id")


@dp.materialized_view(
    name="quest_scratch.probe_pk_parent",
    schema=(
        "parent_id BIGINT NOT NULL, CONSTRAINT probe_pk_parent_pk PRIMARY KEY (parent_id) NORELY"
    ),
)
def probe_pk_parent() -> DataFrame:
    """PK constraints on an MV, fed from a private dataset by unqualified name."""
    return spark.read.table("_probe_private")


@dp.materialized_view(
    name="quest_scratch.probe_fk_child",
    schema=(
        "child_id BIGINT NOT NULL, parent_id BIGINT, "
        "CONSTRAINT probe_fk_child_pk PRIMARY KEY (child_id) NORELY, "
        "CONSTRAINT probe_fk_child_parent_fk FOREIGN KEY (parent_id) "
        "REFERENCES workspace.quest_scratch.probe_pk_parent (parent_id) NORELY"
    ),
)
def probe_fk_child() -> DataFrame:
    """FK constraints between two MVs of the same pipeline."""
    return spark.read.table("quest_scratch.probe_pk_parent").select(
        (F.col("parent_id") + 10).alias("child_id"), "parent_id"
    )


@dp.materialized_view(name="quest_scratch.probe_not_null", schema="id BIGINT NOT NULL")
def probe_not_null() -> DataFrame:
    """What a NULL in a NOT NULL column does. Refreshed alone, in its own update."""
    return spark.range(2).select(F.when(F.col("id") == 1, None).otherwise(F.col("id")).alias("id"))
