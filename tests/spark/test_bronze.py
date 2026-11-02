from pathlib import Path

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType

from quest.contracts.loader import contract_by_role
from quest.ingest.landing import land, sha256_hex
from quest.transforms.bronze import (
    POPULATION_RECORD_KEYS,
    RESCUED_DATA,
    bronze_schema,
    explode_population,
    read_spec,
    with_lineage,
)
from quest.transforms.silver import flag_population
from tests.spark.conftest import read_landed

pytestmark = pytest.mark.spark


def test_bronze_schema_is_all_strings_in_contract_order() -> None:
    contract = contract_by_role("series")

    schema = bronze_schema(contract)

    assert schema.fieldNames() == list(contract.column_names)
    assert {type(f.dataType) for f in schema.fields} == {StringType}


def test_bronze_alldata_reads_verbatim(spark: SparkSession, landed_dir: Path) -> None:
    contract = contract_by_role("observations")
    df = with_lineage(read_landed(spark, landed_dir, contract))

    alldata = df.where("_file_name = 'pr.data.1.AllData'")

    assert alldata.count() == 1409
    first = alldata.where(
        "series_id LIKE 'PRS30006013%' AND year = '1987' AND period = 'Q01'"
    ).first()
    assert first is not None
    assert first["series_id"] == "PRS30006013      "
    assert first["value"] == "     140.444"
    assert dict(df.dtypes)["value"] == "string"


def test_bronze_data_table_reads_both_data_files(spark: SparkSession, landed_dir: Path) -> None:
    df = with_lineage(read_landed(spark, landed_dir, contract_by_role("observations")))

    names = {row["_file_name"] for row in df.select("_file_name").distinct().collect()}

    assert names == {"pr.data.0.Current", "pr.data.1.AllData"}


def test_lineage_columns_parsed_from_path(
    spark: SparkSession, landed_dir: Path, fixtures_dir: Path
) -> None:
    df = with_lineage(read_landed(spark, landed_dir, contract_by_role("series")))

    row = df.first()

    assert row is not None
    assert row["_file_name"] == "pr.series"
    assert row["_sha256"] == sha256_hex((fixtures_dir / "bls" / "pr.series").read_bytes())
    assert row["_source_path"].endswith(f"/sha256={row['_sha256']}/pr.series")
    assert row["_ingested_at"] is not None


def test_seasonal_blank_line_not_a_row(spark: SparkSession, landed_dir: Path) -> None:
    df = read_landed(spark, landed_dir, contract_by_role("seasonal"))

    assert df.count() == 2


def test_population_explodes_11_rows_as_strings(spark: SparkSession, landed_dir: Path) -> None:
    contract = contract_by_role("population")
    df = explode_population(with_lineage(read_landed(spark, landed_dir, contract)), contract)

    rows = {row["year"]: row for row in df.collect()}

    assert len(rows) == 11
    assert rows["2013"]["population"] == "316128839.0"
    assert rows["2013"]["nation_id"] == "01000US"
    assert rows["2013"]["page_total"] == "11"
    assert rows["2013"]["table_id"] == "B01003"
    assert rows["2013"]["_file_name"] == "acs_population_nation.json"
    assert {dtype for name, dtype in df.dtypes if not name.startswith("_")} == {"string"}


def test_population_record_keys_match_contract_aliases() -> None:
    aliases = [c.source_aliases[0] for c in contract_by_role("population").columns]

    assert [key.lower() for key in POPULATION_RECORD_KEYS] == aliases


def test_lineage_adds_null_rescued_data_when_reader_has_none(
    spark: SparkSession, landed_dir: Path
) -> None:
    # The JSON read requests no rescued column, so with_lineage supplies it.
    df = with_lineage(read_landed(spark, landed_dir, contract_by_role("population")))

    assert dict(df.dtypes)["_rescued_data"] == "string"
    assert df.where("_rescued_data IS NOT NULL").count() == 0


def test_csv_spec_keeps_bad_lines_in_the_corrupt_record_column() -> None:
    # Auto Loader's rescuedDataColumn matches CSV columns by header name, and BLS headers are
    # padded and renamed, so it nulls every value (workspace finding, 2026-11-03).
    spec = read_spec(contract_by_role("series"))

    assert spec.options["columnNameOfCorruptRecord"] == RESCUED_DATA
    assert "rescuedDataColumn" not in spec.options
    assert spec.schema.fieldNames()[-1] == RESCUED_DATA


def test_json_spec_requests_no_rescued_column() -> None:
    # The DataUSA envelope always has keys the schema skips, so a rescued column would never
    # be NULL and could not signal anything.
    spec = read_spec(contract_by_role("population"))

    assert "rescuedDataColumn" not in spec.options


def test_renamed_population_key_is_quarantined_not_dropped(
    spark: SparkSession, fixtures_dir: Path, tmp_path: Path
) -> None:
    body = (fixtures_dir / "datausa_population.json").read_bytes()
    land(tmp_path, "datausa", "acs_population_nation.json", body.replace(b'"Population"', b'"Pop"'))
    contract = contract_by_role("population")

    exploded = explode_population(with_lineage(read_landed(spark, tmp_path, contract)), contract)
    reasons = {tuple(row["dq_reasons"]) for row in flag_population(exploded, "01000US").collect()}

    assert reasons == {("NON_NUMERIC_POPULATION", "NON_INTEGRAL_POPULATION")}


def test_line_with_wrong_field_count_keeps_its_raw_text(
    spark: SparkSession, fixtures_dir: Path, tmp_path: Path
) -> None:
    header, good = (fixtures_dir / "bls" / "pr.series").read_bytes().split(b"\r\n")[:2]
    short = b"\t".join(good.split(b"\t")[:5])
    long = good + b"\tEXTRA"
    land(tmp_path, "bls_pr", "pr.series", b"\r\n".join([header, good, short, long, b""]))

    rows = with_lineage(read_landed(spark, tmp_path, contract_by_role("series"))).collect()

    assert [row[RESCUED_DATA] for row in rows] == [None, short.decode(), long.decode()]
    # Columns still map by position, so the padded header names never matter.
    assert [row["series_id"] for row in rows] == [good.split(b"\t")[0].decode()] * 3
