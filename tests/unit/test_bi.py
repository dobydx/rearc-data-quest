"""Keeps the dashboard, the DQ alert and the Genie space in step with the files they embed."""

import importlib.util
import json
import re
from pathlib import Path
from typing import Any

import pytest
import yaml

ROOT = Path(__file__).parents[2]
SQL_DIR = ROOT / "src" / "sql"
DASHBOARD = ROOT / "src" / "dashboards" / "data_quest.lvdash.json"
ALERT_YAML = ROOT / "resources" / "quest_dq_alert.alert.yml"


@pytest.fixture(scope="module")
def dashboard() -> dict[str, Any]:
    loaded: dict[str, Any] = json.loads(DASHBOARD.read_text())
    return loaded


def test_each_dashboard_dataset_is_its_sql_file(dashboard: dict[str, Any]) -> None:
    datasets = {d["name"]: "".join(d["queryLines"]) for d in dashboard["datasets"]}
    files = {p.stem: p.read_text() for p in (SQL_DIR / "dashboard").glob("*.sql")}

    assert datasets == files


def test_every_widget_reads_a_defined_dataset(dashboard: dict[str, Any]) -> None:
    names = {d["name"] for d in dashboard["datasets"]}
    used = {
        query["query"]["datasetName"]
        for page in dashboard["pages"]
        for item in page["layout"]
        for query in item["widget"].get("queries", [])
    }

    assert used == names


def test_dashboard_has_the_three_pages(dashboard: dict[str, Any]) -> None:
    assert [p["displayName"] for p in dashboard["pages"]] == ["Answers", "Data quality", "Explorer"]


def test_answers_page_is_full_width_and_keeps_the_q2_table(dashboard: dict[str, Any]) -> None:
    (page,) = [p for p in dashboard["pages"] if p["name"] == "answers"]
    layout = {
        item["widget"]["name"]: tuple(item["position"].values()) for item in page["layout"]
    }  # (x, y, width, height)

    assert layout == {
        "answers_intro": (0, 0, 12, 2),
        "q1_mean": (0, 2, 4, 3),
        "q1_stddev": (4, 2, 4, 3),
        "q1_years": (8, 2, 4, 3),
        "q2_sector": (0, 5, 4, 1),
        "q2_unit": (4, 5, 4, 1),
        "q2_partial": (8, 5, 4, 1),
        "q2_table": (0, 6, 12, 9),
        "q3_value": (0, 15, 12, 5),
        "q3_population": (0, 20, 12, 5),
    }


def test_alert_query_is_the_sql_file_with_the_catalog_named() -> None:
    alert = yaml.safe_load(ALERT_YAML.read_text())["resources"]["alerts"]["quest_dq_alert"]

    query = alert["query_text"].replace("${var.catalog}.", "")

    assert query == (SQL_DIR / "alert_dq.sql").read_text()
    assert alert["evaluation"]["source"]["name"] == "issues"
    # Paused by decision: dev runs nothing on a schedule; it is evaluated by hand.
    assert alert["schedule"]["pause_status"] == "PAUSED"


GENIE = ROOT / "src" / "genie"
GOLD_SQL = ROOT / "src" / "pipeline" / "transformations" / "gold.sql"
GOLD_TABLES = (
    "population_stats_2013_2018",
    "series_best_year",
    "prs30006032_q01_with_population",
    "productivity_observations",
)
TRUSTED = {
    "q1.sql": "population_stats_2013_2018",
    "q2.sql": "series_best_year",
    "q3.sql": "prs30006032_q01_with_population",
}


@pytest.fixture(scope="module")
def space() -> dict[str, Any]:
    loaded: dict[str, Any] = json.loads((GENIE / "data_quest.geniespace.json").read_text())
    return loaded


def _gold_columns(table: str) -> set[str]:
    ddl = GOLD_SQL.read_text().split(f"MATERIALIZED VIEW quest_gold.{table} (", 1)[1]
    return set(re.findall(r"^\s+(\w+) [A-Z]+", ddl.split("CONSTRAINT", 1)[0], re.MULTILINE))


def test_trusted_sql_reads_only_its_gold_table() -> None:
    # Genie reads the published gold answers, so its queries cannot fork the gold.sql logic.
    for file_name, table in TRUSTED.items():
        sql = (GENIE / "trusted_sql" / file_name).read_text()
        selected = re.search(r"SELECT(.*?)FROM", sql, re.DOTALL)

        assert re.findall(r"\b(?:FROM|JOIN)\s+([\w.]+)", sql) == [f"quest_gold.{table}"]
        assert selected is not None
        columns = {c.strip() for c in selected.group(1).split(",")}
        assert columns <= _gold_columns(table), file_name


def test_genie_space_embeds_the_genie_files(space: dict[str, Any]) -> None:
    questions = [
        re.sub(r"^\d+\.\s*", "", line)
        for line in (GENIE / "sample_questions.md").read_text().splitlines()
        if line.strip()
    ]
    trusted = {
        "".join(e["question"]): "".join(e["sql"])
        for e in space["instructions"]["example_question_sqls"]
    }
    (instructions,) = space["instructions"]["text_instructions"]

    assert len(questions) == 5
    assert sorted("".join(q["question"]) for q in space["config"]["sample_questions"]) == sorted(
        questions
    )
    assert trusted == {
        questions[i]: (GENIE / "trusted_sql" / name).read_text() for i, name in enumerate(TRUSTED)
    }
    assert "".join(instructions["content"]) == (GENIE / "instructions.md").read_text()


def test_genie_space_lists_only_the_gold_tables(space: dict[str, Any]) -> None:
    # The space format has no default catalog, so identifiers name it; it must match the bundle.
    catalog = yaml.safe_load((ROOT / "databricks.yml").read_text())["variables"]["catalog"]

    assert [t["identifier"] for t in space["data_sources"]["tables"]] == sorted(
        f"{catalog['default']}.quest_gold.{t}" for t in GOLD_TABLES
    )


def test_committed_bi_json_is_what_the_builder_writes(
    dashboard: dict[str, Any], space: dict[str, Any]
) -> None:
    spec = importlib.util.spec_from_file_location("build_bi", ROOT / "scripts" / "build_bi.py")
    assert spec is not None and spec.loader is not None
    build_bi = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(build_bi)

    assert build_bi.build_dashboard() == dashboard
    assert build_bi.build_genie_space() == space
