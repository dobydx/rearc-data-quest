"""Regenerate the dashboard and Genie definitions from their committed sources.

The dashboard embeds the queries in ``src/sql/dashboard/*.sql``; the Genie space embeds
``src/genie/instructions.md``, ``sample_questions.md`` and ``trusted_sql/*.sql``. Edit those files,
then run ``make bi`` (this script). ``tests/unit/test_bi.py`` fails while the JSON is stale.
"""

import json
import re
import uuid
from pathlib import Path
from typing import Any

import yaml

ROOT = Path(__file__).parents[1]
SQL_DIR = ROOT / "src" / "sql" / "dashboard"
GENIE_DIR = ROOT / "src" / "genie"
DASHBOARD_JSON = ROOT / "src" / "dashboards" / "data_quest.lvdash.json"
GENIE_JSON = GENIE_DIR / "data_quest.geniespace.json"

GOLD_TABLES = (
    "population_stats_2013_2018",
    "series_best_year",
    "prs30006032_q01_with_population",
    "productivity_observations",
)
# Trusted query file -> index of the sample question it answers (1-based).
TRUSTED_SQL = {"q1.sql": 1, "q2.sql": 2, "q3.sql": 3}
EXPLORER_DEFAULTS = {
    "sector": "Nonfarm Business",
    "measure": "Labor productivity (output per hour)",
    "duration": "% Change same quarter 1 year ago",
}
Widget = dict[str, Any]

# ---- dashboard pieces -------------------------------------------------------------------------


def _dataset(name: str, display_name: str) -> dict[str, Any]:
    dataset: dict[str, Any] = {
        "name": name,
        "displayName": display_name,
        "queryLines": (SQL_DIR / f"{name}.sql").read_text().splitlines(keepends=True),
    }
    if name == "explorer_series_values":
        dataset["parameters"] = [
            {
                "displayName": key,
                "keyword": key,
                "dataType": "STRING",
                "defaultSelection": {
                    "values": {"dataType": "STRING", "values": [{"value": value}]}
                },
            }
            for key, value in EXPLORER_DEFAULTS.items()
        ]
    return dataset


_DISPLAY_AS = {
    "string": "string",
    "integer": "number",
    "float": "number",
    "decimal": "number",
    "datetime": "datetime",
    "boolean": "boolean",
}


def _column(field: str, kind: str, order: int) -> dict[str, Any]:
    # The full key set the dashboard editor writes for a table column.
    column: dict[str, Any] = {
        "fieldName": field,
        "booleanValues": ["false", "true"],
        "imageUrlTemplate": "{{ @ }}",
        "imageTitleTemplate": "{{ @ }}",
        "imageWidth": "",
        "imageHeight": "",
        "linkUrlTemplate": "{{ @ }}",
        "linkTextTemplate": "{{ @ }}",
        "linkTitleTemplate": "{{ @ }}",
        "linkOpenInNewTab": True,
        "type": kind,
        "displayAs": _DISPLAY_AS[kind],
        "visible": True,
        "order": 100000 + order,
        "title": field,
        "allowSearch": kind == "string",
        "alignContent": "left" if kind in ("string", "boolean") else "right",
        "allowHTML": False,
        "highlightLinks": False,
        "useMonospaceFont": False,
        "preserveWhitespace": False,
    }
    if kind == "integer":
        column["numberFormat"] = "0"
    return column


def _frame(title: str, description: str | None) -> dict[str, Any]:
    frame: dict[str, Any] = {"showTitle": True, "title": title}
    if description:
        frame |= {"showDescription": True, "description": description}
    return frame


def _pos(x: int, y: int, width: int, height: int) -> dict[str, int]:
    return {"x": x, "y": y, "width": width, "height": height}


def _field(name: str, expression: str | None = None) -> dict[str, str]:
    return {"name": name, "expression": expression or f"`{name}`"}


def _main_query(dataset: str, fields: list[dict[str, str]], disaggregated: bool) -> list[Widget]:
    query = {"datasetName": dataset, "fields": fields, "disaggregated": disaggregated}
    return [{"name": "main_query", "query": query}]


def _text(name: str, markdown: str, position: dict[str, int]) -> Widget:
    lines = markdown.splitlines(keepends=True)
    widget = {"name": name, "multilineTextboxSpec": {"lines": lines}}
    return {"widget": widget, "position": position}


def _table(
    name: str,
    dataset: str,
    columns: list[tuple[str, str]],
    title: str,
    position: dict[str, int],
    description: str | None = None,
) -> Widget:
    spec = {
        "version": 1,
        "widgetType": "table",
        "encodings": {"columns": [_column(c, k, i) for i, (c, k) in enumerate(columns)]},
        "frame": _frame(title, description),
    }
    queries = _main_query(dataset, [_field(c) for c, _ in columns], disaggregated=True)
    return {"widget": {"name": name, "queries": queries, "spec": spec}, "position": position}


def _counter(
    name: str, dataset: str, value: str, title: str, position: dict[str, int], description: str
) -> Widget:
    spec = {
        "version": 2,
        "widgetType": "counter",
        "encodings": {"value": {"fieldName": value, "displayName": title}},
        "frame": _frame(title, description),
    }
    queries = _main_query(dataset, [_field(value)], disaggregated=True)
    return {"widget": {"name": name, "queries": queries, "spec": spec}, "position": position}


def _axis(field: str, scale: str, title: str) -> dict[str, Any]:
    return {
        "fieldName": field,
        "scale": {"type": scale},
        "axis": {"title": title},
        "displayName": title,
    }


def _color(field: str) -> dict[str, Any]:
    return {"fieldName": field, "scale": {"type": "categorical"}, "displayName": field}


def _line(
    name: str,
    dataset: str,
    x: tuple[str, str],
    y: tuple[str, str],
    title: str,
    position: dict[str, int],
    color: str | None = None,
    description: str | None = None,
) -> Widget:
    """A single-axis line chart; ``x`` and ``y`` are ``(field, axis title)``."""
    (x_field, x_title), (y_field, y_title) = x, y
    x_scale = "temporal" if x_field.endswith("date") else "quantitative"
    encodings: dict[str, Any] = {
        "x": _axis(x_field, x_scale, x_title),
        "y": _axis(y_field, "quantitative", y_title),
    }
    fields = [_field(x_field), _field(y_field)]
    if color:
        encodings["color"] = _color(color)
        fields.append(_field(color))
    spec = {
        "version": 3,
        "widgetType": "line",
        "encodings": encodings,
        "frame": _frame(title, description),
    }
    queries = _main_query(dataset, fields, disaggregated=True)
    return {"widget": {"name": name, "queries": queries, "spec": spec}, "position": position}


def _bar(
    name: str,
    dataset: str,
    x: tuple[str, str],
    y: tuple[str, str, str],
    color: str,
    title: str,
    position: dict[str, int],
    description: str | None = None,
) -> Widget:
    """A stacked bar chart; ``x`` is ``(field, title)``, ``y`` is ``(name, expression, title)``."""
    (x_field, x_title), (y_name, y_expression, y_title) = x, y
    spec = {
        "version": 3,
        "widgetType": "bar",
        "encodings": {
            "x": _axis(x_field, "categorical", x_title),
            "y": _axis(y_name, "quantitative", y_title),
            "color": _color(color),
        },
        "frame": _frame(title, description),
    }
    fields = [_field(x_field), _field(y_name, y_expression), _field(color)]
    queries = _main_query(dataset, fields, disaggregated=False)
    return {"widget": {"name": name, "queries": queries, "spec": spec}, "position": position}


def _options_query(name: str, dataset: str, column: str) -> dict[str, Any]:
    # The editor adds this count to every filter query; it drives cross-filter narrowing.
    associativity = _field(
        f"{column}_associativity", "COUNT_IF(`associative_filter_predicate_group`)"
    )
    query = {
        "datasetName": dataset,
        "fields": [_field(column), associativity],
        "disaggregated": False,
    }
    return {"name": name, "query": query}


def _column_filter(
    name: str, dataset: str, column: str, title: str, position: dict[str, int]
) -> Widget:
    """A multi-select that filters ``dataset`` on ``column``."""
    spec = {
        "version": 2,
        "widgetType": "filter-multi-select",
        "encodings": {"fields": [{"fieldName": column, "displayName": title, "queryName": name}]},
        "frame": _frame(title, None),
    }
    queries = [_options_query(name, dataset, column)]
    return {"widget": {"name": name, "queries": queries, "spec": spec}, "position": position}


def _parameter_filter(
    name: str,
    options: tuple[str, str],
    target: tuple[str, str],
    title: str,
    position: dict[str, int],
) -> Widget:
    """A single-select over ``options`` (dataset, column) that sets ``target`` (dataset, param)."""
    (options_dataset, column), (dataset, parameter) = options, target
    options_name, parameter_name = f"{name}_options", f"{name}_parameter"
    parameter_query = {
        "datasetName": dataset,
        "parameters": [{"name": parameter, "keyword": parameter}],
        "disaggregated": False,
    }
    spec = {
        "version": 2,
        "widgetType": "filter-single-select",
        "encodings": {
            "fields": [
                {"fieldName": column, "displayName": title, "queryName": options_name},
                {"parameterName": parameter, "queryName": parameter_name},
            ]
        },
        "frame": _frame(title, None),
    }
    queries = [
        _options_query(options_name, options_dataset, column),
        {"name": parameter_name, "query": parameter_query},
    ]
    return {"widget": {"name": name, "queries": queries, "spec": spec}, "position": position}


# ---- the dashboard ----------------------------------------------------------------------------

Q1 = "answers_q1_population_stats"
Q2 = "answers_q2_series_best_year"
Q3 = "answers_q3_prs30006032_q01"
OPTIONS, VALUES = "explorer_series_options", "explorer_series_values"


def _answers_page() -> list[Widget]:
    intro = (
        "## Quest answers\n"
        "\n"
        "Q1: US population 2013 to 2018 (DataUSA, ACS 1-year). \n"
        "\n"
        "Q2: best year per BLS series, Q01 to Q04 summed, Q05 never added. \n"
        "\n"
        "Q3: PRS30006032 Q01 with that year's population."
    )
    q2_columns = [
        ("series_id", "string"),
        ("series_label", "string"),
        ("unit_of_measure", "string"),
        ("best_year", "integer"),
        ("best_year_value_sum", "decimal"),
        ("quarters_in_best_year", "integer"),
        ("is_partial_year", "boolean"),
        ("tied_year_count", "integer"),
        ("has_quarterly_data", "boolean"),
    ]
    return [
        _text("answers_intro", intro, _pos(0, 0, 12, 2)),
        _counter(
            "q1_mean",
            Q1,
            "population_mean",
            "Q1 mean population",
            _pos(0, 2, 4, 3),
            "Unrounded mean over 2013 to 2018",
        ),
        _counter(
            "q1_stddev",
            Q1,
            "population_stddev_samp",
            "Q1 standard deviation (sample)",
            _pos(4, 2, 4, 3),
            "stddev_samp, n - 1",
        ),
        _counter(
            "q1_years",
            Q1,
            "n_years",
            "Q1 years with a value",
            _pos(8, 2, 4, 3),
            "6 means the window is complete",
        ),
        _column_filter("q2_sector", Q2, "sector_name", "Sector", _pos(0, 5, 4, 1)),
        _column_filter("q2_unit", Q2, "unit_of_measure", "Unit of measure", _pos(4, 5, 4, 1)),
        _column_filter("q2_partial", Q2, "is_partial_year", "Partial best year", _pos(8, 5, 4, 1)),
        _table(
            "q2_table",
            Q2,
            q2_columns,
            "Q2 best year per series",
            _pos(0, 6, 12, 9),
            "A sum of % changes has no economic meaning; read it as the literal answer to the "
            "question. Annual-only series have no best year.",
        ),
        # Two single-axis charts, not one dual-axis chart: a % change and a population share no
        # unit, and two scales on one plot suggest a correlation that is only the scaling.
        _line(
            "q3_value",
            Q3,
            ("year", "Year"),
            ("value", "Q01 value (% change)"),
            "Q3 PRS30006032 Q01 value by year",
            _pos(0, 15, 12, 5),
        ),
        _line(
            "q3_population",
            Q3,
            ("year", "Year"),
            ("population", "Population"),
            "Q3 US population by year",
            _pos(0, 20, 12, 5),
            description="Same years as above. Gaps are years DataUSA has no value for, "
            "2020 among them.",
        ),
    ]


def _data_quality_page() -> list[Widget]:
    intro = (
        "## Data quality\n"
        "Latest validate run first, then ingest history, quarantine, pipeline expectations "
        "and freshness.\n"
    )
    checks = [
        ("check_code", "string"),
        ("table_name", "string"),
        ("severity", "string"),
        ("status", "string"),
        ("observed_value", "string"),
        ("threshold", "string"),
        ("failing_rows", "integer"),
        ("run_id", "string"),
    ]
    quarantine = [("table_name", "string"), ("reason", "string"), ("quarantined_rows", "integer")]
    expectations = [
        ("dataset", "string"),
        ("expectation", "string"),
        ("passed_records", "integer"),
        ("failed_records", "integer"),
        ("pass_rate", "float"),
    ]
    freshness = [
        ("source", "string"),
        ("file_name", "string"),
        ("status", "string"),
        ("http_last_modified", "datetime"),
        ("days_since_modified", "integer"),
        ("days_since_pointer_update", "integer"),
    ]
    runs = [
        ("started_at", "datetime"),
        ("status", "string"),
        ("trigger", "string"),
        ("code_version", "string"),
        ("duration_s", "integer"),
        ("versions_landed", "integer"),
        ("pointers_flipped", "integer"),
        ("files_failed", "integer"),
        ("error_message", "string"),
    ]
    return [
        _text("dq_intro", intro, _pos(0, 0, 6, 1)),
        _table(
            "dq_checks",
            "dq_checks_latest",
            checks,
            "Latest dq_checks",
            _pos(0, 1, 6, 8),
            "Problems sort first. quest_dq_alert fires on any warn, fail or error here.",
        ),
        _bar(
            "dq_runs",
            "dq_ingest_runs",
            ("started_at", "Run start"),
            ("sum(duration_s)", "SUM(`duration_s`)", "Duration (s)"),
            "status",
            "Ingest runs",
            _pos(0, 9, 3, 6),
        ),
        _bar(
            "dq_actions",
            "dq_run_file_actions",
            ("run_started_at", "Run start"),
            ("count(*)", "COUNT(`*`)", "Files"),
            "action",
            "Per-file actions by run",
            _pos(3, 9, 3, 6),
            "A no-op run is all SKIPPED_FINGERPRINT.",
        ),
        _table(
            "dq_quarantine",
            "dq_quarantine_by_reason",
            quarantine,
            "Quarantine by reason",
            _pos(0, 15, 3, 6),
        ),
        _table(
            "dq_expectations",
            "dq_expectation_pass_rates",
            expectations,
            "Expectation pass rates (latest update)",
            _pos(3, 15, 3, 6),
        ),
        _table(
            "dq_freshness",
            "dq_freshness",
            freshness,
            "Freshness",
            _pos(0, 21, 6, 6),
            "Some BLS files (pr.period, pr.txt) are years old by design; STALE_SOURCE watches "
            "pr.data.1.AllData.",
        ),
        _table("dq_runs_table", "dq_ingest_runs", runs, "Ingest runs detail", _pos(0, 27, 6, 6)),
    ]


def _explorer_page() -> list[Widget]:
    intro = (
        "## Explorer\n"
        "Pick a sector, measure and duration; together they name one series. Q05 annual values "
        "are left out.\n"
    )
    rows = [
        ("series_id", "string"),
        ("series_label", "string"),
        ("unit_of_measure", "string"),
        ("year", "integer"),
        ("period", "string"),
        ("value", "decimal"),
        ("is_revised", "boolean"),
    ]
    return [
        _text("explorer_intro", intro, _pos(0, 0, 6, 1)),
        _parameter_filter(
            "explorer_sector",
            (OPTIONS, "sector_name"),
            (VALUES, "sector"),
            "Sector",
            _pos(0, 1, 2, 1),
        ),
        _parameter_filter(
            "explorer_measure",
            (OPTIONS, "measure_text"),
            (VALUES, "measure"),
            "Measure",
            _pos(2, 1, 2, 1),
        ),
        _parameter_filter(
            "explorer_duration",
            (OPTIONS, "duration_text"),
            (VALUES, "duration"),
            "Duration",
            _pos(4, 1, 2, 1),
        ),
        _line(
            "explorer_line",
            VALUES,
            ("period_start_date", "Quarter start"),
            ("value", "Value (see unit in legend)"),
            "Quarterly values",
            _pos(0, 2, 6, 8),
            color="series",
        ),
        _table("explorer_table", VALUES, rows, "Rows", _pos(0, 10, 6, 6)),
    ]


def build_dashboard() -> dict[str, Any]:
    """Return the AI/BI dashboard definition: three pages over the dashboard datasets."""
    datasets = [
        _dataset(Q1, "Q1 population stats"),
        _dataset(Q2, "Q2 series best year"),
        _dataset(Q3, "Q3 PRS30006032 Q01 with population"),
        _dataset("dq_checks_latest", "Latest dq_checks"),
        _dataset("dq_ingest_runs", "Ingest runs"),
        _dataset("dq_run_file_actions", "Per-file actions"),
        _dataset("dq_quarantine_by_reason", "Quarantine by reason"),
        _dataset("dq_expectation_pass_rates", "Expectation pass rates"),
        _dataset("dq_freshness", "Freshness"),
        _dataset(OPTIONS, "Explorer filter options"),
        _dataset(VALUES, "Explorer series values"),
    ]
    pages = [
        ("answers", "Answers", _answers_page()),
        ("data_quality", "Data quality", _data_quality_page()),
        ("explorer", "Explorer", _explorer_page()),
    ]
    return {
        "datasets": datasets,
        "pages": [
            {"name": name, "displayName": title, "pageType": "PAGE_TYPE_CANVAS", "layout": layout}
            for name, title, layout in pages
        ],
        "uiSettings": {"theme": {"widgetHeaderAlignment": "ALIGNMENT_UNSPECIFIED"}},
    }


# ---- the Genie space --------------------------------------------------------------------------


def _stable_id(key: str) -> str:
    # The space format wants 32-hex ids; deriving them from a name keeps rebuilds byte-stable.
    return uuid.uuid5(uuid.NAMESPACE_URL, f"rearc-data-quest/genie/{key}").hex


def _bundle_catalog() -> str:
    variables = yaml.safe_load((ROOT / "databricks.yml").read_text())["variables"]
    catalog: str = variables["catalog"]["default"]
    return catalog


def build_genie_space() -> dict[str, Any]:
    """Return the Genie space definition: the gold tables, instructions and trusted queries."""
    questions = [
        re.sub(r"^\d+\.\s*", "", line)
        for line in (GENIE_DIR / "sample_questions.md").read_text().splitlines()
        if line.strip()
    ]
    # The format has no default catalog, so table identifiers name the bundle's.
    catalog = _bundle_catalog()
    sample_questions = [
        {"id": _stable_id(f"q{i}"), "question": [q]} for i, q in enumerate(questions, start=1)
    ]
    trusted = [
        {
            "id": _stable_id(f"sql{i}"),
            "question": [questions[i - 1]],
            "sql": (GENIE_DIR / "trusted_sql" / name).read_text().splitlines(keepends=True),
        }
        for name, i in TRUSTED_SQL.items()
    ]
    instructions = (GENIE_DIR / "instructions.md").read_text().splitlines(keepends=True)
    return {
        "version": 2,
        "config": {"sample_questions": sorted(sample_questions, key=lambda q: q["id"])},
        "data_sources": {
            "tables": [{"identifier": f"{catalog}.quest_gold.{t}"} for t in sorted(GOLD_TABLES)]
        },
        "instructions": {
            "text_instructions": [{"id": _stable_id("instructions"), "content": instructions}],
            "example_question_sqls": sorted(trusted, key=lambda q: q["id"]),
        },
    }


def main() -> None:
    """Write both JSON files."""
    for path, content in ((DASHBOARD_JSON, build_dashboard()), (GENIE_JSON, build_genie_space())):
        path.write_text(json.dumps(content, indent=2) + "\n")
        print(f"wrote {path.relative_to(ROOT)}")


if __name__ == "__main__":
    main()
