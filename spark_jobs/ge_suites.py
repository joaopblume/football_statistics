"""Great Expectations validation for the Silver Spark job.

Defines expectation suites per Silver table and validates the in-memory Spark
DataFrames (where the data already lives) using GE's ephemeral context. Returns
results in the pipeline's check shape ({check_name, status, details}) so they
merge into report.json and flow to pipeline_quality_checks via the existing
record_quality_report bridge — GE becomes the validation engine, the hand-rolled
gate becomes a fast pre-check.

Used by silver_job.py. Requires great_expectations (baked into football-spark).
"""

from __future__ import annotations

# Expectation suites per table: (column, expectation_kwargs) declaratives.
# Each entry produces one GE expectation; we map GE success -> pass/fail.
_SUITES = {
    "players": [
        ("expect_column_values_to_not_be_null", {"column": "player"}),
        ("expect_column_values_to_not_be_null", {"column": "team"}),
        ("expect_table_row_count_to_be_between", {"min_value": 1}),
    ],
    "teams": [
        ("expect_column_values_to_not_be_null", {"column": "team_name"}),
        ("expect_table_row_count_to_be_between", {"min_value": 1}),
    ],
    "match_statistics": [
        ("expect_column_values_to_not_be_null", {"column": "game"}),
        ("expect_column_values_to_be_unique", {"column": "game"}),
        ("expect_table_row_count_to_be_between", {"min_value": 1}),
    ],
    "player_match_stats": [
        ("expect_column_values_to_not_be_null", {"column": "game"}),
        ("expect_table_row_count_to_be_between", {"min_value": 1}),
    ],
}


def run_ge_validation(spark, dataframes: dict) -> list[dict]:
    """Validate *dataframes* ({table_name: spark_df}) against the suites.

    Returns a list of check dicts: one per expectation, with
    ``status`` = 'pass' | 'fail' and a short ``details`` string. Any GE/setup
    error degrades to a single 'warn' check rather than breaking the job.
    """
    try:
        import great_expectations as gx
        from great_expectations import expectations as gxe
    except Exception as exc:  # noqa: BLE001
        return [{"check_name": "ge_available", "status": "warn",
                 "details": f"great_expectations import failed: {exc}"}]

    checks: list[dict] = []
    context = gx.get_context(mode="ephemeral")
    source = context.data_sources.add_spark(name="silver_spark")

    # GE expectation classes are PascalCase of the snake_case method names.
    def _expectation(name: str, kwargs: dict):
        # snake_case method name -> PascalCase GE expectation class
        cls_name = "".join(p.capitalize() for p in name.split("_"))
        return getattr(gxe, cls_name)(**kwargs)

    for table, exps in _SUITES.items():
        df = dataframes.get(table)
        if df is None:
            continue
        try:
            asset = source.add_dataframe_asset(name=f"asset_{table}")
            batch_def = asset.add_batch_definition_whole_dataframe(f"bd_{table}")
            suite = context.suites.add(gx.ExpectationSuite(name=f"suite_{table}"))
            for name, kwargs in exps:
                suite.add_expectation(_expectation(name, kwargs))
            vdef = context.validation_definitions.add(
                gx.ValidationDefinition(name=f"vd_{table}", data=batch_def, suite=suite)
            )
            result = vdef.run(batch_parameters={"dataframe": df})
            for r in result.results:
                etype = r["expectation_config"]["type"]
                col = r["expectation_config"]["kwargs"].get("column", "-")
                ok = bool(r["success"])
                checks.append({
                    "check_name": f"ge_{table}.{etype}.{col}",
                    "status": "pass" if ok else "fail",
                    "details": f"GE {etype}({col}) success={ok}",
                })
        except Exception as exc:  # noqa: BLE001
            checks.append({"check_name": f"ge_{table}", "status": "warn",
                           "details": f"GE validation error: {str(exc)[:120]}"})
    return checks
