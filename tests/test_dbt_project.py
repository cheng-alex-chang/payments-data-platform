"""Static guards for the dbt project (snowflake_etl/dbt).

`dbt parse` in CI proves the project compiles; these tests pin the *invariants* that a future
edit could silently drop -- the forward-fill, the LEFT-join-don't-drop contract, the
validation gates, and the no-secrets-in-profile rule.
"""
from __future__ import annotations

from pathlib import Path

import yaml

DBT_DIR = Path(__file__).resolve().parents[1] / "snowflake_etl" / "dbt"
MODELS = DBT_DIR / "models"


def _read(relpath: str) -> str:
    return (DBT_DIR / relpath).read_text(encoding="utf-8")


def test_expected_models_and_tests_exist() -> None:
    for relpath in (
        "models/staging/stg_payments.sql",
        "models/staging/stg_fx_rates.sql",
        "models/marts/dim_fx_rates.sql",
        "models/marts/fct_payments_usd.sql",
        "models/marts/agg_payments_by_currency.sql",
        "models/schema.yml",
        "models/sources.yml",
        "tests/fact_reconciles_to_payments.sql",
        "tests/no_null_or_zero_fx_rate.sql",
        "tests/usd_payments_unchanged.sql",
    ):
        assert (DBT_DIR / relpath).is_file(), relpath


def test_staging_dedups_and_marts_use_refs() -> None:
    stg = _read("models/staging/stg_payments.sql")
    assert "QUALIFY ROW_NUMBER()" in stg                      # snapshot dedup survives
    assert "{{ source('raw', 'raw_payments') }}" in stg       # reads the declared source

    fct = _read("models/marts/fct_payments_usd.sql")
    # LEFT JOIN keeps unmatched payments so the not_null test catches them (vs. silent drop).
    assert "LEFT JOIN {{ ref('dim_fx_rates') }}" in fct
    assert "ROUND(p.amount * d.rate_to_usd, 2) AS usd_amount" in fct


def test_dim_fx_rates_forward_fills_gaps() -> None:
    dim = _read("models/marts/dim_fx_rates.sql")
    assert "LAST_VALUE(rate_to_usd) IGNORE NULLS" in dim   # carry last known rate forward
    assert "FIRST_VALUE(rate_to_usd) IGNORE NULLS" in dim  # cover the leading edge
    assert "is_filled" in dim                              # gaps flagged, not hidden


def test_schema_declares_the_validation_gates() -> None:
    schema = yaml.safe_load(_read("models/schema.yml"))
    tests_by_model = {
        model["name"]: {
            column["name"]: column.get("tests", [])
            for column in model.get("columns", [])
        }
        for model in schema["models"]
    }

    fct = tests_by_model["fct_payments_usd"]
    assert "not_null" in fct["usd_amount"]                   # no unmatched USD amount
    assert {"unique", "not_null"} <= set(fct["payment_id"])  # grain: one row per payment


def test_profiles_use_env_vars_only() -> None:
    profile = _read("profiles.yml")
    for field in ("account", "user", "password"):
        # every credential field is env_var-driven -- no literal secrets in git
        assert f"env_var('SNOWFLAKE_{field.upper()}'" in profile

    parsed = yaml.safe_load(profile)
    output = parsed["payments_fx"]["outputs"]["trial"]
    assert output["password"].startswith("{{ env_var(")


def test_materializations_match_the_old_runner() -> None:
    project = yaml.safe_load(_read("dbt_project.yml"))
    models = project["models"]["payments_fx"]
    assert models["staging"]["+materialized"] == "view"
    assert models["marts"]["+materialized"] == "table"
