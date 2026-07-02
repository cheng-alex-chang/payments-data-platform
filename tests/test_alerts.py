from __future__ import annotations

import importlib.util
import logging
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

import pytest

_PATH = Path(__file__).resolve().parents[1] / "airflow" / "dags" / "alerts.py"
_spec = importlib.util.spec_from_file_location("repo_alerts", _PATH)
assert _spec is not None and _spec.loader is not None
alerts = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(alerts)


def _context() -> dict:
    return {
        "dag": SimpleNamespace(dag_id="snowflake_fx_etl"),
        "task_instance": SimpleNamespace(task_id="dbt_test", log_url="http://airflow/log"),
        "logical_date": "2026-07-02T00:00:00",
        "exception": RuntimeError("gate failed"),
    }


def test_posts_slack_compatible_payload_when_url_set(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("ALERT_WEBHOOK_URL", "https://hooks.example/T/B/x")
    with mock.patch.object(alerts.requests, "post") as post:
        alerts.notify_failure(_context())

    post.assert_called_once()
    (url,), kwargs = post.call_args
    assert url == "https://hooks.example/T/B/x"
    assert kwargs["timeout"] == alerts.TIMEOUT_SECONDS
    payload = kwargs["json"]
    assert payload["text"].endswith("snowflake_fx_etl.dbt_test")  # Slack renders this
    assert payload["error"] == "gate failed"
    assert payload["log_url"] == "http://airflow/log"


def test_noops_with_warning_when_url_unset(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    monkeypatch.delenv("ALERT_WEBHOOK_URL", raising=False)
    with mock.patch.object(alerts.requests, "post") as post, caplog.at_level(logging.WARNING):
        alerts.notify_failure(_context())

    post.assert_not_called()
    assert any("ALERT_WEBHOOK_URL not set" in r.message for r in caplog.records)


def test_never_raises_even_when_post_fails(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("ALERT_WEBHOOK_URL", "https://hooks.example/T/B/x")
    with mock.patch.object(alerts.requests, "post", side_effect=ConnectionError("down")):
        alerts.notify_failure(_context())  # must swallow -- a broken alert must not mask the failure


def test_survives_a_sparse_context(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("ALERT_WEBHOOK_URL", raising=False)
    alerts.notify_failure({})  # missing dag/ti/exception must not crash the callback