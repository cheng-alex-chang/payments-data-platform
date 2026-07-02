"""Failure alerting for the Airflow DAGs: POST to a webhook, no-op when unconfigured.

Wired as ``on_failure_callback`` in both DAGs' default_args. If ``ALERT_WEBHOOK_URL`` is set
(e.g. a free Slack incoming webhook), a task failure POSTs a small JSON payload -- ``text``
makes it render in Slack out of the box, the structured fields serve anything else. When the
env var is unset (local dev, CI) the callback logs a warning and does nothing, so the DAGs
never depend on an external service to run.

A callback must never raise -- a broken alert should not mask the original task failure --
so every error here is swallowed and logged.
"""
from __future__ import annotations

import logging
import os
from typing import Any

import requests

LOGGER = logging.getLogger(__name__)
TIMEOUT_SECONDS = 5


def notify_failure(context: dict[str, Any]) -> None:
    """Airflow on_failure_callback: report the failed task to the alert webhook."""
    dag_id = getattr(context.get("dag"), "dag_id", "unknown")
    task_id = getattr(context.get("task_instance"), "task_id", "unknown")
    payload = {
        "text": f":red_circle: Airflow task failed: {dag_id}.{task_id}",
        "dag_id": dag_id,
        "task_id": task_id,
        "logical_date": str(context.get("logical_date") or context.get("execution_date") or ""),
        "error": str(context.get("exception") or ""),
        "log_url": getattr(context.get("task_instance"), "log_url", None),
    }

    url = os.getenv("ALERT_WEBHOOK_URL")
    if not url:
        LOGGER.warning("ALERT_WEBHOOK_URL not set; failure alert not sent: %s", payload["text"])
        return

    try:
        response = requests.post(url, json=payload, timeout=TIMEOUT_SECONDS)
        response.raise_for_status()
        LOGGER.info("Failure alert sent for %s.%s", dag_id, task_id)
    except Exception:  # noqa: BLE001 - a broken alert must never mask the task failure
        LOGGER.exception("Failed to send failure alert for %s.%s", dag_id, task_id)
