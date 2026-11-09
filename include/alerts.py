"""Failure alerts for the DAGs: always logged, and posted to a webhook when one is set."""

import json
import logging
import os
import urllib.request

logger = logging.getLogger(__name__)


def notify_failure(context: dict) -> None:
    """on_failure_callback: say which task failed, when, and where to look."""
    ti = context["task_instance"]
    message = (
        f"Grid Pulse: task {ti.dag_id}.{ti.task_id} failed "
        f"(run {context.get('run_id')}, try {ti.try_number}). Log: {getattr(ti, 'log_url', 'n/a')}"
    )
    logger.error(message)

    webhook = os.environ.get("ALERT_WEBHOOK_URL")
    if not webhook:
        return
    request = urllib.request.Request(
        webhook, data=json.dumps({"text": message}).encode(), headers={"Content-Type": "application/json"}
    )
    try:
        urllib.request.urlopen(request, timeout=10)
    except OSError as error:  # an alert must never hide the original failure
        logger.warning("Could not post the alert: %s", error)
