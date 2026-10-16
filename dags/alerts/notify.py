"""Email alerts (Gmail SMTP) and Airflow callbacks.

Configured from the environment (.env): SMTP_USER, SMTP_PASSWORD (a Gmail app password)
and ALERT_EMAIL_TO (comma-separated). When they are missing, alerts are logged instead,
and sending never raises, so a broken alert channel can't fail a pipeline task.
"""
import json
import logging
import os
import smtplib
from datetime import datetime, timedelta, timezone
from email.message import EmailMessage

log = logging.getLogger(__name__)

SMTP_HOST = "smtp.gmail.com"
SMTP_PORT = 465
SUBJECT_PREFIX = "perf-pipeline"
THROTTLE_VARIABLE = "alert_last_sent"
THROTTLE_HOURS = 12


def build_message(severity: str, subject: str, body: str, sender: str, recipients: list[str]) -> EmailMessage:
    msg = EmailMessage()
    msg["Subject"] = f"[{severity}] {SUBJECT_PREFIX}: {subject}"
    msg["From"] = sender
    msg["To"] = ", ".join(recipients)
    msg.set_content(body)
    return msg


def send_alert(severity: str, subject: str, body: str, smtp_class=smtplib.SMTP_SSL) -> bool:
    """Send one alert email. Returns True if it was sent, False if it was only logged."""
    sender = os.environ.get("SMTP_USER", "")
    password = os.environ.get("SMTP_PASSWORD", "")
    recipients = [r.strip() for r in os.environ.get("ALERT_EMAIL_TO", "").split(",") if r.strip()]
    if not (sender and password and recipients):
        log.warning("Alert channel not configured (SMTP_USER/SMTP_PASSWORD/ALERT_EMAIL_TO); "
                    "logging instead:\n[%s] %s\n%s", severity, subject, body)
        return False
    try:
        with smtp_class(SMTP_HOST, SMTP_PORT, timeout=30) as smtp:
            smtp.login(sender, password)
            smtp.send_message(build_message(severity, subject, body, sender, recipients))
    except Exception:  # never let alerting break the pipeline
        log.exception("Failed to send alert email:\n[%s] %s\n%s", severity, subject, body)
        return False
    log.info("Alert email sent: [%s] %s", severity, subject)
    return True


# --- Throttling: don't repeat the same finding on every monitor run ---------------------------

def should_send(key: str, now: datetime, last_sent: dict, window_hours: float = THROTTLE_HOURS) -> bool:
    previous = last_sent.get(key)
    return previous is None or now - datetime.fromisoformat(previous) >= timedelta(hours=window_hours)


def load_last_sent() -> dict:
    from airflow.models import Variable

    return json.loads(Variable.get(THROTTLE_VARIABLE, default_var="{}"))


def save_last_sent(last_sent: dict) -> None:
    from airflow.models import Variable

    Variable.set(THROTTLE_VARIABLE, json.dumps(last_sent))


# --- Airflow callbacks ----------------------------------------------------------------------

def task_failure_callback(context):
    """CRITICAL: a task failed after using up its retries (includes timeouts from execution_timeout)."""
    ti = context["task_instance"]
    body = "\n".join([
        f"DAG:          {ti.dag_id}",
        f"Task:         {ti.task_id}",
        f"Run:          {context['run_id']}",
        f"Logical date: {context['logical_date']}",
        f"Try:          {ti.try_number}",
        f"Error:        {context.get('exception')!r}",
        f"Log:          {ti.log_url}",
    ])
    send_alert("CRITICAL", f"task failed: {ti.dag_id}.{ti.task_id}", body)


def dag_failure_callback(context):
    """CRITICAL: the whole run failed. Lists what failed and what never ran (stale downstream data)."""
    dag_run = context["dag_run"]
    tis = dag_run.get_task_instances()
    failed = sorted(ti.task_id for ti in tis if ti.state == "failed")
    not_run = sorted(ti.task_id for ti in tis if ti.state in ("upstream_failed", "skipped", None))
    lines = [
        f"DAG:          {dag_run.dag_id}",
        f"Run:          {dag_run.run_id}",
        f"Reason:       {context.get('reason') or 'one or more tasks failed'}",
        f"Failed:       {', '.join(failed) or '-'}",
        f"Did not run:  {', '.join(not_run) or '-'}",
    ]
    if "dbt_build" in not_run or "dbt_build" in failed:
        lines.append("\nImpact: dbt_build did not complete, so silver and gold do not include this run's data.")
    send_alert("CRITICAL", f"DAG run failed: {dag_run.dag_id}", "\n".join(lines))


def make_slow_task_callback(soft_limits_seconds: dict):
    """WARNING when a task succeeded but took longer than its soft limit (an early sign of trouble)."""

    def slow_task_callback(context):
        ti = context["task_instance"]
        limit = soft_limits_seconds.get(ti.task_id)
        if limit is None or ti.start_date is None:
            return
        duration = ti.duration or (datetime.now(timezone.utc) - ti.start_date).total_seconds()
        if duration > limit:
            send_alert(
                "WARNING",
                f"slow task: {ti.dag_id}.{ti.task_id} took {duration:.0f}s",
                f"Task {ti.task_id} succeeded but took {duration:.0f}s (soft limit {limit}s).\n"
                f"Run: {context['run_id']}\nLog: {ti.log_url}",
            )

    return slow_task_callback
