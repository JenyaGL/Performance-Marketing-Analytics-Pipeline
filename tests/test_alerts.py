import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
import pytest
from alerts import checks  # noqa: E402
from alerts.checks import (  # noqa: E402
    CRITICAL,
    WARNING,
    Finding,
    check_cost,
    check_freshness,
    check_kpis,
    check_rejects,
    check_silver_lag,
    check_volume,
    estimate_cost_usd,
    format_findings,
    run_check,
)
from alerts.notify import (  # noqa: E402
    dag_failure_callback,
    make_slow_task_callback,
    send_alert,
    should_send,
    task_failure_callback,
)


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "dags"))



NOW = datetime(2026, 10, 16, 12, 0, tzinfo=timezone.utc)
TIB = 2**40


# --- checks ---------------------------------------------------------------------------------

def test_freshness_flags_stale_and_empty_tables():
    findings = check_freshness(
        {"raw_traffic": NOW - timedelta(hours=2), "raw_events": NOW - timedelta(hours=30), "raw_conversions": None},
        NOW,
    )
    assert {f.key for f in findings} == {"stale:raw_events", "stale:raw_conversions"}
    assert all(f.severity == CRITICAL for f in findings)
    assert "30h" in next(f.message for f in findings if f.key == "stale:raw_events")


def test_silver_lag_detects_unprocessed_batch():
    t1, t2 = NOW - timedelta(hours=5), NOW - timedelta(hours=1)
    models = {"raw_traffic": "stg_ad_traffic", "raw_events": "stg_funnel_events", "raw_conversions": "stg_conversions"}
    findings = check_silver_lag(
        {"raw_traffic": (t2, t1), "raw_events": (t2, t2), "raw_conversions": (t2, None)}, models
    )
    assert {f.key for f in findings} == {"silver_behind:raw_traffic", "silver_behind:raw_conversions"}
    assert all(f.severity == CRITICAL for f in findings)
    assert next(f for f in findings if f.key == "silver_behind:raw_traffic").message.startswith(
        "silver.stg_ad_traffic is behind bronze.raw_traffic")


def test_rejects_warns_only_when_lines_were_quarantined():
    findings = check_rejects({"raw_traffic_rejects": (2, ["invalid JSON", "wrong type"]), "raw_events_rejects": (0, [])})
    assert len(findings) == 1 and findings[0].severity == WARNING
    assert "2 line(s)" in findings[0].message and "invalid JSON" in findings[0].message


@pytest.mark.parametrize(
    "sizes, expected_key",
    [
        ([1000, 1000, 1050, 990, 1010], None),  # normal
        ([400, 1000, 1050, 990, 1010], "volume_low:raw_traffic"),  # 40% of average
        ([2500, 1000, 1050, 990, 1010], "volume_high:raw_traffic"),  # 247% of average
        ([10, 1000, 1000], None),  # not enough history to judge
        ([], None),
    ],
)
def test_volume_anomaly(sizes, expected_key):
    findings = check_volume({"raw_traffic": sizes})
    assert [f.key for f in findings] == ([expected_key] if expected_key else [])


def day(date, cpl, epc, leads=50):
    return {"report_date": date, "cpl": cpl, "epc": epc, "leads": leads}


def test_kpi_spike_is_flagged_and_noise_is_not():
    history = [day(f"2026-10-0{i}", 10.0, 3.0) for i in range(1, 8)]
    assert check_kpis([day("2026-10-08", 11.0, 3.2)] + history) == []  # +10%: normal
    findings = check_kpis([day("2026-10-08", 18.0, 3.0)] + history)  # CPL +80%
    assert [f.key for f in findings] == ["kpi:cpl:2026-10-08"] and "+80%" in findings[0].message
    assert check_kpis([day("2026-10-08", 18.0, 3.0, leads=5)] + history) == []  # too few leads to trust
    assert check_kpis([day("2026-10-08", 18.0, 3.0)] + history[:2]) == []  # not enough history


@pytest.mark.parametrize(
    "usd, severity",
    [(0.40, None), (0.60, WARNING), (1.20, CRITICAL)],
)
def test_cost_thresholds(usd, severity):
    findings = check_cost(int(usd / checks.USD_PER_TIB * TIB))
    assert [f.severity for f in findings] == ([severity] if severity else [])


def test_cost_estimate():
    assert estimate_cost_usd(TIB) == pytest.approx(6.25)
    assert estimate_cost_usd(None) == 0


def test_format_findings_one_email_critical_first():
    severity, subject, body = format_findings([
        Finding(WARNING, "rejects:x", "some rejects"),
        Finding(CRITICAL, "stale:y", "no data"),
    ])
    assert severity == CRITICAL
    assert subject == "monitor found 1 critical, 1 warning"
    assert body.splitlines()[0] == "[CRITICAL] no data"


# --- throttling ------------------------------------------------------------------------------

def test_should_send_throttles_repeats_within_window():
    last = {"stale:raw_events": (NOW - timedelta(hours=3)).isoformat()}
    assert not should_send("stale:raw_events", NOW, last)
    assert should_send("stale:raw_events", NOW + timedelta(hours=10), last)  # 13h later
    assert should_send("rejects:raw_traffic_rejects", NOW, last)  # never sent


# --- sending ---------------------------------------------------------------------------------

class FakeSMTP:
    sent = []

    def __init__(self, host, port, timeout):
        self.host, self.port = host, port

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def login(self, user, password):
        self.user = user

    def send_message(self, msg):
        FakeSMTP.sent.append(msg)


@pytest.fixture
def smtp_env(monkeypatch):
    monkeypatch.setenv("SMTP_USER", "bot@gmail.com")
    monkeypatch.setenv("SMTP_PASSWORD", "app-password")
    monkeypatch.setenv("ALERT_EMAIL_TO", "me@gmail.com, team@gmail.com")
    FakeSMTP.sent = []


def test_send_alert_formats_email(smtp_env):
    assert send_alert(CRITICAL, "task failed: x", "details", smtp_class=FakeSMTP)
    msg = FakeSMTP.sent[0]
    assert msg["Subject"] == "[CRITICAL] perf-pipeline: task failed: x"
    assert msg["To"] == "me@gmail.com, team@gmail.com"
    assert msg.get_content().strip() == "details"


def test_send_alert_without_config_logs_and_does_not_raise(monkeypatch, caplog):
    for var in ("SMTP_USER", "SMTP_PASSWORD", "ALERT_EMAIL_TO"):
        monkeypatch.delenv(var, raising=False)
    assert send_alert(WARNING, "subject", "body") is False
    assert "not configured" in caplog.text and "subject" in caplog.text


def test_send_alert_smtp_error_does_not_raise(smtp_env):
    class BrokenSMTP(FakeSMTP):
        def login(self, user, password):
            raise OSError("auth failed")

    assert send_alert(CRITICAL, "s", "b", smtp_class=BrokenSMTP) is False


# --- callbacks -------------------------------------------------------------------------------

def fake_ti(task_id, state=None, duration=None):
    return SimpleNamespace(dag_id="ingest_to_bronze", task_id=task_id, try_number=2, state=state,
                           log_url="http://localhost:8080/log", duration=duration,
                           start_date=NOW - timedelta(seconds=duration or 0))


@pytest.fixture
def captured(monkeypatch):
    calls = []
    monkeypatch.setattr("alerts.notify.send_alert", lambda *args, **kw: calls.append(args))
    return calls


def test_task_failure_callback_message(captured):
    task_failure_callback({"task_instance": fake_ti("load_conversions"), "run_id": "manual__1",
                           "logical_date": NOW, "exception": ValueError("reject rate 100%")})
    severity, subject, body = captured[0]
    assert severity == CRITICAL and subject == "task failed: ingest_to_bronze.load_conversions"
    assert "reject rate 100%" in body and "http://localhost:8080/log" in body


def test_dag_failure_callback_reports_stale_downstream(captured):
    tis = [fake_ti("load_traffic", "success"), fake_ti("load_conversions", "failed"),
           fake_ti("dbt_build", "upstream_failed")]
    dag_run = SimpleNamespace(dag_id="ingest_to_bronze", run_id="manual__1", get_task_instances=lambda: tis)
    dag_failure_callback({"dag_run": dag_run, "reason": "task_failure"})
    severity, subject, body = captured[0]
    assert severity == CRITICAL
    assert "Failed:       load_conversions" in body and "Did not run:  dbt_build" in body
    assert "silver and gold do not include" in body


def test_slow_task_callback_warns_only_over_soft_limit(captured):
    callback = make_slow_task_callback({"dbt_build": 360})
    callback({"task_instance": fake_ti("dbt_build", duration=120), "run_id": "r"})
    assert captured == []
    callback({"task_instance": fake_ti("dbt_build", duration=500), "run_id": "r"})
    assert captured[0][0] == WARNING and "took 500s" in captured[0][1]
    callback({"task_instance": fake_ti("unknown_task", duration=9999), "run_id": "r"})
    assert len(captured) == 1


def test_run_check_turns_a_crash_into_a_critical_finding():
    def denied():
        raise PermissionError("403 Access Denied: bigquery.jobs.listAll")

    findings = run_check("cost", denied)
    assert findings[0].severity == CRITICAL and findings[0].key == "check_error:cost"
    assert "listAll" in findings[0].message
    assert run_check("ok", lambda: []) == []
