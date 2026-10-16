"""Watch the pipeline from the outside: silent failures, data anomalies and BigQuery cost.

Runs independently of ingest_to_bronze, so it notices when ingestion did not run, stopped
part-way, or finished with suspicious data. All findings of one run go out in one email,
and a finding already reported in the last 12h is not repeated.
"""
import os
from datetime import datetime, timedelta, timezone

from airflow.decorators import dag, task

from alerts.checks import (
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
from alerts.notify import load_last_sent, save_last_sent, send_alert, should_send, task_failure_callback
from bronze.loader import BRONZE_DATASET, ENTITIES

GCP_PROJECT = os.environ.get("GCP_PROJECT")
BQ_LOCATION = os.environ.get("BQ_LOCATION", "US")
MAX_BYTES_BILLED = 1_000_000_000  # refuse any monitoring query that would scan more than 1 GB

SILVER_MODELS = {"raw_traffic": "stg_ad_traffic", "raw_events": "stg_funnel_events", "raw_conversions": "stg_conversions"}


@dag(
    dag_id="pipeline_monitor",
    schedule="0 */6 * * *",
    start_date=datetime(2026, 10, 1),
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(minutes=30),
    default_args={
        "retries": 1,
        "retry_delay": timedelta(minutes=2),
        "execution_timeout": timedelta(minutes=10),
        "on_failure_callback": task_failure_callback,
    },
    tags=["monitoring"],
)
def pipeline_monitor():
    @task
    def run_checks():
        from google.cloud import bigquery

        client = bigquery.Client(project=GCP_PROJECT, location=BQ_LOCATION)
        config = bigquery.QueryJobConfig(maximum_bytes_billed=MAX_BYTES_BILLED)

        def query(sql):
            return list(client.query(sql, job_config=config).result())

        bronze = f"`{GCP_PROJECT}.{BRONZE_DATASET}"
        silver = f"`{GCP_PROJECT}.silver"
        now = datetime.now(timezone.utc)

        def watermarks():
            # Bronze vs silver high-water marks: freshness, and "dbt did not process the latest batch".
            result = {}
            for e in ENTITIES:
                row = query(f"select (select max(_ingested_at) from {bronze}.{e.table}`) b, "
                            f"(select max(_ingested_at) from {silver}.{SILVER_MODELS[e.table]}`) s")[0]
                result[e.table] = (row.b, row.s)
            return result

        def rejects():
            result = {}
            for e in ENTITIES:
                row = query(f"select count(*) n, array_agg(error order by line_number limit 3) samples "
                            f"from {bronze}.{e.rejects_table}` "
                            f"where _ingested_at > timestamp_sub(current_timestamp(), interval 24 hour)")[0]
                result[e.rejects_table] = (row.n, list(row.samples or []))
            return check_rejects(result)

        def volume():
            sizes = {}
            for e in ENTITIES:
                rows = query(f"select _batch_id, count(*) n, min(_ingested_at) t from {bronze}.{e.table}` "
                             f"group by _batch_id order by t desc limit 8")
                sizes[e.table] = [r.n for r in rows]
            return check_volume(sizes)

        def kpis():
            # Exclude today: its click-date totals are still filling in.
            days = [dict(r.items()) for r in query(
                f"select report_date, cpl, epc, leads from `{GCP_PROJECT}.gold.mart_kpi_daily_totals` "
                f"where report_date < current_date() order by report_date desc limit 8")]
            return check_kpis(days)

        bytes_billed = []

        def cost():
            bytes_billed.append(query(
                f"select coalesce(sum(total_bytes_billed), 0) b "
                f"from `{GCP_PROJECT}`.`region-{BQ_LOCATION.lower()}`.INFORMATION_SCHEMA.JOBS_BY_PROJECT "
                f"where creation_time >= timestamp_trunc(current_timestamp(), month)")[0].b)
            print(f"Month-to-date BigQuery bytes billed: {bytes_billed[0]:,} "
                  f"(~${estimate_cost_usd(bytes_billed[0]):.4f})")
            return check_cost(bytes_billed[0])

        marks = {}
        findings = run_check("watermarks", lambda: marks.update(watermarks()) or [])
        findings += run_check("freshness", lambda: check_freshness({t: b for t, (b, _) in marks.items()}, now))
        findings += run_check("silver_lag", lambda: check_silver_lag(marks, SILVER_MODELS))
        findings += run_check("rejects", rejects)
        findings += run_check("volume", volume)
        findings += run_check("kpis", kpis)
        findings += run_check("cost", cost)

        for f in findings:
            print(f"[{f.severity}] {f.key}: {f.message}")
        if not findings:
            print("All checks passed.")
            return

        last_sent = load_last_sent()
        new = [f for f in findings if should_send(f.key, now, last_sent)]
        if not new:
            print(f"{len(findings)} finding(s) already reported in the last 12h; not emailing again.")
            return
        if send_alert(*format_findings(new)):
            last_sent.update({f.key: now.isoformat() for f in new})
            save_last_sent(last_sent)

    run_checks()


pipeline_monitor()
