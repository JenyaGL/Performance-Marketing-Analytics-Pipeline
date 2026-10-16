"""Generate mock performance-marketing files, append them to BigQuery bronze, then run dbt.

ensure_bronze >> generate_files >> [load_traffic, load_events, load_conversions] >> dbt_build

Dual-ingestion guard: every file gets a content-derived _batch_id. A load task first checks
whether that batch already exists in its bronze table and skips if so, so retries and
re-triggers on the same file never append it twice.

Dead-letter quarantine: lines that fail validation go to bronze.raw_<entity>_rejects with the
reason, and the rest of the batch still loads. An empty batch, or one with more than
MAX_REJECT_RATE bad lines, is treated as a broken pull and fails before anything is loaded.
"""
import os
from datetime import datetime, timedelta, timezone

from airflow.decorators import dag, task
from airflow.exceptions import AirflowSkipException
from airflow.operators.bash import BashOperator
from airflow.operators.python import get_current_context

from bronze.loader import (
    BRONZE_DATASET,
    ENTITIES,
    REJECTS_SCHEMA,
    check_batch_acceptable,
    compute_batch_id,
    split_records,
    table_schema,
)

GCP_PROJECT = os.environ.get("GCP_PROJECT")
BQ_LOCATION = os.environ.get("BQ_LOCATION", "US")
DATA_DIR = "/opt/airflow/data"
LANDING_ROOT = f"{DATA_DIR}/landing"
DBT_DIR = "/opt/airflow/dbt"


def _bq_client():
    from google.cloud import bigquery

    return bigquery.Client(project=GCP_PROJECT, location=BQ_LOCATION)


def _to_bq_schema(fields):
    from google.cloud import bigquery

    return [bigquery.SchemaField(name, typ, mode=mode) for name, typ, mode in fields]


def _bq_schema(entity_name):
    return _to_bq_schema(table_schema(entity_name))


def _batch_exists(client, table_id: str, batch_id: str) -> bool:
    from google.cloud import bigquery

    rows = client.query(
        f"SELECT COUNT(1) AS n FROM `{table_id}` WHERE _batch_id = @batch_id",
        job_config=bigquery.QueryJobConfig(
            query_parameters=[bigquery.ScalarQueryParameter("batch_id", "STRING", batch_id)]
        ),
    ).result()
    return next(iter(rows)).n > 0


def _append(client, rows: list[dict], table_id: str, schema) -> int:
    from google.cloud import bigquery

    job = client.load_table_from_json(
        rows,
        table_id,
        job_config=bigquery.LoadJobConfig(
            schema=schema,
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        ),
    )
    job.result()
    if job.output_rows != len(rows):
        raise ValueError(f"{table_id}: loaded {job.output_rows} rows, expected {len(rows)}")
    return job.output_rows


def _landing_dir() -> str:
    return f"{LANDING_ROOT}/{get_current_context()['ts_nodash']}"


@dag(
    dag_id="ingest_to_bronze",
    schedule="@daily",
    start_date=datetime(2026, 10, 1),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=2)},
    # Trigger with config {"faults": "malformed_lines"} (comma-separated) to rehearse a broken pull.
    params={"faults": ""},
    tags=["bronze", "performance"],
)
def ingest_to_bronze():
    @task
    def ensure_bronze():
        from google.cloud import bigquery

        client = _bq_client()
        dataset = bigquery.Dataset(f"{GCP_PROJECT}.{BRONZE_DATASET}")
        dataset.location = BQ_LOCATION
        client.create_dataset(dataset, exists_ok=True)

        for entity in ENTITIES:
            for table_name, schema in (
                (entity.table, _bq_schema(entity.name)),
                (entity.rejects_table, _to_bq_schema(REJECTS_SCHEMA)),
            ):
                table = bigquery.Table(f"{GCP_PROJECT}.{BRONZE_DATASET}.{table_name}", schema=schema)
                table.time_partitioning = bigquery.TimePartitioning(
                    type_=bigquery.TimePartitioningType.DAY, field="_ingested_at"
                )
                table.clustering_fields = ["_batch_id"]
                client.create_table(table, exists_ok=True)

    generate_files = BashOperator(
        task_id="generate_files",
        bash_command=(
            f"DATA_OUTPUT_DIR={LANDING_ROOT}/{{{{ ts_nodash }}}}/ FAULTS='{{{{ params.faults }}}}' "
            f"python {DATA_DIR}/generate_data.py"
        ),
    )

    @task
    def load_entity(entity_name: str, table_name: str, rejects_table_name: str):
        path = os.path.join(_landing_dir(), f"{entity_name}.ndjson")
        table_id = f"{GCP_PROJECT}.{BRONZE_DATASET}.{table_name}"
        rejects_table_id = f"{GCP_PROJECT}.{BRONZE_DATASET}.{rejects_table_name}"
        batch_id = compute_batch_id(entity_name, path)
        client = _bq_client()

        # The main table is the "batch complete" marker, so it is checked first and written last.
        if _batch_exists(client, table_id, batch_id):
            raise AirflowSkipException(f"{batch_id} already in {table_id}; skipping to avoid dual ingestion")

        ingested_at = datetime.now(timezone.utc).isoformat()
        with open(path) as f:
            good, rejects = split_records(f, entity_name, batch_id, ingested_at, path)
        print(f"{batch_id}: {len(good)} valid lines, {len(rejects)} rejected")
        for r in rejects[:10]:
            print(f"  rejected line {r['line_number']}: {r['error']}")

        # Broken pull (empty, or too many bad lines): fail before writing anything, so a retry starts clean.
        check_batch_acceptable(len(good), len(rejects))

        if rejects and not _batch_exists(client, rejects_table_id, batch_id):
            _append(client, rejects, rejects_table_id, _to_bq_schema(REJECTS_SCHEMA))
            print(f"Quarantined {len(rejects)} lines in {rejects_table_id}")
        loaded = _append(client, good, table_id, _bq_schema(entity_name))
        print(f"Loaded {loaded} rows into {table_id} as {batch_id}")

    # Unit tests run after the models: the incremental unit test mocks `this`, whose column
    # types dbt reads from the existing table, so it would fail on the very first build.
    dbt_args = f"--project-dir {DBT_DIR} --profiles-dir {DBT_DIR}"
    dbt_build = BashOperator(
        task_id="dbt_build",
        bash_command=(
            f"/opt/dbt-venv/bin/dbt build --exclude-resource-type unit_test {dbt_args} && "
            f"/opt/dbt-venv/bin/dbt test --select test_type:unit {dbt_args}"
        ),
        trigger_rule="none_failed",
    )

    loads = [
        load_entity.override(task_id=f"load_{e.table.removeprefix('raw_')}")(e.name, e.table, e.rejects_table)
        for e in ENTITIES
    ]
    ensure_bronze() >> generate_files >> loads >> dbt_build


ingest_to_bronze()
