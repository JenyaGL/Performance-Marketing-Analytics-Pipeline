"""Generate mock performance-marketing files, append them to BigQuery bronze, then run dbt.

ensure_bronze >> generate_files >> [load_traffic, load_events, load_conversions] >> dbt_build

Dual-ingestion guard: every file gets a content-derived _batch_id. A load task first checks
whether that batch already exists in its bronze table and skips if so, so retries and
re-triggers on the same file never append it twice.
"""
import os
from datetime import datetime, timedelta, timezone

from airflow.decorators import dag, task
from airflow.exceptions import AirflowSkipException
from airflow.operators.bash import BashOperator
from airflow.operators.python import get_current_context

from bronze.loader import BRONZE_DATASET, ENTITIES, compute_batch_id, enrich_records, table_schema

GCP_PROJECT = os.environ.get("GCP_PROJECT")
BQ_LOCATION = os.environ.get("BQ_LOCATION", "US")
DATA_DIR = "/opt/airflow/data"
LANDING_ROOT = f"{DATA_DIR}/landing"
DBT_DIR = "/opt/airflow/dbt"


def _bq_client():
    from google.cloud import bigquery

    return bigquery.Client(project=GCP_PROJECT, location=BQ_LOCATION)


def _bq_schema(entity_name):
    from google.cloud import bigquery

    return [bigquery.SchemaField(name, typ, mode=mode) for name, typ, mode in table_schema(entity_name)]


def _landing_dir() -> str:
    return f"{LANDING_ROOT}/{get_current_context()['ts_nodash']}"


@dag(
    dag_id="ingest_to_bronze",
    schedule="@daily",
    start_date=datetime(2026, 10, 1),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=2)},
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
            table = bigquery.Table(
                f"{GCP_PROJECT}.{BRONZE_DATASET}.{entity.table}", schema=_bq_schema(entity.name)
            )
            table.time_partitioning = bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.DAY, field="_ingested_at"
            )
            table.clustering_fields = ["_batch_id"]
            client.create_table(table, exists_ok=True)

    generate_files = BashOperator(
        task_id="generate_files",
        bash_command=f"DATA_OUTPUT_DIR={LANDING_ROOT}/{{{{ ts_nodash }}}}/ python {DATA_DIR}/generate_data.py",
    )

    @task
    def load_entity(entity_name: str, table_name: str):
        from google.cloud import bigquery

        path = os.path.join(_landing_dir(), f"{entity_name}.ndjson")
        table_id = f"{GCP_PROJECT}.{BRONZE_DATASET}.{table_name}"
        batch_id = compute_batch_id(entity_name, path)
        client = _bq_client()

        already_loaded = client.query(
            f"SELECT COUNT(1) AS n FROM `{table_id}` WHERE _batch_id = @batch_id",
            job_config=bigquery.QueryJobConfig(
                query_parameters=[bigquery.ScalarQueryParameter("batch_id", "STRING", batch_id)]
            ),
        ).result()
        if next(iter(already_loaded)).n > 0:
            raise AirflowSkipException(f"{batch_id} already in {table_id}; skipping to avoid dual ingestion")

        ingested_at = datetime.now(timezone.utc).isoformat()
        with open(path) as f:
            records = enrich_records(f, batch_id, ingested_at, path)

        job = client.load_table_from_json(
            records,
            table_id,
            job_config=bigquery.LoadJobConfig(
                schema=_bq_schema(entity_name),
                source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
                write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
            ),
        )
        job.result()
        if job.output_rows != len(records):
            raise ValueError(f"{table_id}: loaded {job.output_rows} rows, expected {len(records)}")
        print(f"Loaded {job.output_rows} rows into {table_id} as {batch_id}")

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
        load_entity.override(task_id=f"load_{e.table.removeprefix('raw_')}")(e.name, e.table)
        for e in ENTITIES
    ]
    ensure_bronze() >> generate_files >> loads >> dbt_build


ingest_to_bronze()
