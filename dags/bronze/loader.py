"""Pure helpers for the bronze ingestion DAG.

No Airflow or BigQuery imports here, so the logic can be unit-tested in isolation.
"""
import hashlib
import json
from dataclasses import dataclass

BRONZE_DATASET = "bronze"

# Metadata columns injected into every bronze row.
METADATA_FIELDS = [
    ("_ingested_at", "TIMESTAMP", "REQUIRED"),
    ("_batch_id", "STRING", "REQUIRED"),
    ("_source_file", "STRING", "REQUIRED"),
]

# Raw columns mirror generate_data.py. `timestamp` stays STRING in bronze and is cast in silver,
# so a malformed value can never fail the load.
RAW_FIELDS = {
    "ad_traffic": [
        ("traffic_id", "STRING"),
        ("partner_id", "STRING"),
        ("campaign_id", "STRING"),
        ("vertical", "STRING"),
        ("traffic_source", "STRING"),
        ("clicks", "INT64"),
        ("cost_usd", "FLOAT64"),
        ("timestamp", "STRING"),
    ],
    "funnel_events": [
        ("event_id", "STRING"),
        ("session_id", "STRING"),
        ("traffic_id", "STRING"),
        ("user_pseudo_id", "STRING"),
        ("step_name", "STRING"),
        ("step_number", "INT64"),
        ("variant_id", "STRING"),
        ("timestamp", "STRING"),
    ],
    "conversions": [
        ("conversion_id", "STRING"),
        ("session_id", "STRING"),
        ("lead_status", "STRING"),
        ("buyer_id", "STRING"),
        ("revenue_usd", "FLOAT64"),
        ("partner_payout_usd", "FLOAT64"),
        ("timestamp", "STRING"),
    ],
}


@dataclass(frozen=True)
class Entity:
    name: str  # generator file stem
    table: str  # bronze table name

    @property
    def filename(self) -> str:
        return f"{self.name}.ndjson"


ENTITIES = [
    Entity("ad_traffic", "raw_traffic"),
    Entity("funnel_events", "raw_events"),
    Entity("conversions", "raw_conversions"),
]


def table_schema(entity_name: str) -> list[tuple[str, str, str]]:
    """Full bronze schema as (name, type, mode) tuples: raw columns + metadata."""
    raw = [(name, typ, "NULLABLE") for name, typ in RAW_FIELDS[entity_name]]
    return raw + METADATA_FIELDS


def compute_batch_id(entity_name: str, path: str) -> str:
    """Deterministic batch id from the file's content.

    The same file always maps to the same id, so a retried or re-triggered load is detectable.
    """
    digest = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            digest.update(chunk)
    return f"{entity_name}_{digest.hexdigest()[:16]}"


def enrich_records(lines, batch_id: str, ingested_at_iso: str, source_file: str) -> list[dict]:
    """Parse NDJSON lines and stamp each record with the ingestion metadata.

    Blank lines are skipped. Duplicates are kept on purpose: bronze is an append-only
    copy of the source, and deduplication happens in silver.
    """
    records = []
    for line in lines:
        line = line.strip()
        if not line:
            continue
        record = json.loads(line)
        record["_ingested_at"] = ingested_at_iso
        record["_batch_id"] = batch_id
        record["_source_file"] = source_file
        records.append(record)
    return records
