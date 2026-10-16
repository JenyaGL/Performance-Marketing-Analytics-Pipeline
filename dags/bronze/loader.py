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
        ("status_updated_at", "STRING"),  # only on restated records (lead returns)
    ],
}


PRIMARY_KEYS = {
    "ad_traffic": "traffic_id",
    "funnel_events": "event_id",
    "conversions": "conversion_id",
}

# Dead-letter table schema: one row per line that could not be loaded, with the reason.
REJECTS_SCHEMA = [
    ("raw_line", "STRING", "REQUIRED"),
    ("error", "STRING", "REQUIRED"),
    ("line_number", "INT64", "REQUIRED"),
] + METADATA_FIELDS

# A batch with more rejects than this is treated as a broken pull and is not loaded at all.
MAX_REJECT_RATE = 0.01


@dataclass(frozen=True)
class Entity:
    name: str  # generator file stem
    table: str  # bronze table name

    @property
    def filename(self) -> str:
        return f"{self.name}.ndjson"

    @property
    def rejects_table(self) -> str:
        return f"{self.table}_rejects"


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


def _type_ok(value, bq_type: str) -> bool:
    if value is None:
        return True  # raw columns are nullable; dbt not_null tests catch missing values downstream
    if bq_type == "INT64":
        return isinstance(value, int) and not isinstance(value, bool)
    if bq_type == "FLOAT64":
        return isinstance(value, (int, float)) and not isinstance(value, bool)
    return isinstance(value, str)


def validate_record(line: str, entity_name: str):
    """Return (record, None) for a loadable line, or (None, reason) for one to quarantine."""
    try:
        record = json.loads(line)
    except json.JSONDecodeError as e:
        return None, f"invalid JSON: {e.msg}"
    if not isinstance(record, dict):
        return None, "not a JSON object"

    key = PRIMARY_KEYS[entity_name]
    if not record.get(key):
        return None, f"missing primary key {key}"

    fields = dict(RAW_FIELDS[entity_name])
    unexpected = sorted(set(record) - set(fields))
    if unexpected:
        return None, f"unexpected field(s): {', '.join(unexpected)}"

    bad_types = [f"{name}={record[name]!r} (expected {typ})" for name, typ in fields.items()
                 if name in record and not _type_ok(record[name], typ)]
    if bad_types:
        return None, f"wrong type: {'; '.join(bad_types)}"
    return record, None


def split_records(lines, entity_name: str, batch_id: str, ingested_at_iso: str, source_file: str):
    """Validate NDJSON lines and stamp them with ingestion metadata.

    Returns (good, rejects). Good records are ready for the bronze table; rejects keep the raw
    line and the reason for the dead-letter table. Blank lines are skipped. Duplicates are kept
    on purpose: bronze is an append-only copy of the source, and deduplication happens in silver.
    """
    metadata = {"_ingested_at": ingested_at_iso, "_batch_id": batch_id, "_source_file": source_file}
    good, rejects = [], []
    for line_number, line in enumerate(lines, start=1):
        line = line.strip()
        if not line:
            continue
        record, error = validate_record(line, entity_name)
        if error:
            rejects.append({"raw_line": line, "error": error, "line_number": line_number, **metadata})
        else:
            good.append({**record, **metadata})
    return good, rejects


def check_batch_acceptable(n_good: int, n_rejects: int, max_reject_rate: float = MAX_REJECT_RATE) -> None:
    """Raise if the batch looks like a broken pull: empty, or too many bad lines to trust."""
    total = n_good + n_rejects
    if n_good == 0:
        raise ValueError(f"no valid records (total lines: {total}); refusing to load an empty or fully broken batch")
    rate = n_rejects / total
    if rate > max_reject_rate:
        raise ValueError(f"reject rate {rate:.1%} ({n_rejects}/{total}) exceeds {max_reject_rate:.1%}; not loading")
