import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "dags"))

from bronze.loader import ENTITIES, METADATA_FIELDS, RAW_FIELDS, compute_batch_id, enrich_records, table_schema  # noqa: E402


@pytest.fixture(scope="module")
def generated_dir(tmp_path_factory):
    out = tmp_path_factory.mktemp("landing")
    subprocess.run(
        [sys.executable, str(ROOT / "data" / "generate_data.py")],
        env={**os.environ, "DATA_OUTPUT_DIR": str(out)},
        check=True,
        capture_output=True,
    )
    return out


def test_generator_writes_all_entity_files_to_output_dir(generated_dir):
    for entity in ENTITIES:
        assert (generated_dir / entity.filename).stat().st_size > 0


def test_schemas_cover_every_generated_field(generated_dir):
    for entity in ENTITIES:
        fields = set()
        with open(generated_dir / entity.filename) as f:
            for line in f:
                fields |= json.loads(line).keys()
        assert fields == {name for name, _ in RAW_FIELDS[entity.name]}, entity.name


def test_batch_id_is_deterministic_and_content_based(tmp_path):
    a, b = tmp_path / "a.ndjson", tmp_path / "b.ndjson"
    a.write_text('{"x": 1}\n')
    b.write_text('{"x": 2}\n')
    assert compute_batch_id("ad_traffic", a) == compute_batch_id("ad_traffic", a)
    assert compute_batch_id("ad_traffic", a) != compute_batch_id("ad_traffic", b)
    assert compute_batch_id("ad_traffic", a).startswith("ad_traffic_")


def test_enrich_adds_metadata_and_keeps_duplicates():
    lines = ['{"event_id": "evt_1"}\n', "\n", '{"event_id": "evt_1"}\n']
    records = enrich_records(lines, "b1", "2026-10-14T00:00:00+00:00", "/x.ndjson")
    assert len(records) == 2  # blank line skipped, duplicate kept for silver to resolve
    for r in records:
        assert r["_batch_id"] == "b1"
        assert r["_ingested_at"] == "2026-10-14T00:00:00+00:00"
        assert r["_source_file"] == "/x.ndjson"


def test_table_schema_appends_required_metadata():
    schema = table_schema("conversions")
    assert schema[-len(METADATA_FIELDS):] == METADATA_FIELDS
    assert all(mode == "NULLABLE" for _, _, mode in schema[: -len(METADATA_FIELDS)])
