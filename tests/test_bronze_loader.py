import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "dags"))

from bronze.loader import (  # noqa: E402
    ENTITIES,
    METADATA_FIELDS,
    RAW_FIELDS,
    check_batch_acceptable,
    compute_batch_id,
    split_records,
    table_schema,
)


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


META = ("b1", "2026-10-14T00:00:00+00:00", "/x.ndjson")
GOOD_EVENT = '{"event_id": "evt_1", "session_id": "s1", "step_number": 1}'


def test_split_adds_metadata_and_keeps_duplicates():
    good, rejects = split_records([GOOD_EVENT, "\n", GOOD_EVENT], "funnel_events", *META)
    assert len(good) == 2 and rejects == []  # blank line skipped, duplicate kept for silver to resolve
    for r in good:
        assert (r["_batch_id"], r["_ingested_at"], r["_source_file"]) == META


@pytest.mark.parametrize(
    "line, reason",
    [
        ('{"event_id": "evt_1", "session_id"', "invalid JSON"),
        ('["evt_1"]', "not a JSON object"),
        ('{"session_id": "s1"}', "missing primary key event_id"),
        ('{"event_id": "", "session_id": "s1"}', "missing primary key event_id"),
        ('{"event_id": "evt_1", "step_number": "two"}', "wrong type: step_number"),
        ('{"event_id": "evt_1", "step_number": true}', "wrong type: step_number"),
        ('{"event_id": "evt_1", "device_type": "mobile"}', "unexpected field(s): device_type"),
    ],
)
def test_split_quarantines_bad_lines_with_reason(line, reason):
    good, rejects = split_records([GOOD_EVENT, line], "funnel_events", *META)
    assert len(good) == 1
    assert len(rejects) == 1
    assert rejects[0]["error"].startswith(reason)
    assert rejects[0]["raw_line"] == line  # original text kept for inspection and replay
    assert rejects[0]["line_number"] == 2
    assert rejects[0]["_batch_id"] == "b1"


def test_split_allows_missing_non_key_fields_and_ints_as_floats():
    good, rejects = split_records(['{"traffic_id": "t1", "cost_usd": 2}'], "ad_traffic", *META)
    assert len(good) == 1 and rejects == []


@pytest.mark.parametrize(
    "n_good, n_rejects, ok",
    [(1000, 0, True), (1000, 10, True), (1000, 11, False), (0, 0, False), (0, 5, False)],
)
def test_check_batch_acceptable(n_good, n_rejects, ok):
    if ok:
        check_batch_acceptable(n_good, n_rejects)
    else:
        with pytest.raises(ValueError):
            check_batch_acceptable(n_good, n_rejects)


def test_table_schema_appends_required_metadata():
    schema = table_schema("conversions")
    assert schema[-len(METADATA_FIELDS):] == METADATA_FIELDS
    assert all(mode == "NULLABLE" for _, _, mode in schema[: -len(METADATA_FIELDS)])
