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


def run_generator(out_dir, **env):
    return subprocess.run(
        [sys.executable, str(ROOT / "data" / "generate_data.py")],
        env={**os.environ, "DATA_OUTPUT_DIR": str(out_dir), **env},
        capture_output=True,
        text=True,
    )


def split_file(path, entity_name):
    with open(path) as f:
        return split_records(f, entity_name, *META)


def test_generator_writes_all_entity_files_to_output_dir(generated_dir):
    for entity in ENTITIES:
        assert (generated_dir / entity.filename).stat().st_size > 0


def test_schemas_cover_every_generated_field(generated_dir):
    for entity in ENTITIES:
        fields = set()
        with open(generated_dir / entity.filename) as f:
            for line in f:
                fields |= json.loads(line).keys()
        assert fields <= {name for name, _ in RAW_FIELDS[entity.name]}, entity.name


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


def test_clean_generator_output_has_no_rejects(generated_dir):
    for entity in ENTITIES:
        good, rejects = split_file(generated_dir / entity.filename, entity.name)
        assert rejects == [] and good
        check_batch_acceptable(len(good), len(rejects))


@pytest.mark.parametrize(
    "fault, entity_name, expected_rejects, reason, loads",
    [
        ("malformed_lines", "ad_traffic", 2, None, True),  # below the 1% threshold: quarantined, batch loads
        ("missing_key", "funnel_events", 1, "missing primary key event_id", True),
        ("schema_drift", "conversions", None, "unexpected field(s): device_type", False),  # every line rejected
        ("empty_file", "conversions", 0, None, False),  # nothing to load: broken pull
    ],
)
def test_fault_injection_is_caught(tmp_path, fault, entity_name, expected_rejects, reason, loads):
    assert run_generator(tmp_path, FAULTS=fault).returncode == 0
    good, rejects = split_file(tmp_path / f"{entity_name}.ndjson", entity_name)
    if expected_rejects is not None:
        assert len(rejects) == expected_rejects
    if reason:
        assert all(r["error"] == reason for r in rejects)
    if loads:
        check_batch_acceptable(len(good), len(rejects))
    else:
        with pytest.raises(ValueError):
            check_batch_acceptable(len(good), len(rejects))


def test_malformed_lines_reasons(tmp_path):
    run_generator(tmp_path, FAULTS="malformed_lines")
    _, rejects = split_file(tmp_path / "ad_traffic.ndjson", "ad_traffic")
    reasons = sorted(r["error"].split(":")[0] for r in rejects)
    assert reasons == ["invalid JSON", "wrong type"]


def test_unknown_fault_is_refused(tmp_path):
    result = run_generator(tmp_path, FAULTS="typo_fault")
    assert result.returncode != 0 and "Unknown FAULTS" in result.stderr


def test_restate_from_emits_lead_returns(tmp_path, generated_dir):
    previous = {}
    with open(generated_dir / "conversions.ndjson") as f:
        for line in f:
            r = json.loads(line)
            previous[r["conversion_id"]] = r
    assert run_generator(tmp_path, RESTATE_FROM=str(generated_dir / "conversions.ndjson")).returncode == 0

    with open(tmp_path / "conversions.ndjson") as f:
        returns = [r for r in map(json.loads, f) if "status_updated_at" in r]
    eligible = [r for r in previous.values() if r["lead_status"] in ("accepted", "qualified")]
    assert len(returns) == int(len(eligible) * 0.05) > 0
    for r in returns:
        original = previous[r["conversion_id"]]  # same business key as an earlier lead
        assert original["lead_status"] in ("accepted", "qualified")
        assert (r["lead_status"], r["revenue_usd"]) == ("rejected", 0.0)
        assert r["timestamp"] == original["timestamp"]  # a return is not a late arrival
    good, rejects = split_file(tmp_path / "conversions.ndjson", "conversions")
    assert rejects == []


def test_missing_restate_file_is_ignored(tmp_path):
    result = run_generator(tmp_path, RESTATE_FROM=str(tmp_path / "nope.ndjson"))
    assert result.returncode == 0 and "Restated" not in result.stdout
