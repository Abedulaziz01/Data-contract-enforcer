
"""
Tests for Phase 3 — ValidationRunner.
Run with: python -m pytest tests/test_phase3_runner.py -v
"""
import json
import os
import tempfile
import pytest
from pathlib import Path
from contracts.runner import run_validation


CONTRACT_PATH = "generated_contracts/week3_extractions.yaml"
CLEAN_DATA    = "outputs/week3/extractions.jsonl"


def test_clean_data_all_structural_pass():
    """Runner on clean data must produce a report with no structural FAILs."""
    with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as tmp:
        tmp_path = tmp.name
    try:
        report = run_validation(CONTRACT_PATH, CLEAN_DATA, tmp_path)
        structural_types = {"required", "type", "enum",
                            "uuid_pattern", "datetime_format"}
        failures = [
            r for r in report["results"]
            if r["status"] == "FAIL"
            and r["check_type"] in structural_types
        ]
        assert len(failures) == 0, (
            f"Structural failures on clean data: {failures}"
        )
        assert report["total_checks"] >= 8, (
            f"Expected >= 8 checks, got {report['total_checks']}"
        )
    finally:
        os.unlink(tmp_path)


def test_null_in_required_field_produces_critical():
    """Runner on data with a null in a required field must produce CRITICAL."""
    # load clean records
    records = []
    with open(CLEAN_DATA) as f:
        for line in f:
            line = line.strip()
            if line:
                records.append(json.loads(line))

    # inject a null into doc_id of the first record
    records[0]["doc_id"] = None

    with tempfile.NamedTemporaryFile(
            mode="w", suffix=".jsonl", delete=False) as tmp_data:
        for r in records:
            tmp_data.write(json.dumps(r) + "\n")
        tmp_data_path = tmp_data.name

    with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as tmp_out:
        tmp_out_path = tmp_out.name

    try:
        report = run_validation(CONTRACT_PATH, tmp_data_path, tmp_out_path)
        critical_fails = [
            r for r in report["results"]
            if r["status"] == "FAIL" and r["severity"] == "CRITICAL"
            and r["check_type"] == "required"
        ]
        assert len(critical_fails) >= 1, (
            "Expected at least 1 CRITICAL required-field failure"
        )
    finally:
        os.unlink(tmp_data_path)
        os.unlink(tmp_out_path)


def test_runner_never_crashes_on_empty_file():
    """Runner must never raise an uncaught exception on any input."""
    with tempfile.NamedTemporaryFile(
            mode="w", suffix=".jsonl", delete=False) as tmp_data:
        tmp_data.write("")   # empty file
        tmp_data_path = tmp_data.name

    with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as tmp_out:
        tmp_out_path = tmp_out.name

    try:
        # must not raise
        report = run_validation(CONTRACT_PATH, tmp_data_path, tmp_out_path)
        assert "report_id" in report
    except Exception as e:
        pytest.fail(f"Runner raised an exception on empty file: {e}")
    finally:
        os.unlink(tmp_data_path)
        os.unlink(tmp_out_path)
EOF