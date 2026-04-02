
"""
Tests for Phase 4 — ViolationAttributor.
Run with: python -m pytest tests/test_phase4_attributor.py -v
"""
import json
import os
import subprocess
import sys
import tempfile
import pytest
from pathlib import Path


VIOLATION_REPORT = "validation_reports/injected_violation.json"
LINEAGE_PATH     = "outputs/week4/lineage_snapshots.jsonl"
CONTRACT_PATH    = "generated_contracts/week3_extractions.yaml"


def test_attributor_produces_violation_log():
    """
    Run attributor against injected violation report.
    violations.jsonl must have at least 1 entry with all required fields.
    """
    with tempfile.NamedTemporaryFile(
            suffix=".jsonl", delete=False, mode="w") as tmp:
        tmp_path = tmp.name

    try:
        result = subprocess.run(
            [
                sys.executable, "contracts/attributor.py",
                "--violation", VIOLATION_REPORT,
                "--lineage",   LINEAGE_PATH,
                "--contract",  CONTRACT_PATH,
                "--output",    tmp_path,
            ],
            capture_output=True,
            text=True,
        )
        assert result.returncode == 0, (
            f"Attributor crashed:\n{result.stderr}"
        )

        lines = [l.strip() for l in open(tmp_path) if l.strip()]
        assert len(lines) >= 1, "violations.jsonl has no entries"

        v = json.loads(lines[0])
        assert "violation_id"  in v, "Missing violation_id"
        assert "check_id"      in v, "Missing check_id"
        assert "blame_chain"   in v, "Missing blame_chain"
        assert "blast_radius"  in v, "Missing blast_radius"

        chain = v["blame_chain"]
        assert len(chain) >= 1, "blame_chain is empty"
        assert "commit_hash"      in chain[0], "Missing commit_hash"
        assert "confidence_score" in chain[0], "Missing confidence_score"
        assert "rank"             in chain[0], "Missing rank"

    finally:
        os.unlink(tmp_path)


def test_attributor_never_crashes_on_no_failures():
    """
    If the violation report has no FAILs, attributor must exit cleanly.
    """
    # build a report with no failures
    clean_report = {
        "report_id":     "test-123",
        "contract_id":   "week3-document-refinery-extractions",
        "snapshot_id":   "abc123",
        "run_timestamp": "2025-01-01T00:00:00+00:00",
        "total_checks":  5,
        "passed":        5,
        "failed":        0,
        "warned":        0,
        "errored":       0,
        "results":       [],
    }
    with tempfile.NamedTemporaryFile(
            mode="w", suffix=".json", delete=False) as tmp_in:
        json.dump(clean_report, tmp_in)
        tmp_in_path = tmp_in.name

    with tempfile.NamedTemporaryFile(
            suffix=".jsonl", delete=False, mode="w") as tmp_out:
        tmp_out_path = tmp_out.name

    try:
        result = subprocess.run(
            [
                sys.executable, "contracts/attributor.py",
                "--violation", tmp_in_path,
                "--lineage",   LINEAGE_PATH,
                "--contract",  CONTRACT_PATH,
                "--output",    tmp_out_path,
            ],
            capture_output=True,
            text=True,
        )
        assert result.returncode == 0, (
            f"Attributor crashed on clean report:\n{result.stderr}"
        )
    finally:
        os.unlink(tmp_in_path)
        os.unlink(tmp_out_path)