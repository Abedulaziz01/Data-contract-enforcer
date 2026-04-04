
"""
Tests for Phase 6 — AI Contract Extensions.
Run with: python -m pytest tests/test_phase6_ai_extensions.py -v
"""
import json
import os
import shutil
import tempfile
import pytest
import numpy as np
from pathlib import Path
from contracts.ai_extensions import (
    check_embedding_drift,
    validate_prompt_inputs,
    check_output_schema_violation_rate,
    load_jsonl,
    extract_fact_texts,
)

EXTRACTIONS_PATH = "outputs/week3/extractions.jsonl"


def test_first_run_returns_baseline_set_and_creates_npz():
    """
    First run on clean extractions must return BASELINE_SET
    and create the embedding_baselines.npz file.
    """
    with tempfile.TemporaryDirectory() as tmpdir:
        baseline_path = os.path.join(tmpdir, "embedding_baselines.npz")
        records = load_jsonl(EXTRACTIONS_PATH)
        texts   = extract_fact_texts(records)

        result = check_embedding_drift(texts, baseline_path=baseline_path)

        assert result["status"] == "BASELINE_SET", (
            f"Expected BASELINE_SET, got {result['status']}"
        )
        assert Path(baseline_path).exists(), (
            "embedding_baselines.npz was not created"
        )


def test_second_run_returns_numeric_drift_score():
    """
    Second run must return a numeric drift_score (not None, not BASELINE_SET).
    """
    with tempfile.TemporaryDirectory() as tmpdir:
        baseline_path = os.path.join(tmpdir, "embedding_baselines.npz")
        records = load_jsonl(EXTRACTIONS_PATH)
        texts   = extract_fact_texts(records)

        # first run — set baseline
        check_embedding_drift(texts, baseline_path=baseline_path)

        # second run — compute drift
        result = check_embedding_drift(texts, baseline_path=baseline_path)

        assert result["status"] in ("PASS", "FAIL"), (
            f"Expected PASS or FAIL on second run, got {result['status']}"
        )
        assert isinstance(result["drift_score"], float), (
            f"drift_score must be float, got {type(result['drift_score'])}"
        )
        assert result["drift_score"] >= 0.0, (
            "drift_score must be >= 0"
        )


def test_quarantine_file_always_created():
    """
    quarantine.jsonl must be created even if all records are valid.
    """
    with tempfile.TemporaryDirectory() as tmpdir:
        quarantine_path = os.path.join(tmpdir, "quarantine.jsonl")
        records = load_jsonl(EXTRACTIONS_PATH)

        valid, quarantined = validate_prompt_inputs(
            records,
            quarantine_path=quarantine_path,
        )

        assert Path(quarantine_path).exists(), (
            "quarantine.jsonl must always be created, even if empty"
        )


def test_violation_rate_detects_invalid_verdicts():
    """
    Records with invalid overall_verdict must be counted as violations.
    """
    records = [
        {"overall_verdict": "PASS"},
        {"overall_verdict": "FAIL"},
        {"overall_verdict": "INVALID_VALUE"},
        {"overall_verdict": "ANOTHER_BAD"},
    ]
    with tempfile.NamedTemporaryFile(
            suffix=".json", delete=False) as tmp:
        tmp_path = tmp.name
    try:
        result = check_output_schema_violation_rate(
            records,
            baseline_rate=None,
            metrics_path=tmp_path,
        )
        assert result["schema_violations"] == 2, (
            f"Expected 2 violations, got {result['schema_violations']}"
        )
        assert result["violation_rate"] == 0.5, (
            f"Expected 0.5, got {result['violation_rate']}"
        )
    finally:
        os.unlink(tmp_path)


def test_no_crash_if_verdicts_missing():
    """
    ai_extensions must not crash if verdicts file is missing.
    Uses graceful ERROR status instead.
    """
    import subprocess, sys
    with tempfile.NamedTemporaryFile(
            suffix=".json", delete=False) as tmp:
        tmp_path = tmp.name
    try:
        result = subprocess.run(
            [
                sys.executable, "contracts/ai_extensions.py",
                "--mode",        "all",
                "--extractions", EXTRACTIONS_PATH,
                "--verdicts",    "outputs/week2/NONEXISTENT.jsonl",
                "--output",      tmp_path,
            ],
            capture_output=True,
            text=True,
        )
        assert result.returncode == 0, (
            f"ai_extensions crashed:\n{result.stderr}"
        )
        with open(tmp_path) as f:
            report = json.load(f)
        llm_check = report["checks"]["llm_output_violation_rate"]
        assert llm_check["status"] == "ERROR", (
            "Expected ERROR status when verdicts file missing"
        )
    finally:
        os.unlink(tmp_path)
