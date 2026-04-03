
"""
Tests for Phase 5 — SchemaEvolutionAnalyzer.
Run with: python -m pytest tests/test_phase5_analyzer.py -v
"""
import json
import subprocess
import sys
import pytest
from pathlib import Path
from contracts.schema_analyzer import classify_change


def test_classify_confidence_range_change_is_breaking():
    """
    The canonical test from the project doc:
    confidence max 1.0 -> 100.0 must be BREAKING.
    """
    result = classify_change(
        "fact_confidence",
        {"type": "number", "maximum": 1.0},
        {"type": "number", "maximum": 100.0},
    )
    assert result[0] == "BREAKING", (
        f"Expected BREAKING, got {result[0]}"
    )
    assert "100.0" in result[1], (
        f"Expected '100.0' in reason, got: {result[1]}"
    )


def test_classify_add_nullable_column_is_compatible():
    """Adding a nullable column must be COMPATIBLE."""
    result = classify_change("new_col", None, {"type": "string", "required": False})
    assert result[0] == "COMPATIBLE"


def test_classify_add_required_column_is_breaking():
    """Adding a required (non-nullable) column must be BREAKING."""
    result = classify_change("new_col", None, {"type": "string", "required": True})
    assert result[0] == "BREAKING"


def test_classify_remove_column_is_breaking():
    """Removing a column must be BREAKING."""
    result = classify_change("old_col", {"type": "string"}, None)
    assert result[0] == "BREAKING"


def test_classify_enum_removal_is_breaking():
    """Removing enum values must be BREAKING."""
    result = classify_change(
        "status",
        {"type": "string", "enum": ["PASS", "FAIL", "WARN"]},
        {"type": "string", "enum": ["PASS", "FAIL"]},
    )
    assert result[0] == "BREAKING"


def test_classify_enum_addition_is_compatible():
    """Adding enum values must be COMPATIBLE."""
    result = classify_change(
        "status",
        {"type": "string", "enum": ["PASS", "FAIL"]},
        {"type": "string", "enum": ["PASS", "FAIL", "WARN"]},
    )
    assert result[0] == "COMPATIBLE"


def test_analyzer_produces_evolution_report():
    """
    Run the full analyzer CLI against controlled snapshots and confirm
    it detects the canonical confidence range breaking change.
    """
    import os
    import tempfile
    import yaml

    repo_root = Path(__file__).resolve().parents[1]

    with tempfile.TemporaryDirectory() as tmpdir:
        tmp_root = Path(tmpdir)
        snapshot_dir = tmp_root / "schema_snapshots" / "week3-document-refinery-extractions"
        snapshot_dir.mkdir(parents=True, exist_ok=True)

        generated_dir = tmp_root / "generated_contracts"
        generated_dir.mkdir(parents=True, exist_ok=True)

        validation_dir = tmp_root / "validation_reports"
        validation_dir.mkdir(parents=True, exist_ok=True)

        old_contract = {
            "id": "week3-document-refinery-extractions",
            "schema": {
                "fact_confidence": {
                    "type": "number",
                    "minimum": 0.0,
                    "maximum": 1.0,
                    "required": True,
                }
            },
            "lineage": {
                "downstream": [
                    {
                        "id": "pipeline::week4-lineage",
                        "fields_consumed": ["fact_confidence"],
                    }
                ]
            },
        }

        new_contract = {
            "id": "week3-document-refinery-extractions",
            "schema": {
                "fact_confidence": {
                    "type": "number",
                    "minimum": 0.0,
                    "maximum": 100.0,
                    "required": True,
                }
            },
            "lineage": {
                "downstream": [
                    {
                        "id": "pipeline::week4-lineage",
                        "fields_consumed": ["fact_confidence"],
                    }
                ]
            },
        }

        with open(snapshot_dir / "20260101_000000.yaml", "w") as f:
            yaml.safe_dump(old_contract, f, sort_keys=False)

        with open(snapshot_dir / "20260102_000000.yaml", "w") as f:
            yaml.safe_dump(new_contract, f, sort_keys=False)

        with open(generated_dir / "week3_extractions.yaml", "w") as f:
            yaml.safe_dump(new_contract, f, sort_keys=False)

        output_path = validation_dir / "schema_evolution.json"

        result = subprocess.run(
            [
                sys.executable,
                str(repo_root / "contracts" / "schema_analyzer.py"),
                "--contract-id", "week3-document-refinery-extractions",
                "--output", str(output_path),
            ],
            capture_output=True,
            text=True,
            cwd=tmp_root,
        )

        assert result.returncode == 0, (
            f"Analyzer crashed:\nstdout:\n{result.stdout}\n\nstderr:\n{result.stderr}"
        )

        with open(output_path) as f:
            report = json.load(f)

        assert report["breaking_changes"] >= 1, (
            f"Expected >= 1 breaking change, got {report['breaking_changes']}"
        )

        breaking_fields = [
            c["field"] for c in report["changes"]
            if c["compatibility"] == "BREAKING"
        ]
        assert "fact_confidence" in breaking_fields, (
            f"fact_confidence not in breaking fields: {breaking_fields}"
        )

        migration_reports = list(validation_dir.glob("migration_impact_*.json"))
        assert migration_reports, "Expected a migration impact report to be generated"
