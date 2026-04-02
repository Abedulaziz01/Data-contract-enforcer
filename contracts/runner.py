
"""
ValidationRunner — executes every clause in a contract YAML against a
data snapshot and produces a structured JSON report.

Usage:
    python contracts/runner.py \
        --contract generated_contracts/week3_extractions.yaml \
        --data outputs/week3/extractions.jsonl \
        --output validation_reports/thursday_baseline.json
"""
import argparse
import hashlib
import json
import re
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd
import yaml
from dotenv import load_dotenv

load_dotenv()

from contracts.models import ValidationResult, ValidationReport

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def load_jsonl(path: str) -> List[Dict]:
    records = []
    with open(path) as f:
        for line in f:
            line = line.strip()
            if line:
                records.append(json.loads(line))
    return records


def sha256_file(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()


def flatten_for_runner(records: List[Dict]) -> pd.DataFrame:
    """Same flattening logic as generator — must stay in sync."""
    rows = []
    for r in records:
        base = {k: v for k, v in r.items()
                if not isinstance(v, (list, dict))}
        facts    = r.get("extracted_facts", [])
        entities = r.get("entities", [])
        if facts:
            for fact in facts:
                fact_flat = {f"fact_{k}": v for k, v in fact.items()}
                if entities:
                    for entity in entities:
                        entity_flat = {f"entity_{k}": v
                                       for k, v in entity.items()}
                        rows.append({**base, **fact_flat, **entity_flat})
                else:
                    rows.append({**base, **fact_flat})
        elif entities:
            for entity in entities:
                entity_flat = {f"entity_{k}": v for k, v in entity.items()}
                rows.append({**base, **entity_flat})
        else:
            rows.append(base)
    return pd.DataFrame(rows)


def make_result(check_id: str, column_name: str, check_type: str,
                status: str, actual_value: str, expected: str,
                severity: str, records_failing: int = 0,
                sample_failing: Optional[List[str]] = None,
                message: str = "") -> Dict:
    return {
        "check_id":        check_id,
        "column_name":     column_name,
        "check_type":      check_type,
        "status":          status,
        "actual_value":    str(actual_value),
        "expected":        str(expected),
        "severity":        severity,
        "records_failing": records_failing,
        "sample_failing":  sample_failing or [],
        "message":         message,
    }


# ---------------------------------------------------------------------------
# STRUCTURAL CHECKS
# ---------------------------------------------------------------------------

def check_required_field(col: str, clause: Dict,
                          df: pd.DataFrame,
                          contract_id: str) -> Optional[Dict]:
    if not clause.get("required", False):
        return None
    check_id = f"{contract_id}.{col}.required"
    if col not in df.columns:
        return make_result(check_id, col, "required",
                           "ERROR", "column missing", "present",
                           "CRITICAL",
                           message=f"Column '{col}' not found in data.")
    null_count = int(df[col].isna().sum())
    if null_count > 0:
        sample = df[df[col].isna()].index.tolist()[:5]
        return make_result(check_id, col, "required",
                           "FAIL",
                           f"null_count={null_count}",
                           "null_count=0",
                           "CRITICAL",
                           records_failing=null_count,
                           sample_failing=[str(s) for s in sample],
                           message=f"{null_count} null values found in required field '{col}'.")
    return make_result(check_id, col, "required",
                       "PASS", "null_count=0", "null_count=0", "LOW")


def check_type_match(col: str, clause: Dict,
                     df: pd.DataFrame,
                     contract_id: str) -> Optional[Dict]:
    expected_type = clause.get("type")
    if expected_type not in ("number", "integer"):
        return None
    check_id = f"{contract_id}.{col}.type"
    if col not in df.columns:
        return make_result(check_id, col, "type",
                           "ERROR", "column missing",
                           f"type={expected_type}", "CRITICAL",
                           message=f"Column '{col}' not found in data.")
    actual_dtype = str(df[col].dtype)
    numeric_ok = pd.api.types.is_numeric_dtype(df[col])
    if not numeric_ok:
        return make_result(check_id, col, "type",
                           "FAIL",
                           f"dtype={actual_dtype}",
                           f"type={expected_type}",
                           "CRITICAL",
                           records_failing=len(df),
                           message=f"Column '{col}' is '{actual_dtype}', expected numeric.")
    return make_result(check_id, col, "type",
                       "PASS", f"dtype={actual_dtype}",
                       f"type={expected_type}", "LOW")


def check_enum_conformance(col: str, clause: Dict,
                            df: pd.DataFrame,
                            contract_id: str) -> Optional[Dict]:
    enum_vals = clause.get("enum")
    if not enum_vals:
        return None
    check_id = f"{contract_id}.{col}.enum"
    if col not in df.columns:
        return make_result(check_id, col, "enum",
                           "ERROR", "column missing",
                           f"enum={enum_vals}", "HIGH",
                           message=f"Column '{col}' not found in data.")
    non_null = df[col].dropna()
    violators = non_null[~non_null.isin(enum_vals)]
    count = len(violators)
    if count > 0:
        sample = violators.astype(str).unique()[:5].tolist()
        return make_result(check_id, col, "enum",
                           "FAIL",
                           f"violating_count={count}, sample={sample}",
                           f"all values in {enum_vals}",
                           "HIGH",
                           records_failing=count,
                           sample_failing=sample,
                           message=f"{count} values not in enum list.")
    return make_result(check_id, col, "enum",
                       "PASS", "all values conform",
                       f"enum={enum_vals}", "LOW")


def check_uuid_pattern(col: str, clause: Dict,
                        df: pd.DataFrame,
                        contract_id: str) -> Optional[Dict]:
    if clause.get("format") != "uuid":
        return None
    check_id = f"{contract_id}.{col}.uuid"
    if col not in df.columns:
        return make_result(check_id, col, "uuid_pattern",
                           "ERROR", "column missing", "format=uuid",
                           "CRITICAL",
                           message=f"Column '{col}' not found in data.")
    pattern = re.compile(r"^[0-9a-f-]{36}$")
    sample_df = df[col].dropna()
    if len(sample_df) > 10000:
        sample_df = sample_df.sample(100, random_state=42)
    violators = sample_df[~sample_df.astype(str).str.match(pattern)]
    count = len(violators)
    if count > 0:
        sample = violators.astype(str).tolist()[:5]
        return make_result(check_id, col, "uuid_pattern",
                           "FAIL",
                           f"violating_count={count}",
                           "pattern=^[0-9a-f-]{36}$",
                           "CRITICAL",
                           records_failing=count,
                           sample_failing=sample,
                           message=f"{count} values do not match UUID pattern.")
    return make_result(check_id, col, "uuid_pattern",
                       "PASS", "all values match UUID pattern",
                       "pattern=^[0-9a-f-]{36}$", "LOW")


def check_datetime_format(col: str, clause: Dict,
                           df: pd.DataFrame,
                           contract_id: str) -> Optional[Dict]:
    if clause.get("format") != "date-time":
        return None
    check_id = f"{contract_id}.{col}.datetime"
    if col not in df.columns:
        return make_result(check_id, col, "datetime_format",
                           "ERROR", "column missing", "format=date-time",
                           "HIGH",
                           message=f"Column '{col}' not found in data.")
    bad = 0
    for val in df[col].dropna():
        try:
            datetime.fromisoformat(str(val).replace("Z", "+00:00"))
        except ValueError:
            bad += 1
    if bad > 0:
        return make_result(check_id, col, "datetime_format",
                           "FAIL",
                           f"unparseable_count={bad}",
                           "format=ISO 8601 date-time",
                           "HIGH",
                           records_failing=bad,
                           message=f"{bad} values could not be parsed as ISO 8601.")
    return make_result(check_id, col, "datetime_format",
                       "PASS", "all values parse as date-time",
                       "format=date-time", "LOW")


# ---------------------------------------------------------------------------
# STATISTICAL CHECKS
# ---------------------------------------------------------------------------

def check_range(col: str, clause: Dict,
                df: pd.DataFrame,
                contract_id: str) -> Optional[Dict]:
    minimum = clause.get("minimum")
    maximum = clause.get("maximum")
    if minimum is None and maximum is None:
        return None
    check_id = f"{contract_id}.{col}.range"
    if col not in df.columns:
        return make_result(check_id, col, "range",
                           "ERROR", "column missing",
                           f"min>={minimum}, max<={maximum}",
                           "CRITICAL",
                           message=f"Column '{col}' not found in data.")
    numeric = pd.to_numeric(df[col], errors="coerce").dropna()
    if numeric.empty:
        return make_result(check_id, col, "range",
                           "ERROR", "no numeric values",
                           f"min>={minimum}, max<={maximum}",
                           "CRITICAL",
                           message=f"Column '{col}' has no numeric values.")
    data_min  = float(numeric.min())
    data_max  = float(numeric.max())
    data_mean = float(numeric.mean())
    violated  = False
    msg_parts = []
    if minimum is not None and data_min < minimum:
        violated = True
        msg_parts.append(f"data min {data_min:.4f} < contract min {minimum}")
    if maximum is not None and data_max > maximum:
        violated = True
        msg_parts.append(f"data max {data_max:.4f} > contract max {maximum}")
    if violated:
        failing = df[
            (pd.to_numeric(df[col], errors="coerce") < (minimum or float("-inf"))) |
            (pd.to_numeric(df[col], errors="coerce") > (maximum or float("inf")))
        ]
        sample = []
        id_col = None
        for candidate in ["fact_id", "doc_id", "event_id", "id"]:
            if candidate in df.columns:
                id_col = candidate
                break
        if id_col:
            sample = failing[id_col].dropna().astype(str).tolist()[:5]
        return make_result(
            check_id, col, "range",
            "FAIL",
            f"max={data_max:.4f}, mean={data_mean:.4f}",
            f"max<={maximum}, min>={minimum}",
            "CRITICAL",
            records_failing=len(failing),
            sample_failing=sample,
            message="; ".join(msg_parts) + ". Breaking change detected."
        )
    return make_result(check_id, col, "range",
                       "PASS",
                       f"min={data_min:.4f}, max={data_max:.4f}",
                       f"min>={minimum}, max<={maximum}",
                       "LOW")


def check_statistical_drift(col: str,
                             df: pd.DataFrame,
                             baselines: Dict,
                             contract_id: str) -> Optional[Dict]:
    if col not in df.columns:
        return None
    if not pd.api.types.is_numeric_dtype(df[col]):
        return None
    check_id = f"{contract_id}.{col}.drift"
    if col not in baselines.get("columns", {}):
        return None   # no baseline yet — will be written after this run
    b            = baselines["columns"][col]
    current_mean = float(df[col].dropna().mean())
    current_std  = float(df[col].dropna().std())
    baseline_std = max(b["stddev"], 1e-9)
    z_score      = abs(current_mean - b["mean"]) / baseline_std
    if z_score > 3:
        return make_result(check_id, col, "statistical_drift",
                           "FAIL",
                           f"mean={current_mean:.4f}, z={z_score:.2f}",
                           f"z<=3 (baseline_mean={b['mean']:.4f})",
                           "HIGH",
                           message=f"{col} mean drifted {z_score:.1f} stddev from baseline.")
    if z_score > 2:
        return make_result(check_id, col, "statistical_drift",
                           "WARN",
                           f"mean={current_mean:.4f}, z={z_score:.2f}",
                           f"z<=2 (baseline_mean={b['mean']:.4f})",
                           "MEDIUM",
                           message=f"{col} mean within warning range ({z_score:.1f} stddev).")
    return make_result(check_id, col, "statistical_drift",
                       "PASS",
                       f"mean={current_mean:.4f}, z={z_score:.2f}",
                       f"z<=2 (baseline_mean={b['mean']:.4f})",
                       "LOW")


# ---------------------------------------------------------------------------
# BASELINE WRITE
# ---------------------------------------------------------------------------

def write_baselines(df: pd.DataFrame,
                    baselines_path: str = "schema_snapshots/baselines.json"):
    Path(baselines_path).parent.mkdir(parents=True, exist_ok=True)
    columns = {}
    for col in df.select_dtypes(include="number").columns:
        columns[col] = {
            "mean":   float(df[col].dropna().mean()),
            "stddev": float(df[col].dropna().std()),
        }
    data = {
        "written_at": datetime.now(timezone.utc).isoformat(),
        "columns":    columns,
    }
    with open(baselines_path, "w") as f:
        json.dump(data, f, indent=2)
    print(f"      Baselines written to {baselines_path} "
          f"({len(columns)} numeric columns)")


def load_baselines(baselines_path: str = "schema_snapshots/baselines.json") -> Dict:
    p = Path(baselines_path)
    if not p.exists():
        return {"columns": {}}
    with open(p) as f:
        return json.load(f)


# ---------------------------------------------------------------------------
# MAIN RUNNER
# ---------------------------------------------------------------------------

def run_validation(contract_path: str, data_path: str,
                   output_path: str) -> Dict:
    # load contract
    with open(contract_path) as f:
        contract = yaml.safe_load(f)

    contract_id  = contract.get("id", "unknown")
    schema       = contract.get("schema", {})
    snapshot_id  = sha256_file(data_path)

    # load and flatten data
    records = load_jsonl(data_path)
    df      = flatten_for_runner(records)

    # load baselines (may be empty on first run)
    baselines = load_baselines()

    results      = []
    baseline_written = False

    # ---- STRUCTURAL CHECKS ----
    for col, clause in schema.items():
        # 1. required
        r = check_required_field(col, clause, df, contract_id)
        if r:
            results.append(r)
        # 2. type match
        r = check_type_match(col, clause, df, contract_id)
        if r:
            results.append(r)
        # 3. enum conformance
        r = check_enum_conformance(col, clause, df, contract_id)
        if r:
            results.append(r)
        # 4. uuid pattern
        r = check_uuid_pattern(col, clause, df, contract_id)
        if r:
            results.append(r)
        # 5. datetime format
        r = check_datetime_format(col, clause, df, contract_id)
        if r:
            results.append(r)

    # ---- STATISTICAL CHECKS ----
    for col, clause in schema.items():
        # 6. range check
        r = check_range(col, clause, df, contract_id)
        if r:
            results.append(r)
        # 7. statistical drift
        r = check_statistical_drift(col, df, baselines, contract_id)
        if r:
            results.append(r)

    # write baselines after first run
    if not baselines.get("columns"):
        write_baselines(df)

    # tally
    passed  = sum(1 for r in results if r["status"] == "PASS")
    failed  = sum(1 for r in results if r["status"] == "FAIL")
    warned  = sum(1 for r in results if r["status"] == "WARN")
    errored = sum(1 for r in results if r["status"] == "ERROR")

    report = {
        "report_id":     str(uuid.uuid4()),
        "contract_id":   contract_id,
        "snapshot_id":   snapshot_id,
        "run_timestamp": datetime.now(timezone.utc).isoformat(),
        "total_checks":  len(results),
        "passed":        passed,
        "failed":        failed,
        "warned":        warned,
        "errored":       errored,
        "results":       results,
    }

    # write output
    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w") as f:
        json.dump(report, f, indent=2)

    return report


def main():
    parser = argparse.ArgumentParser(description="ValidationRunner")
    parser.add_argument("--contract", required=True,
                        help="Path to contract YAML")
    parser.add_argument("--data",     required=True,
                        help="Path to data JSONL file")
    parser.add_argument("--output",   required=True,
                        help="Path to write validation report JSON")
    args = parser.parse_args()

    print(f"Running validation...")
    print(f"  Contract : {args.contract}")
    print(f"  Data     : {args.data}")
    print(f"  Output   : {args.output}")

    report = run_validation(args.contract, args.data, args.output)

    print(f"\nResults:")
    print(f"  Total checks : {report['total_checks']}")
    print(f"  Passed       : {report['passed']}")
    print(f"  Failed       : {report['failed']}")
    print(f"  Warned       : {report['warned']}")
    print(f"  Errored      : {report['errored']}")
    print(f"\nReport written to: {args.output}")

    # print any failures clearly
    failures = [r for r in report["results"]
                if r["status"] in ("FAIL", "ERROR")]
    if failures:
        print(f"\nFAILURES DETECTED:")
        for r in failures:
            print(f"  [{r['severity']}] {r['check_id']}: {r['message']}")
    else:
        print("\nAll checks passed.")


if __name__ == "__main__":
    main()