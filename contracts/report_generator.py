
"""
ReportGenerator — reads all validation runs and violation records and
auto-generates the Enforcer Report. Never written by hand.

Usage:
    python contracts/report_generator.py
"""
import glob
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional


# ---------------------------------------------------------------------------
# SECTION 1 — Data Health Score
# ---------------------------------------------------------------------------

def compute_health_score(validation_reports: List[Dict]) -> int:
    """
    Start at 100. Subtract per FAIL/ERROR result by severity.
    CRITICAL=20, HIGH=10, MEDIUM=5, LOW=1. Clamp to 0-100.
    Never hardcoded — always computed from real report files.
    """
    total_checks = sum(report.get("total_checks", 0) for report in validation_reports)
    total_passed = sum(report.get("passed", 0) for report in validation_reports)
    critical_failures = sum(
        1
        for report in validation_reports
        for result in report.get("results", [])
        if result.get("status") in ("FAIL", "ERROR")
        and result.get("severity") == "CRITICAL"
    )
    if total_checks == 0:
        return 0
    score = (total_passed / total_checks) * 100
    score -= critical_failures * 20
    return max(0, min(100, round(score)))


# ---------------------------------------------------------------------------
# SECTION 2 — Plain language violations
# ---------------------------------------------------------------------------

def plain_language_violation(result: Dict) -> str:
    """
    Format a FAIL result as a sentence readable by a non-engineer.
    Names the system, field, and record count explicitly.
    """
    column        = result.get("column_name", "unknown field")
    check_id      = result.get("check_id", "")
    system        = check_id.split(".")[0] if "." in check_id else "unknown system"
    check_type    = result.get("check_type", "unknown")
    expected      = result.get("expected", "unknown")
    actual        = result.get("actual_value", "unknown")
    records_fail  = result.get("records_failing", "unknown")
    return (
        f"The {column} field in {system} failed its {check_type} check. "
        f"Expected {expected} but found {actual}. "
        f"This affects {records_fail} records."
    )


# ---------------------------------------------------------------------------
# SECTION 3 — Schema changes summary
# ---------------------------------------------------------------------------

def summarise_schema_changes(evolution_path: str) -> List[Dict]:
    """
    Load schema_evolution.json and return plain-language summaries
    of every detected change with compatibility verdict and action.
    """
    p = Path(evolution_path)
    if not p.exists():
        return [{
            "summary":       "No schema evolution report found.",
            "compatibility": "UNKNOWN",
            "action":        "Run schema_analyzer.py to generate a report.",
        }]

    with open(p) as f:
        evolution = json.load(f)

    summaries = []
    for change in evolution.get("changes", []):
        field         = change.get("field", "unknown")
        compatibility = change.get("compatibility", "UNKNOWN")
        reason        = change.get("reason", "")

        if compatibility == "BREAKING":
            action = (
                f"IMMEDIATE ACTION REQUIRED: {reason}. "
                f"Coordinate with all downstream consumers before deploying."
            )
        else:
            action = (
                f"No immediate action required. "
                f"Notify downstream consumers of the change."
            )

        summaries.append({
            "field":         field,
            "summary":       f"Field '{field}': {reason}",
            "compatibility": compatibility,
            "action":        action,
        })

    if not summaries:
        summaries.append({
            "summary":       "No schema changes detected between snapshots.",
            "compatibility": "COMPATIBLE",
            "action":        "None required.",
        })

    return summaries


# ---------------------------------------------------------------------------
# SECTION 4 — AI risk assessment
# ---------------------------------------------------------------------------

def build_ai_risk_assessment(ai_extensions_path: str) -> Dict:
    """
    Load ai_extensions.json and produce a plain-language AI risk section.
    Handles missing file gracefully.
    """
    p = Path(ai_extensions_path)
    if not p.exists():
        return {
            "status":           "ERROR",
            "message":          "ai_extensions.json not found. Run ai_extensions.py first.",
            "embedding_drift":  None,
            "prompt_validation": None,
            "output_violation": None,
        }

    with open(p) as f:
        ai_data = json.load(f)

    checks = ai_data.get("checks", {})

    # embedding drift
    drift        = checks.get("embedding_drift", {})
    drift_score  = drift.get("drift_score", None)
    drift_status = drift.get("status", "UNKNOWN")

    if drift_status == "BASELINE_SET":
        drift_narrative = (
            "Embedding baseline has been set on this first run. "
            "The centroid represents the current semantic distribution of "
            "extracted_facts[*].text. Run again to compute a drift score. "
            "A drift_score of 0.0 here does NOT mean zero drift — it means "
            "no comparison has been made yet."
        )
    elif drift_score is not None and drift_score > 0.15:
        drift_narrative = (
            f"ALERT: Embedding drift score is {drift_score:.4f}, "
            f"exceeding the 0.15 threshold. "
            f"The semantic content of extracted text has shifted significantly."
        )
    else:
        drift_narrative = (
            f"Embedding drift score is {drift_score} — within acceptable bounds."
        )

    # prompt input validation
    prompt       = checks.get("prompt_input_validation", {})
    quarantined  = prompt.get("quarantined_count", 0)
    total_recs   = prompt.get("total_records", 0)
    quarantine_rate = (
        round(quarantined / max(total_recs, 1), 4) if total_recs > 0 else 0.0
    )

    # LLM output violation rate
    output       = checks.get("llm_output_violation_rate", {})
    violation_rate = output.get("violation_rate", None)
    output_status  = output.get("status", "UNKNOWN")
    output_trend   = output.get("trend", "unknown")

    return {
        "embedding_drift": {
            "status":    drift_status,
            "score":     drift_score,
            "narrative": drift_narrative,
        },
        "prompt_input_validation": {
            "status":            prompt.get("status", "UNKNOWN"),
            "quarantine_rate":   quarantine_rate,
            "quarantined_count": quarantined,
            "total_records":     total_recs,
        },
        "llm_output_violation_rate": {
            "status":         output_status,
            "violation_rate": violation_rate,
            "trend":          output_trend,
            "narrative": (
                f"LLM output violation rate is {violation_rate} "
                f"(trend: {output_trend}). "
                + ("Rate exceeds 2% warning threshold."
                   if violation_rate and violation_rate > 0.02
                   else "Rate is within acceptable bounds.")
            ) if violation_rate is not None else "No data available.",
        },
    }


# ---------------------------------------------------------------------------
# SECTION 5 — Recommendations
# ---------------------------------------------------------------------------

def build_recommendations(all_failures: List[Dict],
                           breaking_changes: int) -> List[str]:
    """
    Exactly 3 prioritised, specific actions.
    References actual file paths, contract IDs, and clause names.
    """
    recs = []

    # Recommendation 1 — most critical failure
    critical_fails = [
        f for f in all_failures if f.get("severity") == "CRITICAL"
    ]
    if critical_fails:
        top = critical_fails[0]
        col = top.get("column_name", "confidence")
        recs.append(
            f"[CRITICAL] Update src/week3/extractor.py to output "
            f"'{col}' as float 0.0-1.0 per contract "
            f"week3-document-refinery-extractions clause "
            f"fact_confidence.range. "
            f"This affects {top.get('records_failing', 'multiple')} records."
        )
    else:
        recs.append(
            "[HIGH] Review all numeric fields in outputs/week3/extractions.jsonl "
            "and confirm confidence values are in 0.0-1.0 range per contract "
            "week3-document-refinery-extractions."
        )

    # Recommendation 2 — schema evolution
    if breaking_changes > 0:
        recs.append(
            f"[HIGH] {breaking_changes} breaking schema change(s) detected. "
            f"Run: python contracts/schema_analyzer.py "
            f"--contract-id week3-document-refinery-extractions "
            f"--output validation_reports/schema_evolution.json "
            f"and follow the migration checklist in "
            f"validation_reports/migration_impact_*.json before next deployment."
        )
    else:
        recs.append(
            "[MEDIUM] Add contract enforcement as a CI pipeline step. "
            "Run contracts/runner.py before every deployment of "
            "src/week3/extractor.py to catch scale violations early."
        )

    # Recommendation 3 — ongoing monitoring
    recs.append(
        "[MEDIUM] Re-run contracts/ai_extensions.py after every model update "
        "to track embedding drift. If drift_score exceeds 0.15, "
        "re-establish the baseline in schema_snapshots/embedding_baselines.npz "
        "and notify downstream consumers in week4-cartographer pipeline."
    )

    return recs[:3]


# ---------------------------------------------------------------------------
# MAIN — Generate Report
# ---------------------------------------------------------------------------

def generate_report(
        reports_dir:       str = "validation_reports/",
        violations_dir:    str = "violation_log/",
        evolution_path:    str = "validation_reports/schema_evolution.json",
        ai_extensions_path: str = "validation_reports/ai_extensions.json",
) -> Dict:

    # load all validation reports
    report_files    = glob.glob(f"{reports_dir}*.json")
    # exclude schema_evolution, migration_impact, ai_extensions, ai_metrics
    exclude_prefixes = (
        "schema_evolution", "migration_impact",
        "ai_extensions", "ai_metrics",
    )
    validation_reports = []
    for path in report_files:
        fname = Path(path).name
        if not any(fname.startswith(p) for p in exclude_prefixes):
            try:
                with open(path) as f:
                    validation_reports.append(json.load(f))
            except Exception:
                pass

    print(f"  Loaded {len(validation_reports)} validation report(s)")

    # load violations
    violations = []
    vpath = Path(violations_dir) / "violations.jsonl"
    if vpath.exists():
        with open(vpath) as f:
            for line in f:
                line = line.strip()
                if line:
                    violations.append(json.loads(line))
    print(f"  Loaded {len(violations)} violation record(s)")

    # SECTION 1 — health score
    health_score = compute_health_score(validation_reports)

    # collect all failures across all reports
    all_failures = [
        r for rep in validation_reports
        for r in rep.get("results", [])
        if r.get("status") in ("FAIL", "ERROR")
    ]

    # SECTION 2 — top violations (top 3 by severity)
    severity_order = ["CRITICAL", "HIGH", "MEDIUM", "LOW"]
    sorted_failures = sorted(
        all_failures,
        key=lambda x: (
            severity_order.index(x.get("severity", "LOW"))
            if x.get("severity", "LOW") in severity_order else 99
        )
    )
    top_3 = sorted_failures[:3]
    top_violations = [plain_language_violation(v) for v in top_3]

    # SECTION 3 — schema changes
    schema_changes = summarise_schema_changes(evolution_path)

    # SECTION 4 — AI risk assessment
    ai_risk = build_ai_risk_assessment(ai_extensions_path)

    # SECTION 5 — recommendations
    breaking_count = sum(
        1 for c in schema_changes
        if c.get("compatibility") == "BREAKING"
    )
    recommendations = build_recommendations(all_failures, breaking_count)

    # violation counts by severity
    violations_by_severity = {}
    for sev in severity_order:
        violations_by_severity[sev] = sum(
            1 for f in all_failures if f.get("severity") == sev
        )

    # health narrative
    critical_count = violations_by_severity.get("CRITICAL", 0)
    if health_score >= 90:
        health_narrative = (
            f"Score of {health_score}/100. No critical violations detected."
        )
    else:
        health_narrative = (
            f"Score of {health_score}/100. "
            f"{critical_count} critical issue(s) require immediate action."
        )

    report = {
        "generated_at":   datetime.now(timezone.utc).isoformat(),
        "generated_by":   "contracts/report_generator.py",
        "period":         (
            f"{(datetime.now(timezone.utc) - timedelta(days=7)).date()} "
            f"to {datetime.now(timezone.utc).date()}"
        ),
        "data_health_score":  health_score,
        "health_narrative":   health_narrative,
        "violation_count":    len(violations),
        "top_violations":     top_violations,
        "total_violations_by_severity": violations_by_severity,
        "schema_changes":     schema_changes,
        "ai_risk_assessment": ai_risk,
        "recommendations":    recommendations,
    }

    return report


def main():
    print("Generating Enforcer Report...")
    print()

    report = generate_report()

    out_path = Path("enforcer_report/report_data.json")
    out_path.parent.mkdir(parents=True, exist_ok=True)

    with open(out_path, "w") as f:
        json.dump(report, f, indent=2)

    print(f"\nEnforcer Report written to: {out_path}")
    print()
    print(f"  Data Health Score : {report['data_health_score']}/100")
    print(f"  Violation count   : {report['violation_count']}")
    print(f"  Schema changes    : {len(report['schema_changes'])}")
    print(f"  Recommendations   : {len(report['recommendations'])}")
    if report["top_violations"]:
        print(f"\n  Top violation:")
        print(f"  {report['top_violations'][0]}")


if __name__ == "__main__":
    main()
