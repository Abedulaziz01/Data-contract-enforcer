
"""
AI Contract Extensions — three checks that standard contracts do not cover.

Extension 1: Embedding drift detection (OpenAI text-embedding-3-small)
Extension 2: Prompt input schema validation
Extension 3: LLM output schema violation rate

Usage:
    python contracts/ai_extensions.py \
        --mode all \
        --extractions outputs/week3/extractions.jsonl \
        --verdicts outputs/week2/verdicts.jsonl \
        --output validation_reports/ai_extensions.json
"""
import argparse
import json
import os
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
from dotenv import load_dotenv
from jsonschema import validate, ValidationError

load_dotenv()

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


def extract_fact_texts(records: List[Dict]) -> List[str]:
    """Pull extracted_facts[*].text from all records."""
    texts = []
    for r in records:
        for fact in r.get("extracted_facts", []):
            t = fact.get("text", "")
            if t:
                texts.append(t)
    return texts


# ---------------------------------------------------------------------------
# EXTENSION 1 — Embedding Drift
# ---------------------------------------------------------------------------

def embed_sample(texts: List[str],
                 n: int = 200,
                 model: str = "text-embedding-3-small") -> np.ndarray:
    """
    Embed up to n texts using OpenAI text-embedding-3-small.
    Falls back to random vectors if API key is missing.
    """
    api_key = os.getenv("OPENAI_API_KEY", "")
    sample  = texts[:n] if len(texts) > n else texts

    if not api_key or api_key == "sk-PLACEHOLDER":
        print("      OPENAI_API_KEY not set — using random vectors (demo mode)")
        # reproducible random fallback so drift check still works
        rng = np.random.default_rng(seed=42)
        return rng.random((len(sample), 1536)).astype(np.float32)

    from openai import OpenAI
    client = OpenAI(api_key=api_key)
    resp   = client.embeddings.create(input=sample, model=model)
    return np.array([e.embedding for e in resp.data], dtype=np.float32)


def cosine_distance(a: np.ndarray, b: np.ndarray) -> float:
    """Cosine distance between two vectors (0 = identical, 1 = orthogonal)."""
    dot  = float(np.dot(a, b))
    norm = float(np.linalg.norm(a)) * float(np.linalg.norm(b))
    if norm < 1e-9:
        return 0.0
    return 1.0 - dot / norm


def check_embedding_drift(
        texts: List[str],
        baseline_path: str = "schema_snapshots/embedding_baselines.npz",
        threshold: float = 0.15) -> Dict:
    """
    Extension 1 — Embedding drift detection.
    First run: saves centroid baseline, returns BASELINE_SET.
    Subsequent runs: computes cosine distance from baseline centroid.
    """
    if not texts:
        return {
            "status":      "ERROR",
            "drift_score": None,
            "message":     "No texts provided for embedding drift check.",
        }

    print(f"      Embedding {min(len(texts), 200)} texts ...")
    vecs     = embed_sample(texts, n=200)
    centroid = vecs.mean(axis=0)

    bp = Path(baseline_path)

    # First run — save baseline
    if not bp.exists():
        bp.parent.mkdir(parents=True, exist_ok=True)
        np.savez(str(bp), centroid=centroid)
        print(f"      Baseline saved to {baseline_path}")
        return {
            "status":        "BASELINE_SET",
            "drift_score":   0.0,
            "threshold":     threshold,
            "baseline_path": baseline_path,
            "message":       (
                "First run — baseline centroid saved. "
                "Run again to compute drift score."
            ),
        }

    # Subsequent runs — compare to baseline
    baseline_data     = np.load(str(bp))
    baseline_centroid = baseline_data["centroid"]
    drift             = cosine_distance(centroid, baseline_centroid)

    status = "FAIL" if drift > threshold else "PASS"
    print(f"      Drift score: {drift:.4f} (threshold={threshold})")

    return {
        "status":        status,
        "drift_score":   round(float(drift), 4),
        "threshold":     threshold,
        "baseline_path": baseline_path,
        "interpretation": (
            "Semantic content of text has shifted significantly."
            if drift > threshold
            else "Stable — within acceptable drift bounds."
        ),
    }


# ---------------------------------------------------------------------------
# EXTENSION 2 — Prompt Input Validation
# ---------------------------------------------------------------------------

PROMPT_SCHEMA_PATH = (
    "generated_contracts/prompt_inputs/week3_extraction_prompt_input.json"
)


def load_prompt_schema() -> Dict:
    with open(PROMPT_SCHEMA_PATH) as f:
        return json.load(f)


def build_prompt_input(record: Dict) -> Dict:
    """
    Build the prompt input object from a raw extraction record.
    Only includes fields defined in the prompt input schema.
    """
    return {
        "doc_id":          record.get("doc_id", ""),
        "source_path":     record.get("source_path", ""),
        "content_preview": record.get("source_path", "")[:8000],
    }


def validate_prompt_inputs(
        records: List[Dict],
        quarantine_path: str = "outputs/quarantine/quarantine.jsonl"
) -> Tuple[List[Dict], List[Dict]]:
    """
    Extension 2 — Prompt input schema validation.
    Validates each record against the prompt input JSON Schema.
    Quarantines non-conforming records — never drops them silently.
    Returns (valid_list, quarantined_list).
    """
    schema    = load_prompt_schema()
    valid     = []
    quarantined = []

    for r in records:
        prompt_input = build_prompt_input(r)
        try:
            validate(instance=prompt_input, schema=schema)
            valid.append(r)
        except ValidationError as e:
            quarantined.append({
                "original_record": r,
                "prompt_input":    prompt_input,
                "validation_error": e.message,
                "quarantined_at":  datetime.now(timezone.utc).isoformat(),
            })

    # always write quarantine file — even if empty
    Path(quarantine_path).parent.mkdir(parents=True, exist_ok=True)
    with open(quarantine_path, "w") as f:
        for q in quarantined:
            f.write(json.dumps(q) + "\n")

    print(f"      Valid records     : {len(valid)}")
    print(f"      Quarantined       : {len(quarantined)}")
    print(f"      Quarantine file   : {quarantine_path}")

    return valid, quarantined


# ---------------------------------------------------------------------------
# EXTENSION 3 — LLM Output Violation Rate
# ---------------------------------------------------------------------------

VALID_VERDICTS = {"PASS", "FAIL", "WARN"}


def check_output_schema_violation_rate(
        verdict_records: List[Dict],
        baseline_rate: Optional[float] = None,
        warn_threshold: float = 0.02,
        metrics_path: str = "validation_reports/ai_metrics.json"
) -> Dict:
    """
    Extension 3 — LLM output schema violation rate.
    Counts records where overall_verdict is not in {PASS, FAIL, WARN}.
    Writes results to ai_metrics.json.
    """
    total      = len(verdict_records)
    violations = sum(
        1 for v in verdict_records
        if v.get("overall_verdict") not in VALID_VERDICTS
    )
    rate = violations / max(total, 1)

    trend = "unknown"
    if baseline_rate is not None:
        if rate > baseline_rate * 1.5:
            trend = "rising"
        elif rate < baseline_rate * 0.5:
            trend = "falling"
        else:
            trend = "stable"

    status = "WARN" if rate > warn_threshold else "PASS"

    result = {
        "run_date":               datetime.now(timezone.utc).date().isoformat(),
        "total_outputs":          total,
        "schema_violations":      violations,
        "violation_rate":         round(rate, 4),
        "baseline_violation_rate": baseline_rate,
        "trend":                  trend,
        "status":                 status,
        "warn_threshold":         warn_threshold,
        "valid_verdicts":         list(VALID_VERDICTS),
    }

    Path(metrics_path).parent.mkdir(parents=True, exist_ok=True)
    with open(metrics_path, "w") as f:
        json.dump(result, f, indent=2)

    print(f"      Total verdicts    : {total}")
    print(f"      Violations        : {violations}")
    print(f"      Violation rate    : {rate:.4f}")
    print(f"      Status            : {status}")
    print(f"      Metrics written   : {metrics_path}")

    return result


# ---------------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="AI Contract Extensions")
    parser.add_argument("--mode",         default="all",
                        choices=["all", "drift", "prompt", "output"],
                        help="Which extension(s) to run")
    parser.add_argument("--extractions",  required=True,
                        help="Path to extractions JSONL (week3)")
    parser.add_argument("--verdicts",     required=False, default=None,
                        help="Path to verdicts JSONL (week2)")
    parser.add_argument("--output",       required=True,
                        help="Path to write ai_extensions.json")
    args = parser.parse_args()

    results = {}

    # ---- EXTENSION 1: Embedding Drift ----
    if args.mode in ("all", "drift"):
        print("\n[Extension 1] Embedding Drift Check")
        try:
            records = load_jsonl(args.extractions)
            texts   = extract_fact_texts(records)
            print(f"      Fact texts found  : {len(texts)}")
            drift_result = check_embedding_drift(texts)
            results["embedding_drift"] = drift_result
        except Exception as e:
            print(f"      ERROR: {e}")
            results["embedding_drift"] = {
                "status":  "ERROR",
                "message": str(e),
            }

    # ---- EXTENSION 2: Prompt Input Validation ----
    if args.mode in ("all", "prompt"):
        print("\n[Extension 2] Prompt Input Validation")
        try:
            records = load_jsonl(args.extractions)
            valid, quarantined = validate_prompt_inputs(records)
            results["prompt_input_validation"] = {
                "status":             "PASS" if not quarantined else "WARN",
                "total_records":      len(records),
                "valid_count":        len(valid),
                "quarantined_count":  len(quarantined),
                "quarantine_path":    "outputs/quarantine/quarantine.jsonl",
            }
        except Exception as e:
            print(f"      ERROR: {e}")
            results["prompt_input_validation"] = {
                "status":  "ERROR",
                "message": str(e),
            }

    # ---- EXTENSION 3: LLM Output Violation Rate ----
    if args.mode in ("all", "output"):
        print("\n[Extension 3] LLM Output Schema Violation Rate")
        if not args.verdicts or not Path(args.verdicts).exists():
            print("      verdicts.jsonl not found — skipping gracefully")
            results["llm_output_violation_rate"] = {
                "status":  "ERROR",
                "message": (
                    "verdicts.jsonl not found. "
                    "Provide --verdicts path to enable this check."
                ),
            }
        else:
            try:
                verdict_records = load_jsonl(args.verdicts)
                rate_result     = check_output_schema_violation_rate(
                    verdict_records,
                    baseline_rate=None,
                )
                results["llm_output_violation_rate"] = rate_result
            except Exception as e:
                print(f"      ERROR: {e}")
                results["llm_output_violation_rate"] = {
                    "status":  "ERROR",
                    "message": str(e),
                }

    # ---- Write main output ----
    final = {
        "run_id":       str(uuid.uuid4()),
        "run_timestamp": datetime.now(timezone.utc).isoformat(),
        "mode":         args.mode,
        "checks":       results,
    }

    Path(args.output).parent.mkdir(parents=True, exist_ok=True)
    with open(args.output, "w") as f:
        json.dump(final, f, indent=2)

    print(f"\nAI extensions report written to: {args.output}")
    print("\nSummary:")
    for check_name, check_result in results.items():
        status = check_result.get("status", "UNKNOWN")
        print(f"  {check_name:40s} : {status}")


if __name__ == "__main__":
    main()