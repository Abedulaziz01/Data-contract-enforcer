"""
AI Contract Extensions - checks that standard contracts do not cover.

Extension 1: Embedding drift detection
Extension 2: Prompt input schema validation
Extension 3: LLM output schema violation rate

Usage:
    python -m contracts.ai_extensions \
        --mode all \
        --extractions outputs/week3/extractions.jsonl \
        --verdicts outputs/week2/verdicts.jsonl \
        --output validation_reports/ai_extensions.json
"""
import argparse
import hashlib
import json
import os
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import numpy as np
from dotenv import load_dotenv
from jsonschema import ValidationError, validate

load_dotenv()


def load_jsonl(path: str) -> List[Dict]:
    records = []
    with open(path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                records.append(json.loads(line))
    return records


def extract_fact_texts(records: List[Dict]) -> List[str]:
    """Pull extracted_facts[*].text from all records."""
    texts = []
    for record in records:
        for fact in record.get("extracted_facts", []):
            text = fact.get("text", "")
            if text:
                texts.append(text)
    return texts


def deterministic_demo_embeddings(texts: List[str], dimension: int = 1536) -> np.ndarray:
    """
    Build deterministic local vectors from input text so demo mode still
    produces repeatable but text-dependent drift results.
    """
    vectors = []
    for text in texts:
        seed = int(hashlib.sha256(text.encode("utf-8")).hexdigest()[:16], 16)
        rng = np.random.default_rng(seed)
        vectors.append(rng.random(dimension, dtype=np.float32))
    return np.array(vectors, dtype=np.float32)


def embed_sample(
    texts: List[str],
    n: int = 200,
    model: str = "openai/text-embedding-3-small",
) -> np.ndarray:
    """
    Embed up to n texts using OpenRouter or OpenAI embeddings when available.
    If only GROQ_API_KEY is configured, fall back to deterministic local
    vectors because this implementation does not call a Groq embeddings API.
    """
    openrouter_api_key = os.getenv("OPENROUTER_API_KEY", "")
    openai_api_key = os.getenv("OPENAI_API_KEY", "")
    groq_api_key = os.getenv("GROQ_API_KEY", "")
    sample = texts[:n] if len(texts) > n else texts

    if openrouter_api_key:
        from openai import OpenAI

        openrouter_model = os.getenv("OPENROUTER_EMBEDDING_MODEL", model)
        client = OpenAI(
            api_key=openrouter_api_key,
            base_url="https://openrouter.ai/api/v1",
        )
        response = client.embeddings.create(input=sample, model=openrouter_model)
        return np.array([item.embedding for item in response.data], dtype=np.float32)

    if openai_api_key and openai_api_key != "sk-PLACEHOLDER":
        from openai import OpenAI

        client = OpenAI(api_key=openai_api_key)
        response = client.embeddings.create(input=sample, model=model)
        return np.array([item.embedding for item in response.data], dtype=np.float32)

    if groq_api_key:
        print("      GROQ_API_KEY detected - using deterministic local vectors (demo mode)")
        return deterministic_demo_embeddings(sample)

    print("      No embeddings API key configured - using deterministic local vectors (demo mode)")
    return deterministic_demo_embeddings(sample)


def cosine_distance(a: np.ndarray, b: np.ndarray) -> float:
    """Cosine distance between two vectors (0 = identical, 1 = orthogonal)."""
    dot = float(np.dot(a, b))
    norm = float(np.linalg.norm(a)) * float(np.linalg.norm(b))
    if norm < 1e-9:
        return 0.0
    return 1.0 - dot / norm


def check_embedding_drift(
    texts: List[str],
    baseline_path: str = "schema_snapshots/embedding_baselines.npz",
    threshold: float = 0.15,
) -> Dict:
    """
    Extension 1 - Embedding drift detection.
    First run saves a centroid baseline, later runs compare against it.
    """
    if not texts:
        return {
            "status": "ERROR",
            "drift_score": None,
            "message": "No texts provided for embedding drift check.",
        }

    print(f"      Embedding {min(len(texts), 200)} texts ...")
    vectors = embed_sample(texts, n=200)
    centroid = vectors.mean(axis=0)

    baseline_file = Path(baseline_path)
    if not baseline_file.exists():
        baseline_file.parent.mkdir(parents=True, exist_ok=True)
        np.savez(str(baseline_file), centroid=centroid)
        print(f"      Baseline saved to {baseline_path}")
        return {
            "status": "BASELINE_SET",
            "drift_score": 0.0,
            "threshold": threshold,
            "baseline_path": baseline_path,
            "message": "First run - baseline centroid saved. Run again to compute drift score.",
        }

    baseline_data = np.load(str(baseline_file))
    baseline_centroid = baseline_data["centroid"]
    drift = cosine_distance(centroid, baseline_centroid)

    status = "FAIL" if drift > threshold else "PASS"
    print(f"      Drift score: {drift:.4f} (threshold={threshold})")

    return {
        "status": status,
        "drift_score": round(float(drift), 4),
        "threshold": threshold,
        "baseline_path": baseline_path,
        "interpretation": (
            "Semantic content of text has shifted significantly."
            if drift > threshold
            else "Stable - within acceptable drift bounds."
        ),
    }


PROMPT_SCHEMA_PATH = "generated_contracts/prompt_inputs/week3_extraction_prompt_input.json"


def load_prompt_schema() -> Dict:
    with open(PROMPT_SCHEMA_PATH, encoding="utf-8") as f:
        return json.load(f)


def build_prompt_input(record: Dict) -> Dict:
    """
    Build the prompt input object from a raw extraction record.
    """
    return {
        "doc_id": record.get("doc_id", ""),
        "source_path": record.get("source_path", ""),
        "content_preview": record.get("source_path", "")[:8000],
    }


def validate_prompt_inputs(
    records: List[Dict],
    quarantine_path: str = "outputs/quarantine/quarantine.jsonl",
) -> Tuple[List[Dict], List[Dict]]:
    """
    Extension 2 - Prompt input schema validation.
    Validates each record against the prompt input JSON Schema and
    quarantines non-conforming records.
    """
    schema = load_prompt_schema()
    valid = []
    quarantined = []

    for record in records:
        prompt_input = build_prompt_input(record)
        try:
            validate(instance=prompt_input, schema=schema)
            valid.append(record)
        except ValidationError as exc:
            quarantined.append(
                {
                    "original_record": record,
                    "prompt_input": prompt_input,
                    "validation_error": exc.message,
                    "quarantined_at": datetime.now(timezone.utc).isoformat(),
                }
            )

    quarantine_file = Path(quarantine_path)
    quarantine_file.parent.mkdir(parents=True, exist_ok=True)
    with open(quarantine_file, "w", encoding="utf-8") as f:
        for item in quarantined:
            f.write(json.dumps(item) + "\n")

    print(f"      Valid records     : {len(valid)}")
    print(f"      Quarantined       : {len(quarantined)}")
    print(f"      Quarantine file   : {quarantine_path}")

    return valid, quarantined


VALID_VERDICTS = {"PASS", "FAIL", "WARN"}


def check_output_schema_violation_rate(
    verdict_records: List[Dict],
    baseline_rate: Optional[float] = None,
    warn_threshold: float = 0.02,
    metrics_path: str = "validation_reports/ai_metrics.json",
) -> Dict:
    """
    Extension 3 - LLM output schema violation rate.
    Counts records where overall_verdict is not in {PASS, FAIL, WARN}.
    """
    total = len(verdict_records)
    violations = sum(
        1 for record in verdict_records if record.get("overall_verdict") not in VALID_VERDICTS
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
        "run_date": datetime.now(timezone.utc).date().isoformat(),
        "total_outputs": total,
        "schema_violations": violations,
        "violation_rate": round(rate, 4),
        "baseline_violation_rate": baseline_rate,
        "trend": trend,
        "status": status,
        "warn_threshold": warn_threshold,
        "valid_verdicts": list(VALID_VERDICTS),
    }

    metrics_file = Path(metrics_path)
    metrics_file.parent.mkdir(parents=True, exist_ok=True)
    with open(metrics_file, "w", encoding="utf-8") as f:
        json.dump(result, f, indent=2)

    print(f"      Total verdicts    : {total}")
    print(f"      Violations        : {violations}")
    print(f"      Violation rate    : {rate:.4f}")
    print(f"      Status            : {status}")
    print(f"      Metrics written   : {metrics_path}")

    return result


def main():
    parser = argparse.ArgumentParser(description="AI Contract Extensions")
    parser.add_argument(
        "--mode",
        default="all",
        choices=["all", "drift", "prompt", "output"],
        help="Which extension(s) to run",
    )
    parser.add_argument("--extractions", required=True, help="Path to extractions JSONL (week3)")
    parser.add_argument("--verdicts", required=False, default=None, help="Path to verdicts JSONL (week2)")
    parser.add_argument("--output", required=True, help="Path to write ai_extensions.json")
    args = parser.parse_args()

    results = {}

    if args.mode in ("all", "drift"):
        print("\n[Extension 1] Embedding Drift Check")
        try:
            records = load_jsonl(args.extractions)
            texts = extract_fact_texts(records)
            print(f"      Fact texts found  : {len(texts)}")
            results["embedding_drift"] = check_embedding_drift(texts)
        except Exception as exc:
            print(f"      ERROR: {exc}")
            results["embedding_drift"] = {"status": "ERROR", "message": str(exc)}

    if args.mode in ("all", "prompt"):
        print("\n[Extension 2] Prompt Input Validation")
        try:
            records = load_jsonl(args.extractions)
            valid, quarantined = validate_prompt_inputs(records)
            results["prompt_input_validation"] = {
                "status": "PASS" if not quarantined else "WARN",
                "total_records": len(records),
                "valid_count": len(valid),
                "quarantined_count": len(quarantined),
                "quarantine_path": "outputs/quarantine/quarantine.jsonl",
            }
        except Exception as exc:
            print(f"      ERROR: {exc}")
            results["prompt_input_validation"] = {"status": "ERROR", "message": str(exc)}

    if args.mode in ("all", "output"):
        print("\n[Extension 3] LLM Output Schema Violation Rate")
        if not args.verdicts or not Path(args.verdicts).exists():
            print("      verdicts.jsonl not found - skipping gracefully")
            results["llm_output_violation_rate"] = {
                "status": "ERROR",
                "message": "verdicts.jsonl not found. Provide --verdicts path to enable this check.",
            }
        else:
            try:
                verdict_records = load_jsonl(args.verdicts)
                results["llm_output_violation_rate"] = check_output_schema_violation_rate(
                    verdict_records,
                    baseline_rate=None,
                )
            except Exception as exc:
                print(f"      ERROR: {exc}")
                results["llm_output_violation_rate"] = {"status": "ERROR", "message": str(exc)}

    final = {
        "run_id": str(uuid.uuid4()),
        "run_timestamp": datetime.now(timezone.utc).isoformat(),
        "mode": args.mode,
        "checks": results,
    }

    output_file = Path(args.output)
    output_file.parent.mkdir(parents=True, exist_ok=True)
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(final, f, indent=2)

    print(f"\nAI extensions report written to: {args.output}")
    print("\nSummary:")
    for check_name, check_result in results.items():
        status = check_result.get("status", "UNKNOWN")
        print(f"  {check_name:40s} : {status}")


if __name__ == "__main__":
    main()
