
"""
ContractGenerator — reads JSONL outputs and produces Bitol YAML contracts.

Usage:
    python contracts/generator.py \
        --source outputs/week3/extractions.jsonl \
        --contract-id week3-document-refinery-extractions \
        --lineage outputs/week4/lineage_snapshots.jsonl \
        --output generated_contracts/
"""
import argparse
import hashlib
import json
import os
import shutil
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import anthropic
import pandas as pd
import yaml
from dotenv import load_dotenv

load_dotenv()

from contracts.models import ColumnProfile, ContractClause

# ---------------------------------------------------------------------------
# STAGE 1 — Load and flatten
# ---------------------------------------------------------------------------

def load_jsonl(path: str) -> List[Dict]:
    """Read a JSONL file into a list of dicts."""
    records = []
    with open(path) as f:
        for line in f:
            line = line.strip()
            if line:
                records.append(json.loads(line))
    return records


def flatten_for_profile(records: List[Dict]) -> pd.DataFrame:
    """
    Flatten nested JSONL to a flat DataFrame for profiling.
    Explodes extracted_facts[] to one row per fact, prefixing keys with fact_.
    Explodes entities[] to one row per entity, prefixing keys with entity_.
    If neither array exists, keeps the record as-is.
    """
    rows = []
    for r in records:
        base = {k: v for k, v in r.items()
                if not isinstance(v, (list, dict))}

        facts = r.get("extracted_facts", [])
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

    df = pd.DataFrame(rows)

    # Warn if fact_confidence is not float64
    if "fact_confidence" in df.columns:
        if str(df["fact_confidence"].dtype) != "float64":
            print(
                f"WARNING: fact_confidence dtype is "
                f"'{df['fact_confidence'].dtype}', expected float64. "
                f"Data may have mixed types — document as contract violation."
            )

    return df


# ---------------------------------------------------------------------------
# STAGE 2 — Per-column profiling
# ---------------------------------------------------------------------------

def profile_column(series: pd.Series, col_name: str) -> ColumnProfile:
    """Build a ColumnProfile for one column."""
    normalized = series.dropna().map(
        lambda v: json.dumps(v, sort_keys=True)
        if isinstance(v, (list, dict))
        else v
    )

    profile = ColumnProfile(
        name=col_name,
        dtype=str(series.dtype),
        null_fraction=float(series.isna().mean()),
        cardinality_estimate=int(normalized.nunique()),
        sample_values=[str(v) for v in normalized.unique()[:5]],
    )
    if pd.api.types.is_numeric_dtype(series):
        profile.stats = {
            "min":    float(series.min()),
            "max":    float(series.max()),
            "mean":   float(series.mean()),
            "p25":    float(series.quantile(0.25)),
            "p50":    float(series.quantile(0.50)),
            "p75":    float(series.quantile(0.75)),
            "p95":    float(series.quantile(0.95)),
            "p99":    float(series.quantile(0.99)),
            "stddev": float(series.std()),
        }
    return profile


# ---------------------------------------------------------------------------
# STAGE 3 — Bitol clause generation
# ---------------------------------------------------------------------------

def infer_type(dtype_str: str) -> str:
    mapping = {
        "float64": "number",
        "float32": "number",
        "int64":   "integer",
        "int32":   "integer",
        "bool":    "boolean",
        "object":  "string",
    }
    return mapping.get(dtype_str, "string")


def column_to_clause(profile: ColumnProfile) -> Dict[str, Any]:
    """
    Map a ColumnProfile to a Bitol contract clause dict.
    Rules applied in order — all that match are applied.
    """
    clause: Dict[str, Any] = {
        "type":     infer_type(profile.dtype),
        "required": profile.null_fraction == 0.0,
    }

    # confidence fields must stay 0.0-1.0
    if "confidence" in profile.name and clause["type"] == "number":
        clause["minimum"]     = 0.0
        clause["maximum"]     = 1.0
        clause["description"] = (
            "Confidence score. Must remain 0.0-1.0 float. "
            "BREAKING if changed to 0-100 integer scale."
        )

    # low-cardinality string columns → enum
    if (profile.cardinality_estimate <= 10
            and profile.dtype == "object"
            and len(profile.sample_values) >= profile.cardinality_estimate):
        clause["enum"] = profile.sample_values

    # _id suffix → uuid format
    if profile.name.endswith("_id"):
        clause["format"]  = "uuid"
        clause["pattern"] = "^[0-9a-f-]{36}$"

    # _at suffix → date-time format
    if profile.name.endswith("_at"):
        clause["format"] = "date-time"

    # numeric range from stats
    if profile.stats and "confidence" not in profile.name:
        clause["minimum"] = profile.stats["min"]
        clause["maximum"] = profile.stats["max"]

    if profile.stats:
        mean_value = profile.stats.get("mean")
        if mean_value is not None and (mean_value > 0.99 or mean_value < 0.01):
            clause["warning"] = (
                f"Suspicious distribution detected: mean={mean_value:.4f}. "
                "Review producer logic and downstream assumptions."
            )

    return clause


def write_baselines(
    df: pd.DataFrame,
    baselines_path: str = "schema_snapshots/baselines.json",
) -> Dict[str, Any]:
    """
    Persist numeric baselines from the generation stage so later validation
    and demos have a stable statistical reference.
    """
    baselines_file = Path(baselines_path)
    baselines_file.parent.mkdir(parents=True, exist_ok=True)

    columns: Dict[str, Dict[str, float]] = {}
    for col in df.select_dtypes(include="number").columns:
        numeric = pd.to_numeric(df[col], errors="coerce").dropna()
        if numeric.empty:
            continue
        columns[col] = {
            "mean": float(numeric.mean()),
            "stddev": float(numeric.std()),
        }

    baseline_payload = {
        "written_at": datetime.now(timezone.utc).isoformat(),
        "columns": columns,
    }
    with open(baselines_file, "w", encoding="utf-8") as f:
        json.dump(baseline_payload, f, indent=2)

    return baseline_payload


# ---------------------------------------------------------------------------
# STAGE 4A — Lineage injection
# ---------------------------------------------------------------------------

def inject_lineage(contract: Dict, lineage_path: str,
                   contract_id: str) -> Dict:
    """
    Read the latest lineage snapshot and find downstream consumers
    of the source system. Inject into contract['lineage'].
    """
    with open(lineage_path) as f:
        lines = [l.strip() for l in f if l.strip()]
    snapshot = json.loads(lines[-1])  # latest snapshot

    source_keyword = contract_id.split("-")[0]  # e.g. 'week3'

    downstream = []
    for edge in snapshot.get("edges", []):
        src = edge.get("source", "")
        tgt = edge.get("target", "")
        if source_keyword in src or source_keyword in tgt:
            downstream.append({
                "id":              tgt,
                "description":     f"Downstream consumer via {edge.get('relationship','UNKNOWN')} edge",
                "fields_consumed": ["doc_id", "extracted_facts"],
                "breaking_if_changed": ["extracted_facts.confidence", "doc_id"],
            })

    contract["lineage"] = {
        "upstream":   [],
        "downstream": downstream,
    }
    return contract


# ---------------------------------------------------------------------------
# STAGE 4B — LLM annotation for ambiguous columns
# ---------------------------------------------------------------------------

AMBIGUOUS_PATTERNS = [
    "hash", "ref", "score", "count", "rate",
    "version", "preview", "excerpt", "canonical",
]


def is_ambiguous(col_name: str) -> bool:
    return any(p in col_name.lower() for p in AMBIGUOUS_PATTERNS)


def annotate_with_llm(col_name: str, table_name: str,
                      sample_values: List[str],
                      adjacent_cols: List[str]) -> Dict:
    """
    Call Claude to annotate ambiguous columns with:
    (a) plain-English description
    (b) business rule as validation expression
    (c) cross-column relationship
    """
    api_key = os.getenv("ANTHROPIC_API_KEY", "")
    if not api_key or api_key.startswith("sk-ant-PLACEHOLDER"):
        return {
            "description":   f"Column {col_name} in {table_name}.",
            "business_rule": "No rule inferred (LLM annotation skipped).",
            "relationships": "None identified.",
        }

    client = anthropic.Anthropic(api_key=api_key)
    prompt = (
        f"You are a data contract specialist.\n"
        f"Table: {table_name}\n"
        f"Column: {col_name}\n"
        f"Sample values: {sample_values}\n"
        f"Adjacent columns: {adjacent_cols}\n\n"
        f"Provide exactly three things as JSON with keys "
        f"'description', 'business_rule', 'relationships':\n"
        f"(a) description: plain-English meaning of this column\n"
        f"(b) business_rule: a validation expression\n"
        f"(c) relationships: any cross-column dependency\n"
        f"Respond with valid JSON only."
    )

    try:
        message = client.messages.create(
            model="claude-sonnet-4-20250514",
            max_tokens=300,
            messages=[{"role": "user", "content": prompt}],
        )
        text = message.content[0].text.strip()
        # strip markdown fences if present
        if text.startswith("```"):
            text = text.split("```")[1]
            if text.startswith("json"):
                text = text[4:]
        return json.loads(text.strip())
    except Exception as e:
        return {
            "description":   f"Column {col_name} in {table_name}.",
            "business_rule": f"Annotation failed: {e}",
            "relationships": "None identified.",
        }


# ---------------------------------------------------------------------------
# STAGE 4C — dbt schema.yml output
# ---------------------------------------------------------------------------

def build_dbt_schema(contract_id: str,
                     schema: Dict[str, Any]) -> Dict:
    """Produce a dbt-compatible schema.yml dict."""
    columns = []
    for col_name, clause in schema.items():
        col: Dict[str, Any] = {"name": col_name, "tests": []}
        if clause.get("required"):
            col["tests"].append("not_null")
        if clause.get("format") == "uuid":
            col["tests"].append("not_null")
        if clause.get("enum"):
            col["tests"].append({
                "accepted_values": {"values": clause["enum"]}
            })
        columns.append(col)

    return {
        "version": 2,
        "models": [{
            "name":        contract_id.replace("-", "_"),
            "description": f"dbt schema tests for {contract_id}",
            "columns":     columns,
        }],
    }


# ---------------------------------------------------------------------------
# MAIN — wire all stages together
# ---------------------------------------------------------------------------

def build_contract(contract_id: str, source_path: str,
                   df: pd.DataFrame,
                   profiles: Dict[str, ColumnProfile]) -> Dict:
    """Assemble the full Bitol contract dict."""
    schema_clauses: Dict[str, Any] = {}
    llm_annotations: Dict[str, Any] = {}
    col_names = list(profiles.keys())

    for col_name, profile in profiles.items():
        clause = column_to_clause(profile)
        schema_clauses[col_name] = clause

        if is_ambiguous(col_name):
            adjacent = [c for c in col_names if c != col_name][:5]
            annotation = annotate_with_llm(
                col_name,
                contract_id,
                profile.sample_values,
                adjacent,
            )
            llm_annotations[col_name] = annotation
            # merge description from LLM if not already set
            if "description" not in clause and annotation.get("description"):
                clause["description"] = annotation["description"]

    contract = {
        "kind":       "DataContract",
        "apiVersion": "v3.0.0",
        "id":         contract_id,
        "info": {
            "title":       f"Contract for {contract_id}",
            "version":     "1.0.0",
            "owner":       "week7-team",
            "description": (
                f"Auto-generated contract from {Path(source_path).name}. "
                f"Generated at {datetime.now(timezone.utc).isoformat()}."
            ),
        },
        "servers": {
            "local": {
                "type":   "local",
                "path":   source_path,
                "format": "jsonl",
            }
        },
        "terms": {
            "usage":       "Internal inter-system data contract. Do not publish.",
            "limitations": "confidence must remain in 0.0-1.0 float range.",
        },
        "schema":          schema_clauses,
        "llm_annotations": llm_annotations,
        "quality": {
            "type": "SodaChecks",
            "specification": {
                "checks": [
                    "row_count >= 1",
                    "missing_count(doc_id) = 0" if "doc_id" in schema_clauses else "row_count >= 1",
                ]
            },
        },
    }
    return contract


def main():
    parser = argparse.ArgumentParser(description="ContractGenerator")
    parser.add_argument("--source",      required=True,
                        help="Path to input JSONL file")
    parser.add_argument("--contract-id", required=True,
                        help="Unique contract identifier")
    parser.add_argument("--lineage",     required=True,
                        help="Path to lineage_snapshots.jsonl")
    parser.add_argument("--output",      required=True,
                        help="Output directory for generated contracts")
    args = parser.parse_args()

    output_dir = Path(args.output)
    output_dir.mkdir(parents=True, exist_ok=True)

    print(f"[1/4] Loading {args.source} ...")
    records = load_jsonl(args.source)
    print(f"      Loaded {len(records)} records.")

    print("[2/4] Flattening and profiling ...")
    df = flatten_for_profile(records)
    print(f"      DataFrame shape: {df.shape}")
    print(f"      Columns: {list(df.columns)}")

    profiles: Dict[str, ColumnProfile] = {}
    for col in df.columns:
        profiles[col] = profile_column(df[col], col)

    baselines = write_baselines(df)
    print(
        "      Baselines written: "
        f"schema_snapshots/baselines.json ({len(baselines['columns'])} numeric columns)"
    )

    print("[3/4] Building Bitol contract ...")
    contract = build_contract(args.contract_id, args.source, df, profiles)

    print("[3/4] Injecting lineage ...")
    contract = inject_lineage(contract, args.lineage, args.contract_id)

    # --- write main YAML ---
    # derive output filename from contract_id
    # week3-document-refinery-extractions -> week3_extractions.yaml
    parts = args.contract_id.split("-")
    short_name = f"{parts[0]}_{parts[-1]}"
    yaml_path = output_dir / f"{short_name}.yaml"

    with open(yaml_path, "w") as f:
        yaml.dump(contract, f, default_flow_style=False,
                  sort_keys=False, allow_unicode=True)
    print(f"      Written: {yaml_path}")

    # --- write dbt yaml ---
    dbt_schema = build_dbt_schema(args.contract_id, contract["schema"])
    dbt_path = output_dir / f"{short_name}_dbt.yml"
    with open(dbt_path, "w") as f:
        yaml.dump(dbt_schema, f, default_flow_style=False,
                  sort_keys=False, allow_unicode=True)
    print(f"      Written: {dbt_path}")

    # --- write timestamped snapshot ---
    snapshot_dir = (Path("schema_snapshots") / args.contract_id)
    snapshot_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    snapshot_path = snapshot_dir / f"{ts}.yaml"
    shutil.copy(yaml_path, snapshot_path)
    print(f"      Snapshot: {snapshot_path}")

    print("[4/4] Done.")
    print(f"\nSummary:")
    print(f"  Contract clauses : {len(contract['schema'])}")
    print(f"  LLM annotations  : {len(contract['llm_annotations'])}")
    print(f"  Downstream nodes  : {len(contract['lineage']['downstream'])}")
    print(f"  Contract YAML     : {yaml_path}")
    print(f"  dbt YAML          : {dbt_path}")
    print(f"  Snapshot          : {snapshot_path}")


if __name__ == "__main__":
    main()
