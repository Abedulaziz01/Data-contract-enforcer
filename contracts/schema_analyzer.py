"""
SchemaEvolutionAnalyzer - diffs consecutive schema snapshots and
classifies detected changes using a compatibility taxonomy.

Usage:
    python -m contracts.schema_analyzer \
        --contract-id week3-document-refinery-extractions \
        --output validation_reports/schema_evolution.json
"""
import argparse
import json
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import yaml


def classify_change(
    field_name: str,
    old_clause: Optional[Dict],
    new_clause: Optional[Dict],
) -> Tuple[str, str, str]:
    """
    Classify a schema change.
    Returns (compatibility, severity, reason).
    """
    if old_clause is None and new_clause is not None:
        if new_clause.get("required", False):
            return (
                "BREAKING",
                "HIGH",
                f"Add non-nullable column '{field_name}' - coordinate with all producers",
            )
        return (
            "COMPATIBLE",
            "LOW",
            f"Add nullable column '{field_name}' - downstream consumers can ignore",
        )

    if old_clause is not None and new_clause is None:
        return (
            "BREAKING",
            "HIGH",
            f"Remove column '{field_name}' - 2-sprint deprecation period mandatory",
        )

    old_type = old_clause.get("type")
    new_type = new_clause.get("type")

    if old_type != new_type:
        widening_pairs = {
            ("integer", "number"),
            ("number", "number"),
        }
        if (old_type, new_type) in widening_pairs:
            return (
                "COMPATIBLE",
                "LOW",
                f"Type widening {old_type} -> {new_type} - validate no precision loss",
            )
        if (
            old_type == "number"
            and new_type == "integer"
            and old_clause.get("minimum") == 0.0
            and old_clause.get("maximum") == 1.0
            and new_clause.get("minimum") == 0
            and new_clause.get("maximum") == 100
        ):
            return (
                "BREAKING",
                "CRITICAL",
                "Narrow type and scale change number 0.0-1.0 -> integer 0-100 - immediate rollback recommended",
            )
        return (
            "BREAKING",
            "HIGH",
            f"Type change {old_type} -> {new_type} - migration plan + rollback mandatory",
        )

    old_max = old_clause.get("maximum")
    new_max = new_clause.get("maximum")
    old_min = old_clause.get("minimum")
    new_min = new_clause.get("minimum")

    if old_max != new_max and old_max is not None:
        return ("BREAKING", "CRITICAL", f"Range change: maximum {old_max} -> {new_max}")

    if old_min != new_min and old_min is not None:
        return ("BREAKING", "CRITICAL", f"Range change: minimum {old_min} -> {new_min}")

    old_enum = set(old_clause.get("enum", []))
    new_enum = set(new_clause.get("enum", []))
    if old_enum != new_enum:
        removed = old_enum - new_enum
        added = new_enum - old_enum
        if removed:
            return (
                "BREAKING",
                "HIGH",
                f"Enum values removed: {sorted(removed)} - treat as breaking change",
            )
        if added:
            return (
                "COMPATIBLE",
                "LOW",
                f"Enum values added: {sorted(added)} - notify all consumers",
            )

    old_req = old_clause.get("required", False)
    new_req = new_clause.get("required", False)
    if not old_req and new_req:
        return (
            "BREAKING",
            "HIGH",
            f"Column '{field_name}' changed to required - coordinate with all producers",
        )

    if old_clause.get("format") != new_clause.get("format"):
        return (
            "BREAKING",
            "HIGH",
            f"Format changed: {old_clause.get('format')} -> {new_clause.get('format')}",
        )

    return ("COMPATIBLE", "LOW", "No material change")


def load_snapshots(contract_id: str, since: Optional[str] = None) -> List[Dict]:
    """
    Load all YAML snapshots for a contract, sorted by filename.
    """
    snapshot_dir = Path("schema_snapshots") / contract_id
    if not snapshot_dir.exists():
        print(f"ERROR: Snapshot directory not found: {snapshot_dir}")
        sys.exit(1)

    yaml_files = sorted(snapshot_dir.glob("*.yaml"))
    if len(yaml_files) < 2:
        print(
            "Need at least 2 snapshots. Run the generator twice -\n"
            "once on clean data, once on modified data."
        )
        sys.exit(1)

    snapshots = []
    for yaml_file in yaml_files:
        if since and yaml_file.stem < since:
            continue
        with open(yaml_file, encoding="utf-8") as f:
            data = yaml.safe_load(f)
        snapshots.append({"filename": yaml_file.name, "schema": data})
    if len(snapshots) < 2:
        print(
            "Need at least 2 snapshots after applying filters. "
            "Adjust --since or generate another snapshot."
        )
        sys.exit(1)
    return snapshots


def detect_rename_candidates(old_schema: Dict, new_schema: Dict) -> List[Dict]:
    """
    Heuristic rename detection: pair removed and added fields with matching
    core clause shape so the diff can explain a rename explicitly.
    """
    removed_fields = [field for field in old_schema.keys() if field not in new_schema]
    added_fields = [field for field in new_schema.keys() if field not in old_schema]
    candidates = []

    for old_field in removed_fields:
        old_clause = old_schema[old_field]
        for new_field in added_fields:
            new_clause = new_schema[new_field]
            if (
                old_clause.get("type") == new_clause.get("type")
                and old_clause.get("required") == new_clause.get("required")
                and old_clause.get("format") == new_clause.get("format")
            ):
                candidates.append(
                    {
                        "field": f"{old_field} -> {new_field}",
                        "compatibility": "BREAKING",
                        "severity": "HIGH",
                        "reason": f"Rename field '{old_field}' -> '{new_field}' - downstream consumers must update references",
                        "old_clause": old_clause,
                        "new_clause": new_clause,
                    }
                )
                break

    return candidates


def build_migration_checklist(field_name: str, reason: str, contract: Dict) -> List[str]:
    """Build an ordered migration checklist with at least 4 steps."""
    downstream = contract.get("lineage", {}).get("downstream", [])
    consumer_list = ", ".join(d.get("id", "unknown") for d in downstream)
    if not consumer_list:
        consumer_list = "all downstream consumers"

    return [
        f"1. FREEZE: Stop all deployments that touch '{field_name}' until migration is complete.",
        f"2. NOTIFY: Alert downstream consumers ({consumer_list}) of the breaking change: {reason}",
        f"3. MIGRATE: Update the producer to output '{field_name}' in the new format. Add an alias field for backward compatibility.",
        "4. VALIDATE: Run ValidationRunner on migrated data. Confirm 0 CRITICAL failures before promoting to production.",
        "5. MONITOR: Re-establish statistical baselines after migration. Watch for drift in the 7 days following deployment.",
        "6. CLEANUP: Remove alias field after all consumers have updated. Document removal in DOMAIN_NOTES.md.",
    ]


def build_rollback_plan(field_name: str, contract_id: str) -> List[str]:
    """Build a rollback plan."""
    return [
        f"1. Revert the producer commit that changed '{field_name}'.",
        "2. Re-run ContractGenerator on reverted data to restore snapshot.",
        "3. Re-run ValidationRunner - confirm all checks PASS.",
        "4. Notify downstream consumers that rollback is complete.",
    ]


def generate_migration_impact(
    contract_id: str,
    breaking_changes: List[Dict],
    old_snapshot: Dict,
    new_snapshot: Dict,
    contract: Dict,
) -> str:
    """
    Write migration_impact_{contract_id}_{timestamp}.json and return its path.
    """
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    filename = f"migration_impact_{contract_id}_{ts}.json"
    out_path = Path("validation_reports") / filename

    downstream = contract.get("lineage", {}).get("downstream", [])

    consumer_failure_modes = []
    for downstream_consumer in downstream:
        consumer_failure_modes.append(
            {
                "consumer": downstream_consumer.get("id", "unknown"),
                "failure_mode": (
                    "Will receive out-of-range values. Any logic assuming 0.0-1.0 "
                    "will produce incorrect results."
                ),
                "fields_at_risk": downstream_consumer.get("fields_consumed", []),
            }
        )

    primary_field = breaking_changes[0]["field"] if breaking_changes else "unknown"
    primary_reason = breaking_changes[0]["reason"] if breaking_changes else "unknown"

    report = {
        "migration_id": str(uuid.uuid4()),
        "contract_id": contract_id,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "snapshot_old": old_snapshot["filename"],
        "snapshot_new": new_snapshot["filename"],
        "breaking_changes": breaking_changes,
        "compatibility_verdict": "BREAKING - immediate action required",
        "blast_radius": {
            "affected_nodes": [d.get("id", "") for d in downstream],
            "affected_pipelines": [
                d.get("id", "")
                for d in downstream
                if "week4" in d.get("id", "").lower()
                or "pipeline" in d.get("id", "").lower()
            ],
        },
        "consumer_failure_modes": consumer_failure_modes,
        "migration_checklist": build_migration_checklist(primary_field, primary_reason, contract),
        "rollback_plan": build_rollback_plan(primary_field, contract_id),
    }

    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2)

    return str(out_path)


def main():
    parser = argparse.ArgumentParser(description="SchemaEvolutionAnalyzer")
    parser.add_argument("--contract-id", required=True, help="Contract ID to analyze")
    parser.add_argument(
        "--since",
        default=None,
        help="Only consider snapshots whose timestamped filename is >= this value (e.g. 20260401_000000)",
    )
    parser.add_argument("--output", required=True, help="Path to write schema_evolution.json")
    args = parser.parse_args()

    print(f"Loading snapshots for: {args.contract_id}")
    snapshots = load_snapshots(args.contract_id, since=args.since)
    print(f"Found {len(snapshots)} snapshots - diffing last two.")

    old_snapshot = snapshots[-2]
    new_snapshot = snapshots[-1]
    old_schema = old_snapshot["schema"].get("schema", {})
    new_schema = new_snapshot["schema"].get("schema", {})

    contract_yaml = Path("generated_contracts") / (
        args.contract_id.split("-")[0] + "_" + args.contract_id.split("-")[-1] + ".yaml"
    )
    contract = {}
    if contract_yaml.exists():
        with open(contract_yaml, encoding="utf-8") as f:
            contract = yaml.safe_load(f)

    print(f"Old snapshot : {old_snapshot['filename']}")
    print(f"New snapshot : {new_snapshot['filename']}")

    all_fields = set(old_schema.keys()) | set(new_schema.keys())

    changes = detect_rename_candidates(old_schema, new_schema)
    breaking_changes = []

    for candidate in changes:
        if candidate["compatibility"] == "BREAKING":
            breaking_changes.append(candidate)

    for field in sorted(all_fields):
        old_clause = old_schema.get(field)
        new_clause = new_schema.get(field)

        if old_clause == new_clause:
            continue

        compatibility, severity, reason = classify_change(field, old_clause, new_clause)
        change_record = {
            "field": field,
            "compatibility": compatibility,
            "severity": severity,
            "reason": reason,
            "old_clause": old_clause,
            "new_clause": new_clause,
        }
        changes.append(change_record)

        if compatibility == "BREAKING":
            breaking_changes.append(change_record)
            symbol = "[BREAKING]"
        else:
            symbol = "[COMPATIBLE]"

        print(f"  {symbol}: {field} - {reason}")

    evolution_report = {
        "report_id": str(uuid.uuid4()),
        "contract_id": args.contract_id,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "snapshot_old": old_snapshot["filename"],
        "snapshot_new": new_snapshot["filename"],
        "total_changes": len(changes),
        "breaking_changes": len(breaking_changes),
        "compatible_changes": len(changes) - len(breaking_changes),
        "changes": changes,
    }

    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(evolution_report, f, indent=2)
    print(f"\nEvolution report written to: {args.output}")

    if breaking_changes:
        impact_path = generate_migration_impact(
            args.contract_id,
            breaking_changes,
            old_snapshot,
            new_snapshot,
            contract,
        )
        print(f"Migration impact report  : {impact_path}")

    print("\nSummary:")
    print(f"  Total changes    : {len(changes)}")
    print(f"  Breaking changes : {len(breaking_changes)}")
    print(f"  Compatible       : {len(changes) - len(breaking_changes)}")


if __name__ == "__main__":
    main()
