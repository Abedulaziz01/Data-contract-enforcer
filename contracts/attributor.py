
"""
ViolationAttributor — traces a validation failure back to the upstream
commit that caused it using the Week 4 lineage graph and git log.

Usage:
    python contracts/attributor.py \
        --violation validation_reports/injected_violation.json \
        --lineage outputs/week4/lineage_snapshots.jsonl \
        --contract generated_contracts/week3_extractions.yaml \
        --output violation_log/violations.jsonl
"""
import argparse
import json
import os
import subprocess
import uuid
from collections import deque
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import yaml
from dotenv import load_dotenv

load_dotenv()


def load_registry(registry_path: str = "contract_registry/subscriptions.yaml") -> List[Dict]:
    path = Path(registry_path)
    if not path.exists():
        return []
    with open(path, encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}
    if isinstance(data, dict):
        return data.get("subscriptions", [])
    if isinstance(data, list):
        return data
    return []


def get_registry_subscribers(contract_id: str, registry_entries: List[Dict]) -> List[Dict]:
    return [entry for entry in registry_entries if entry.get("contract_id") == contract_id]


# ---------------------------------------------------------------------------
# STEP 1 — Lineage traversal
# ---------------------------------------------------------------------------

def find_upstream_files(failing_column: str,
                        lineage_snapshot: Dict) -> List[Dict]:
    """
    BFS from the failing schema element through the lineage graph.
    Stops at the first external boundary.
    Returns all FILE type nodes that produce the failing column.
    """
    nodes = {n["node_id"]: n for n in lineage_snapshot.get("nodes", [])}
    edges = lineage_snapshot.get("edges", [])

    # build adjacency: target -> list of sources (upstream direction)
    upstream_map: Dict[str, List[str]] = {}
    for edge in edges:
        src = edge.get("source", "")
        tgt = edge.get("target", "")
        if tgt not in upstream_map:
            upstream_map[tgt] = []
        upstream_map[tgt].append(src)

    # seed BFS — start from any node whose id contains the column's system
    col_system = failing_column.split(".")[0]  # e.g. 'week3'
    seeds = [
        nid for nid in nodes
        if col_system in nid.lower()
    ]

    visited = set()
    queue   = deque(seeds)
    file_nodes = []
    distances: Dict[str, int] = {seed: 0 for seed in seeds}

    while queue:
        current = queue.popleft()
        if current in visited:
            continue
        visited.add(current)

        node = nodes.get(current, {})
        if node.get("type") == "FILE":
            file_nodes.append({**node, "lineage_hops": distances.get(current, 1)})

        # traverse upstream
        for upstream_id in upstream_map.get(current, []):
            if upstream_id not in visited:
                distances[upstream_id] = distances.get(current, 0) + 1
                queue.append(upstream_id)

    # fallback — if nothing found, return all FILE nodes
    if not file_nodes:
        file_nodes = [
            {**n, "lineage_hops": 1}
            for n in nodes.values()
            if n.get("type") == "FILE"
        ]

    return file_nodes


# ---------------------------------------------------------------------------
# STEP 2 — Git blame
# ---------------------------------------------------------------------------

def get_recent_commits(file_path: str,
                       repo_root: str,
                       days: int = 14) -> List[Dict]:
    """
    Run git log on the file from the correct repo root.
    Returns list of commit dicts.
    """
    cmd = [
        "git", "log",
        "--follow",
        f"--since={days} days ago",
        "--format=%H|%ae|%ai|%s",
        "--",
        file_path,
    ]
    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            cwd=repo_root,   # CRITICAL — must point to repo root
            timeout=15,
        )
        commits = []
        for line in result.stdout.strip().split("\n"):
            if "|" in line:
                parts = line.split("|", 3)
                if len(parts) == 4:
                    hash_, author, ts, msg = parts
                    commits.append({
                        "commit_hash":      hash_.strip(),
                        "author":           author.strip(),
                        "commit_timestamp": ts.strip(),
                        "commit_message":   msg.strip(),
                    })
        return commits
    except Exception as e:
        print(f"      git log failed for {file_path}: {e}")
        return []


# ---------------------------------------------------------------------------
# STEP 3 — Confidence scoring
# ---------------------------------------------------------------------------

def score_candidates(commits: List[Dict],
                     violation_timestamp: str,
                     lineage_distance: int) -> List[Dict]:
    """
    Score each commit candidate.
    Formula: base = 1.0 - (days_since_commit x 0.1) - (lineage_distance x 0.2)
    Clamp to 0-1. Return top 5 sorted descending.
    Never return fewer than 1 candidate.
    """
    try:
        v_time = datetime.fromisoformat(
            violation_timestamp.replace("Z", "+00:00"))
    except Exception:
        v_time = datetime.now(timezone.utc)

    scored = []
    for rank, commit in enumerate(commits[:5], start=1):
        try:
            ts = commit["commit_timestamp"]
            # git outputs timestamps like "2025-01-14 09:00:00 +0000"
            ts_clean = ts.replace(" +", "+").replace(" -", "-")
            if " " in ts_clean:
                parts = ts_clean.rsplit(" ", 1)
                ts_clean = parts[0] + parts[1] if len(parts) == 2 else ts_clean
            c_time = datetime.fromisoformat(ts_clean)
        except Exception:
            c_time = datetime.now(timezone.utc)

        days_diff = abs((v_time - c_time).days)
        score = 1.0 - (days_diff * 0.1) - (lineage_distance * 0.2)
        score = max(0.0, min(1.0, score))

        scored.append({
            "rank":             rank,
            "file_path":        commit.get("file_path", "unknown"),
            "commit_hash":      commit["commit_hash"],
            "author":           commit["author"],
            "commit_timestamp": commit["commit_timestamp"],
            "commit_message":   commit["commit_message"],
            "confidence_score": round(score, 3),
        })

    scored.sort(key=lambda x: x["confidence_score"], reverse=True)

    # re-rank after sort
    for i, s in enumerate(scored, start=1):
        s["rank"] = i

    # NEVER return fewer than 1 — use placeholder if git returned nothing
    if not scored:
        scored = [{
            "rank":             1,
            "file_path":        "unknown — git returned no commits",
            "commit_hash":      "0000000000000000000000000000000000000000",
            "author":           "unknown",
            "commit_timestamp": violation_timestamp,
            "commit_message":   "No recent commits found in git history",
            "confidence_score": 0.0,
        }]

    return scored


# ---------------------------------------------------------------------------
# STEP 4 — Blast radius from contract lineage
# ---------------------------------------------------------------------------

def compute_blast_radius(
    contract: Dict,
    records_failing: int,
    registry_subscribers: List[Dict],
    lineage_distance: int,
) -> Dict:
    """
    Blast radius comes from contract.lineage.downstream[]
    NOT from re-traversing the lineage graph.
    """
    downstream = contract.get("lineage", {}).get("downstream", [])
    affected_nodes = [d.get("id", "") for d in downstream]
    registry_nodes = [entry.get("subscriber_id", "") for entry in registry_subscribers]
    combined_nodes = list(dict.fromkeys([*registry_nodes, *affected_nodes]))
    affected_pipelines = [
        node_id for node_id in combined_nodes
        if "pipeline" in node_id.lower()
        or "cartographer" in node_id.lower()
        or "week4" in node_id.lower()
    ]
    subscriber_details = [
        {
            "subscriber_id": entry.get("subscriber_id", "unknown"),
            "validation_mode": entry.get("validation_mode", "unknown"),
            "contact": entry.get("contact", "unknown"),
            "contamination_depth": 1 + lineage_distance,
        }
        for entry in registry_subscribers
    ]
    return {
        "affected_nodes":     combined_nodes,
        "affected_pipelines": affected_pipelines,
        "estimated_records":  records_failing,
        "subscriber_details": subscriber_details,
        "contamination_depth": 1 + lineage_distance,
    }


# ---------------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="ViolationAttributor")
    parser.add_argument("--violation", required=True,
                        help="Path to validation report JSON")
    parser.add_argument("--lineage",   required=True,
                        help="Path to lineage_snapshots.jsonl")
    parser.add_argument("--contract",  required=True,
                        help="Path to contract YAML")
    parser.add_argument("--output",    required=True,
                        help="Path to violations.jsonl output")
    parser.add_argument("--registry",  default="contract_registry/subscriptions.yaml",
                        help="Path to authored subscription registry YAML")
    args = parser.parse_args()

    # load inputs
    with open(args.violation) as f:
        report = json.load(f)

    with open(args.lineage) as f:
        lines = [l.strip() for l in f if l.strip()]
    lineage_snapshot = json.loads(lines[-1])

    with open(args.contract) as f:
        contract = yaml.safe_load(f)

    registry_entries = load_registry(args.registry)
    registry_subscribers = get_registry_subscribers(
        contract.get("id", "unknown"),
        registry_entries,
    )

    # repo roots from .env
    week3_repo = os.getenv("WEEK3_REPO_PATH", os.getcwd())
    week4_repo = os.getenv("WEEK4_REPO_PATH", os.getcwd())

    # find FAIL results
    failures = [r for r in report.get("results", [])
                if r["status"] == "FAIL"]

    if not failures:
        print("No FAIL results found in violation report.")
        print("Nothing to attribute.")
        return

    print(f"Found {len(failures)} FAIL result(s) to attribute.")

    Path(args.output).parent.mkdir(parents=True, exist_ok=True)

    violation_records = []

    for failure in failures:
        print(f"\nAttributing: {failure['check_id']}")

        # STEP 1 — lineage traversal
        check_id   = failure.get("check_id", "")
        file_nodes = find_upstream_files(check_id, lineage_snapshot)
        print(f"  Upstream FILE nodes found: {len(file_nodes)}")

        # STEP 2 — git blame on each upstream file
        all_commits = []
        for node in file_nodes:
            meta      = node.get("metadata", {})
            file_path = meta.get("path", node.get("node_id", ""))
            repo_root = week3_repo
            lineage_hops = int(node.get("lineage_hops", 1))

            print(f"  Running git log on: {file_path} (cwd={repo_root})")
            commits = get_recent_commits(file_path, repo_root, days=14)
            print(f"  Commits found: {len(commits)}")

            for c in commits:
                c["file_path"] = file_path
                c["lineage_hops"] = lineage_hops
            all_commits.extend(commits)

        # STEP 3 — score candidates
        violation_ts   = report.get("run_timestamp",
                                    datetime.now(timezone.utc).isoformat())
        lineage_distance = min(
            [int(node.get("lineage_hops", 1)) for node in file_nodes],
            default=1,
        )
        scored         = score_candidates(all_commits, violation_ts,
                                          lineage_distance=lineage_distance)

        # STEP 4 — blast radius + write
        records_failing = failure.get("records_failing", 0)
        blast_radius    = compute_blast_radius(
            contract,
            records_failing,
            registry_subscribers,
            lineage_distance,
        )

        violation_record = {
            "violation_id": str(uuid.uuid4()),
            "check_id":     check_id,
            "detected_at":  violation_ts,
            "blame_chain":  scored,
            "blast_radius": blast_radius,
        }
        violation_records.append(violation_record)

        print(f"  Blame chain candidates: {len(scored)}")
        print(f"  Top candidate score   : {scored[0]['confidence_score']}")
        print(f"  Blast radius nodes    : {len(blast_radius['affected_nodes'])}")

    # append to violations.jsonl
    with open(args.output, "a") as f:
        for vr in violation_records:
            f.write(json.dumps(vr) + "\n")

    print(f"\nWritten {len(violation_records)} violation record(s) to {args.output}")


if __name__ == "__main__":
    main()
