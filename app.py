import json
import copy
from pathlib import Path
from typing import Any, Dict, List, Optional

import streamlit as st
import yaml


ROOT = Path(__file__).resolve().parent
DEMO_ROOT = ROOT / "demo_workspace"


def load_json(path: Path) -> Optional[Dict[str, Any]]:
    if not path.exists():
        return None
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def load_yaml(path: Path) -> Optional[Dict[str, Any]]:
    if not path.exists():
        return None
    with open(path, encoding="utf-8") as f:
        return yaml.safe_load(f)


def load_jsonl(path: Path) -> List[Dict[str, Any]]:
    if not path.exists():
        return []
    items: List[Dict[str, Any]] = []
    with open(path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                items.append(json.loads(line))
    return items


def read_text(path: Path) -> str:
    if not path.exists():
        return ""
    return path.read_text(encoding="utf-8")


def write_jsonl(path: Path, records: List[Dict[str, Any]]):
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        for record in records:
            f.write(json.dumps(record) + "\n")


def metric_card(label: str, value: Any, help_text: str = ""):
    st.metric(label, value, help=help_text or None)


def status_badge(status: str):
    status = status.upper()
    color = {
        "PASS": "#1f7a4d",
        "FAIL": "#b42318",
        "WARN": "#b54708",
        "ERROR": "#7a271a",
        "BASELINE_SET": "#155eef",
        "COMPATIBLE": "#1f7a4d",
        "BREAKING": "#b42318",
    }.get(status, "#475467")
    st.markdown(
        f"""
        <div style="display:inline-block;padding:0.25rem 0.6rem;border-radius:999px;
        background:{color};color:white;font-size:0.8rem;font-weight:600;">
        {status}
        </div>
        """,
        unsafe_allow_html=True,
    )


def show_code_file(path: Path, language: str):
    if path.exists():
        st.code(read_text(path), language=language)
    else:
        st.warning(f"Missing file: {path}")


def show_json_file(path: Path):
    payload = load_json(path)
    if payload is None:
        st.warning(f"Missing file: {path}")
    else:
        st.json(payload)


def demo_talk_track(stage_key: str, changed_text: str, scenario_text: str):
    col1, col2 = st.columns(2)
    with col1:
        if st.button("What changed?", key=f"{stage_key}_changed"):
            st.info(changed_text)
    with col2:
        if st.button("Why this test matters", key=f"{stage_key}_scenario"):
            st.success(scenario_text)


def show_yaml_file(path: Path):
    payload = load_yaml(path)
    if payload is None:
        st.warning(f"Missing file: {path}")
    else:
        st.code(yaml.safe_dump(payload, sort_keys=False), language="yaml")


def latest_migration_report() -> Optional[Path]:
    candidates = sorted((ROOT / "validation_reports").glob("migration_impact_*.json"))
    return candidates[-1] if candidates else None


def find_fact_confidence_clause(contract: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    if not contract:
        return None
    return contract.get("schema", {}).get("fact_confidence")


def scenario_missing_doc_id(records: List[Dict[str, Any]]) -> str:
    records[0]["doc_id"] = None
    return "Set the first record's doc_id to null."


def scenario_invalid_entity_type(records: List[Dict[str, Any]]) -> str:
    records[0]["entities"][0]["type"] = "INSTITUTION"
    return "Changed the first entity type from a valid enum to INSTITUTION."


def scenario_bad_timestamp(records: List[Dict[str, Any]]) -> str:
    records[0]["extracted_at"] = "not-a-date"
    return "Changed extracted_at on the first record to an invalid timestamp."


def scenario_confidence_scale(records: List[Dict[str, Any]]) -> str:
    for record in records:
        for fact in record.get("extracted_facts", []):
            fact["confidence"] = round(fact["confidence"] * 100, 1)
    return "Scaled every fact confidence from 0.0-1.0 to 0-100."


DEMO_SCENARIOS = {
    "Missing required doc_id": {
        "slug": "missing_doc_id",
        "mutator": scenario_missing_doc_id,
        "why": "Shows a required-field failure without changing the rest of the dataset.",
    },
    "Invalid entity enum": {
        "slug": "invalid_entity_enum",
        "mutator": scenario_invalid_entity_type,
        "why": "Shows how enum validation catches values outside the allowed entity types.",
    },
    "Bad extracted_at timestamp": {
        "slug": "bad_timestamp",
        "mutator": scenario_bad_timestamp,
        "why": "Shows date-time format enforcement using a single isolated bad value.",
    },
    "Confidence scale break": {
        "slug": "confidence_scale_break",
        "mutator": scenario_confidence_scale,
        "why": "Shows the strongest breaking-change demo: range failure plus statistical drift.",
    },
}


def run_demo_scenario(label: str) -> Dict[str, Any]:
    from contracts.runner import run_validation

    scenario = DEMO_SCENARIOS[label]
    records = copy.deepcopy(load_jsonl(ROOT / "outputs" / "week3" / "extractions.jsonl"))
    description = scenario["mutator"](records)

    data_path = DEMO_ROOT / "outputs" / f"{scenario['slug']}.jsonl"
    report_path = DEMO_ROOT / "validation_reports" / f"{scenario['slug']}.json"
    write_jsonl(data_path, records)
    report = run_validation(
        str(ROOT / "generated_contracts" / "week3_extractions.yaml"),
        str(data_path),
        str(report_path),
    )

    return {
        "label": label,
        "description": description,
        "why": scenario["why"],
        "data_path": data_path,
        "report_path": report_path,
        "report": report,
        "sample_record": records[0],
    }


def build_app():
    st.set_page_config(
        page_title="Data Contract Enforcer Demo",
        page_icon="DCE",
        layout="wide",
        initial_sidebar_state="expanded",
    )

    st.markdown(
        """
        <style>
        .block-container {padding-top: 1.5rem; padding-bottom: 2rem; max-width: 1280px;}
        .hero {
            padding: 1.25rem 1.4rem;
            border-radius: 20px;
            background: linear-gradient(135deg, #0f172a 0%, #1d4ed8 55%, #93c5fd 100%);
            color: white;
            margin-bottom: 1.2rem;
            box-shadow: 0 18px 45px rgba(15, 23, 42, 0.22);
        }
        .hero h1 {margin: 0 0 0.45rem 0; font-size: 2.3rem;}
        .hero p {margin: 0; font-size: 1rem; opacity: 0.95;}
        .section-note {
            padding: 0.9rem 1rem;
            border-radius: 14px;
            background: #f8fafc;
            border: 1px solid #e2e8f0;
            margin-bottom: 1rem;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )

    report = load_json(ROOT / "enforcer_report" / "report_data.json") or {}
    clean_report = load_json(ROOT / "validation_reports" / "clean_run.json") or {}
    violated_report = load_json(ROOT / "validation_reports" / "injected_violation.json") or {}
    schema_report = load_json(ROOT / "validation_reports" / "schema_evolution.json") or {}
    ai_report = load_json(ROOT / "validation_reports" / "ai_extensions.json") or {}
    ai_metrics = load_json(ROOT / "validation_reports" / "ai_metrics.json") or {}
    baselines = load_json(ROOT / "schema_snapshots" / "baselines.json") or {}
    week3_contract = load_yaml(ROOT / "generated_contracts" / "week3_extractions.yaml") or {}
    week5_contract = load_yaml(ROOT / "generated_contracts" / "week5_records.yaml") or {}
    violations = load_jsonl(ROOT / "violation_log" / "violations.jsonl")
    migration_report_path = latest_migration_report()
    migration_report = load_json(migration_report_path) if migration_report_path else None

    st.markdown(
        """
        <div class="hero">
            <h1>Data Contract Enforcer</h1>
            <p>Interactive demo of Week 7 contract generation, validation, attribution,
            schema evolution, AI risk checks, and the final Enforcer Report.</p>
        </div>
        """,
        unsafe_allow_html=True,
    )

    summary_cols = st.columns(5)
    with summary_cols[0]:
        metric_card("Health Score", report.get("data_health_score", "N/A"))
    with summary_cols[1]:
        metric_card("Violations", report.get("violation_count", len(violations)))
    with summary_cols[2]:
        metric_card("Schema Changes", len(report.get("schema_changes", [])))
    with summary_cols[3]:
        metric_card("AI Checks", len(ai_report.get("checks", {})))
    with summary_cols[4]:
        metric_card("Recommendations", len(report.get("recommendations", [])))

    st.sidebar.title("Demo Flow")
    stage = st.sidebar.radio(
        "Jump to stage",
        [
            "Overview",
            "1. Clean Contract Generation",
            "2. Clean Validation Baseline",
            "3. Injected Breaking Change",
            "4. Failed Validation",
            "5. Attribution",
            "6. Schema Evolution",
            "7. AI Extensions",
            "8. Enforcer Report",
            "9. Safe Demo Scenarios",
        ],
    )

    st.sidebar.markdown("### Run locally")
    st.sidebar.code("streamlit run app.py", language="bash")
    st.sidebar.markdown("### Key outputs")
    st.sidebar.caption("The dashboard reads from generated YAML/JSON/JSONL files already present in this repo.")

    if stage == "Overview":
        st.subheader("End-to-End Story")
        st.markdown(
            """
            <div class="section-note">
            This demo follows one complete reliability story: generate a contract from clean Week 3 data,
            validate the baseline, inject a breaking confidence-scale change, catch it with both range and drift
            checks, trace impact through attribution, inspect schema evolution, review AI-specific risk checks,
            and end on the Enforcer Report that summarizes what happened.
            </div>
            """,
            unsafe_allow_html=True,
        )
        stages = [
            ("Contract generation", "Turn Week 3 data into a machine-checkable contract and dbt schema."),
            ("Baseline validation", "Establish passing checks and numeric drift baselines."),
            ("Violation injection", "Change confidence from 0.0-1.0 to 0-100."),
            ("Failure detection", "Catch both direct range failure and statistical drift."),
            ("Attribution", "Write blame-chain candidates and blast radius."),
            ("Schema evolution", "Show whether the contract changed between snapshots."),
            ("AI extensions", "Track embedding drift, prompt validation, and LLM output schema rate."),
            ("Final report", "Summarize health score, top violation, and recommendations."),
        ]
        for title, desc in stages:
            st.markdown(f"- **{title}**: {desc}")

    elif stage == "1. Clean Contract Generation":
        st.subheader("Clean Contract Generation")
        demo_talk_track(
            "contract_generation",
            "We turn raw Week 3 extraction data into a formal contract. The important change here is that assumptions in code become explicit rules in YAML.",
            "This stage matters because we cannot validate or protect data unless we first define what the data is supposed to look like.",
        )
        st.code(
            "python -m contracts.generator --source outputs/week3/extractions.jsonl "
            "--contract-id week3-document-refinery-extractions "
            "--lineage outputs/week4/lineage_snapshots.jsonl --output generated_contracts/",
            language="bash",
        )
        clause = find_fact_confidence_clause(week3_contract)
        info_cols = st.columns([1.1, 1.4])
        with info_cols[0]:
            st.markdown("**Week 3 contract highlights**")
            if clause:
                st.write("`fact_confidence` range rule")
                st.json(clause)
            else:
                st.warning("`fact_confidence` clause not found.")
            st.markdown("**Generated outputs**")
            st.write("- `generated_contracts/week3_extractions.yaml`")
            st.write("- `generated_contracts/week3_extractions_dbt.yml`")
            latest_snapshot = sorted((ROOT / "schema_snapshots" / "week3-document-refinery-extractions").glob("*.yaml"))
            if latest_snapshot:
                st.write(f"- Latest snapshot: `{latest_snapshot[-1].name}`")
        with info_cols[1]:
            st.markdown("**Contract YAML**")
            show_yaml_file(ROOT / "generated_contracts" / "week3_extractions.yaml")

    elif stage == "2. Clean Validation Baseline":
        st.subheader("Clean Validation Baseline")
        demo_talk_track(
            "clean_validation",
            "The clean dataset is tested against the generated contract and everything should pass. This also writes numeric baselines for future drift checks.",
            "This stage matters because it gives us a trusted healthy starting point. Later failures only make sense when we have a baseline to compare against.",
        )
        st.code(
            "python -m contracts.runner --contract generated_contracts/week3_extractions.yaml "
            "--data outputs/week3/extractions.jsonl --output validation_reports/clean_run.json",
            language="bash",
        )
        cols = st.columns(4)
        with cols[0]:
            metric_card("Total Checks", clean_report.get("total_checks", "N/A"))
        with cols[1]:
            metric_card("Passed", clean_report.get("passed", "N/A"))
        with cols[2]:
            metric_card("Failed", clean_report.get("failed", "N/A"))
        with cols[3]:
            metric_card("Errored", clean_report.get("errored", "N/A"))
        st.markdown("**Baseline file established**")
        if baselines:
            st.json(baselines)
        else:
            st.warning("No baselines file found.")
        with st.expander("Open clean_run.json", expanded=False):
            show_json_file(ROOT / "validation_reports" / "clean_run.json")

    elif stage == "3. Injected Breaking Change":
        st.subheader("Injected Breaking Change")
        demo_talk_track(
            "injected_change",
            "We intentionally change confidence values from 0.0-1.0 into 0-100. That simulates a realistic upstream breaking change.",
            "This stage matters because a strong demo should prove the system can catch a real bad change, not just show that healthy data passes.",
        )
        st.code("python create_violation.py", language="bash")
        st.info("This step changes Week 3 `confidence` from 0.0-1.0 to 0-100.")
        violated_records = load_jsonl(ROOT / "outputs" / "week3" / "extractions_violated.jsonl")
        if violated_records:
            first = violated_records[0]
            confidences = [fact["confidence"] for fact in first.get("extracted_facts", [])]
            cols = st.columns(2)
            with cols[0]:
                metric_card("Violated records", len(violated_records))
            with cols[1]:
                metric_card("Sample confidence max", max(confidences) if confidences else "N/A")
            st.write("Sample confidence values from first violated record:")
            st.write(confidences)
        else:
            st.warning("No violated extraction file found.")

    elif stage == "4. Failed Validation":
        st.subheader("Failed Validation")
        demo_talk_track(
            "failed_validation",
            "The validator now catches the broken confidence field in two ways: the direct range rule fails, and the statistical drift rule also fails.",
            "This stage matters because it shows the system can catch both obvious contract violations and deeper behavioral shifts in the data.",
        )
        st.code(
            "python -m contracts.runner --contract generated_contracts/week3_extractions.yaml "
            "--data outputs/week3/extractions_violated.jsonl --output validation_reports/injected_violation.json",
            language="bash",
        )
        cols = st.columns(4)
        with cols[0]:
            metric_card("Total Checks", violated_report.get("total_checks", "N/A"))
        with cols[1]:
            metric_card("Passed", violated_report.get("passed", "N/A"))
        with cols[2]:
            metric_card("Failed", violated_report.get("failed", "N/A"))
        with cols[3]:
            metric_card("Warned", violated_report.get("warned", "N/A"))
        failures = [r for r in violated_report.get("results", []) if r.get("status") == "FAIL"]
        st.markdown("**Key failing checks**")
        if failures:
            for failure in failures:
                st.markdown(f"**{failure['check_id']}**")
                status_badge(failure["status"])
                st.write(f"Severity: `{failure['severity']}`")
                st.write(f"Actual: `{failure['actual_value']}`")
                st.write(f"Expected: `{failure['expected']}`")
                st.write(failure["message"])
                st.divider()
        else:
            st.info("No FAIL results found.")
        with st.expander("Open injected_violation.json", expanded=False):
            show_json_file(ROOT / "validation_reports" / "injected_violation.json")

    elif stage == "5. Attribution":
        st.subheader("Attribution")
        demo_talk_track(
            "attribution",
            "After detecting the failure, we try to trace where it likely came from and which downstream systems are affected.",
            "This stage matters because detection alone is not enough in production. Teams need to know where to investigate and what else might break.",
        )
        st.code(
            "python -m contracts.attributor --violation validation_reports/injected_violation.json "
            "--lineage outputs/week4/lineage_snapshots.jsonl "
            "--contract generated_contracts/week3_extractions.yaml "
            "--output violation_log/violations.jsonl",
            language="bash",
        )
        if not violations:
            st.warning("No violation log records found.")
        else:
            metric_cols = st.columns(3)
            with metric_cols[0]:
                metric_card("Violation Records", len(violations))
            with metric_cols[1]:
                metric_card("Blast Radius Nodes", len(violations[0].get("blast_radius", {}).get("affected_nodes", [])))
            with metric_cols[2]:
                metric_card("Top Candidate Score", violations[0]["blame_chain"][0].get("confidence_score", "N/A"))

            first_violation = violations[0]
            st.markdown(f"**Check ID**: `{first_violation.get('check_id')}`")
            st.markdown("**Blame chain**")
            st.json(first_violation.get("blame_chain", []))
            st.markdown("**Blast radius**")
            st.json(first_violation.get("blast_radius", {}))
            st.caption(
                "In the current synthetic setup, the blame chain falls back to a placeholder candidate "
                "because the lineage file references source paths with no local git history."
            )

    elif stage == "6. Schema Evolution":
        st.subheader("Schema Evolution")
        demo_talk_track(
            "schema_evolution",
            "This compares snapshots over time and tells us whether a contract change is compatible or breaking.",
            "This stage matters because not every change should trigger the same response. Safe changes can move forward, but breaking changes need migration planning.",
        )
        st.code(
            "python -m contracts.schema_analyzer --contract-id week3-document-refinery-extractions "
            "--output validation_reports/schema_evolution.json",
            language="bash",
        )
        cols = st.columns(3)
        with cols[0]:
            metric_card("Total Changes", schema_report.get("total_changes", "N/A"))
        with cols[1]:
            metric_card("Breaking", schema_report.get("breaking_changes", "N/A"))
        with cols[2]:
            metric_card("Compatible", schema_report.get("compatible_changes", "N/A"))
        st.markdown("**Current diff report**")
        show_json_file(ROOT / "validation_reports" / "schema_evolution.json")
        st.markdown("**Breaking vs compatible**")
        st.write(
            "Breaking changes require coordination and a migration plan. Compatible changes can usually be introduced "
            "without forcing all consumers to change immediately."
        )
        if migration_report:
            st.markdown("**Latest migration impact report**")
            st.json(migration_report)
        else:
            st.info("No migration impact report is present yet. The current schema diff report shows no breaking changes.")

    elif stage == "7. AI Extensions":
        st.subheader("AI Extensions")
        demo_talk_track(
            "ai_extensions",
            "This adds AI-specific checks: semantic drift, prompt input validation, and structured LLM output quality.",
            "This stage matters because normal schema checks are not enough for AI workflows. The AI layer can fail even when regular fields still look fine.",
        )
        st.code(
            "python -m contracts.ai_extensions --mode all --extractions outputs/week3/extractions.jsonl "
            "--verdicts outputs/week2/verdicts.jsonl --output validation_reports/ai_extensions.json",
            language="bash",
        )
        checks = ai_report.get("checks", {})
        drift = checks.get("embedding_drift", {})
        prompt = checks.get("prompt_input_validation", {})
        output = checks.get("llm_output_violation_rate", {})

        cols = st.columns(3)
        with cols[0]:
            metric_card("Embedding Drift", drift.get("drift_score", "N/A"))
            status_badge(drift.get("status", "UNKNOWN"))
        with cols[1]:
            metric_card("Prompt Quarantined", prompt.get("quarantined_count", "N/A"))
            status_badge(prompt.get("status", "UNKNOWN"))
        with cols[2]:
            metric_card("Output Violation Rate", output.get("violation_rate", ai_metrics.get("violation_rate", "N/A")))
            status_badge(output.get("status", "UNKNOWN"))

        sub_tabs = st.tabs(["Embedding Drift", "Prompt Validation", "LLM Output Rate"])
        with sub_tabs[0]:
            st.json(drift)
        with sub_tabs[1]:
            st.json(prompt)
        with sub_tabs[2]:
            st.json(output if output else ai_metrics)

    elif stage == "8. Enforcer Report":
        st.subheader("Enforcer Report")
        demo_talk_track(
            "enforcer_report",
            "All the technical outputs are summarized into one human-readable report with a health score, top violation, and recommended actions.",
            "This stage matters because stakeholders usually do not want raw JSON. They want a clear summary of what happened and what to do next.",
        )
        st.code("python contracts/report_generator.py", language="bash")
        cols = st.columns(3)
        with cols[0]:
            metric_card("Health Score", report.get("data_health_score", "N/A"))
        with cols[1]:
            metric_card("Violation Count", report.get("violation_count", "N/A"))
        with cols[2]:
            metric_card("Recommendations", len(report.get("recommendations", [])))

        st.markdown("**Health narrative**")
        st.write(report.get("health_narrative", "No health narrative found."))

        left, right = st.columns([1.15, 1])
        with left:
            st.markdown("**Top violation**")
            top_violations = report.get("top_violations", [])
            if top_violations:
                st.error(top_violations[0])
            else:
                st.info("No top violation found.")
            st.markdown("**Recommendations**")
            for idx, recommendation in enumerate(report.get("recommendations", []), start=1):
                st.write(f"{idx}. {recommendation}")
        with right:
            st.markdown("**Report JSON**")
            show_json_file(ROOT / "enforcer_report" / "report_data.json")

    elif stage == "9. Safe Demo Scenarios":
        st.subheader("Safe Demo Scenarios")
        st.markdown(
            """
            <div class="section-note">
            These buttons create isolated test cases under <code>demo_workspace/</code>.
            Your main <code>outputs/</code>, generated contracts, and committed validation files stay untouched.
            </div>
            """,
            unsafe_allow_html=True,
        )
        selected_label = st.selectbox("Choose a safe scenario", list(DEMO_SCENARIOS.keys()))
        st.write(f"**Why use it**: {DEMO_SCENARIOS[selected_label]['why']}")

        if st.button("Run selected safe scenario", key="run_safe_scenario"):
            result = run_demo_scenario(selected_label)
            st.session_state["latest_demo_scenario"] = result

        scenario_result = st.session_state.get("latest_demo_scenario")
        if scenario_result:
            st.markdown(f"### {scenario_result['label']}")
            st.info(scenario_result["description"])
            cols = st.columns(4)
            with cols[0]:
                metric_card("Total Checks", scenario_result["report"].get("total_checks", "N/A"))
            with cols[1]:
                metric_card("Passed", scenario_result["report"].get("passed", "N/A"))
            with cols[2]:
                metric_card("Failed", scenario_result["report"].get("failed", "N/A"))
            with cols[3]:
                metric_card("Errored", scenario_result["report"].get("errored", "N/A"))

            failures = [r for r in scenario_result["report"].get("results", []) if r.get("status") == "FAIL"]
            if failures:
                st.markdown("**Observed effect**")
                for failure in failures:
                    st.markdown(f"- `{failure['check_id']}` -> `{failure['message']}`")
            else:
                st.success("This scenario did not produce FAIL results.")

            exp1, exp2, exp3 = st.expander("Modified sample record"), st.expander("Scenario validation report"), st.expander("Generated files")
            with exp1:
                st.json(scenario_result["sample_record"])
            with exp2:
                st.json(scenario_result["report"])
            with exp3:
                st.write(f"Data file: `{scenario_result['data_path']}`")
                st.write(f"Report file: `{scenario_result['report_path']}`")


if __name__ == "__main__":
    build_app()
