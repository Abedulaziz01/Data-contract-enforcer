# Data Contract Enforcer

Data Contract Enforcer is a practical reliability project for detecting, explaining, and reporting data breakages across a multi-stage pipeline. It generates machine-readable contracts from JSONL datasets, validates clean and broken runs against those contracts, attributes failures through lineage, evaluates schema evolution, adds AI-specific quality checks, and summarizes everything in a stakeholder-friendly report and Streamlit dashboard.

## Why This Project Exists

Modern data and AI pipelines often fail silently. A job can still run, dashboards can still refresh, and models can still produce output even when the underlying data has changed in a dangerous way.

This project turns those hidden assumptions into explicit checks:

- Contract generation defines what valid data looks like.
- Validation verifies whether current data still matches that contract.
- Drift baselines capture what normal numeric behavior looks like.
- Attribution helps trace failures to likely upstream sources.
- Schema evolution compares snapshots to classify safe vs. breaking change.
- AI extensions add checks for prompt inputs, structured outputs, and embedding drift.
- Final reporting translates technical findings into clear operational guidance.

## Core Capabilities

- Generate Bitol-style data contracts from real JSONL data.
- Generate dbt-style companion YAML for downstream testing workflows.
- Validate datasets against required, pattern, enum, range, and drift checks.
- Establish numeric baselines for later comparison.
- Inject a canonical Week 3 confidence-scale violation for testing.
- Attribute failures using lineage and git history where available.
- Compare schema snapshots over time.
- Run AI-focused checks on prompt inputs, LLM outputs, and semantic drift.
- Produce a final enforcer report with health score, top violations, and recommendations.
- Present the full workflow in a Streamlit demo app.

## Repository Layout

```text
contracts/
  __init__.py
  ai_extensions.py
  attributor.py
  generator.py
  models.py
  report_generator.py
  runner.py
  schema_analyzer.py
generated_contracts/
enforcer_report/
outputs/
  traces/
  week1/
  week2/
  week3/
  week4/
  week5/
schema_snapshots/
tests/
validation_reports/
violation_log/
app.py
create_violation.py
README.md
DESIGN.md
```

## Tech Stack

- Python 3.11+
- Pandas and NumPy
- JSON Schema and YAML tooling
- GitPython
- OpenAI-compatible clients for embeddings
- Streamlit for the demo UI

## Setup

Create and activate a virtual environment:

```powershell
py -3.13 -m venv .venv
.\.venv\Scripts\Activate.ps1
```

Install the project:

```powershell
python -m ensurepip --upgrade
python -m pip install --upgrade pip
python -m pip install -e .
```

Run tests if needed:

```powershell
python -m pytest -v
```

## Included Data

The repository already includes synthetic project datasets:

- [`outputs/week1/intent_records.jsonl`](/c:/Users/user/Desktop/mll/week7/Data-contract-enforcer/outputs/week1/intent_records.jsonl)
- [`outputs/week2/verdicts.jsonl`](/c:/Users/user/Desktop/mll/week7/Data-contract-enforcer/outputs/week2/verdicts.jsonl)
- [`outputs/week3/extractions.jsonl`](/c:/Users/user/Desktop/mll/week7/Data-contract-enforcer/outputs/week3/extractions.jsonl)
- [`outputs/week4/lineage_snapshots.jsonl`](/c:/Users/user/Desktop/mll/week7/Data-contract-enforcer/outputs/week4/lineage_snapshots.jsonl)
- [`outputs/week5/events.jsonl`](/c:/Users/user/Desktop/mll/week7/Data-contract-enforcer/outputs/week5/events.jsonl)
- [`outputs/traces/runs.jsonl`](/c:/Users/user/Desktop/mll/week7/Data-contract-enforcer/outputs/traces/runs.jsonl)

## End-to-End Workflow

### 1. Generate the Week 3 contract

```powershell
python -m contracts.generator --source outputs/week3/extractions.jsonl --contract-id week3-document-refinery-extractions --lineage outputs/week4/lineage_snapshots.jsonl --output generated_contracts/
```

Key output:

- [`generated_contracts/week3_extractions.yaml`](/c:/Users/user/Desktop/mll/week7/Data-contract-enforcer/generated_contracts/week3_extractions.yaml)

Important rule:

- `fact_confidence` must remain a numeric value between `0.0` and `1.0`

### 2. Validate clean Week 3 data

```powershell
python -m contracts.runner --contract generated_contracts/week3_extractions.yaml --data outputs/week3/extractions.jsonl --output validation_reports/clean_run.json
```

Key outputs:

- [`validation_reports/clean_run.json`](/c:/Users/user/Desktop/mll/week7/Data-contract-enforcer/validation_reports/clean_run.json)
- [`schema_snapshots/baselines.json`](/c:/Users/user/Desktop/mll/week7/Data-contract-enforcer/schema_snapshots/baselines.json)

Expected result:

- clean data passes all checks
- numeric baselines are established for drift detection

### 3. Inject the canonical confidence-scale violation

```powershell
python create_violation.py
```

Key output:

- [`outputs/week3/extractions_violated.jsonl`](/c:/Users/user/Desktop/mll/week7/Data-contract-enforcer/outputs/week3/extractions_violated.jsonl)

What changes:

- `fact_confidence` values are transformed from `0.0-1.0` to `0-100`

### 4. Validate the violated dataset

```powershell
python -m contracts.runner --contract generated_contracts/week3_extractions.yaml --data outputs/week3/extractions_violated.jsonl --output validation_reports/injected_violation.json
```

Key output:

- [`validation_reports/injected_violation.json`](/c:/Users/user/Desktop/mll/week7/Data-contract-enforcer/validation_reports/injected_violation.json)

Expected result:

- `fact_confidence.range` fails
- `fact_confidence.drift` fails

### 5. Attribute the failure

```powershell
python -m contracts.attributor --violation validation_reports/injected_violation.json --lineage outputs/week4/lineage_snapshots.jsonl --contract generated_contracts/week3_extractions.yaml --output violation_log/violations.jsonl
```

Key output:

- [`violation_log/violations.jsonl`](/c:/Users/user/Desktop/mll/week7/Data-contract-enforcer/violation_log/violations.jsonl)

Expected result:

- failed checks are converted into violation records
- downstream impact is summarized through blast radius metadata

### 6. Run schema evolution analysis

```powershell
python -m contracts.schema_analyzer --contract-id week3-document-refinery-extractions --snapshots schema_snapshots/week3-document-refinery-extractions --output validation_reports/schema_evolution.json
```

Key output:

- [`validation_reports/schema_evolution.json`](/c:/Users/user/Desktop/mll/week7/Data-contract-enforcer/validation_reports/schema_evolution.json)

### 7. Run AI extensions

```powershell
python -m contracts.ai_extensions --mode all --extractions outputs/week3/extractions.jsonl --verdicts outputs/week2/verdicts.jsonl --output validation_reports/ai_extensions.json
```

Key outputs:

- [`validation_reports/ai_extensions.json`](/c:/Users/user/Desktop/mll/week7/Data-contract-enforcer/validation_reports/ai_extensions.json)
- [`validation_reports/ai_metrics.json`](/c:/Users/user/Desktop/mll/week7/Data-contract-enforcer/validation_reports/ai_metrics.json)

### 8. Generate the final enforcer report

```powershell
python -m contracts.report_generator
```

Key output:

- [`enforcer_report/report_data.json`](/c:/Users/user/Desktop/mll/week7/Data-contract-enforcer/enforcer_report/report_data.json)

## Streamlit Demo

Run the dashboard:

```powershell
streamlit run app.py
```

The app walks through:

- Overview metrics
- Clean contract generation
- Clean validation baseline
- Injected breaking change
- Failed validation
- Attribution
- Schema evolution
- AI extensions
- Final enforcer report
- Safe demo scenarios

## Current Highlights

Based on the current sample outputs in this repository:

- Health Score: `70/100`
- Violations: `2`
- Recommendations: `3`
- Clean validation run: `30 passed`, `0 failed`
- Violated validation run: `28 passed`, `2 failed`
- Main breakage: `fact_confidence` scaled from `0.0-1.0` to `0-100`

## Notes

- Run package modules from the repository root using `python -m contracts.<module>`.
- On PowerShell, use one-line commands or backticks for multiline commands. Bash-style trailing `\` will not work.
- Attribution may fall back to placeholder blame entries when lineage points to paths not present in local git history.
- The schema evolution view can show `0` changes even when validation fails, because data values may change without the contract snapshot changing.

## Documentation

- [`README.md`](/c:/Users/user/Desktop/mll/week7/Data-contract-enforcer/README.md)
- [`DESIGN.md`](/c:/Users/user/Desktop/mll/week7/Data-contract-enforcer/DESIGN.md)

## Contact

Built and maintained by `abduvaio`.

Let's connect and make the project better.
