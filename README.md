# Data Contract Enforcer

Week 7 implementation of a data contract enforcement workflow for synthetic Week 1-5 platform outputs. The project currently covers contract generation, validation, injected violation detection, lineage-based attribution, and baseline tests for the implemented phases.

## What This Repo Does

- Generates Bitol-style contract YAML from JSONL datasets.
- Generates parallel dbt-style schema test YAML.
- Validates datasets against generated contracts.
- Writes statistical baselines for numeric drift detection.
- Injects the canonical Week 3 confidence-scale violation.
- Attributes detected failures using the Week 4 lineage snapshot and git history when available.

## Repository Layout

```text
contracts/
  __init__.py
  models.py
  generator.py
  runner.py
  attributor.py
generated_contracts/
validation_reports/
violation_log/
schema_snapshots/
outputs/
  week1/
  week2/
  week3/
  week4/
  week5/
  traces/
tests/
create_violation.py
README.md
```

## Prerequisites

- Python 3.11+ recommended
- PowerShell or a compatible terminal
- A virtual environment

## Environment Setup

Create and activate a local virtual environment:

```powershell
py -3.13 -m venv .venv
.\.venv\Scripts\Activate.ps1
```

Install the project in editable mode:

```powershell
python -m ensurepip --upgrade
python -m pip install --upgrade pip
python -m pip install -e .
```

If you want to run tests:

```powershell
python -m pip install pytest
```

## Input Data

The repo already contains synthetic input data in canonical project folders:

- [`outputs/week1/intent_records.jsonl`](/c:/Users/user/Desktop/mll/week7/Data-contract-enforcer/outputs/week1/intent_records.jsonl)
- [`outputs/week2/verdicts.jsonl`](/c:/Users/user/Desktop/mll/week7/Data-contract-enforcer/outputs/week2/verdicts.jsonl)
- [`outputs/week3/extractions.jsonl`](/c:/Users/user/Desktop/mll/week7/Data-contract-enforcer/outputs/week3/extractions.jsonl)
- [`outputs/week4/lineage_snapshots.jsonl`](/c:/Users/user/Desktop/mll/week7/Data-contract-enforcer/outputs/week4/lineage_snapshots.jsonl)
- [`outputs/week5/events.jsonl`](/c:/Users/user/Desktop/mll/week7/Data-contract-enforcer/outputs/week5/events.jsonl)
- [`outputs/traces/runs.jsonl`](/c:/Users/user/Desktop/mll/week7/Data-contract-enforcer/outputs/traces/runs.jsonl)

## How To Run

### 1. Generate the Week 3 contract

```powershell
python -m contracts.generator --source outputs/week3/extractions.jsonl --contract-id week3-document-refinery-extractions --lineage outputs/week4/lineage_snapshots.jsonl --output generated_contracts/
```

Expected outputs:

- [`generated_contracts/week3_extractions.yaml`](/c:/Users/user/Desktop/mll/week7/Data-contract-enforcer/generated_contracts/week3_extractions.yaml)
- [`generated_contracts/week3_extractions_dbt.yml`](/c:/Users/user/Desktop/mll/week7/Data-contract-enforcer/generated_contracts/week3_extractions_dbt.yml)
- a timestamped snapshot under [`schema_snapshots/week3-document-refinery-extractions`](/c:/Users/user/Desktop/mll/week7/Data-contract-enforcer/schema_snapshots/week3-document-refinery-extractions)

### 2. Generate the Week 5 contract

```powershell
python -m contracts.generator --source outputs/week5/events.jsonl --contract-id week5-event-records --lineage outputs/week4/lineage_snapshots.jsonl --output generated_contracts/
```

Expected outputs:

- [`generated_contracts/week5_records.yaml`](/c:/Users/user/Desktop/mll/week7/Data-contract-enforcer/generated_contracts/week5_records.yaml)
- [`generated_contracts/week5_records_dbt.yml`](/c:/Users/user/Desktop/mll/week7/Data-contract-enforcer/generated_contracts/week5_records_dbt.yml)
- a timestamped snapshot under [`schema_snapshots/week5-event-records`](/c:/Users/user/Desktop/mll/week7/Data-contract-enforcer/schema_snapshots/week5-event-records)

### 3. Run validation on clean Week 3 data

```powershell
python -m contracts.runner --contract generated_contracts/week3_extractions.yaml --data outputs/week3/extractions.jsonl --output validation_reports/thursday_baseline.json
```

Expected output:

- [`validation_reports/thursday_baseline.json`](/c:/Users/user/Desktop/mll/week7/Data-contract-enforcer/validation_reports/thursday_baseline.json)
- [`schema_snapshots/baselines.json`](/c:/Users/user/Desktop/mll/week7/Data-contract-enforcer/schema_snapshots/baselines.json) on first run

Expected behavior:

- the report should show a clean baseline run
- the current sample run produced 27 passed checks, 0 failed, 0 warned, 0 errored

### 4. Inject the canonical Week 3 confidence-scale violation

```powershell
python create_violation.py
```

Expected output:

- [`outputs/week3/extractions_violated.jsonl`](/c:/Users/user/Desktop/mll/week7/Data-contract-enforcer/outputs/week3/extractions_violated.jsonl)

Expected behavior:

- confidence values are transformed from `0.0-1.0` to `0-100`

### 5. Run validation on violated Week 3 data

```powershell
python -m contracts.runner --contract generated_contracts/week3_extractions.yaml --data outputs/week3/extractions_violated.jsonl --output validation_reports/injected_violation.json
```

Expected output:

- [`validation_reports/injected_violation.json`](/c:/Users/user/Desktop/mll/week7/Data-contract-enforcer/validation_reports/injected_violation.json)

Expected behavior:

- the `fact_confidence.range` check should fail
- the `fact_confidence.drift` check should fail
- the current sample run produced 30 total checks, 28 passed, 2 failed

### 6. Create a second Week 3 snapshot from violated data

```powershell
python -m contracts.generator --source outputs/week3/extractions_violated.jsonl --contract-id week3-document-refinery-extractions --lineage outputs/week4/lineage_snapshots.jsonl --output generated_contracts/
```

Expected behavior:

- writes another timestamped file under [`schema_snapshots/week3-document-refinery-extractions`](/c:/Users/user/Desktop/mll/week7/Data-contract-enforcer/schema_snapshots/week3-document-refinery-extractions)
- this provides multiple snapshots for later schema evolution comparisons

### 7. Attribute the detected violation

```powershell
python -m contracts.attributor --violation validation_reports/injected_violation.json --lineage outputs/week4/lineage_snapshots.jsonl --contract generated_contracts/week3_extractions.yaml --output violation_log/violations.jsonl
```

Expected output:

- [`violation_log/violations.jsonl`](/c:/Users/user/Desktop/mll/week7/Data-contract-enforcer/violation_log/violations.jsonl)

Expected behavior:

- reads FAIL results from the injected violation report
- finds upstream candidate files from the lineage graph
- attempts git attribution
- writes one JSONL violation record per failed check

## Quick Verification Commands

Verify the shared models import:

```powershell
python -c "from contracts.models import ColumnProfile, ValidationResult; print('models OK')"
```

Verify the runner test file is syntactically valid:

```powershell
python -m py_compile tests/test_phase3_runner.py
```

Verify the attributor test file is syntactically valid:

```powershell
python -m py_compile tests/test_phase4_attributor.py
```

Run the current tests:

```powershell
python -m pytest tests/test_phase3_runner.py -v
python -m pytest tests/test_phase4_attributor.py -v
```

## Notes

- Use `python -m contracts.<module>` from the repo root when running package scripts. This avoids `ModuleNotFoundError: No module named 'contracts'`.
- On PowerShell, prefer one-line commands or backticks for multiline commands. Bash-style trailing `\` will not work.
- The current attribution flow may produce fallback blame-chain entries when the synthetic lineage file points to source paths that do not exist in local git history.
- The current Week 5 flattening path profiles only scalar top-level event fields. Nested `payload` and `metadata` fields are not fully flattened yet.

## Current Outputs

Key generated artifacts already present in this repo:

- [`generated_contracts/week3_extractions.yaml`](/c:/Users/user/Desktop/mll/week7/Data-contract-enforcer/generated_contracts/week3_extractions.yaml)
- [`generated_contracts/week3_extractions_dbt.yml`](/c:/Users/user/Desktop/mll/week7/Data-contract-enforcer/generated_contracts/week3_extractions_dbt.yml)
- [`generated_contracts/week5_records.yaml`](/c:/Users/user/Desktop/mll/week7/Data-contract-enforcer/generated_contracts/week5_records.yaml)
- [`generated_contracts/week5_records_dbt.yml`](/c:/Users/user/Desktop/mll/week7/Data-contract-enforcer/generated_contracts/week5_records_dbt.yml)
- [`validation_reports/thursday_baseline.json`](/c:/Users/user/Desktop/mll/week7/Data-contract-enforcer/validation_reports/thursday_baseline.json)
- [`validation_reports/injected_violation.json`](/c:/Users/user/Desktop/mll/week7/Data-contract-enforcer/validation_reports/injected_violation.json)
- [`violation_log/violations.jsonl`](/c:/Users/user/Desktop/mll/week7/Data-contract-enforcer/violation_log/violations.jsonl)

## Next Steps

- Implement `contracts/schema_analyzer.py`
- Implement `contracts/ai_extensions.py`
- Implement `contracts/report_generator.py`
- Expand Week 5 flattening and validation coverage for nested event payloads
