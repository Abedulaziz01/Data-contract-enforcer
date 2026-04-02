# Design Overview

## Purpose

The Data Contract Enforcer monitors structured outputs from the Week 1-5 systems, converts observed schema patterns into machine-checkable contracts, validates new snapshots against those contracts, and records failures in a format that supports attribution and downstream impact analysis.

This implementation currently covers four working stages:

- contract generation
- baseline and violated-data validation
- synthetic violation injection
- lineage-based violation attribution

## System Flow

The current end-to-end flow is:

1. Generate canonical synthetic JSONL inputs under `outputs/`
2. Run `contracts.generator` to produce a contract YAML, dbt YAML, and timestamped schema snapshot
3. Run `contracts.runner` on clean data to produce a baseline validation report and numeric drift baselines
4. Run `create_violation.py` to inject the canonical Week 3 confidence-scale breaking change
5. Run `contracts.runner` again on violated data to produce a failure report
6. Run `contracts.attributor` to convert failed checks into structured violation records with blame-chain candidates and blast radius

## Component Design

### `contracts/models.py`

Provides shared dataclasses used across the repo:

- `ColumnProfile`
- `ContractClause`
- `ValidationResult`
- `ValidationReport`

These models provide a common internal shape for profiling, contract construction, and report output.

### `contracts/generator.py`

The generator is responsible for:

- loading JSONL inputs
- flattening nested structures into a tabular profiling shape
- computing per-column profiles
- mapping column profiles into Bitol-style contract clauses
- generating dbt-compatible schema output
- injecting downstream lineage context
- writing timestamped schema snapshots

#### Design choices

- The Week 3 flattening logic explodes `extracted_facts` and `entities` into a rectangular DataFrame because validation rules are easier to generate from scalar columns.
- List and dict values are normalized during profiling so uniqueness checks do not fail on unhashable values such as `fact_entity_refs`.
- LLM annotation is optional. If `ANTHROPIC_API_KEY` is missing, the generator falls back to deterministic placeholder annotations instead of failing.
- Output file naming is shortened from contract id to the current repo convention, for example `week3-document-refinery-extractions` becomes `week3_extractions.yaml`.

#### Current limitation

Week 5 event flattening currently keeps only scalar top-level fields and does not yet expand nested `payload` and `metadata` structures.

### `contracts/runner.py`

The runner executes generated contract clauses against a dataset snapshot and writes a structured JSON report.

It currently implements:

- required-field checks
- numeric type checks
- enum conformance checks
- UUID-format checks
- date-time checks
- numeric range checks
- statistical drift checks based on stored baselines

#### Design choices

- The runner never intentionally fails the full run because of a single missing column. Missing-column cases are returned as `ERROR` results inside the report so the process remains inspectable.
- Baselines are written on the first successful run to `schema_snapshots/baselines.json`.
- The same flattening strategy is reused in generator and runner to keep field naming consistent across generation and validation.

#### Observed behavior

On clean Week 3 synthetic data, the runner currently produces a fully passing baseline report.

On violated Week 3 data, the runner correctly detects:

- a `fact_confidence.range` failure
- a `fact_confidence.drift` failure

### `create_violation.py`

This script injects the project’s canonical breaking change:

- transforms Week 3 `confidence` values from `0.0-1.0` to `0-100`

It produces `outputs/week3/extractions_violated.jsonl`, which is then used to prove the runner catches both direct range violations and silent statistical drift.

### `contracts/attributor.py`

The attributor reads a validation report and converts every `FAIL` result into a violation log record.

It currently performs:

- lineage traversal to find upstream `FILE` nodes
- git log lookup for those file candidates
- candidate scoring based on temporal proximity and lineage distance
- blast-radius construction from contract lineage metadata
- JSONL append into `violation_log/violations.jsonl`

#### Design choices

- Attribution runs even when git history is incomplete. In that case, the module emits a fallback blame candidate rather than failing.
- Blast radius is derived from the contract’s lineage section, not by re-running lineage traversal, which keeps the logic consistent with generated contract metadata.

#### Current limitation

The synthetic lineage graph references file paths that do not exist in local git history, so git attribution often returns zero commits and falls back to placeholder blame entries.

## Data Model Strategy

The project uses three main artifact types:

- source JSONL data in `outputs/`
- generated contracts and snapshots in `generated_contracts/` and `schema_snapshots/`
- validation and attribution outputs in `validation_reports/` and `violation_log/`

This separation keeps input data, inferred contract state, and enforcement output clearly distinct.

## Verification Strategy

The current repo uses three layers of verification:

- direct script execution for generator, runner, and attributor
- output inspection via JSON and YAML checks
- focused Python tests in `tests/test_phase3_runner.py` and `tests/test_phase4_attributor.py`

This gives coverage at both the script and file-output levels even though the project is still incomplete.

## Known Gaps

The following modules are still planned but not implemented in the current codebase:

- `contracts/schema_analyzer.py`
- `contracts/ai_extensions.py`
- `contracts/report_generator.py`

Additional improvements still needed:

- fuller flattening of Week 5 event records
- stronger git attribution using real upstream repos
- richer lineage-aware consumer mapping per field instead of per contract
- alignment review for nullable fields such as Week 3 `page_ref`

## Why The Design Looks This Way

The implementation is optimized for the Week 7 project rubric:

- it demonstrates contract generation from real-looking data
- it proves validation on clean and broken snapshots
- it records machine-readable failures
- it shows a lineage-driven attribution path

That makes the system useful both as a submission artifact and as a base for the missing Phase 3 and Phase 4 modules.
