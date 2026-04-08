# Wildberries Analytics Parser - Project State (Stage 9)

## Current State
Project is in V1 operationally-ready state after Stage 9 documentation hardening.

Implemented components:
- suggest
- filter
- serp
- sellers

Implemented platform capabilities:
- unified CLI
- SQLite state and run history
- resume checkpoints
- retry/backoff with jitter
- contracts and smoke validation
- doctor/self-check
- Web UI (thin control layer)
- execution manifests (`run_reports`)
- retention cleanup command
- strict config validation
- singleton run locking
- normalized error codes

## Core Rules (Authoritative)
1. Pipeline logic stays in CLI/runners/components.
2. Web UI remains control-only and must not duplicate component logic.
3. Paths and IO go through centralized modules (`paths.py`, `csv_io.py`, `contracts.py`, `state_db.py`).
4. `run_id` is generated once per run and shared across components of a pipeline run.
5. Historical outputs are run-scoped; latest mirrors are secondary convenience outputs.

## Pipeline
Targets:
- monthly: `suggest -> filter`
- daily: `filter -> serp -> sellers`

## Data/State Locations
- raw/staging/marts: `data/{layer}/{component}/{run_id}`
- latest mirrors: `data/{layer}/{component}/latest`
- logs: `data/logs/app.log`, `data/logs/json.log`
- sqlite: `state/sqlite/wb_pipeline_state.sqlite`
- checkpoints: `state/checkpoints/`
- run reports: `state/run_reports/{run_id}.json`, `state/run_reports/latest.json`
- lock file: `state/locks/pipeline.lock`

## State DB Contract
Tables:
- `runs`
- `tasks`
- `errors`
- `checkpoints`

Error records include:
- `severity` (`critical`, `non_critical`)
- normalized `error_code`

Checkpoint key contract:
- suggest: `prefix|letter|depth`
- filter: `stage|group`
- serp: `query|page`
- sellers: `seller_id`

## Reliability Contract
- bounded retry with exponential backoff + jitter
- contracts validation after each stage
- status model:
  - run/task: `success`, `partial`, `failed`, `not_ready`
- lock-based singleton protection for conflicting launches

## V1 Operational Constraints (Known Limits)
- Primary runtime target: Windows
- Linux deployment is prepared architecturally but not default production runtime yet
- No Docker in V1
- No external DB integration (SQLite only)
- No proxy/multi-account orchestration
- No heavy full-card product parsing

## Post-V1 Improvements Already Landed
- execution run summary / manifest
- strict config validation layer
- retention cleanup policy
- singleton locking
- normalized error taxonomy

## After-V1 Direction (Not in Stage 9)
- Linux VPS operational rollout playbook
- enhanced monitoring/alerting
- optional external DB sink
- deeper seller/product enrichment if source stability allows
