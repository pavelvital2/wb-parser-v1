# Wildberries Unified Parser V1 - Architecture

## 1. System Purpose
Unified production pipeline for Wildberries data collection/preparation with historical storage, resumability, and operator control via CLI and Web UI.

## 2. Core Runtime Flow
Components:
1. `suggest`
2. `filter`
3. `serp`
4. `sellers`

Pipeline targets:
- `monthly`: `suggest -> filter`
- `daily`: `filter -> serp -> sellers`

`run_id`:
- generated once per command invocation in `app/common/runner.py`
- format: `YYYYMMDD_HHMMSSZ`
- same `run_id` is propagated via `RunContext` into all components of one pipeline run

## 3. Data Layers and History
Storage is run-scoped:
- `data/raw/{component}/{run_id}/`
- `data/staging/{component}/{run_id}/`
- `data/marts/{component}/{run_id}/`

Latest mirrors are published for downstream consumption:
- `data/{layer}/{component}/latest/`

Design intent:
- raw = as received from source
- staging = normalized records
- marts = stable analytical snapshots

## 4. Component Contracts
Common service fields in CSV outputs (where applicable):
- `run_id`
- `component`
- `collected_at_utc`
- `source_system`
- `source_type`
- `source_ref`
- `status`
- `error_message`

Component outputs:
- suggest:
  - raw: `suggest_alpha_raw.csv`
  - staging: `suggest_alpha_staging.csv`
- filter:
  - raw: `filter_candidates_raw.csv`
  - staging: `debug_scores.csv`
  - mart: `top_queries.csv`, `queries.txt`
- serp:
  - raw: `products_raw.csv`, `pages_raw_index.csv`, full page JSON files
  - staging: `products_staging.csv`
  - mart: `products_daily.csv`
  - exports: `products_for_sellers.csv`, `products_daily_preview.csv`
- sellers:
  - raw: `sellers_raw.csv`
  - staging: `sellers_staging.csv`
  - mart: `sellers_daily.csv`
  - bridge: `seller_query_product_bridge.csv`

## 5. Shared Infrastructure (`app/common`)
- `config.py` - YAML load + env injection
- `config_validation.py` - strict config checks
- `paths.py` - centralized path construction + latest publish
- `csv_io.py` - unified CSV IO (`;`, `utf-8-sig`)
- `retry.py` - bounded exponential backoff + jitter
- `contracts.py` - validation/smoke contracts
- `state_db.py` - SQLite state model
- `runner.py` - target orchestration and status aggregation
- `run_context.py` - `run_id` and per-component context
- `run_lock.py` - singleton lock protection
- `run_report.py` - execution manifests (`state/run_reports`)
- `cleanup.py` - retention policy cleanup
- `error_codes.py` + `constants.py` - normalized error taxonomy

## 6. State and Operational Metadata
SQLite (`state/sqlite/wb_pipeline_state.sqlite`) tables:
- `runs` - run-level status and totals
- `tasks` - component-level status
- `errors` - error records with severity and normalized code
- `checkpoints` - resume positions

Checkpoint keys:
- suggest: `prefix|letter|depth`
- filter: `stage|group`
- serp: `query|page`
- sellers: `seller_id`

Run reports:
- `state/run_reports/{run_id}.json`
- `state/run_reports/latest.json`

## 7. Reliability Model
- retry/backoff with jitter, bounded attempts
- contracts/smoke after each component
- critical vs non-critical error split
- run/task statuses: `success`, `partial`, `failed`, `not_ready`
- lock file to prevent conflicting parallel starts

## 8. Web UI Architecture (Thin Control Layer)
Module: `app/webui`

Stack:
- FastAPI
- Jinja2 server-rendered templates
- cookie session auth

Responsibilities:
- login/logout
- dashboard/latest outputs
- run history from SQLite
- logs tail
- file browser/download (path traversal protected)
- config editing (`prefixes`, `query_rules`, `config.yaml`)
- Wordstat upload
- non-blocking action starts via `subprocess.Popen(main.py run ... )`

Non-goal:
- Web UI does not implement business parsing logic and does not replace CLI runners.

## 9. Latest Outputs Strategy
Every stage writes historical files by `run_id` and then publishes selected artifacts into `latest` folders.
Downstream stages consume latest marts by default (configurable).

## 10. Retention/Cleanup Policy
`retention` section in config defines TTL by area:
- logs, raw, staging, marts, exports, run_reports

`cleanup` command:
- default dry-run
- `--apply` actually deletes matched files
- keeps `latest` paths protected

## 11. Scheduling Model
V1 scheduling target:
- Windows Task Scheduler

Future portability:
- Linux cron/systemd timer with same CLI commands and config file.

(See `docs/SCHEDULING.md` for concrete operations.)

## 12. Security Baseline
- cookie path via env/config, no hardcoded secrets
- Web UI credentials via env
- signed session cookies
- safe path checks in file/log endpoints
- repo runtime artifacts ignored via `.gitignore`
