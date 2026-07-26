# Wildberries Unified Parser V1

Unified production-oriented Python parser for Wildberries data collection and preparation.

V1 includes:
- `suggest` (autocomplete collection)
- `filter` (query scoring/selection)
- `serp` (search products collection)
- `sellers` (seller enrichment)
- unified CLI + SQLite state
- retry/backoff, resume/checkpoints, contracts/smoke checks
- doctor/self-check, cleanup policy
- Web UI control layer
- execution manifests (`run_reports`)

## Stage Status
Completed:
- Stage 1: Architecture baseline
- Stage 2: Core infrastructure (paths/config/state/logging/CLI)
- Stage 3: Suggest
- Stage 4: Filter
- Stage 5: SERP
- Stage 6: Sellers
- Stage 7: Reliability layer (validation/smoke/retry/resume/statuses/doctor)
- Stage 8: Web UI
- Stage 9: Documentation and V1 operational contour

Post-V1 improvements (already implemented):
- Execution Manifest / Run Summary
- Strict Config Validation
- Retention / Cleanup Policy
- Run Locking / Singleton Protection
- Normalized Error Codes

## Environment Requirements
- Windows (primary V1 runtime)
- Python 3.14.3
- `pip`
- Playwright Chromium (for `suggest`)

## Install
```powershell
py -m pip install -r requirements.txt
py -m playwright install chromium
```

## Configuration
Main runtime config:
- `C:\parser_new\config\config.yaml`

Reference template:
- `C:\parser_new\config\config.example.yaml`

Query rules:
- `C:\parser_new\config\query_rules.yaml`

Prefixes:
- `C:\parser_new\config\prefixes.txt`

Environment variables (secrets/paths):
- `WB_COOKIE_FILE` (path to WB cookie file for SERP)
- `WEBUI_ADMIN_PASSWORD` (Web UI operator password)
- `WEBUI_SECRET_KEY` (signed session cookie key)

Example:
- `C:\parser_new\.env.example`

Important:
- `.env` is not auto-loaded by code.
- Set env vars in shell/session (or Task Scheduler action) before launch.

## Project Layout (Operational)
- `C:\parser_new\app\` - application code
- `C:\parser_new\config\` - runtime configs and operator-editable inputs
- `C:\parser_new\data\raw\` - raw layer (historical by run)
- `C:\parser_new\data\staging\` - normalized layer (historical by run)
- `C:\parser_new\data\marts\` - analytical layer (historical by run + latest)
- `C:\parser_new\data\logs\` - `app.log`, `json.log`
- `C:\parser_new\state\sqlite\` - state DB
- `C:\parser_new\state\checkpoints\` - resume checkpoints
- `C:\parser_new\state\run_reports\` - per-run manifests + `latest.json`
- `C:\parser_new\state\locks\` - singleton lock file
- `C:\parser_new\exports\` - operator-facing exports

## Run ID / Resume / History
- One `run_id` is generated once per command invocation (`YYYYMMDD_HHMMSSZ`).
- For pipeline runs (`run daily`, `run monthly`), the same `run_id` is shared by all components in that pipeline run.
- Resume checkpoint keys:
  - suggest: `prefix|letter|depth`
  - filter: `stage|group`
  - serp: `query|page`
  - sellers: `seller_id`
- State tables (`state/sqlite/wb_pipeline_state.sqlite`):
  - `runs`, `tasks`, `errors`, `checkpoints`

## CLI Commands
Use from `C:\parser_new`:

```powershell
py main.py --config config/config.yaml doctor
py main.py --config config/config.yaml validate
py main.py --config config/config.yaml runs --limit 20
```

Single components:
```powershell
py main.py --config config/config.yaml run suggest
py main.py --config config/config.yaml run filter
py main.py --config config/config.yaml run serp
py main.py --config config/config.yaml run sellers
```

Pipelines:
```powershell
py main.py --config config/config.yaml run monthly
py main.py --config config/config.yaml run daily
```

Dry-run / job-id:
```powershell
py main.py --config config/config.yaml run serp --dry-run --job-id manual_check
```

## Local Pre-Push Check (Linux VPS)
Before pushing parser_wb changes from `/home/pavel/projects/parser_wb`, run:

```bash
scripts/run_pre_push_check.sh
```

The check is local-only. It does not call Wildberries and does not print runtime
secrets. It runs path safety checks, whitespace checks, shell syntax checks,
`validate`, warehouse dry-run/check, and `pytest`.

For a quick staged-path guard:

```bash
scripts/run_pre_push_check.sh --staged-only
```

The check fails if staged/tracked/untracked git-visible paths include runtime or
secret-like files such as `state/`, `data/warehouse/`, `data/logs/`,
`config/*cookie*`, `config/runtime.env*`, `*request_headers*`, browser
`storage_state`, or handoff scratch files.

## WB Access Runbook (Linux VPS)
Current WB access/cookie operating rules are documented in:

```text
docs/WB_ACCESS_COOKIE_RUNBOOK.md
docs/WB_PROXY_ONLY_RUNBOOK.md
```

Use it before changing WB proxy/cookie/header runtime, interpreting `429`/`498`,
or processing a new browser Copy-as-cURL export.

## Isolated WB Regional SERP (Linux VPS)

Versioned query packs, the region registry and collection plans are documented
in `docs/WB_QUERY_PACKS_REGIONS.md`. A reviewed enabled plan is run only through:

```bash
scripts/run_wb_collection_plan.sh \
  --config config/config.yaml \
  --plan-file config/wb/collection_plans/PLAN.json \
  --no-publish
```

The launcher loads the required proxy-only runtime. Regional results stay under
`data/{raw,staging,marts}/serp_scoped/{plan}/{region}/{run_id}` and never update
global latest, seller input, run-report latest or Warehouse. Tracked regional
plans and regions remain disabled until a separate owner-approved enable
change.

Retention cleanup:
```powershell
py main.py --config config/config.yaml cleanup
py main.py --config config/config.yaml cleanup --apply
```

## Web UI
Start:
```powershell
$env:WEBUI_ADMIN_PASSWORD="change_me"
$env:WEBUI_SECRET_KEY="change_me_long_random_secret"
py -m uvicorn app.webui.app:app --host 127.0.0.1 --port 8080
```

Open:
- [http://127.0.0.1:8080](http://127.0.0.1:8080)

Pages:
- `/` dashboard
- `/actions` run suggest/filter/serp/sellers/monthly/daily
- `/runs` run history from SQLite
- `/logs` logs tail
- `/files` raw/staging/marts/exports browser + download
- `/config/prefixes`
- `/config/wordstat`
- `/config/query-rules`
- `/config/main`

Web UI is a thin control layer over CLI and does not duplicate pipeline logic.

## Data Outputs (Key Files)
All component outputs are historical by `run_id`:
- `data/raw/{component}/{run_id}/...`
- `data/staging/{component}/{run_id}/...`
- `data/marts/{component}/{run_id}/...`

Latest mirrors are also maintained:
- `data/{layer}/{component}/latest/...`

### Suggest
- raw: `suggest_alpha_raw.csv`
- staging: `suggest_alpha_staging.csv`

### Filter
- raw: `filter_candidates_raw.csv`
- staging: `debug_scores.csv`
- mart: `top_queries.csv`, `queries.txt`
- export: `exports/queries.txt`

### SERP
- raw: `products_raw.csv`, `pages_raw_index.csv` + page JSON files in `data/raw/serp/{run_id}/...`
- staging: `products_staging.csv`
- mart: `products_daily.csv`
- export: `exports/products_for_sellers.csv`
- preview: `exports/products_daily_preview.csv`

### Sellers
- raw: `sellers_raw.csv`
- staging: `sellers_staging.csv`
- mart: `sellers_daily.csv`
- bridge: `seller_query_product_bridge.csv` (query -> product -> seller)

## Logs, Reports, Diagnostics
Logs:
- `data/logs/app.log` (text, rotation)
- `data/logs/json.log` (JSON lines, rotation)

Run reports:
- `state/run_reports/{run_id}.json`
- `state/run_reports/latest.json`

Doctor:
- validates directories, config, state DB schema, and key latest outputs.

## Reliability Features (Operational)
- Retry/backoff with jitter (max attempts from config, default 5)
- Contracts/smoke validation in each runner
- Resume checkpoints per component
- Status model: `success`, `partial`, `failed`, `not_ready`
- Error severity: `critical`, `non_critical`
- Normalized error codes in state DB
- Run lock (`state/locks/pipeline.lock`) to prevent conflicting parallel starts

## Typical Operator Scenarios
1. First run from scratch
- set env vars (`WB_COOKIE_FILE`, Web UI auth)
- run `doctor`
- run `run monthly`
- verify `data/marts/filter/latest/top_queries.csv`
- run `run daily`
- verify `data/marts/serp/latest/products_daily.csv` and `data/marts/sellers/latest/sellers_daily.csv`

2. Daily operation
- upload new Wordstat CSV files to `data/raw/wordstat/` (or via Web UI)
- run `run daily`
- inspect `/runs`, `/logs`, run report `latest.json`

3. Monthly operation
- update prefixes/rules if needed
- run `run monthly`
- verify filter outputs and exported `queries.txt`

4. If one component fails
- check `/runs`, `/logs`, `state/run_reports/{run_id}.json`, `errors` table
- fix input/config/source issue
- rerun same target; checkpoints support resume

5. Cleanup routine
- first: `cleanup` (dry-run)
- then: `cleanup --apply` only if matched set is expected

## Scheduling
Detailed instructions:
- `C:\parser_new\docs\SCHEDULING.md`

Short version (Windows Task Scheduler):
- daily at 00:00: `py main.py --config config/config.yaml run daily`
- monthly at 00:00 (day 1): `py main.py --config config/config.yaml run monthly`

Locking prevents overlapping pipeline starts.

## Test / Sanity Commands
```powershell
py -m pytest -q
py main.py --config config/config.yaml doctor
py main.py --config config/config.yaml cleanup
```

## Current V1 Limits
- No Docker in V1
- No external DB (SQLite only)
- No proxy/multi-account orchestration
- No heavy full-card product parsing
- Linux scheduler migration is documented, not implemented as default runtime
