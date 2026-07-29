# WB query packs and regions

Status: isolated regional SERP implementation; tracked plans and regions are
disabled, and live execution requires a separate owner approval

## Scope

Stage 1 adds versioned, read-only configuration contracts and strict Python
loaders for future scoped WB collection:

- query packs with stable category/query IDs;
- a disabled region registry;
- disabled collection plans;
- exact-byte source SHA-256 values;
- fail-closed query-pack provenance;
- a canonical, secret-free effective-plan snapshot contract for Stage 2.

Stage 1 itself did not add runtime behavior. The isolated runner adds practical
query-pack, region and depth selection with scoped storage, but does not enable
the tracked plan or change the nightly pipeline. The guarded A-B-A pilot remains
a separate diagnostic mode and is not required for ordinary regional
collection.

## Tracked configuration

```text
config/wb/query_packs/{query_pack_id}/{version}.json
config/wb/regions.json
config/wb/collection_plans/{collection_plan_id}.json
config/wb/execution_matrices/{execution_matrix_id}.json
```

`four-region-nightly-v1.json` is the production scheduling projection. It can
list multiple enabled, reviewed query-pack/plan pairs without parser code
changes, but has its own top-level activation flag. It remains disabled until
the joint coordinator cutover; its only current enabled entry is
`shevron-core@2026-07-26.1`.

The production-shaped matrix command is:

```bash
scripts/run_wb_four_region_nightly.sh \
  --matrix-file config/wb/execution_matrices/four-region-nightly-v1.json \
  --no-publish
```

The runner executes enabled entries sequentially through the same four-region
collection, seller and warehouse pipeline used by the one-plan command. Matrix
state is stored under
`state/wb_execution_matrices/{execution_matrix_id}/runs/{matrix_run_id}`.
Each entry pins exact matrix, plan, query-pack and region-registry hashes plus
its child run ID. A checkpoint resume validates these bytes, skips completed
entries and resumes only the current child run. The deduplication identity is
`run_date + marketplace + query_pack_id/version + region_id + query_id` and is
checked against the regional warehouse before collection.

The matrix validates the reviewed new-run window once and gives every serial
child the same bounded absolute deadline. A later pack may therefore begin
after the one-plan start grace only as an explicit matrix continuation; it
cannot extend the matrix cutoff. If the remaining window is insufficient, the
pending entry is checkpointed only when its child state is resumable or proven
pristine, and a later approved invocation continues without repeating prior
entries.

`state/wb_execution_matrices/{execution_matrix_id}/latest.json` is updated
atomically only after every enabled entry has completed collection, sellers and
regional warehouse ingestion. A partial, failed or interrupted matrix run
leaves the previous matrix latest unchanged. The existing one-plan
`--plan-file` command remains available for audited manual compatibility; the
coordinator command uses `--matrix-file`.

The first pack,
`config/wb/query_packs/shevron-core/2026-07-26.1.json`, is a versioned copy of
the normalized 30-query sequence in `exports/queries.txt`. The legacy TXT file
remains the only input to the existing nightly SERP command.

The Moscow and Rostov-on-Don registry entries use the coordinates recorded in
the approved research document. Both entries are disabled. Their `dest_id` and
resolution fields are null/unresolved because the observed WB destination
values are dated evidence, not stable configuration.

The tracked plan is also disabled and fixes these non-runtime policies:

- three explicit query IDs;
- Moscow and Rostov-on-Don;
- depth 100;
- `publication_mode=none`;
- sellers disabled;
- proxy rotation disabled.

## Loader API

Module:

```text
app/serp/collection_plan.py
```

Public Stage 1 functions:

```python
load_query_pack(path)
load_region_registry(path)
load_collection_plan(path)
load_collection_plan_bundle(
    project_root=...,
    plan_path=...,
    region_registry_path=...,
    provenance_path=None,
)
register_query_pack_provenance(
    provenance_path=...,
    query_pack=...,
    project_root=...,
)
build_effective_plan_snapshot(
    bundle,
    resolved_destinations=...,
    page_size=...,
    endpoint_policy=...,
)
canonical_effective_plan_bytes(snapshot)
canonical_effective_plan_sha256(snapshot)
```

Loading documents without `provenance_path` is read-only. Passing a provenance
path explicitly records the immutable `(query_pack_id, version) -> exact-byte
SHA-256` mapping. Stage 2 must call this only while holding the documented
plan-specific lock. Reusing an identity with a different hash fails closed;
malformed provenance is never overwritten. The only accepted provenance target
is `state/wb_collection_plans/provenance/query_pack_versions.json` below the
project root; symlinked parents or targets fail closed.

Bundle loading is restricted to the project configuration tree. The plan must
be a regular JSON file directly under `config/wb/collection_plans`, the region
registry must be exactly `config/wb/regions.json`, and the referenced query pack
must be a regular JSON file under `config/wb/query_packs`. Paths outside the
project, nested plan paths, symlinks and symlink-based path escapes fail closed.

## Hash semantics

The source hashes are SHA-256 over the exact bytes read:

- `query_pack_sha256`;
- `collection_plan_sha256`;
- `region_registry_sha256`.

Whitespace or a final-newline change therefore changes a source hash even if
the parsed JSON has the same values.

The future effective-plan hash uses canonical UTF-8 JSON:

```text
ensure_ascii=false
sort_keys=true
separators=(",", ":")
allow_nan=false
no trailing newline
```

`build_effective_plan_snapshot()` is pure and returns an in-memory object. It
requires an enabled plan, enabled selected queries/categories/regions, exact
source hashes, a 100-item page size, stable non-secret endpoint IDs in their
initial configured order, and resolver values with
`dest_resolution_status=resolved_not_sent`. The schema has an exact key
allowlist and has no fields for cookies, request headers, credentials, tokens,
proxy URLs or full egress IPs.

`dest_id_observed` is limited to the non-secret WB destination contract
`[+-]?[0-9]{1,16}`. Resolution timestamps must be valid RFC 3339 UTC values
using `T` and either `Z` or `+00:00`. When
`require_distinct_destinations=true`, duplicate observed destination IDs fail
closed.

Stage 1 does not call this function for the committed disabled pilot plan and
does not create an effective-plan runtime file.

## Isolated regional runner

The audited Linux launcher is:

```bash
scripts/run_wb_collection_plan.sh \
  --config config/config.yaml \
  --plan-file config/wb/collection_plans/{collection_plan_id}.json \
  --no-publish
```

The shell launcher loads ignored `config/runtime.env` before Python/config
loading and then calls `scripts/run_wb_collection_plan.py`. The shared
proxy-required guard still fails closed before a session can be constructed.
The command rejects a disabled plan, so the committed plan cannot perform live
HTTP.

The runner:

- acquires daily, pipeline, warehouse-refresh and collection-plan locks
  non-blockingly in that order and retains them through final manifest fsync;
- resolves all configured `xinfo.dest` values under the held locks, writes the
  immutable effective-plan snapshot, then collects the region scopes serially;
- checks one unchanged egress identity through the same explicitly configured
  proxy route and requests session, using secret-free per-request headers, and
  stores only a masked value plus an experiment-local salted hash;
- sends the exact resolved value as the search `dest`;
- derives pages from the plan depth: supported depth is `100..1000` in
  100-item increments, with exactly 100 unique valid products required per
  page;
- uses the production-configured primary/fallback endpoint list in the same
  active-first order as `SerpEngine`; retryable production statuses may advance
  once through the remaining configured endpoints for that page, while a
  successful endpoint becomes first for the next page;
- uses the production `sleep_between_pages_ms` and
  `sleep_between_queries_ms` values between scoped requests;
- does not use the production retry loop, deferred retry, proxy rotation or
  endpoint selection from historical run evidence;
- records the successful `endpoint_id` in product rows, page indexes and
  checkpoints, plus sanitized endpoint attempt counts in the run manifest;
- records `resolved_and_sent` only as client-side request lifecycle evidence;
- never claims that the search server applied the destination;
- refuses to start within five minutes of the 23:45 MSK preflight cutoff;
- for plans deeper than 500, estimates the remaining worst-case request and
  pacing window using every configured endpoint as a possible sequential
  attempt per page, and refuses to start when that window plus a 15-minute
  safety reserve would overlap the nightly 00:15 MSK collection.

All outputs remain under:

```text
data/{raw,staging,marts}/serp_scoped/{plan}/{region}/{run_id}/
state/wb_collection_plans/{plan}/{run_id}/
```

The production-ready manual plan is:

```text
config/wb/collection_plans/shevron-moscow-rostov-top1000-v1.json
```

It contains all 30 `shevron-core` queries in pack order, Moscow and
Rostov-on-Don, depth 1000 (10 pages per query, 600 pages total). The tracked
plan and both tracked regions remain disabled. Enabling or executing it needs
separate owner authorization.

Deep runs use one immutable segment per `region_id + query_id`, at most 10
pages. Every segment has an egress check before and after its search pages.
Within one process, a successful end check is reused as the next segment's
start check. Across resumed processes, egress may differ; each segment must
still be internally constant. Evidence stores only masked identities and
run-local hashes, never a full IP.

Pages are first written below `pending_segments`. Only after the segment end
check succeeds does an immutable segment record authorize idempotent promotion
to canonical raw/checkpoint paths. An interrupted segment is not reusable and
is recollected in full, limiting automatic repetition to one query (10 pages).
Confirmed segments are validated by exact source/effective hashes, metadata
and raw/checkpoint checksums before reuse. On resume, each manifest reference
must exactly match its canonical segment record. Enabled region/query scope,
pages `1..N`, task-derived scoped paths, endpoint counters and constant
hash-only egress evidence are validated for every segment before any promotion
or network I/O. All segments and artifacts are validated before canonical
promotion starts, so a later corrupt reference cannot cause partial recovery.

Deep resumable runs use effective-plan schema v2. It binds resume to
hash-only provenance for the ordered endpoint URLs, canonical request
parameters and configured proxy route. It does not store those values. A
fingerprint mismatch fails before resolver, egress or search I/O. Existing
schema-v1 snapshots for legacy depth <=500 remain valid and unchanged.

Failed or discarded segment attempts remain in a cumulative sanitized history
across every resume. `endpoint_usage` therefore reports actual HTTP attempts
and successful page responses, including discarded work, while `totals`
reports only canonical confirmed pages. Segment IDs are deduplicated and all
history counters, scopes and endpoint IDs are validated before resolver or
search traffic. If the end egress differs, both checks are retained only as
masked values and run-local hashes; that segment is never reusable.

Publication is a recoverable two-phase transition. The manifest remains
`complete=false` with `status=publication_pending` until the immutable
per-region generation manifests and the atomic dual-region latest pointer are
durable. Resume can reconcile that state without WB network calls. A matching
already-durable pointer is accepted idempotently; only then is the manifest
finalized as `success` and `complete=true`.

Resume is explicit and keeps the original run identity:

```bash
scripts/run_wb_collection_plan.sh \
  --config config/config.yaml \
  --plan-file config/wb/collection_plans/shevron-moscow-rostov-top1000-v1.json \
  --no-publish \
  --resume-run-id YYYYMMDD_HHMMSSZ
```

Final CSV files are rebuilt deterministically in plan/region/query/page order
from confirmed canonical pages. The plan-level scoped latest pointer is:

```text
state/wb_collection_plans/{plan}/latest.json
```

It atomically references two immutable region manifests under
`latest_generations/{run_id}/`. Consumers must resolve the plan pointer first;
partially written generation files are not visible. The pointer changes only
after the whole plan and both regions are complete. Failure, partial result or
interruption leaves the previous pointer unchanged.

The runner never writes global SERP latest, seller exports, global run-report
latest or warehouse data.

The selected region or region set, query IDs and depth come only from the
versioned collection plan. The query text comes only from its versioned query
pack. The destination is resolved for the selected `region_id` under held
locks, then the exact resolved value is sent on every corresponding search
request. This is resolver-and-request provenance, not proof that WB applied the
destination server-side.

## Validation

Loaders reject:

- unknown schema versions and unknown/missing keys;
- duplicate JSON keys;
- invalid or duplicate stable IDs;
- duplicate normalized query text;
- unknown category, query or region references;
- enabled queries under disabled categories;
- enabled plans referencing disabled packs, queries, categories or regions;
- external, non-canonical or symlinked bundle source paths;
- unsafe query-pack paths;
- depth outside `100..1000` in 100-item increments, a mismatched expected page
  count, or unsafe publication/sellers/rotation modes;
- non-null destination observations in tracked Stage 1 region config;
- unsafe destination IDs, non-UTC/non-RFC-3339 timestamps and duplicate
  resolved destinations;
- malformed provenance and query-pack identity/hash mismatches.

## Stop gate

The tracked plan and regions remain disabled. This implementation does not
authorize a resolver/search run or guarded pilot. Scoped publication, warehouse
migration, Parser Data API changes and scheduling remain unapproved.

## Stage 3 guarded pilot

The A-B-A contract is available only through the audited launcher:

```text
scripts/run_wb_guarded_regional_pilot.sh
```

The launcher accepts no arguments, loads and hashes ignored
`config/runtime.env` before Python/config loading, and executes the fixed
`--no-publish --guarded-pilot` plan. It does not bypass plan or region
validation. The tracked plan and both tracked regions remain `enabled=false`;
a future live pilot requires a separate reviewed enable commit followed by a
disable commit and explicit owner approval.

While holding the existing daily, pipeline, warehouse-refresh and plan locks,
the guarded runner performs this fixed sequence:

1. validate and fix the exact config, collection-plan, region-registry and
   query-pack source paths, then hash those sources together with the protected
   production file set and user crontab without persisting their contents.
   The config hash must equal the exact-byte SHA-256 retained by `AppConfig`
   from the same bytes used for YAML parsing;
2. check neutral egress and resolve Moscow and Rostov-on-Don destinations;
3. probe the primary endpoint and at most one fallback, then pin the first
   usable endpoint. A usable 100-product probe is retained only in memory and
   reused as the first Moscow/shevron page after independent identity and
   payload validation; probe evidence never contains the payload;
4. write an immutable effective snapshot containing the actual pinned endpoint;
5. persist the reused first Moscow page through the ordinary scoped
   raw/checkpoint/output path, collect the other two Moscow pages, check egress,
   collect three Rostov-on-Don pages, check egress, repeat the first Moscow
   query, then perform the final egress check;
6. write endpoint, request-budget, repeat-control and comparison evidence;
7. re-hash the same fixed source and production path set, fail on any missing,
   changed, symlinked or non-regular source, and retain all locks through final
   manifest fsync. A successful manifest takes its three source SHA-256 values
   from this confirmed after-snapshot rather than only from initial loading.

Before the first resolver/search/egress request, Stage 3.2 also fails closed
unless runtime provenance, an explicit structurally valid proxy, loaded request
headers, explicit cookie-required mode, disabled rotation and one concrete
proxied requests session are all confirmed. Evidence contains only
schema/status/boolean/count/hash provenance.

The WB request budget is fail-closed and counts attempts before transport I/O:
`geo<=2`, `endpoint_probe<=2`, `regional_search=5`, `repeat_search=1`,
`total_wb<=10`. The primary route uses `9` WB requests and the fallback route
uses `10`; the reused probe is one of the six main regional pages, while the
repeat remains separate. Neutral egress checks are recorded separately. There
are no retries, second pages, endpoint switches after pinning, proxy rotations,
sellers, warehouse refreshes, publication or notification.

Every endpoint probe, ordinary regional search and repeat is paced by a
monotonic clock. The first attempt has no delay; each later attempt starts no
earlier than `17` seconds plus jitter from `0` through `2` seconds after the
previous attempt. The reusable probe counts as the previous attempt. HTTP
`429`/`498` blocks all subsequent WB calls with zero retry. A strict numeric
`Retry-After` of `1..120` seconds controls cooldown when valid; otherwise
cooldown is `45` seconds. After cooldown, at most one neutral egress check
through the same proxy session is allowed. The hard runtime cap is 18 minutes
and the start gate requires at least 20 minutes before 23:45 MSK.

The Moscow repeat is stored under the Moscow scoped raw run and in a separate
control artifact; it is not appended to the 600 primary product rows. Regional
membership/position comparison is emitted only when the Moscow A-A membership
Jaccard is at least `0.95`. Otherwise comparison is `not_eligible` and contains
no regional difference rows. The artifact is limited to SERP membership and
position evidence and makes no sales, demand or ranking-causality claim.

Stage 3 state artifacts are immutable files under the scoped run:

```text
state/wb_collection_plans/{plan}/{run_id}/endpoint_preflight.json
state/wb_collection_plans/{plan}/{run_id}/contour_preflight.json
state/wb_collection_plans/{plan}/{run_id}/request_budget.json
state/wb_collection_plans/{plan}/{run_id}/search_pacing.json
state/wb_collection_plans/{plan}/{run_id}/rate_limit.json
state/wb_collection_plans/{plan}/{run_id}/control/moscow_repeat.json
state/wb_collection_plans/{plan}/{run_id}/comparison.json
state/wb_collection_plans/{plan}/{run_id}/protected_evidence.json
```

Endpoint evidence contains only endpoint IDs, outcome, HTTP status and safe
error codes. Protected evidence contains only relative paths, presence status
and SHA-256 values. Neither contract stores endpoint URLs, URL queries, full
egress IP, cookies, headers, proxy values, credentials or crontab contents.
An endpoint is usable only when the transport returns the literal boolean
`suitable=true`, an integer (not boolean) HTTP status `200`, and no error code;
other truthy or malformed values fail closed.

## Four-Region Phase A

`config/wb/collection_plans/shevron-four-regions-top1000-v2.json` is the
production-shaped but disabled four-region definition for Moscow,
Rostov-on-Don, Novosibirsk and Kazan. It uses all 30 pinned `shevron-core`
queries and maximum depth 1000, so a complete generation contains at most
1200 pages.
Depth is a maximum: v2 requires a consistent payload `total` and records
1-10 pages per query. A terminal short page is valid only when it exactly
completes `min(total, 1000)`; empty or inconsistent payloads fail closed.

The compatible one-plan full-pipeline launcher is:

```bash
scripts/run_wb_four_region_nightly.sh \
  --config config/config.yaml \
  --plan-file config/wb/collection_plans/shevron-four-regions-top1000-v2.json \
  --no-publish
```

The wrapper remains the fixed Parser Nightly Coordinator adapter target. Under
the coordinator it accepts only the reviewed execution matrix, retains the inherited
`marketplace_collection_lock_v3` validation FD, honors the coordinator absolute
deadline and emits `marketplace_parser_result_v3`. See
`docs/WB_NIGHTLY_COORDINATOR_ADAPTER.md`. The older two-region plans are not a
fallback.

Matrix resume requires the matrix command plus `--resume-run-id`. One-plan
resume remains compatible with the one-plan command. The launcher must not
be scheduled or used live until the owner approves a controlled window and the
matrix, referenced plans and exactly four regions are enabled in a reviewed
change.

If collection is already complete but downstream was stopped by its own
runtime cutoff, use the same launcher with `--downstream-only-run-id`. It
revalidates the immutable scoped generation and resumes seller checkpoints
without another SERP request.

The v2 runtime window is bounded and resumable. A new run can start only in its
reviewed 00:15-00:45 MSK window. One invocation is capped at six hours and at
23:00 MSK. The runner estimates the next atomic 10-page query segment, checks
the deadline before every network attempt and repeats at most the unfinished
segment after interruption. It does not lower the production timeout and uses
only production SERP page/query pacing.

Position facts are keyed by run, region, query and absolute position. Repeated
product IDs at different positions remain separate facts; only seller input is
deduplicated by product. Reports include actual rows, the 120000 maximum
capacity and `duplicate_product_positions`.

Regional warehouse facts retain the existing query-position price, brand,
supplier, rating, stock, status and collection-time fields when present.
Warehouse-owned `regional_run_quality` and `regional_query_quality` tables
carry complete run/query terminal, count, egress and source-hash evidence;
future Parser Data API code must read these facts rather than arbitrary parser
state files.

For legacy Yaroslavl quality rows, query/page/position expected and successful
columns have observed-history semantics: both sides contain counts derived
from the actual legacy position facts, or zero when none exist. The separate
legacy `items_ok` metric remains the source daily-run metric. Duration is
derived only from a valid ordered UTC start/finish pair.

Scoped seller resume rewrites its mart deterministically to one latest row per
expected supplier. A completed checkpoint must match a successful mart row.
Returned `processed_sellers` and `items_ok` describe the complete verified
mart, while `invocation_processed_sellers` records only work performed by the
current invocation.

Before seller collection, each `nmId` selects the first plan-ordered occurrence
with a non-empty `supplier_id`. When all occurrences lack a supplier, the first
position is retained and counted in `missing_supplier_products`. This selection
does not alter the region-query-product-position bridge.

Warehouse ingestion uses a bounded DuckDB runtime in production:
`memory_limit=1GiB`, `threads=2`, with private mode-`700` spill sessions under
ignored `data/warehouse/wb_regional/tmp`. Each clean close removes its own
session; only stale named sessions older than 24 hours are eligible for later
cleanup.

Downstream sellers, regional warehouse and owner-report state start only after
all four scoped region generations are complete. They never publish global
latest. Existing global warehouse history is synchronized read-only and
incrementally into the regional warehouse as legacy `yaroslavl` data using
stable keys and row hashes. New runs append; mutation or disappearance of an
already imported fact fails closed. Yaroslavl is not part of the future
collection plan.
