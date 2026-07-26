# WB regional query packs implementation plan

Date: 2026-07-26

Status: proposed, not approved for implementation

Research: `docs/research/WB_REGIONAL_QUERY_PACKS_RESEARCH_20260726.md`

## 1. Mandatory stop gate

This document is a plan only.

Do not implement any stage until the owner explicitly approves this plan.
Approval to implement one stage is not approval for later stages, production
publication, warehouse exposure, Parser Data API changes or scheduling.

No production cron, proxy, cookie, request-header, runtime or legacy query-list
change is part of the initial implementation.

## 2. Invariants

1. `exports/queries.txt` remains the exact default source for the current
   nightly 30-query shevron collection.
2. The nightly wrapper remains `serp -> sellers -> warehouse`.
3. Global paths under `data/{raw,staging,marts}/serp/latest`, sellers latest,
   `exports/products_for_sellers.csv`, `state/run_reports/latest.json` and the
   WB warehouse remain untouched by regional/manual plans.
4. A regional row is invalid without explicit `collection_plan_id`,
   `query_pack_id`, `query_pack_version`, `query_id`, `category_id`,
   `region_id`, and destination evidence.
5. Destination IDs are resolved from a dated WB geo response and the search
   request records the value sent. A modern search response that does not echo
   destination is not described as server-side verification. IDs are not
   invented or assumed permanent.
6. Initial regional collection is serial, page 1 only, no proxy rotation and
   no publication.
7. No regional run starts while the nightly wrapper, SERP, sellers or warehouse
   refresh is active.
8. Regional data is not visible in existing warehouse/API queries until an
   explicit schema/API migration passes its own acceptance gate.

## 3. Proposed contracts

### 3.1 Query-pack registry

Proposed path:

```text
config/wb/query_packs/{query_pack_id}/{version}.json
```

Proposed schema:

```json
{
  "schema_version": "wb_query_pack_v1",
  "query_pack_id": "shevron-core",
  "version": "YYYY-MM-DD.N",
  "enabled": true,
  "categories": [
    {
      "category_id": "shevrons",
      "name": "Шевроны",
      "enabled": true
    }
  ],
  "queries": [
    {
      "query_id": "shevron",
      "category_id": "shevrons",
      "text": "шеврон",
      "enabled": true
    }
  ]
}
```

Validation:

- schema version is exact;
- IDs match `[a-z0-9][a-z0-9_-]*`;
- pack version is immutable once used by a successful run;
- category/query IDs are unique;
- normalized query text is unique within a pack;
- every enabled query references an enabled category;
- disabled entries are retained for history;
- no runtime secrets or destination IDs are stored in a query pack.

The first pack must be generated from, and regression-tested against, the
current `exports/queries.txt` without modifying that file.

### 3.2 Region registry

Proposed path:

```text
config/wb/regions.json
```

Proposed schema:

```json
{
  "schema_version": "wb_region_registry_v1",
  "regions": [
    {
      "region_id": "moscow",
      "region_name": "Москва",
      "enabled": false,
      "resolver": "wb_geo_xinfo",
      "latitude": "owner-approved value",
      "longitude": "owner-approved value",
      "address_label": "owner-approved value",
      "dest_id": null,
      "dest_verified_at_utc": null
    }
  ]
}
```

`dest_id` remains null in the initial committed registry. The runtime resolver
must obtain `xinfo.dest`, verify the response contract and record the dated
value in the run manifest. Persisting a resolved destination in versioned
configuration requires a separate owner decision after repeated stability
checks.

Coordinates and address labels are not secrets, but still require owner
approval because they define the experiment.

### 3.3 Collection plan

Proposed path:

```text
config/wb/collection_plans/{collection_plan_id}.json
```

Proposed schema:

```json
{
  "schema_version": "wb_collection_plan_v1",
  "collection_plan_id": "shevron-moscow-rostov-top100-pilot-v1",
  "enabled": false,
  "query_pack_file": "config/wb/query_packs/shevron-core/YYYY-MM-DD.N.json",
  "query_ids": [
    "shevron",
    "shevrony",
    "shevron-na-lipuchke"
  ],
  "region_set": [
    "moscow",
    "rostov-on-don"
  ],
  "depth": 100,
  "schedule_id": "manual-pilot",
  "publication_mode": "none",
  "sellers_mode": "disabled",
  "proxy_rotation_mode": "disabled",
  "quality": {
    "expected_queries_per_region": 3,
    "expected_pages_per_query": 1,
    "max_page_errors": 0,
    "require_constant_egress": true,
    "require_distinct_destinations": true
  }
}
```

Depth must be divisible by the effective page size or explicitly round up and
record the final page size. The initial pilot accepts only `depth=100`.

Schedule identity is metadata. Cron expressions must not be embedded in packs
or plans.

### 3.4 Expanded task and row identity

Proposed immutable task fields:

```text
collection_scope
collection_plan_id
query_pack_id
query_pack_version
query_id
category_id
query
query_group
region_id
region_name
dest_id_observed
dest_verified_at_utc
page
page_size
depth
```

Checkpoint identity must include at least:

```text
collection_plan_id | query_pack_version | region_id | query_id | page
```

Do not use query text as the sole stable key.

### 3.5 Scoped storage

Proposed component namespace:

```text
data/raw/serp_scoped/{collection_plan_id}/{region_id}/{run_id}/
data/staging/serp_scoped/{collection_plan_id}/{region_id}/{run_id}/
data/marts/serp_scoped/{collection_plan_id}/{region_id}/{run_id}/
```

Potential future scoped latest, disabled in the pilot:

```text
data/{raw,staging,marts}/serp_scoped/{collection_plan_id}/{region_id}/latest/
```

Run state:

```text
state/wb_collection_plans/{collection_plan_id}/{run_id}/manifest.json
state/wb_collection_plans/{collection_plan_id}/{run_id}/regions/{region_id}.json
state/locks/wb_collection_plan.flock
```

The directories do not match the current warehouse patterns
`data/marts/serp/*/products_daily.csv` or
`data/raw/serp/*/pages_raw_index.csv`. This prevents accidental ingestion.

No pilot file is copied to global latest, sellers input, global run-report
latest or warehouse.

## 4. Proposed file changes by stage

The paths below are exact implementation targets, not changes authorized by
this research commit.

### Stage 0: owner approval

Owner decisions required:

- approve option C from the research report;
- approve the two pilot regions and coordinate/address inputs;
- approve the exact three pilot queries;
- approve the request and timing budget;
- approve whether dated `dest` observations may appear in non-secret manifests.

Stop if any decision is missing.

### Stage 1: schemas and loaders, no HTTP

New files:

```text
config/wb/query_packs/shevron-core/{approved-version}.json
config/wb/regions.json
config/wb/collection_plans/shevron-moscow-rostov-top100-pilot-v1.json
app/serp/collection_plan.py
tests/test_wb_collection_plan.py
tests/test_wb_query_pack_legacy_compat.py
docs/WB_QUERY_PACKS_REGIONS.md
```

Required behavior:

- load and validate immutable pack, region registry and plan;
- expand only enabled selected queries/regions;
- prove that the first shevron pack has the same normalized text and order as
  `exports/queries.txt`;
- reject missing/duplicate IDs, unknown category/region, disabled references,
  unsupported depth and enabled plans without owner-approved region inputs;
- no network calls and no change to existing CLI behavior.

Acceptance gate: full tests and unchanged legacy dry-run output.

### Stage 2: isolated no-publish runner

New files:

```text
scripts/run_wb_collection_plan.py
tests/test_wb_collection_plan_runner.py
tests/test_wb_scoped_paths.py
```

Minimal modifications:

```text
app/serp/engine.py
app/serp/runner.py
app/common/paths.py
app/common/run_report.py
app/common/cli.py
```

Required behavior:

- add an explicit CLI command such as
  `collection-plan --plan-file ... --no-publish`;
- leave `run serp` and all existing aliases unchanged;
- resolve destination before each region scope and record safe evidence;
- build one serial scope at a time;
- use one unchanged proxy/header/cookie contour;
- disable rotation and cross-region retries for the pilot;
- never call `publish_latest_output()` in `publication_mode=none`;
- never write `exports/products_for_sellers.csv`;
- never write `state/run_reports/latest.json`;
- use scoped checkpoints and manifests;
- acquire locks in the same order as production: non-blocking
  `state/locks/products_sellers_daily.flock`, then the pipeline lock, then
  `state/locks/wb_collection_plan.flock`;
- reject execution if production collection or warehouse locks are occupied.

Acceptance gate: filesystem tests prove that no protected path is opened for
write.

### Stage 3: pilot tests and guarded execution

Focused tests:

- pack/plan/region schema validation;
- legacy TXT input order and deduplication unchanged;
- task identity includes pack, query and region;
- checkpoint isolation across identical query text in two regions;
- request parameters contain the scope destination;
- missing geo response, missing `xinfo.dest`, equal destinations and changed
  egress fail closed;
- HTTP 429/498/invalid JSON/empty products fail the scope without rotation;
- page size/depth enforcement;
- no-publish blocks all global latest, exports, run-report latest and warehouse
  writes;
- active nightly lock blocks regional execution;
- partial one-region output cannot be presented as complete;
- secrets and full IP are absent from logs/manifests.

Full project tests and `scripts/run_pre_push_check.sh` remain mandatory.

### Stage 4: optional scoped latest

Requires owner approval after pilot review.

Potential modifications:

```text
app/common/paths.py
app/serp/runner.py
scripts/notify_wb_collection_plan.py
docs/WB_QUERY_PACKS_REGIONS.md
tests/test_wb_scoped_publication.py
```

Publication is atomic per `collection_plan_id + region_id`. A failed or partial
region must retain its previous scoped latest. Global latest remains untouched.
The notifier must identify plan, pack version, region completeness and
freshness; it must not use the nightly notification title.

### Stage 5: warehouse migration

Requires a separate owner approval and backup/check gate.

Proposed changes in `parser_wb`:

```text
scripts/wb_warehouse.py
tests/test_wb_warehouse.py
docs/WB_WAREHOUSE.md
```

Schema additions to `product_snapshots`, `serp_pages` and `query_positions`:

```text
collection_scope        -- global | regional
collection_plan_id
query_pack_id
query_pack_version
query_id
category_id
region_id
region_name
dest_id_observed
dest_verified_at_utc
```

Recommended identity for regional query positions:

```text
run_id + collection_plan_id + region_id + query_id + product_id
```

Migration rules:

- legacy rows backfill `collection_scope='global'`;
- legacy identity fields remain null, except an explicit synthetic
  `collection_plan_id='legacy-nightly'` only if approved and documented;
- regional sources are discovered through a new explicit glob, not by widening
  the legacy glob;
- duplicate checks include region and query identity;
- manifest gains `schema_version`, per-scope/per-region row counts and source
  completeness;
- build remains deterministic and dry-run capable;
- old warehouse remains the rollback generation until build and check pass.

Do not expose regional tables through API in the same deployment step.

### Stage 6: Parser Data API migration

Separate repository:

```text
/home/pavel/projects/parser_data_api/parser_data_api/main.py
/home/pavel/projects/parser_data_api/parser_data_api/wb_aggregates.py
/home/pavel/projects/parser_data_api/parser_data_api/wb_store_comparison.py
/home/pavel/projects/parser_data_api/tests/test_api.py
/home/pavel/projects/parser_data_api/README.md
/home/pavel/projects/parser_data_api/docs/WB_SELLER_GUIDE.md
```

Required API contract:

- existing endpoints default to `collection_scope=global`;
- additive filters: `collection_scope`, `collection_plan_id`,
  `query_pack_id`, `query_pack_version`, `category_id`, `query_id`,
  `region_id`;
- `collection_scope=all` must be explicit and cannot silently aggregate
  positions across regions;
- comparison endpoints require the same region on both dates unless a dedicated
  cross-region comparison endpoint is used;
- freshness/quality metadata is per region and plan;
- cursor scope includes all new filters;
- OpenAPI documents that `dest_id_observed` is an internal dated observation,
  not a public stable WB identifier.

Recommended dedicated endpoint:

```text
GET /warehouse/wb/aggregates/regional-serp-comparison
```

It should compare two explicit region IDs for one pack/version/date and return
intersection, region-only products, position deltas, repeatability evidence and
complete/partial metadata. It remains read-only.

Acceptance gate: backward-compatibility tests prove unchanged responses for
default global-only requests.

### Stage 7: scheduling

Not part of the pilot.

Only after several successful manual runs may the owner approve a separate
schedule. It must:

- use a separate wrapper and log;
- take the shared production exclusion lock plus a plan-specific lock;
- avoid the preflight/nightly window;
- have an explicit request budget and no implicit full collection;
- stop before the nightly run if its deadline would overlap;
- never modify the nightly cron entry;
- report regional status separately.

## 5. Pilot protocol: 3 queries x top-100 x 2 regions

Pilot regions:

- Moscow;
- Rostov-on-Don.

Pilot queries, subject to owner confirmation:

- `шеврон`;
- `шевроны`;
- `шеврон на липучке`.

Protocol:

1. Confirm clean Git state, no active collection and free production locks.
   Acquire the daily, pipeline and plan locks in the documented order before
   resolving egress or destination.
2. Record SHA-256 for runtime, cookie, headers, `exports/queries.txt`, global
   latest, run-report latest, warehouse DB/manifest and crontab.
3. Record masked egress plus one experiment-local salted hash.
4. Resolve both destinations from approved coordinates/address labels.
5. Require two distinct non-empty destination values.
6. Resolve the usable endpoint once using the existing ordered endpoint list.
   Record primary/fallback statuses, then pin the successful endpoint only for
   this no-publish pilot.
7. Collect regions serially, one HTTP page per query, maximum 100 products.
8. Check egress before region A, between A and B, and after B.
9. Repeat the first Moscow query after Rostov as A-B-A control.
10. Write only scoped pilot run files and manifests.
11. Compare Moscow vs Rostov only after checking Moscow repeatability.
12. Recompute all protected SHA-256 and crontab hash.
13. Stop and report. Do not publish, run sellers, refresh warehouse or notify
    as a nightly run.

Expected request budget:

- 2 WB geo requests;
- at most 2 endpoint-resolution requests;
- 6 regional search page requests;
- 1 Moscow repeat page;
- total maximum: 11 WB HTTP requests;
- neutral egress checks are separate and use the same proxy.

Any retry, proxy rotation or second page is forbidden in the pilot.

## 6. Pilot quality criteria

Collection completeness:

- 3/3 queries attempted for each region;
- 6/6 regional pages HTTP 200;
- exactly 100 product rows per query/region;
- no duplicate `(region_id, query_id, product_id)` within a page;
- no malformed product IDs;
- endpoint, pack version and destination evidence recorded.

Isolation:

- one unchanged egress identity throughout;
- no rotation call;
- no cookie/header/runtime change;
- no protected SHA change;
- global latest/run report/warehouse/crontab unchanged;
- no sellers call.

Comparison quality:

- Moscow repeat Jaccard is recorded for all repeated controls;
- recommended minimum control Jaccard: `0.95`;
- regional difference is considered evidence only when it exceeds the observed
  A-A repeat variation;
- position and membership differences are reported separately;
- no claim of demand, sales or causal ranking effect is made from SERP alone.

Failure policy:

- fail closed on any missing region proof, changed egress, HTTP error, payload
  anomaly, duplicate row, short page or protected SHA change;
- retain scoped diagnostics only;
- do not continue to the other region after a channel/identity failure.

## 7. Nightly protection

- Pilot runs only in a daytime maintenance window approved by the owner.
- The regional launcher must take a non-blocking shared exclusion lock before
  any HTTP request.
- The launcher calculates a hard deadline before the 23:45 preflight window
  and refuses to start or stops safely before overlap.
- Proxy rotation remains disabled.
- Cookie keeper and cookie promotion are not invoked by the regional runner.
- Existing `validation.max_error_ratio.serp`, endpoints, throttling and nightly
  wrapper are not changed.
- Regional errors never write global state and never trigger sellers or
  warehouse.

## 8. Rollback

Before warehouse/API stages:

- disable the collection plan;
- stop the separate regional launcher;
- remove only scoped latest pointers if they were approved;
- leave immutable run artifacts for audit or remove them later through a
  separately approved retention action;
- no legacy rollback is needed because global code paths and data remain
  unchanged.

After warehouse migration:

- restore the previous verified warehouse generation;
- disable the explicit regional source glob;
- verify global query counts and API global-only responses.

After API migration:

- deploy the previous Parser Data API Git version;
- confirm `/health`, OpenAPI and global WB smoke;
- warehouse data can retain regional rows because the previous API does not
  discover or expose them.

Cron rollback is out of initial scope. If a later regional schedule is approved,
its block must be independently removable without touching the current
`parser_wb products_sellers_daily` block.

## 9. Open questions and unconfirmed assumptions

1. Owner approval of the exact coordinates/address labels is pending.
2. The two observed `dest` values are not confirmed stable and are not approved
   for hardcoding.
3. `get-geo-info` and consumer search are internal, undocumented interfaces;
   there is no official stability/SLA statement.
4. Modern successful search responses do not consistently echo the accepted
   destination.
5. The acceptable control repeatability threshold needs owner confirmation.
6. Mobile egress may rotate outside a short pilot; long-run same-IP guarantees
   are unproven.
7. Whether additional packs need sellers enrichment is undecided. Initial
   recommendation is no.
8. Retention for scoped raw/staging artifacts is undecided.
9. Warehouse migration strategy (in-place v2 rebuild versus separate regional
   tables/views) needs a dedicated design review.
10. Parser Data API naming and whether a dedicated regional comparison endpoint
    is preferred need approval in the API repository.
11. No schedule, request budget beyond the pilot, or production SLA has been
    approved.

## 10. Acceptance sequence

```text
research accepted
  -> owner approves Stage 1 only
  -> schema/loader tests
  -> owner approves Stage 2 only
  -> isolated no-publish runner tests
  -> owner approves bounded pilot
  -> pilot report and protected-state audit
  -> STOP
  -> owner separately approves scoped publication and/or warehouse/API stages
```

The mandatory state after this plan is: implementation not started, awaiting
owner approval.
