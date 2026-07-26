# WB Four-Region Nightly ExecPlan

Date: 2026-07-26

## Scope

Phase A prepares, but does not schedule or execute, a four-region WB pipeline:

`regional SERP -> deduplicated sellers -> regional warehouse -> notification state`

Target collection regions, in fixed order:

1. `moscow`
2. `rostov-on-don`
3. `novosibirsk`
4. `kazan`

The plan uses the pinned `shevron-core@2026-07-26.1` pack in its existing
30-query order and maximum depth 1000. That is at most 10 pages per query,
300 pages per region, 1200 pages and 120000 position rows per complete run.
Queries with a smaller proven WB payload total finish on their terminal page,
so successful actual totals may be lower.

Parser Data API changes are a separate repository and stage.

## Current Flow

The production cron calls `scripts/run_products_sellers_daily.sh` at 00:15
MSK. It performs the legacy global `SERP -> sellers -> WB warehouse ->
Telegram` flow and publishes global `latest`. Its wrapper, cron, runtime,
cookies, headers, proxy and data are protected in Phase A.

The existing collection-plan runner is isolated and no-publish. For depth
greater than 500 it already provides:

- serial region/query execution;
- one immutable resumable segment per region/query, at most 10 pages;
- proxy-only transport and source/transport fingerprints;
- constant egress verification inside each segment;
- fail-closed checkpoint and raw-page validation;
- atomic plan-level regional latest publication only after every region is
  complete.

## Target Contracts

### Collection

- New plan: `shevron-four-regions-top1000-v2`.
- Plan and all four region entries remain disabled until an audited live gate.
- Resolver output is recorded as resolved-and-sent evidence, not proof that
  WB applied a destination server-side.
- Normal page/query pacing and request timeout come only from:
  `serp.sleep_between_pages_ms`, `serp.sleep_between_queries_ms` and
  `runtime.http_timeout_seconds`.
- No endpoint switch outside the existing ordered primary/fallback policy.
- No direct network fallback and no proxy rotation.
- No global latest, sellers, warehouse or notification stage starts from a
  partial collection.
- For v2 only, payload `total` is mandatory and stable inside a query segment.
  A short final page is accepted only when its exact count agrees with
  `min(total, 1000)` and preceding 100-item pages. Empty, inconsistent or
  anti-bot payloads fail the segment. Existing v1 plans remain strict.

### Bounded Runtime

The 1200-page plan must not use `pages * all endpoints * full timeout` as a
claim that the entire run will complete in one invocation. That value exceeds
one day and makes a resumable plan impossible to start.

The reviewed v2 contract is:

- new run start: 00:15 MSK through 00:45 MSK;
- maximum one invocation: 21600 seconds (6 hours);
- absolute cutoff: 23:00 MSK, 75 minutes before the next 00:15 boundary;
- resume start requires at least 1800 seconds before the effective deadline;
- finalization reserve: 60 seconds;
- effective invocation deadline is the earlier of start + 6 hours and 23:00.

The safety estimate covers the next atomic query segment, not all remaining
pages. It includes up to 10 pages, every configured endpoint timeout, exact
production page/query pacing, resolver/egress allowance and finalization
reserve. A new or resumed invocation starts only when this segment can finish
inside the bounded window.

The deadline is checked before each resolver, egress and search network
attempt, before filesystem publication and before final manifest publication.
At cutoff an unfinished segment is discarded. Earlier verified segments stay
immutable and reusable; at most the current 10-page query repeats. Sellers and
warehouse remain blocked until all 120 verified segments are complete.
Observed durations from run reports may be reported, but never replace this
hard runtime contract.

Downstream uses a separate bounded invocation under the same absolute cutoff.
Its seller checkpoint is rechecked before every seller HTTP attempt, and
warehouse/final pointer publication recheck the deadline. A completed SERP run
whose downstream window expires is continued with an explicit
`--downstream-only-run-id`; SERP is not repeated.

Before final cutover, downstream additionally runs under the explicit
`pre_cutover_legacy_nightly_protected_v1` execution contract. Before taking
any shared lock it rejects starts during the protected interval beginning at
the plan's 00:15 MSK boundary and rejects starts with less than the plan's
minimum resume window before the next 00:15 boundary. The protected interval
uses the reviewed maximum invocation duration from the plan. The final
supervisor/cutover must replace this mode in a separate reviewed change; it
must not silently bypass the guard.

### Scoped Sellers

A complete four-region generation is converted deterministically into:

- one deduplicated product input keyed by `nmId`, choosing the first row by
  plan region order, query order and absolute position that has a non-empty
  `supplier_id`; if every occurrence is missing a supplier, the first row is
  retained and counted in `missing_supplier_products`;
- a full region-query-product-position bridge retaining every actual position
  row, up to the 120000-row capacity;
- one seller request per unique `supplier_id`, regardless of how many regions
  contain its products.

Seller raw/staging/marts and checkpoints are scoped by plan and run. They do
not publish legacy seller latest. Seller selection never filters or rewrites
the full position bridge. Success and failure preview state expose
`missing_supplier_products`; it is null when complete seller inputs were not
available to calculate it.

### Regional Warehouse

Regional data is stored separately from `data/warehouse/wb` in
`data/warehouse/wb_regional`. New position facts are idempotent by
`run_id, region_id, query_id, absolute_position`; `product_id` is an attribute.
Repeated products at different positions are preserved and reported as
`duplicate_product_positions`. Product deduplication is applied only to seller
input.

`regional_query_positions` preserves the existing analytical surface where
the source provides it: collection time/status, `imt_id`, `brand_id`,
supplier name, final/base/sale prices, discount, rating, feedbacks, quantity
and source provenance. `regional_seller_snapshots` likewise retains the legacy
query/product counts and references. Row hashes cover all retained business
fields.

Run quality is warehouse-owned, not reconstructed by API readers from parser
state. `regional_run_quality` stores one region/run fact with source hashes,
actual/max query/page/position counts, status, timing, endpoint usage and
duplicate-position quality. `regional_query_quality` stores terminal payload
total, capped total, actual pages/positions, terminal reason, duplicate count,
segment hash and egress verification for every region/query. These tables are
the Phase A source contract for future regional summary, changes, movers,
seller changes, run-quality and aggregate API work.

The scoped `regional_run_quality.source_row_sha256` is computed from every
retained column except the hash itself, including timing/deadline, query-pack
provenance, actual and maximum counters, endpoint usage and source manifest
hash. Any retained-field change therefore changes the row identity.

Existing global WB warehouse history is migrated read-only into the regional
warehouse with:

- `region_id=yaroslavl`;
- `region_name=Ярославль`;
- `region_provenance=legacy_global_assigned_yaroslavl`.

This preserves and labels historical data. `yaroslavl` is not a future
collection region and is not included in the new four-region plan.

Legacy import is a transactional, set-based DuckDB `ATTACH`/`INSERT SELECT`
sync. Stable keys and per-row SHA-256 values permit new legacy runs to append.
Changing or removing any already imported legacy row fails closed and rolls
back the entire sync. A whole DuckDB file hash is not used as data identity,
because the unchanged production warehouse continues to rebuild before
cutover. Legacy `query_positions`, `seller_daily_metrics` and
`daily_run_quality` are synchronized so historical analytics remain available.

Legacy run-quality counts are observational because historical plans did not
carry the regional expected-count contract. For each legacy `run_id`,
`queries_expected/queries_ok`, `pages_max/pages_ok` and
`positions_max/positions_ok` all contain the corresponding distinct/actual
counts derived from `query_positions`. They are zero when that run has no
position facts. `items_ok`, `items_error` and `components_count` remain the
independent source metrics from `daily_run_quality`; `items_ok` is never
relabelled as a position count. `duration_seconds` is recomputed from a valid
RFC 3339 UTC `started_at_utc`/`finished_at_utc` pair and is null when that pair
is invalid or reversed.

Regional warehouse and legacy sync must use the shared bounded DuckDB
connection contract: `memory_limit=1GiB`, at most two threads and a private
mode-`700` spill directory below ignored
`data/warehouse/wb_regional/tmp`. Clean shutdown removes its own session
directory; a later locked invocation may remove only stale `session-*`
directories older than 24 hours. Opening the legacy sync through an unbounded
DuckDB connection fails closed. The sync checks DuckDB's actual
`current_setting(memory_limit)`, `current_setting(threads)` and
`current_setting(temp_directory)` values; an application-owned marker cannot
substitute for the active engine settings.

Offline full-source rehearsal on 2026-07-26 used a copied/read-only production
source and a temporary regional database. With the production runtime contract
it completed first sync plus `no_changes` sync in 20.76 seconds at
`max_rss_kb=1261052`, preserving 878994 position facts, 24617 seller facts and
69 run-quality facts. The rehearsal did not open the production regional
database for writing.

The post-audit rehearsal after actual-setting and observed-quality validation
used the same bounded contract and full copied/read-only source. First sync was
`updated`, the immediate repeat was `no_changes`, elapsed time was 20.04
seconds and `max_rss_kb=1239648`. Target counts remained exactly 878994
positions, 24617 sellers and 69 run-quality rows.

### Notification

Phase A writes a scoped owner-report preview/state only. It includes every
region, total pages/positions, unique products, seller status/counts,
warehouse status and an explicit partial/failure reason. It does not send a
Telegram message and does not modify the existing notifier.

Once downstream starts, it is the sole writer of failure state. The state
records the actual stage, completed input totals, available seller result or
interrupted seller progress, warehouse status and a class-only sanitized
failure reason. The launcher may write a collection preview only before
downstream has begun.

## Locking And Publication

Collection retains the established lock order:

1. `products_sellers_daily.flock`
2. guarded `pipeline.lock`
3. `wb_warehouse_refresh.flock`
4. `wb_collection_plan.flock`

Downstream orchestration reacquires the same order, verifies immutable scoped
latest and source hashes, then writes only scoped seller/bridge, regional
warehouse and notification state. Any lock conflict fails before downstream
network or publication.

Regional latest pointers are visible only after the complete collection plan.
Downstream state is complete only after sellers and regional warehouse
succeed. Partial or interrupted runs cannot alter global latest or the prior
complete regional downstream generation.

## Migration Order

1. Merge Phase A code and disabled definitions after independent audit.
2. Perform an offline warehouse migration rehearsal and row/key checks.
3. Audit one controlled four-region no-publish live run.
4. Audit resume behavior if the controlled run is interrupted.
5. Validate deduplicated sellers and regional warehouse from that complete run.
6. Implement Parser Data API regional read-only contracts in its own repo.
7. Add Telegram delivery from the approved preview contract.
8. Replace the legacy cron only in a separate reviewed scheduling change.

## Rollback

- Keep the current cron and wrapper byte-identical until the final scheduling
  stage.
- Keep all new plans/regions disabled by default.
- A failed collection resumes by the same `run_id`; it never falls back to
  global publication.
- A failed seller or warehouse stage leaves the previous regional downstream
  generation visible.
- Disable the future schedule to return immediately to the unchanged legacy
  nightly flow. Historical Yaroslavl and scoped regional data are retained.

## Readiness Gates

- exact 30-query order and four-region order;
- depth 1000 maximum, 1-10 proven pages/query, at most 1200 pages total;
- exact production pacing/timeout source assertions;
- early start accepted, late start rejected, cutoff/resume verified;
- corruption and source/transport mismatch rejected before network;
- deterministic product dedup and complete position bridge;
- seller fetch count based on unique suppliers;
- separate warehouse, position-keyed facts and incremental Yaroslavl sync;
- failure leaves previous regional latest and all global latest unchanged;
- notification preview covers four regions and failure states;
- focused tests, full pytest, pre-push and protected hash comparison;
- explicit owner stop gate before any live run or cron change.
