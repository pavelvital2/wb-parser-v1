# WB regional SERP and query packs: evidence report

Date: 2026-07-26

Status: research only

Branch baseline: `0b6effee151c97887b6c7d78901471a5c98266f6`

## 1. Scope

This report answers two questions:

1. Can WB page-1 search results for two destinations be collected through the
   same external IP without changing the production proxy, cookies or headers?
2. How should additional query groups be represented without breaking the
   current nightly `exports/queries.txt -> serp -> sellers -> warehouse` path?

No parser, configuration, cron, runtime, query list, warehouse, API or
production data was changed. One bounded read-only/no-publish HTTP experiment
was used because repository data and official documentation establish that
geolocation matters, but do not prove the current internal destination
mechanism or same-IP feasibility.

## 2. Evidence hierarchy and limitations

Evidence was considered in this order:

1. current `parser_wb` code and saved parser outputs;
2. official Wildberries documentation;
3. current responses from WB hosts through the existing production access
   channel;
4. an external historical technical source only as supporting context.

Wildberries does not document its consumer search endpoint or the `dest`
parameter in the official Seller API. The following are therefore facts only
for the observed versions and dates, not stable public contracts:

- `user-geo-data.wildberries.ru/get-geo-info`;
- `www.wildberries.ru/__internal/u-search/exactmatch/ru/common/v18/search`;
- `search.wb.ru/exactmatch/ru/male/v18/search`;
- the `xinfo` response shape and `dest` values.

The report does not claim that a `dest` value is permanent, globally unique,
or sufficient for every WB surface. No destination IDs are proposed as
hardcoded constants.

## 3. Current parser architecture

### 3.1 Query input

| Area | Current behavior | Constraint / extension point |
|---|---|---|
| Runtime query list | `config/config.yaml` points `serp.input_files.queries_txt` to `exports/queries.txt`; the active file has 30 lines | This is the production nightly contract and must remain the default |
| Filter fallback | `_load_query_tasks()` first reads `data/marts/filter/latest/top_queries.csv` when it exists, then adds unseen lines from `exports/queries.txt` | The union is deduplicated by normalized query text, not by a stable query ID |
| Task model | `app/serp/engine.py::QueryTask` contains only `query` and `niche` | There is no `category_id`, `query_pack_id`, `query_id`, plan, depth or region |
| Query group | CSV `query_group` is populated from `QueryTask.niche`; TXT queries use an empty value | All 27,088 rows in the 2026-07-25 latest product mart have an empty `query_group` |
| Checkpoint key | `query + "|" + page` | Two packs or regions with the same query would collide inside one run |

### 3.2 WB request

`SerpEngine` creates one `requests.Session` for the run. It applies:

- configured headers plus the ignored runtime header file;
- optional cookie;
- one configured proxy URL;
- global `serp.request_params`;
- `query` and `page` overrides per request;
- primary endpoint followed by configured fallback endpoints;
- retries, deferred retry and optional proxy rotation after page errors.

The tracked config contains one global `request_params.dest`. It is not attached
to `QueryTask` and is not written to page index, product rows, run report or
warehouse. The current endpoint selection is also run-global.

This means changing `dest` globally can affect a complete run, but the resulting
rows cannot prove which destination produced them. Changing `dest` between
tasks without expanding row identity would mix regional observations under the
same logical keys.

### 3.3 Storage and publication

Current paths are run-scoped only by component and `run_id`:

```text
data/raw/serp/{run_id}/
data/staging/serp/{run_id}/
data/marts/serp/{run_id}/
```

If SERP quality is within `validation.max_error_ratio.serp`, the same filenames
are copied to:

```text
data/raw/serp/latest/
data/staging/serp/latest/
data/marts/serp/latest/
```

The published mart is also copied to `exports/products_for_sellers.csv`.
There is no scope dimension in `ProjectPaths.publish_latest_output()`.
Consequently, a regional run through the current `SerpEngine.run()` could
overwrite global latest and feed regional results to sellers.

The page index and product schemas contain `query` and `query_group`, but no
region, destination, query pack, collection plan or publication scope.

### 3.4 Run reports, cron and notification

- `app/common/runner.py` creates a global timestamp `run_id`, takes the common
  pipeline lock and writes `state/run_reports/{run_id}.json` plus
  `state/run_reports/latest.json`.
- The report stores pipeline/component totals, notes and file references, but
  no region or query pack identity.
- User crontab starts `scripts/run_products_sellers_daily.sh` at `00:15` MSK.
- The wrapper performs preflight, `serp`, `sellers`, then non-fatal warehouse
  refresh. It retries SERP once after a one-hour cooldown.
- `scripts/notify_products_sellers_daily.py` counts queries from
  `exports/queries.txt` and reads only global latest files.

An additional plan must therefore have a separate launcher/report scope and
must not reuse the nightly notifier as if it were the legacy collection.

### 3.5 Warehouse

`scripts/wb_warehouse.py` rebuilds from:

```text
data/marts/serp/*/products_daily.csv
data/marts/sellers/*/sellers_daily.csv
data/marts/sellers/*/seller_query_product_bridge.csv
data/raw/serp/*/pages_raw_index.csv
state/run_reports/*.json
```

The `query_positions` view contains `run_id`, date, `query`, `query_group`,
position and product/supplier metrics. It has no region, destination, query pack
or collection plan columns. Its current natural observation identity is
effectively date/run + query + product, which is insufficient once one query is
collected for multiple destinations.

The latest manifest observed during research contained 878,994
`product_snapshots` and 9,308 `serp_pages`. It has no schema version field.

Regional directories must not match the current warehouse globs until a schema
migration is implemented and validated.

### 3.6 Parser Data API

The live `http://127.0.0.1:8787/openapi.json` exposed 56 total paths and 25 WB
warehouse paths during research. No OpenAPI parameter name matched
`region`, `dest` or `location`.

Current WB endpoints query the regionless `query_positions` view. Adding
regional rows before API migration would make market and store analytics merge
different destinations. API responses need explicit region filters and an
explicit scope policy before regional warehouse data becomes visible.

## 4. What Wildberries officially confirms

Official WB seller documentation confirms that location is relevant to search:

- In
  [Search results and product ranking](https://seller.wildberries.ru/instructions/ru/ru/material/item-search-results-and-ranking),
  WB states that delivery zones and delivery/assembly time influence search
  placement, and that delivery time is measured from product storage to the
  buyer's receiving location. The same page lists geolocation among conditions
  that can change a position observed during a check.
- In
  [Geography of orders](https://seller.wildberries.ru/instructions/ru/ru/material/regional-shipment-report),
  WB defines locality using the delivery pickup point and source warehouse and
  reports order/delivery metrics by region.
- The official
  [WB API reports documentation](https://dev.wildberries.ru/en/docs/openapi/reports)
  exposes seller sales grouped by regions, but does not document consumer SERP,
  `dest`, or a public destination lookup contract.

These sources prove that buyer destination can be relevant to ranking and
availability. They do not prove how the current website encodes a destination.

## 5. Internal interface evidence

### 5.1 Repository and saved responses

- `config/config.yaml` sends a global `dest` in every SERP request.
- Some saved 2026-06 raw responses contain a response-side
  `params.dest`. Recent successful 2026-07 responses often omit `params`
  entirely.
- The saved values do not identify a city and are not sufficient to build a
  destination registry.

An old external
[Stack Overflow answer](https://ru.stackoverflow.com/questions/1512200/)
describes obtaining `xinfo` from `user-geo-data.wildberries.ru/get-geo-info`
using latitude, longitude and address. This is an unofficial 2023 source and
was used only to identify a hypothesis for direct verification.

### 5.2 Coordinates used by the experiment

Coordinates were obtained immediately before the WB experiment through
OpenStreetMap Nominatim:

- [Moscow lookup](https://nominatim.openstreetmap.org/search?format=jsonv2&limit=1&q=%D0%9C%D0%BE%D1%81%D0%BA%D0%B2%D0%B0%2C%20%D0%A0%D0%BE%D1%81%D1%81%D0%B8%D1%8F):
  `55.6255780, 37.6063916`;
- [Rostov-on-Don lookup](https://nominatim.openstreetmap.org/search?format=jsonv2&limit=1&q=%D0%A0%D0%BE%D1%81%D1%82%D0%BE%D0%B2-%D0%BD%D0%B0-%D0%94%D0%BE%D0%BD%D1%83%2C%20%D0%A0%D0%BE%D1%81%D1%81%D0%B8%D1%8F):
  `47.2222596, 39.7198736`.

These are experiment inputs, not WB region identifiers.

## 6. Bounded same-IP smoke

### 6.1 Safety envelope

Time: 2026-07-26, approximately 10:31-10:34 MSK.

- active production collection: none;
- daily lock: free;
- existing proxy, cookies, headers and runtime used without modification;
- proxy rotation endpoint: not called;
- parser/wrapper/sellers/warehouse: not started;
- only query `шеврон`;
- only page 1 and at most 100 products per response;
- all response data held in memory; no raw/staging/marts/report/latest output;
- egress checked through the same proxy before, between and after region calls;
- full external IP was never printed or stored.

The first attempt made one WB geo request and then stopped locally before the
search HTTP request because the non-ASCII Referer was not URL-encoded. The
corrected complete experiment made eight WB HTTP requests. Total WB HTTP
requests across both attempts: nine, below the limit of twelve.

### 6.2 Observed destination resolution

On 2026-07-26, the internal geo endpoint returned HTTP 200 and an `xinfo`
containing only `appType`, `curr`, `dest`, and `spp`:

| Experiment label | Observed `dest` |
|---|---:|
| Moscow | `-535680` |
| Rostov-on-Don | `-2228364` |

These are dated observations. They must not be treated as stable configuration
until repeated verification and owner approval.

### 6.3 Search results

The primary internal endpoint returned HTTP 498 for each search. The existing
configured `search.wb.ru/.../male/v18/search` fallback returned HTTP 200 and
100 products.

| Comparison | Intersection | Left only | Right only | Jaccard | Same absolute position |
|---|---:|---:|---:|---:|---:|
| Moscow A1 vs Rostov B | 88 | 12 | 12 | 0.785714 | 16 |
| Moscow A1 vs Moscow A2 | 100 | 0 | 0 | 1.000000 | 100 |

The top-10 and top-100 hashes differed between Moscow and Rostov. Both hashes
were identical for the Moscow repeat.

Egress identity remained constant at masked form `81.222.x.x`; all four checks
had the same ephemeral salted SHA-256 prefix `65b278001083855d`. The salt was
not persisted, so this fingerprint is evidence only inside this experiment.

### 6.4 Conclusion supported by the smoke

For the tested query, endpoint version, credentials and time window:

- two distinct geo-derived destination values were sent through one unchanged
  external IP and both searches returned HTTP 200 through the configured
  fallback;
- the requests produced materially different top-100 results;
- repeating Moscow after Rostov reproduced the same 100 products in the same
  order;
- therefore the A-B-A result is strong evidence that same-IP regional
  comparison is technically feasible without proxy rotation for a bounded
  serial collection.

This is an inference from the controlled request difference and repeat control.
The modern search payload did not echo the destination, so it does not
independently prove which destination the server applied.

This does not prove:

- long-term stability of `dest` or `get-geo-info`;
- that every query or product category differs by region;
- that the destination was the only possible source of variation;
- that a multi-hour regional run will keep one mobile egress IP;
- that WB permits or supports these undocumented interfaces;
- that responses are free from bot-specific randomization. WB itself warns
  that bots may receive random search responses.

## 7. Query-pack requirements

A versioned registry should represent identity separately from execution:

- `category_id`: stable business grouping;
- `query_pack_id` and immutable version;
- `query_id`: stable identity inside a pack;
- query text;
- enabled/disabled state;
- optional owner/source metadata.

A collection plan should reference one exact pack version and define:

- `collection_plan_id`;
- enabled/manual/scheduled state;
- depth;
- schedule identity, not a cron expression embedded in query data;
- region set;
- publication mode;
- independent quality thresholds;
- endpoint policy and rotation policy;
- compatibility mode.

The current 30-line `exports/queries.txt` must remain the implicit
`legacy-nightly` plan until an explicitly approved migration. Query packs must
not silently replace, regenerate or reorder it.

## 8. Architecture options

| Option | Reliability | Anti-bot risk | Operations | Compatibility | Data quality / cost |
|---|---|---|---|---|---|
| A. Rewrite global `exports/queries.txt` and global `dest`, then run existing SERP | Low: mutable shared inputs and global latest | High: easy to trigger full retries/rotation | Superficially simple, hard to audit/rollback | Poor: can overwrite nightly latest and feed sellers | Lowest implementation cost, unacceptable provenance |
| B. One multi-pack/multi-region `SerpEngine` run with region fields on each task | Medium: one process and session, but failures/checkpoints cross scopes | Medium/high: one bad region can trigger retries or rotation for all | One launcher, complex state and partial recovery | Requires invasive changes to task keys, reports and publication | Efficient requests; high coupling and migration risk |
| C. Versioned packs plus serial per-plan/per-region run contexts and scoped storage | High: each scope validates and fails independently | Lowest practical risk: bounded serial calls, no rotation in pilot | More explicit files, but inspectable and reversible | Best: legacy path remains untouched | More metadata/storage; strongest provenance and warehouse/API path |

## 9. Recommendation

Choose option C.

The collection plan should expand a pack into serial region-specific scopes.
Every row and report must carry explicit plan, pack, query and region identity.
Regional outputs must use a separate component namespace such as
`serp_scoped`; current global `serp/latest`, sellers input, run-report latest
and warehouse globs remain untouched.

The first implementation must be manual and no-publish. Only after the
3-query, 2-region pilot passes should the owner decide whether to authorize
scoped latest, warehouse migration, API exposure or scheduling.

## 10. Open questions

1. Is `get-geo-info` acceptable as an operational dependency despite being
   undocumented?
2. Should destination values be resolved for every run, or cached with a short
   verification TTL? Current evidence is insufficient to choose a stable TTL.
3. Can WB return a response-side destination proof for successful modern
   search responses? Recent payloads omit `params`.
4. What level of Moscow A-B-A repeatability is required before regional
   differences are considered significant?
5. Should regional plans ever allow proxy rotation? The pilot recommendation
   is fail closed with rotation disabled.
6. Which additional query categories and owners are approved after the legacy
   shevron pack?
7. Should sellers enrichment run for regional SERP? Recommendation for the
   first scope is no; seller identity is not region-specific and the existing
   global sellers path must remain isolated.
8. How long should scoped raw/staging data be retained?
9. Should Parser Data API default to global-only data or require an explicit
   `scope=global|regional|all` once regional data exists? Recommendation:
   default global-only.

Implementation details and stop gates are defined in
`docs/tasks/WB_REGIONAL_QUERY_PACKS_PLAN_20260726.md`.
