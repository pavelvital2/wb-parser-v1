# TZ: WB Warehouse Aggregates For Parser Data API

Date: 2026-07-05

Owner: Parser VPS agent / `parser_wb`

Reviewer: Seller VPS lead agent

## Short Outcome

Add read-only aggregate endpoints to Parser Data API so Seller VPS agents can ask
business questions about WB visibility without downloading raw warehouse rows.

The goal is not to increase `limit` from 500 to 1000. The goal is to return
small, already aggregated datasets that are useful for SEO, card backlog,
promotion review, and niche/category analysis.

## Context

Current WB warehouse lives on Parser VPS:

```text
/home/pavel/projects/parser_wb/data/warehouse/wb/wb.duckdb
```

Parser Data API lives on Parser VPS:

```text
/home/pavel/projects/parser_data_api
```

Seller VPS accesses it through local tunnel:

```text
http://127.0.0.1:8787
```

Already available read-only endpoints:

```text
/warehouse/wb/summary
/warehouse/wb/query-positions
/warehouse/wb/daily-changes
/warehouse/wb/top-movers
/warehouse/wb/seller-changes
/warehouse/wb/run-quality
```

Current raw-row endpoint limit is intentionally capped at 500. Do not solve this
task by simply raising the limit.

## Hard Boundaries

Do not change:

- WB parser collection logic;
- cron;
- proxy;
- cookies;
- request headers;
- `runtime.env`;
- WB endpoints used by parser;
- `latest` publication logic;
- existing warehouse files;
- seller project code on Seller VPS.

Do not add write endpoints. Parser Data API remains read-only.

Do not copy full WB warehouse datasets to Seller VPS or seller project folders.

Do not use parser data as sales evidence. WB parser gives visibility, positions,
prices, ratings, feedbacks, seller/product context and parser-visible stock.
Sales, orders, margin, official stock and ad efficiency must be joined later on
Seller VPS from seller APIs/reports.

## Important Assumptions

1. Parser Data API may not know which products belong to a particular seller
   unless the caller provides filters such as `supplier_id`, `product_id`, or
   `brand`.
2. `total_quantity` in `query_positions` is parser-visible marketplace stock,
   not a guaranteed official seller stock number.
3. SEO candidates from parser are visibility candidates, not final SEO
   recommendations. Seller agent must join query demand/search frequency and
   card content before recommending title/tags/description changes.
4. Promotion candidates from parser are visibility candidates, not bid/apply
   recommendations. Seller agent must join sales, margin, stock and campaign
   data before promotion decisions.

## Store Ownership Rule

Parser Data API must not contain hardcoded store ownership logic.

It must not know:

- Vital Shevron own products;
- Vital Sewing own products;
- TAKTERRA own products;
- any fixed "our supplier_id" list.

Store ownership is resolved on Seller VPS.

Seller agents pass ownership filters explicitly:

- `supplier_id`;
- `product_id` / WB `nmID`;
- `brand` only as fallback;
- `exclude_supplier_id` for competitor analysis.

If no ownership filter is passed, aggregate endpoints return market-level
analytics, not "our products" analytics.

## Analytics Sources Used For This Scope

This scope is based on the analytics patterns used by marketplace sellers and
analytics tools:

- WB official Search Queries report/API: queries shoppers used, card ranks in
  search, ranking/visibility factors and position shifts.
- Ozon official analytics: search query analytics, product visibility and sales
  funnel are separate analytical blocks.
- Professional marketplace analytics services usually separate seller, brand,
  product, category/niche, SEO/search query, price, stock, promotion and
  profitability analytics.

Source links for documentation references:

```text
WB Search Queries API:
https://dev.wildberries.ru/en/knowledge-base/articles/019d49a4-0f6a-70bb-a87b-c1237a1a0714/search-queries-for-your-products

SellerStats feature overview:
https://sellerstats.ru/

Seerfar keyword optimization:
https://www.seerfar.com/en/features/

Similarweb on-site marketplace search optimization:
https://www.similarweb.com/corp/retail/on-site-search-optimization/

Signals.top Ozon analytics feature overview:
https://signals.top/en/

Market Ninja scraper feature overview:
https://www.marketninja.ru/en/ozon
```

## Design Principle: Raw Data vs Business Questions

Do not expose "more raw rows" as the main solution. Endpoints should answer
business questions:

- Where are our products by query?
- Which products are visible but weak?
- Which queries are uncovered?
- Who owns the top of the search page?
- Which competitors/brands/suppliers dominate?
- Which products gained or lost positions?
- Which products have price/rating/review disadvantages?
- Which niches look concentrated or weakly defended?
- Which products are SEO/promotion candidates after Seller VPS joins sales,
  stock, margin and ad data?

## Analytics Capability Matrix

Parser Data API can calculate these from WB warehouse:

- SERP positions;
- top-N visibility;
- query coverage;
- competitor/supplier/brand presence;
- price ranges and price position context;
- rating/review context;
- parser-visible stock context;
- new/lost products in top-N;
- position movement and SERP volatility;
- product/query/supplier/brand snapshots;
- market/niche visibility summaries.

Parser Data API must not claim these without Seller VPS data:

- sales;
- orders;
- revenue;
- margin;
- profitability;
- ad spend;
- ROAS/DRR/ACoS;
- official stock;
- conversion funnel;
- true demand/frequency unless a seller-side query report supplies it.

For those metrics, Parser API should return `seller_side_required_signals`.

## Implementation Phasing

Implement all P0 endpoints now. Add P1 endpoints if the implementation remains
small and tests are clear. If P1 makes the change too large, document them in
README as planned endpoints and stop after P0.

P0 means required for immediate Seller VPS analytics.
P1 means important for professional/niche analytics but can follow in the next
task if needed.

## Required Aggregate Endpoints

Implement under this prefix:

```text
/warehouse/wb/aggregates/...
```

All endpoints must require the existing bearer token, use DuckDB read-only mode,
and return compact JSON.

### P0. `/warehouse/wb/aggregates/store-query-positions`

Purpose: answer the owner's direct question: "What are our positions for this
exact query?"

Parameters:

```text
query: required exact string
date: optional YYYY-MM-DD, default latest warehouse date
top_n: optional int, default 30, min 1, max 100
supplier_id: optional, repeatable or comma-separated
product_id: optional, repeatable or comma-separated
brand: optional exact string fallback
include_competitor_context: optional bool, default true
limit: optional int, default 100, max 500
```

Return:

```text
run_date
query
top_n
ownership_filter_applied = true | false
matched_count
matched_in_top_n_count
best_matched_position
rows: [
  product_id
  product_name
  brand
  supplier_id
  supplier_name
  absolute_position
  position_status = top_n | outside_top_n
  final_price
  rating
  feedbacks
  total_quantity
]
competitor_context: optional [
  position
  product_id
  product_name
  brand
  supplier_id
  supplier_name
  final_price
  rating
  feedbacks
]
```

If ownership filters are provided and no rows are found, return:

```text
matched_count = 0
best_matched_position = null
rows = []
position_status_summary = not_found_for_filter
```

Do not hardcode store ownership.

### 1. `/warehouse/wb/aggregates/visibility-gaps`

Purpose: products that are visible in parser data but are outside `top_n`, have
parser-visible stock, and therefore may need SEO/card/promotion review.

Parameters:

```text
date: optional YYYY-MM-DD, default latest warehouse date
top_n: optional int, default 30, min 1, max 100
min_stock: optional int, default 1
supplier_id: optional, repeatable or comma-separated
product_id: optional, repeatable or comma-separated
brand: optional exact string
query: optional exact string
limit: optional int, default 100, max 500
```

Return rows:

```text
run_date
query
product_id
product_name
brand
supplier_id
supplier_name
best_position
top_n
position_gap
final_price
rating
feedbacks
total_quantity
visibility_status = outside_top_n
```

Rules:

- Deduplicate by `run_date + query + product_id`.
- Use best available `absolute_position` for the product/query/date.
- Include only rows where `best_position > top_n`.
- Include only rows where `total_quantity >= min_stock` when stock is present.

### 2. `/warehouse/wb/aggregates/query-coverage`

Purpose: show which queries have or do not have selected seller/products in top
N.

Parameters:

```text
date: optional YYYY-MM-DD, default latest warehouse date
top_n: optional int, default 30, min 1, max 100
supplier_id: optional, repeatable or comma-separated
product_id: optional, repeatable or comma-separated
brand: optional exact string
limit: optional int, default 100, max 500
only_gaps: optional bool, default false
```

Return rows:

```text
run_date
query
top_n
total_products_in_top_n
matched_products_in_top_n
matched_products_visible_anywhere
matched_best_position
matched_best_product_id
matched_best_product_name
matched_total_stock
coverage_status = covered | gap | no_filter
```

Rules:

- If no seller/product/brand filter is provided, return `coverage_status =
  no_filter` and general query summary.
- If a filter is provided and `matched_products_in_top_n = 0`, status is `gap`.
- `only_gaps=true` returns only `gap` rows.

### 3. `/warehouse/wb/aggregates/competitors-top`

Purpose: compact competitor picture inside top N for a query/date.

Parameters:

```text
query: required exact string
date: optional YYYY-MM-DD, default latest warehouse date
top_n: optional int, default 30, min 1, max 100
exclude_supplier_id: optional, repeatable or comma-separated
limit: optional int, default 100, max 500
```

Return rows grouped by supplier:

```text
run_date
query
supplier_id
supplier_name
products_in_top_n
best_position
avg_position
min_price
median_price
max_price
avg_rating
total_feedbacks
total_parser_stock
sample_product_ids
sample_product_names
```

Rules:

- Exclude suppliers from `exclude_supplier_id` after filtering top N.
- `sample_*` fields must be short and bounded, for example first 3 products.

### 4. `/warehouse/wb/aggregates/top-movers`

Purpose: aggregated growth/drop list, more business-friendly than raw
`/warehouse/wb/top-movers`.

Parameters:

```text
current_date: optional YYYY-MM-DD, default latest warehouse date
previous_date: optional YYYY-MM-DD, default previous available date
query: optional exact string
supplier_id: optional, repeatable or comma-separated
product_id: optional, repeatable or comma-separated
direction: optional enum both|improved|declined, default both
min_abs_delta: optional int, default 1
limit: optional int, default 100, max 500
```

Return rows:

```text
current_date
previous_date
query
product_id
product_name
brand
supplier_id
supplier_name
previous_position
current_position
position_delta
movement_status = improved | declined
current_final_price
previous_final_price
current_rating
previous_rating
current_feedbacks
previous_feedbacks
current_total_quantity
previous_total_quantity
```

Rules:

- Deduplicate by `date + query + product_id` before comparing.
- `position_delta = previous_position - current_position`.
- Positive delta means improved.

### 5. `/warehouse/wb/aggregates/seo-visibility-candidates`

Purpose: parser-side shortlist for seller SEO/card review.

Parameters:

```text
date: optional YYYY-MM-DD, default latest warehouse date
top_n: optional int, default 30
min_stock: optional int, default 1
supplier_id: optional, repeatable or comma-separated
product_id: optional, repeatable or comma-separated
brand: optional exact string
limit: optional int, default 100, max 500
```

Return rows:

```text
run_date
product_id
product_name
brand
supplier_id
supplier_name
visible_query_count
outside_top_n_query_count
best_position
worst_position
avg_position
total_parser_stock
avg_rating
total_feedbacks
sample_gap_queries
candidate_reason
```

Rules:

- This endpoint must not claim search demand.
- `candidate_reason` examples:
  - `stock_visible_but_outside_top_n`
  - `many_queries_low_visibility`
  - `needs_seller_side_demand_join`

### 6. `/warehouse/wb/aggregates/promotion-visibility-candidates`

Purpose: parser-side shortlist for later Seller VPS promotion review.

Parameters:

```text
date: optional YYYY-MM-DD, default latest warehouse date
top_n: optional int, default 30
min_stock: optional int, default 1
supplier_id: optional, repeatable or comma-separated
product_id: optional, repeatable or comma-separated
brand: optional exact string
limit: optional int, default 100, max 500
```

Return rows:

```text
run_date
product_id
product_name
brand
supplier_id
supplier_name
visible_query_count
outside_top_n_query_count
best_position
avg_position
total_parser_stock
median_final_price
avg_rating
total_feedbacks
sample_queries
candidate_reason
seller_side_required_signals
```

Rules:

- This is not a bid recommendation.
- `seller_side_required_signals` must include:
  `sales`, `official_stock`, `margin`, `campaign_state`.

### 7. `/warehouse/wb/aggregates/market-summary`

Purpose: compact per-query market snapshot for niche/category analysis.

Parameters:

```text
date: optional YYYY-MM-DD, default latest warehouse date
query: optional exact string
top_n: optional int, default 30
limit: optional int, default 100, max 500
```

Return rows:

```text
run_date
query
top_n
products_in_top_n
suppliers_in_top_n
top_supplier_id
top_supplier_name
top_supplier_products
top_supplier_share
min_price
median_price
max_price
avg_rating
median_feedbacks
total_parser_stock
low_review_products_count
low_rating_products_count
```

Rules:

- Use this endpoint only as market-visibility context.
- Do not infer sales or profit from it.

## P1 Aggregate Endpoints For Professional Analytics

Add these now if practical. If not, document them as next-scope endpoints and
make sure P0 code structure can support them without rewrite.

### P1. `/warehouse/wb/aggregates/product-position-history`

Purpose: position history for one product or selected store products across
queries.

Parameters:

```text
product_id: optional, repeatable or comma-separated
supplier_id: optional, repeatable or comma-separated
brand: optional exact string fallback
query: optional exact string
date_from: optional YYYY-MM-DD
date_to: optional YYYY-MM-DD
limit: optional int, default 500, max 500
```

Return rows:

```text
run_date
query
product_id
product_name
supplier_id
supplier_name
absolute_position
position_delta_vs_previous_seen_date
final_price
rating
feedbacks
total_quantity
```

Use cases:

- daily SEO monitoring;
- "did the card improve after changes?";
- promotion before/after visibility checks.

### P1. `/warehouse/wb/aggregates/query-product-matrix`

Purpose: compact matrix for a set of products across selected queries.

Parameters:

```text
date: optional YYYY-MM-DD, default latest warehouse date
query: optional repeatable or comma-separated
product_id: optional repeatable or comma-separated
supplier_id: optional repeatable or comma-separated
brand: optional exact string fallback
top_n: optional int, default 100
limit: optional int, default 500, max 500
```

Return rows:

```text
product_id
product_name
supplier_id
supplier_name
visible_query_count
top_10_query_count
top_30_query_count
outside_top_30_query_count
best_position
avg_position
queries_top_30
queries_outside_top_30
queries_not_seen_if_requested
```

Use cases:

- SEO coverage map;
- content backlog prioritization;
- query cannibalization check between own products.

### P1. `/warehouse/wb/aggregates/price-position-map`

Purpose: understand how price relates to positions for a query/top-N.

Parameters:

```text
query: required exact string
date: optional YYYY-MM-DD, default latest warehouse date
top_n: optional int, default 30
supplier_id: optional, repeatable or comma-separated
brand: optional exact string
limit: optional int, default 100, max 500
```

Return:

```text
run_date
query
top_n
min_price
p25_price
median_price
p75_price
max_price
own_or_filtered_min_price
own_or_filtered_median_price
price_position_rows: [
  absolute_position
  product_id
  product_name
  supplier_id
  supplier_name
  final_price
  price_band = low | mid | high | unknown
  rating
  feedbacks
]
```

Use cases:

- price competitiveness;
- repricing context;
- niche price-band selection.

### P1. `/warehouse/wb/aggregates/rating-review-gaps`

Purpose: find products that are weak against top competitors by rating/reviews.

Parameters:

```text
date: optional YYYY-MM-DD, default latest warehouse date
query: optional exact string
top_n: optional int, default 30
supplier_id: optional repeatable or comma-separated
product_id: optional repeatable or comma-separated
brand: optional exact string fallback
limit: optional int, default 100, max 500
```

Return rows:

```text
run_date
query
product_id
product_name
supplier_id
supplier_name
absolute_position
rating
feedbacks
top_n_median_rating
top_n_median_feedbacks
rating_gap
feedbacks_gap
gap_reason
```

Use cases:

- identify cards where review collection or product quality work is needed;
- compare our cards with current top competitors.

### P1. `/warehouse/wb/aggregates/brand-supplier-share`

Purpose: concentration and dominance view by query or across latest warehouse.

Parameters:

```text
date: optional YYYY-MM-DD, default latest warehouse date
query: optional exact string
top_n: optional int, default 30
group_by: optional enum supplier|brand, default supplier
limit: optional int, default 100, max 500
```

Return rows:

```text
run_date
query
group_by
entity_id
entity_name
products_in_top_n
share_in_top_n
best_position
avg_position
median_price
avg_rating
total_feedbacks
sample_products
```

Use cases:

- niche concentration;
- competitor dominance;
- brand pressure.

### P1. `/warehouse/wb/aggregates/new-lost-top`

Purpose: detect products/suppliers that entered or left top-N between two dates.

Parameters:

```text
current_date: optional YYYY-MM-DD, default latest warehouse date
previous_date: optional YYYY-MM-DD, default previous available date
query: optional exact string
top_n: optional int, default 30
entity: optional enum product|supplier|brand, default product
limit: optional int, default 100, max 500
```

Return rows:

```text
current_date
previous_date
query
entity
entity_id
entity_name
change_status = entered_top_n | left_top_n
previous_best_position
current_best_position
sample_products
```

Use cases:

- competitor alerts;
- new competitor detection;
- lost visibility checks.

### P1. `/warehouse/wb/aggregates/serp-volatility`

Purpose: measure how unstable a query is between dates.

Parameters:

```text
current_date: optional YYYY-MM-DD, default latest warehouse date
previous_date: optional YYYY-MM-DD, default previous available date
query: optional exact string
top_n: optional int, default 30
limit: optional int, default 100, max 500
```

Return rows:

```text
current_date
previous_date
query
top_n
products_current_top_n
products_previous_top_n
entered_count
left_count
same_product_count
avg_abs_position_delta
volatility_score
```

Use cases:

- distinguish stable niches from volatile search pages;
- understand whether movement is likely caused by market noise.

### P1. `/warehouse/wb/aggregates/niche-opportunities`

Purpose: parser-side shortlist for niche/category research.

Parameters:

```text
date: optional YYYY-MM-DD, default latest warehouse date
query: optional exact string
top_n: optional int, default 30
limit: optional int, default 100, max 500
```

Return rows:

```text
run_date
query
top_n
suppliers_in_top_n
brands_in_top_n
top_supplier_share
median_price
price_spread
median_rating
median_feedbacks
low_review_products_count
low_rating_products_count
stocked_products_count
opportunity_flags
required_external_signals
```

Rules:

- `opportunity_flags` may include parser-based signals such as:
  - `fragmented_top`
  - `many_low_review_products`
  - `wide_price_spread`
  - `low_rating_top_products`
  - `needs_demand_and_margin_validation`
- This endpoint must not say "profitable niche". Profit requires Seller VPS
  margin, cost, fees, logistics, ad cost and demand data.

### P1. `/warehouse/wb/aggregates/content-proxy-gaps`

Purpose: proxy for card-content review based only on parser-visible fields.

Parameters:

```text
date: optional YYYY-MM-DD, default latest warehouse date
query: optional exact string
supplier_id: optional repeatable or comma-separated
product_id: optional repeatable or comma-separated
brand: optional exact string fallback
limit: optional int, default 100, max 500
```

Return rows:

```text
run_date
query
product_id
product_name
supplier_id
supplier_name
absolute_position
title_length
query_terms_in_title_count
query_terms_missing_from_title
rating
feedbacks
final_price
content_proxy_reason
```

Rules:

- This is only a proxy. Real content audit belongs on Seller VPS and must use
  actual card title, description, attributes, photos and demand evidence.

### P1. `/warehouse/wb/aggregates/query-discovery`

Purpose: find related queries where a selected product/supplier/brand appears.

Parameters:

```text
product_id: optional repeatable or comma-separated
supplier_id: optional repeatable or comma-separated
brand: optional exact string fallback
date: optional YYYY-MM-DD, default latest warehouse date
limit: optional int, default 100, max 500
```

Return rows:

```text
run_date
query
matched_product_count
best_position
avg_position
sample_products
query_discovery_reason
```

Use cases:

- find search phrases where our products already appear;
- expand SEO monitoring query list;
- discover weak-but-visible queries for future demand validation.

## Shared Helpers To Add

Add helper functions in `parser_data_api/main.py` or a small local module if the
file becomes too large:

- parse comma-separated or repeatable query params;
- resolve latest and previous WB run dates;
- deduplicate `query_positions` by date/query/product;
- clamp `limit` to existing API max, do not raise global raw limit;
- serialize DuckDB rows to JSON safely.

If splitting into a module, keep imports simple and tests explicit.

## Tests

Extend `/home/pavel/projects/parser_data_api/tests/test_api.py`.

Required test coverage:

1. Auth is still required for new endpoints.
2. Temporary DuckDB fixture with at least:
   - two dates;
   - two queries;
   - own/matched supplier;
   - competitor supplier;
   - product inside top N;
   - product outside top N;
   - product with stock;
   - movement improved/declined.
3. `/visibility-gaps` returns outside-top-N stocked products.
4. `/query-coverage?only_gaps=true` returns gaps.
5. `/competitors-top` groups by supplier.
6. `/top-movers` respects direction and date comparison.
7. `/seo-visibility-candidates` does not include sales fields.
8. `/promotion-visibility-candidates` includes
   `seller_side_required_signals`.
9. Limits above max return validation error or are clamped consistently with
   existing API behavior. Prefer the same behavior as current endpoints.

Run:

```bash
cd /home/pavel/projects/parser_data_api
/home/Codex/agent-tools/parser-data-api-python/bin/python -m py_compile parser_data_api/main.py tests/test_api.py
/home/Codex/agent-tools/parser-data-api-python/bin/python -m pytest -q
```

## Manual Smoke Through Seller VPS Tunnel

After tests pass, restart only Parser Data API:

```bash
sudo systemctl restart parser-data-api.service
systemctl is-active parser-data-api.service
```

From Seller VPS, verify with `/home/pavel/.parser-data-api.env`:

```bash
set -a
. /home/pavel/.parser-data-api.env
set +a

curl -fsS \
  -H "Authorization: Bearer $PARSER_DATA_API_TOKEN" \
  "$PARSER_DATA_API_BASE_URL/warehouse/wb/aggregates/market-summary?limit=3"

curl -fsS -G \
  -H "Authorization: Bearer $PARSER_DATA_API_TOKEN" \
  --data-urlencode "query=шеврон" \
  --data-urlencode "top_n=30" \
  --data-urlencode "limit=3" \
  "$PARSER_DATA_API_BASE_URL/warehouse/wb/aggregates/competitors-top"
```

Do not print token values.

## Documentation

Update:

```text
/home/pavel/projects/parser_data_api/README.md
/home/pavel/projects/parser_wb/docs/WB_WAREHOUSE.md
```

Document:

- endpoints;
- parameters;
- that they are read-only;
- that parser data is not sales evidence;
- that seller agents must join sales/stock/margin/promotion data on Seller VPS.

## Completion Criteria

The task is done only when:

- all required endpoints exist in OpenAPI;
- tests pass;
- service is active after restart;
- smoke through Seller VPS tunnel succeeds;
- README and WB warehouse docs are updated;
- no parser collection logic changed;
- no secrets printed or committed;
- final report lists:
  - files changed;
  - endpoints added;
  - exact test commands and results;
  - smoke commands and results;
  - remaining limitations.

## Non-Blocking Questions

No blocking questions for implementation.

Assumption to confirm later with Seller agent: exact `supplier_id`/own product
filters for Vital Shevron, TAKTERRA and Vital Sewing should be supplied by
Seller VPS tools. Parser Data API should stay generic and filter by parameters.
