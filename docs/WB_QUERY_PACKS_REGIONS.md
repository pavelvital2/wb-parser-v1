# WB query packs and regions

Status: Stage 3.2 guarded pilot implementation; live execution requires a new
owner approval

## Scope

Stage 1 adds versioned, read-only configuration contracts and strict Python
loaders for future scoped WB collection:

- query packs with stable category/query IDs;
- a disabled region registry;
- disabled collection plans;
- exact-byte source SHA-256 values;
- fail-closed query-pack provenance;
- a canonical, secret-free effective-plan snapshot contract for Stage 2.

Stage 1 itself did not add runtime behavior. Stage 2 adds an explicit isolated
runner and scoped storage contract, but does not enable the tracked pilot or
change the nightly pipeline.

## Tracked configuration

```text
config/wb/query_packs/{query_pack_id}/{version}.json
config/wb/regions.json
config/wb/collection_plans/{collection_plan_id}.json
```

The first pack,
`config/wb/query_packs/shevron-core/2026-07-26.1.json`, is a versioned copy of
the normalized 30-query sequence in `exports/queries.txt`. The legacy TXT file
remains the only input to the existing nightly SERP command.

The Moscow and Rostov-on-Don registry entries use the coordinates recorded in
the approved research document. Both entries are disabled. Their `dest_id` and
resolution fields are null/unresolved because the observed WB destination
values are dated evidence, not stable configuration.

The pilot plan is also disabled and fixes these non-runtime policies:

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
register_query_pack_provenance(provenance_path=..., query_pack=...)
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
malformed provenance is never overwritten.

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
source hashes, a 100-item page size, stable non-secret endpoint IDs with one
pinned endpoint, and resolver values with
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

## Stage 2 isolated runner

The opt-in command is:

```text
python main.py --config config/config.yaml collection-plan \
  --plan-file config/wb/collection_plans/{collection_plan_id}.json \
  --no-publish
```

The equivalent dedicated launcher is
`scripts/run_wb_collection_plan.py`. The command fails closed for a disabled
plan. The committed pilot remains disabled, so neither command performs live
HTTP unless a later owner-approved change enables an eligible plan.

The runner:

- acquires daily, pipeline, warehouse-refresh and collection-plan locks
  non-blockingly in that order and retains them through final manifest fsync;
- resolves all configured `xinfo.dest` values under the held locks, writes the
  immutable effective-plan snapshot, then collects the region scopes serially;
- checks one unchanged egress identity through the same explicitly configured
  proxy route and requests session, using secret-free per-request headers, and
  stores only a masked value plus an experiment-local salted hash;
- sends the exact resolved value as the search `dest`;
- performs one request per task against one pinned endpoint, without retry,
  fallback switching or proxy rotation;
- records `resolved_and_sent` only as client-side request lifecycle evidence;
- never claims that the search server applied the destination;
- refuses to start within five minutes of the 23:45 MSK preflight cutoff.

All outputs remain under:

```text
data/{raw,staging,marts}/serp_scoped/{plan}/{region}/{run_id}/
state/wb_collection_plans/{plan}/{run_id}/
```

There is no scoped `latest` in Stage 2. The runner never writes global SERP
latest, seller exports, global run-report latest or warehouse data.

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
- unsupported depth or unsafe publication/sellers/rotation modes;
- non-null destination observations in tracked Stage 1 region config;
- unsafe destination IDs, non-UTC/non-RFC-3339 timestamps and duplicate
  resolved destinations;
- malformed provenance and query-pack identity/hash mismatches.

## Stop gate

The tracked plan and regions remain disabled. Stage 3.2 code/test work does not
authorize another resolver/search smoke or pilot. Scoped publication, warehouse
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
