# WB query packs and regions

Status: Stage 1 implemented; runtime collection is not implemented

## Scope

Stage 1 adds versioned, read-only configuration contracts and strict Python
loaders for future scoped WB collection:

- query packs with stable category/query IDs;
- a disabled region registry;
- disabled collection plans;
- exact-byte source SHA-256 values;
- fail-closed query-pack provenance;
- a canonical, secret-free effective-plan snapshot contract for Stage 2.

Stage 1 does not add a CLI command, perform HTTP requests, resolve
destinations, create run snapshots, write parser state, publish latest data,
run sellers, refresh the warehouse or change the nightly pipeline.

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

Stage 2 remains unapproved in this implementation. There is no regional runner,
HTTP/smoke/pilot, destination resolver, runtime snapshot, lock orchestration,
scoped storage, warehouse migration, Parser Data API change or schedule.
