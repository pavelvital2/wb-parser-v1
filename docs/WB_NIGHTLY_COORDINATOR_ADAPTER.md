# WB Nightly Coordinator Adapter

## Scope

The parser-owned adapter targets only the four-region WB pipeline:

```text
scripts/run_wb_four_region_nightly.sh
config/wb/collection_plans/shevron-four-regions-top1000-v2.json
```

The Moscow/Rostov two-region plans are not coordinator fallbacks. The v2 plan
and all four regions remain disabled until a separate reviewed cutover.

The coordinator contract versions are:

- result: `marketplace_parser_result_v3`;
- shared lock: `marketplace_collection_lock_v3`;
- unsafe-cleanup quarantine: `marketplace_collection_quarantine_v1`;
- activation check: `parser_coordinator_contract_check_v2`.

## Execution Contract

The shell wrapper immediately enters
`scripts/wb_nightly_coordinator_adapter.py`. The adapter performs these steps
before loading `config/runtime.env` or starting parser work:

1. validate the complete coordinator environment, invocation identity,
   absolute deadline and exact result path;
2. validate or acquire the host locks in fixed order `guard -> validation`;
3. inspect the quarantine marker while both locks are held;
4. validate the exact four-region command and reject other plans;
5. load the required runtime environment while retaining the validation FD;
6. pass that FD to the four-region Python descendant.

The coordinator owns the guard lock. Its inherited validation FD is checked
against the coordinator ancestor process, secure lock inode and kernel lock.
An official standalone invocation acquires both locks itself. Neither path
unlinks, replaces or unlocks host lock files.

The absolute coordinator deadline caps the existing plan deadline. It is
passed to collection and downstream guards and never extends their local
cutoffs.

## Terminal Result

The adapter writes one exact JSON result to the path supplied by the
coordinator. The file is written by temporary-file replace, file and directory
`fsync`, verified byte-for-byte, and has mode `0440`. It is the final adapter
action before the matching process exit.

Outcomes are:

| Outcome | Exit | WB meaning |
|---|---:|---|
| `success` | 0 | collection and verified downstream are complete |
| `checkpoint` | 76 | immutable scoped state can be resumed |
| `deferred` | 75 | local lock/start/deadline gate refused work |
| `hard_failure` | 2 | no safe automatic resume is claimed |

The immutable scoped collection `run_id` is used as `run_ref`. A checkpoint
uses the same value as `resume_ref`. A completed collection is not fetched
again on downstream resume. A failed coordinator resume does not request a
third automatic resume.

## Official Entrants

The reviewed collection wrappers enter the same lock/quarantine adapter when
the secure coordinator lock path exists:

- `scripts/run_products_sellers_daily.sh`;
- `scripts/run_wb_collection_plan.sh`;
- `scripts/run_wb_guarded_regional_pilot.sh`;
- `scripts/run_wb_live_component.sh`.
- `scripts/run_wb_cookie_renewal.sh`;
- `scripts/run_wb_nightly_preflight.sh`;
- `scripts/run_wb_access_tool.sh`.
- `scripts/run_wb_warehouse_refresh.sh`.

Cookie renewal, access preflight and the manual access tool are included
because they can issue WB access requests or mutate cookie state. Their local
shell locks, including the standalone warehouse refresh locks, use dynamically
allocated descriptors so they cannot overwrite the inherited validation
descriptor. The warehouse refresh invoked by nightly remains a descendant of
the same lease; a direct manual refresh enters through passthrough.

The persistent browser is intentionally not run under a short-lived
passthrough lease: it would retain the marketplace host lock indefinitely.
Once the secure coordinator lock directory exists,
`run_wb_persistent_session.sh` fails closed and
`run_wb_persistent_watchdog.sh` becomes a no-op before runtime loading, local
state writes or browser activity. Re-enabling persistent browser maintenance
requires a separately reviewed coordinator-aware lifecycle.

The directory-presence gate is intentional during migration: before the
owner-approved coordinator cutover, the existing cron continues to use its
current lock contour. Cutover must first install the root-owned secure lock
directory and precreated lock files. Once that directory exists, failure to
validate either lock or the quarantine path is fail-closed.

Direct live `main.py run ...`, component aliases, `collection-plan`, and direct
execution of the inner four-region Python launcher are also refused after the
secure path appears unless they inherit the adapter validation FD. Use the
reviewed wrappers; setting a marker environment variable without a valid held
descriptor is insufficient.

The contract checker verifies the active bootstrap source and hashes the
wrapper, Python adapter, runtime loader, critical four-region modules, tracked
config, plan, registry and query pack. It performs no network calls.

## Cutover Gates

Before coordinator activation:

1. install the secure lock layout from the coordinator repository;
2. set executable wrappers/checker to `0755` and attested data/Python files to
   non-group-writable parser-owned modes (the plan target is `0644`);
3. verify the v2 plan and all four regions are still disabled;
4. run both initial and resume contract checks from the deployed parser path;
5. run the parser and coordinator full suites in the approved maintenance
   window;
6. perform the separate owner-reviewed cron/systemd cutover.

This implementation does not change cron, enable flags, runtime secrets,
cookies, request headers, proxy settings, global latest, sellers or warehouse
data.
