# WB Exact Same-Day Resume Cutoff Transition

## Scope

This is a one-run recovery contract. It applies only to:

- coordinator run `nightly-20260801-66cbc819ed0f`;
- coordinator stage `wb_resume`;
- WB matrix and collection run `20260801_183812Z`;
- transition ID `wb-20260801-183812Z-cutoff-2300-2359-v1`;
- deadline change `2026-08-01T20:00:00Z` to `2026-08-01T20:59:00Z`.

It does not change the tracked collection plan, immutable effective plan,
verified segment/checkpoint bytes, query pack, region registry, endpoint order,
request parameters, proxy route, runtime inputs, depth, publication rules,
sellers contract or Warehouse contract.

## Parser Contract

The parser accepts the transition only when all exact IDs above are present,
the invocation is a resume, and the inherited absolute deadline is exactly
`2026-08-01T20:59:00Z`. The source plan still has `23:00` as its reviewed
cutoff. Only the in-memory deadline used by this exact resume is extended to
`23:59 MSK`.

Before network I/O, the parser verifies the original plan, query pack, region
registry, effective-plan SHA-256, full stored transport fingerprint and the
approved code/input-manifest attestation transition. It then writes immutable,
durable matrix authorization evidence. The collection manifest records the
validated transition evidence when it durably checkpoints progress.

Verified query segments remain immutable and are reused. The first unfinished
query segment is rebuilt from page 1; at most ten pages are repeated. A partial
segment is never promoted. Collection, sellers, regional Warehouse and final
publication share the same `20:59Z` deadline and finalization reserve.

## Coordinator Contract

The coordinator requires a separate reviewed run-scoped continuation. It must:

1. retain the same coordinator run and WB resume reference;
2. create a unique invocation/result path under the existing lock-v3 lease;
3. set the stage to `wb_resume` and deadline to exactly `20:59Z`;
4. add `MARKETPLACE_COORDINATOR_CUTOFF_TRANSITION_ID` only after reserved
   environment cleanup, with the exact transition ID above;
5. preserve all other coordinator contract values and command arguments;
6. refuse the invocation after `20:29Z`, because the unchanged minimum resume
   window is 1800 seconds;
7. refuse any cross-midnight continuation and never create a new WB run ID.

The exact parser command remains the matrix resume command with
`--resume-run-id 20260801_183812Z`. No direct state edit, effective-plan edit,
manual checkpoint promotion or alternate launcher is permitted.

## Acceptance Gates

- old effective-plan and verified segment/checkpoint SHA-256 values match;
- current endpoint/request/proxy/runtime fingerprints match the stored values;
- approved input-manifest transition matches the exact built parser target;
- matrix transition evidence is absent before validation and durable before
  the resumed entry executes;
- resume starts at the first unverified query scope;
- final collection, sellers, Warehouse and authoritative publication complete
  before `20:59Z`;
- any mismatch fails closed before network or publication.
