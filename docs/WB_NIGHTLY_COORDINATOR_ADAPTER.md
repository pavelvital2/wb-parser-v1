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
5. verify the versioned input manifest, exact centralized Python
   binary/version/hash and the approved installed dependency tree;
6. parse `config/runtime.env` in-process with the strict dotenv grammar, bind
   the canonical cookie/header files and hash effective runtime inputs without
   exposing their values;
7. recheck the absolute deadline immediately before process creation;
8. pass both host-lock descriptors to a Linux subreaper supervisor, which
   retains them until every pinned child identity and adopted descendant has
   exited.

The coordinator owns the guard lock. Its inherited validation FD is checked
against the coordinator ancestor process, secure lock inode and kernel lock.
An official standalone invocation acquires both locks itself. A wrapped shell
entry validates the exact inherited guard and validation descriptors, their
inode/owner/mode and quarantine state before runtime loading. The
`PARSER_WB_LOCK_V3_WRAPPED` flag is routing metadata only and grants no
authority. Neither path unlinks or replaces host lock files, and lease release
closes descriptors without issuing `LOCK_UN` on a shared open-file
description.

Every official passthrough target follows the same attestation sequence as the
four-region target: verify the tracked manifest, load runtime with the strict
in-process parser, capture the exact manifest/runtime digests, verify them
immediately before supervisor spawn, and pass them unchanged to the child.
Consequently, `integrity_gate()` is active in coordinator descendants rather
than silently degrading to a no-op.

The absolute coordinator deadline caps the existing plan deadline. It is
passed to collection and downstream guards and never extends their local
cutoffs.

## Terminal Result

The adapter writes one exact JSON result to the path supplied by the
coordinator. The writer refuses symlink components and non-regular targets,
writes a same-directory exclusive temporary file, performs file `fsync`,
rechecks attested inputs inside the writer immediately before `rename`, then
performs directory `fsync` and exact byte verification. The result has mode
`0440` and is the final adapter action before the matching process exit.

Outcomes are:

| Outcome | Exit | WB meaning |
|---|---:|---|
| `success` | 0 | collection and verified downstream are complete |
| `checkpoint` | 76 | immutable scoped state can be resumed |
| `deferred` | 75 | local lock/start/deadline gate refused work |
| `hard_failure` | 2 | no safe automatic resume is claimed |

The immutable scoped collection `run_id` expected by the invocation is used as
`run_ref`; child status cannot replace it. A checkpoint is emitted only for a
strictly validated, non-empty resumable state with verified segment records
and remaining collection or publication work. It uses the same value as
`resume_ref`. Empty, corrupt, stale or wrong-run state is a hard failure. A
completed collection is not fetched again on downstream resume. A failed
coordinator resume does not request a third automatic resume.

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

Direct live `main.py run ...`, component aliases, `cleanup`,
`collection-plan`, `scripts/run_wb_collection_plan.py`, and direct execution
of the inner four-region Python launcher are also refused after the secure path
appears unless they inherit the complete adapter lease. Relative passthrough
commands are canonicalized inside the project and must match the exact
allowlist. Use the reviewed wrappers; setting a marker environment variable
without valid held descriptors is insufficient.

The contract checker returns at most 32 direct coordinator roots, including
the attested verifier and
`config/wb/nightly_coordinator_adapter_inputs.json`. That versioned manifest
contains the complete tracked Python/shell/config/query-pack graph and exact
SHA-256 values, `requirements.txt`, the exact centralized Python executable
path/version/hash, and a deterministic digest of the approved installed
site-packages tree. Installed `__pycache__`, `.pyc`, and `.pyo` entries are
included with their bytes, type, owner, mode, and link-count constraints;
addition, removal, or mutation invalidates the manifest. Coordinator children
run with `PYTHONDONTWRITEBYTECODE=1`, so execution cannot rewrite the attested
bytecode tree. The adapter pins the manifest SHA per invocation and
rechecks the graph, Python/dependencies and hash-only effective runtime inputs
before each stage and inside every durable publication writer. The coordinator
path never shell-sources `runtime.env`; the strict parser accepts only reviewed
dotenv assignments and the single approved shared proxy-env include. The
effective `WB_COOKIE_FILE` must resolve to the canonical ignored
`config/wb_cookie.txt`; substitutes fail before child execution. Runtime values,
cookies, headers and proxy URLs are never written to evidence. The checks
perform no network calls.

The subreaper does not use a numeric process-group ID as ownership evidence.
It parses `/proc/<pid>/stat` after the closing command-name parenthesis, pins
each process by `(pid, starttime)`, discovers descendants and adopted children,
and rechecks identity before signalling. Working `pidfd_open` and
`pidfd_send_signal` are mandatory and are tested before child spawn; there is
no numeric-PID signal fallback. A reused PID or PGID therefore cannot redirect
a termination signal. If pidfd signalling fails for a still-matching identity,
the supervisor fails closed and retains the host-lock FDs until every owned
descendant terminates without an unsafe signal.

The activation checker has an absolute approved-Python shebang and is invoked
directly by the coordinator. It therefore runs with
`PATH=/usr/bin:/bin` without selecting a different system Python. All tracked
influencing files, the resolved Python binary and every retained dependency
file/directory must be parser-owned and neither group- nor world-writable.
The exact checker command is:

```text
/home/pavel/projects/parser_wb/scripts/check_nightly_coordinator_contract.py
```

Latest outputs, run reports, warehouse manifests and warehouse refresh state
use the shared durable atomic writer. It rejects symlink ancestors/leaves,
world-writable paths and hardlinked leaves, fsyncs the same-directory temporary
file and containing directory, retains the temporary FD through rename, and
proves that the committed leaf is the same inode with the exact encoded bytes
and one link. Copy publication also retains and rechecks the source FD,
pathname identity and bytes before and after commit. Input attestation runs
inside the writer immediately before commit and again during post-commit
verification. A transient hardlink backup remains durable through all
publication-critical fsync and proof steps. Rollback is allowed only while the
target is still the exact inode and bytes published by that writer. A later
cooperative writer is never overwritten by compensation from an earlier
writer. Backup cleanup happens after durable success; an unlink or
cleanup-fsync failure is reported as maintenance debt and does not turn an
already durable commit into a false publication failure.

All official durable writers account for rollback cleanup debt in the
lease-protected registry `state/wb_durable_cleanup_debt`. The
`wb_durable_cleanup_debt_v1` marker is created and fsynced before its rollback
hardlink. Recovery enumerates only this exact registry and removes a backup
only after canonical marker, owner, type, mode, link count, parent inode,
target identity, backup inode and content-hash proofs pass. Unknown entries,
symlinks, changed ownership or unprovable metadata fail closed and are never
deleted. The global limit is `3`: one or two proven but temporarily
uncleanable entries are returned as explicit cleanup debt, while the third
entry prevents a trusted success and later publications fail before commit
until a verified lease-holder can sweep the proven debt. This bounds debt
across latest files, run reports, scoped state, warehouse state and terminal
coordinator results rather than per target name. A cleanup failure after a
durable commit does not roll that commit back; it changes the writer result to
tracked debt or, at the limit, a fail-closed outcome. The coordinator checker
pins this schema and threshold, and terminal publication runs the same debt
preflight under the inherited host lease.

A failed pre-commit check or partial temporary write cannot replace the prior
publication. The stricter no-group-write rule applies to influencing inputs;
existing runtime outputs created under the host `umask 0002` are safely
replaced with writer-selected modes.

## Operational Threat Model

The application-level guarantee assumes cooperative official writers running
under the same Unix UID and serialized by the validated host lease. FD
anchoring, exact-byte proofs, CAS-safe rollback, and pidfd identity checks are
still required inside that model.

A malicious file owner, arbitrary same-UID process outside the official
entrypoints, or privileged/root/sudo actor is outside this application-level
guarantee. Protecting against that actor requires a separate deployment stage
with a dedicated OS service user and service/filesystem isolation. The current
contract must not be described as protection against such an actor.

After lock-v3 cutover, WebUI config/upload mutations require the same validated
lease and WebUI collection actions use `scripts/run_wb_live_component.sh`.
Without a valid descendant lease they fail closed. Read-only `doctor`, `runs`
and WebUI run listing open SQLite in read-only/query-only mode and must not
create directories or migrate schema implicitly.

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
