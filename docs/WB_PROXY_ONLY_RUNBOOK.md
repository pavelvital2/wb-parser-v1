# WB proxy-only collection contract

## Boundary

Every outbound request that obtains or verifies WB marketplace data must use
the explicitly configured marketplace proxy. The primary neutral egress
identity check therefore uses the same proxied session as the regional
resolver and search requests. There is no direct fallback for WB marketplace
data.

The proxy-only boundary does not include:

- Telegram notifications;
- GitHub traffic;
- local filesystem, SQLite, DuckDB or warehouse work;
- local/internal control-plane APIs.

The local proxy-rotation and health endpoints remain on their existing
control-plane route.
After a successful rotation, every subsequent WB request still uses a newly
created, explicitly proxied marketplace session. The rotation endpoint is not a
marketplace data endpoint and must never receive query text, cookies, request
headers or proxy credentials.

Some mobile networks allow WB while blocking neutral public IP-check services.
When the primary neutral check fails with a network error, the collection-plan
transport may use the local Proxy Health API as control-plane evidence. This
fallback:

- is derived from the ignored `PARSER_WB_PROXY_ROTATE_URL`, but removes its
  path, query and any token before requesting `/health`;
- uses a separate direct session with `trust_env=false`, no proxy, cookies,
  marketplace headers or credentials;
- requires HTTP 200, JSON `ok=true`,
  `marketplaceTransportVerified=true` and a syntactically valid
  `external_ip`;
- is latched for the remaining lifetime of that transport instance, avoiding a
  repeated five-second timeout before every segment;
- never carries WB data and never changes the proxy route used by the resolver
  or search requests.

An unhealthy/malformed Proxy Health response remains fail-closed. This
control-plane fallback does not prove the external IP by an independent public
service when `external_ip_verified=false`; it proves that the managed proxy
controller reports a valid identity and has verified the marketplace
transport. A spontaneous upstream IP change not observed by that controller
remains a documented residual risk.

## Required launch contract

Tracked launchers load ignored `config/runtime.env` with
`scripts/wb_runtime_env.sh`. The loader verifies that the file is a readable
regular non-symlink, computes SHA-256 before and after loading, and exports only
the runtime-loaded marker and exact-byte hash as provenance.

Live component entrypoints:

```text
scripts/run_products_sellers_daily.sh
scripts/run_wb_nightly_preflight.sh
scripts/run_wb_cookie_renewal.sh
scripts/run_wb_persistent_session.sh
scripts/run_wb_persistent_watchdog.sh
scripts/run_wb_live_component.sh {suggest|serp|sellers|daily|monthly} ...
scripts/run_wb_access_tool.sh {smoke|ensure|refresh|renew} ...
scripts/run_wb_collection_plan.sh --config ... --plan-file ... --no-publish
scripts/run_wb_guarded_regional_pilot.sh
```

Direct Python invocation of a marketplace component is not a bypass. The
shared guard requires the audited runtime marker, its valid SHA-256 provenance
and the configured proxy environment variable before constructing a network
session or browser context.

## Shared guard

`app/common/proxy_required.py` is the common fail-closed contract:

- accepts only structurally valid HTTP/HTTPS/SOCKS proxy URLs supported by the
  selected client;
- reads the proxy only from the configured environment variable;
- disables `requests.Session.trust_env`;
- explicitly assigns the same route to both HTTP and HTTPS;
- verifies route provenance on the configured session;
- creates an explicit Playwright proxy configuration;
- exposes only status booleans and SHA-256 provenance for evidence.

Missing runtime provenance, a missing/malformed proxy, an unsupported browser
proxy scheme or a proxy not applied to the concrete client fails before the
first marketplace request.

## Live path inventory

| Path | Entrypoint | Client | Proxy enforcement |
|---|---|---|---|
| Nightly/manual SERP | daily wrapper, live-component launcher | `requests.Session` | shared guard in `SerpEngine._build_session` |
| Sellers enrichment | daily wrapper, live-component launcher | `requests.Session` | shared guard in `SellersEngine._build_session` |
| Suggest/browser | live-component launcher, Web UI | Playwright persistent context | shared browser route before context launch |
| Cookie API smoke | access/renewal/preflight wrappers | `requests.Session` | shared guard before smoke loop |
| WB HTML smoke | access/renewal/preflight wrappers | `requests.Session` | shared guard before request |
| Cookie browser refresh | access/renewal/preflight wrappers | Playwright browser/context | shared browser route before launch |
| Persistent WB browser | persistent wrapper/watchdog | Playwright persistent context | shared browser route before launch |
| Persistent egress evidence | persistent wrapper/watchdog | `requests.Session` | shared proxied session |
| Regional geo resolver | collection-plan or guarded pilot launcher | one `requests.Session` | required runtime loader and shared guard |
| Regional scoped SERP | collection-plan launcher | same `requests.Session` | production endpoint order/params/headers through shared guard |
| Regional probe/search/repeat | guarded pilot launcher | same `requests.Session` | Stage 3.2 contour preflight and shared guard |
| Regional neutral egress | collection-plan or guarded pilot launcher | same `requests.Session`; bounded local health fallback only after network error | primary check uses the same proxy route/session; fallback is control-plane evidence only |

The watchdog's tmux/process checks are local. Any keeper/browser repair it
starts inherits the required runtime provenance and still passes the component
guard.

## Stage 3.2 guarded pilot

The only allowed future live command is:

```text
scripts/run_wb_guarded_regional_pilot.sh
```

This command remains behind the owner approval and tracked enable/disable
commit gates. It uses one session and one route, zero retries or rotations, a
pinned endpoint and monotonic pacing for every WB search-endpoint attempt:

```text
first attempt: no wait
later attempts: at least 17 seconds plus jitter in [0, 2] seconds
```

The WB budget is `geo=2`, `endpoint_probe<=2`, `regional_search=5`,
`repeat_search=1`, `total_wb<=10`, `retry=0`. A reusable probe is the prior
search attempt and therefore the next query is paced.

HTTP `429` or `498` is terminal for WB calls in that run. A strict numeric
`Retry-After` delta from `1` through `120` seconds is used when present;
otherwise the safe cooldown is `45` seconds. After cooldown, at most one
neutral proxied egress check is allowed. There is no WB retry.

The hard runtime cap is 18 minutes. The start gate requires at least 20 minutes
before the 23:45 MSK cutoff. Evidence stores only schema/status/booleans/counts,
safe error codes and hashes. It never stores proxy URLs, hosts, credentials,
cookies, headers or full IP addresses.

## Verification

Regression tests monkeypatch network/browser constructors and prove:

- zero marketplace calls without loaded runtime/proxy;
- explicit proxy assignment on normal requests and Playwright paths;
- no direct fallback for marketplace data;
- bounded secret-free Proxy Health fallback only after a neutral-check network
  error, with unhealthy responses rejected;
- one regional session/route;
- pacing, budget, deadline and terminal rate-limit behavior;
- local rotation/Telegram/GitHub routing is outside this data-plane guard.

No live HTTP is required for these tests.
