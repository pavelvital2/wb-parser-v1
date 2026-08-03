# Scheduling Guide (V1)

This document describes production V1 scheduling for:
- Windows Task Scheduler (primary)
- Linux cron/systemd timer (portable adaptation)

## 1. Commands to Schedule
Daily pipeline:
```powershell
py C:\parser_new\main.py --config C:\parser_new\config\config.yaml run daily
```

Monthly pipeline:
```powershell
py C:\parser_new\main.py --config C:\parser_new\config\config.yaml run monthly
```

## 2. Required Runtime Context
Working directory:
- `C:\parser_new`

Required env variables for scheduled user session:
- `WB_COOKIE_FILE`
- `WEBUI_ADMIN_PASSWORD` (only needed if same account also runs Web UI)
- `WEBUI_SECRET_KEY` (only needed if same account also runs Web UI)

## 3. Windows Task Scheduler (GUI)
Create two tasks:

### Task A: Daily pipeline
- Name: `WB Parser Daily`
- Trigger: Daily, 00:00
- Action:
  - Program/script: `C:\Windows\System32\WindowsPowerShell\v1.0\powershell.exe`
  - Add arguments:
    ```powershell
    -NoProfile -ExecutionPolicy Bypass -Command "$env:WB_COOKIE_FILE='C:\parser_new\config\wb_cookie.txt'; Set-Location 'C:\parser_new'; & py C:\parser_new\main.py --config C:\parser_new\config\config.yaml run daily *> C:\parser_new\data\logs\scheduler_daily.log"
    ```
- Start in: `C:\parser_new`

### Task B: Monthly pipeline
- Name: `WB Parser Monthly`
- Trigger: Monthly, day 1, 00:00
- Action:
  - Program/script: `C:\Windows\System32\WindowsPowerShell\v1.0\powershell.exe`
  - Add arguments:
    ```powershell
    -NoProfile -ExecutionPolicy Bypass -Command "$env:WB_COOKIE_FILE='C:\parser_new\config\wb_cookie.txt'; Set-Location 'C:\parser_new'; & py C:\parser_new\main.py --config C:\parser_new\config\config.yaml run monthly *> C:\parser_new\data\logs\scheduler_monthly.log"
    ```
- Start in: `C:\parser_new`

Recommended task options:
- "Run whether user is logged on or not"
- "Run with highest privileges"
- If task is already running: `Do not start a new instance`

Note:
- Pipeline-level singleton lock (`state/locks/pipeline.lock`) also prevents conflicting parallel starts.
- The lock file contains PID/run metadata and is also held with an advisory
  file lock while a run is active.
- `state/locks/pipeline.lock.guard` is a persistent recovery guard used only to
  serialize lock acquire/recovery/create; it is not an active run marker.

## 4. Verification Checklist (Windows)
After first trigger run, verify:
1. Task Scheduler `Last Run Result` is success.
2. New row exists in `py C:\parser_new\main.py --config C:\parser_new\config\config.yaml runs --limit 5`.
3. `C:\parser_new\state\run_reports\latest.json` was updated.
4. Output files updated in `data/marts/*/latest`.
5. `scheduler_daily.log` or `scheduler_monthly.log` has no critical traceback.

## 5. Linux Adaptation (Short)
Use the same CLI commands.

### cron example
```bash
# daily 00:00
0 0 * * * cd /opt/parser_new && WB_COOKIE_FILE=/opt/parser_new/config/wb_cookie.txt /usr/bin/python3 main.py --config config/config.yaml run daily >> data/logs/cron_daily.log 2>&1

# monthly day 1 00:00
0 0 1 * * cd /opt/parser_new && WB_COOKIE_FILE=/opt/parser_new/config/wb_cookie.txt /usr/bin/python3 main.py --config config/config.yaml run monthly >> data/logs/cron_monthly.log 2>&1
```

### Current Linux schedule for this host

Current production schedule on `/home/pavel/projects/parser_wb` is product and
seller collection only. It intentionally does not run `daily`, `filter`, or
`suggest`.

The user crontab contains:
```bash
15 0 * * * /home/pavel/projects/parser_wb/scripts/run_products_sellers_daily.sh >> /home/pavel/projects/parser_wb/data/logs/cron_products_sellers.log 2>&1
```

The host timezone is `Europe/Moscow`, so the cron entry runs daily at 00:15 MSK.

Before the 00:15 collection, the host runs a WB access preflight twice:

```cron
45 23 * * * /home/pavel/projects/parser_wb/scripts/run_wb_nightly_preflight.sh >> /home/pavel/projects/parser_wb/data/logs/wb_nightly_preflight.log 2>&1
5 0 * * * /home/pavel/projects/parser_wb/scripts/run_wb_nightly_preflight.sh >> /home/pavel/projects/parser_wb/data/logs/wb_nightly_preflight.log 2>&1
```

The preflight checks the current `config/wb_cookie.txt` with SERP/API smoke.
When it passes, the cookie is saved as an ignored mode-`600` known-good backup
under `state/wb_known_good/`. When it fails, the preflight tries to restore the
latest smoke-valid known-good backup before attempting keeper refresh. If access
cannot be restored, it sends an early Telegram notification to the `parser_wb`
topic and exits non-zero. This preflight deliberately treats API smoke as the
hard gate for the nightly SERP; persistent browser HTML state is useful for
repair but is not the only condition, because the API/cookie contour can remain
usable while browser HTML is blocked.

The wrapper executes:
```bash
/home/Codex/agent-tools/parser_wb-python/bin/python main.py --config config/config.yaml run serp
/home/Codex/agent-tools/parser_wb-python/bin/python main.py --config config/config.yaml run sellers
```

Before `serp`, the wrapper runs `scripts/wb_cookie_keeper.py ensure`. The
keeper checks the current WB cookie with a smoke SERP request. The retired
legacy wrapper remains byte-compatible and is not the automatic browser-renewal
entrypoint; bounded headed renewal belongs to the official lock-v3 maintenance
and preflight wrappers described below. Use
`PARSER_WB_KEEPER_DISABLED=1` to skip this check and
`PARSER_WB_KEEPER_SAMPLE_COUNT` to control how many queries are smoke-checked.
The wrapper default is `3`. Smoke accepts configured
SERP fallback endpoints and uses `serp.smoke_min_successes`; production config
requires at least 2 successful smoke queries out of the default 3-query sample.
This tolerates one temporary failed smoke but blocks a full SERP when 2 of 3
queries already fail. Full SERP publication is still guarded by
`validation.max_error_ratio.serp`.

The user's crontab contains a separate WB cookie/access maintenance job every
30 minutes:
```cron
7,37 * * * * /home/pavel/projects/parser_wb/scripts/run_wb_cookie_renewal.sh >> /home/pavel/projects/parser_wb/data/logs/wb_cookie_renewal.log 2>&1
```

Current direct-3proxy steady state uses `scripts/wb_cookie_keeper.py ensure` by
default through `PARSER_WB_COOKIE_RENEW_COMMAND=ensure` in ignored
`config/runtime.env`. This checks the current `config/wb_cookie.txt` with the
same SERP/API smoke gate and only attempts browser refresh if smoke fails. The
refresh is a headed system-Chrome invocation under Xvfb and lock-v3 with a
10-minute outer hard cap,
using an ignored mode-`700` persistent profile and the required proxy. Chrome
uses browser-native headers; parser API headers/Authorization and Playwright
`storage_state` are not injected. A WB-only cookie candidate is promoted only
after browser HTTP-200/non-antibot validation and proxy-only exact `3/3` API
smoke with `authorization-policy=if_present` and the reviewed plan horizon.
Promotion is atomic to mode `600` with a hash-proven ignored backup. Browser or
candidate API `429`/`498`, or browser timeout, leaves production unchanged and
sets a 30-minute refresh cooldown. The implementation is offline-tested but is
not live-proven until a separately authorized smoke succeeds.

Detailed WB access/cookie repair rules are in `docs/WB_ACCESS_COOKIE_RUNBOOK.md`.
Use that runbook before changing proxy/cookie/header runtime or processing a new
browser Copy-as-cURL export.

For the current direct Windows 3proxy WB channel, `config/runtime.env` sources
`/home/pavel/.marketplace-proxy.env`, sets `PARSER_WB_PROXY_URL` from the direct
HTTP proxy, and sets `PARSER_WB_REQUEST_HEADERS_FILE` to an ignored mode-`600`
secret headers file derived from the verified owner Opera session. The
successful gate is the API/SERP contour: 3-query keeper smoke returned HTTP 200
for all three sample queries through this runtime. A clean Playwright/Chrome
profile through the same proxy returned WB HTML `HTTP 498`, so the persistent
browser watchdog is disabled with `PARSER_WB_PERSISTENT_WATCHDOG_ENABLED=0` for
this runtime and must not be treated as the hard gate for collection.

The direct-3proxy fallback channel has an explicit no-cookie gate. With the
secret Opera-derived API headers loaded, `wb_cookie_keeper.py smoke
--without-cookie --sample-count 3` returned HTTP 200 product responses for all
three sample queries through `search.wb.ru`; without those headers the fallback
returned HTTP 429. Runtime therefore sets `PARSER_WB_COOKIE_REQUIRED=0` and
`PARSER_WB_COOKIELESS_FALLBACK_OK=1`: cookies remain preferred, but an expired
or missing `config/wb_cookie.txt` is not a hard collection failure while the
header+fallback smoke passes. This is not a proven browser cookie renewal path;
clean Playwright and Opera cURL replay through the same proxy still returned WB
HTTP 498 and no `Set-Cookie`.

When enabled, the host keeps a persistent WB browser session alive through tmux session
`wb_persistent_session`. The user confirmed that the mobile proxy rotates
external IP automatically every 5 minutes by design, so one failed heartbeat
during an IP change must not immediately kill the browser session. A local
watchdog checks only local state every minute and restarts the tmux session only
when the session is missing, the heartbeat is stale, state is unreadable, or
several new failed heartbeats are observed in a row:

```cron
* * * * * /home/pavel/projects/parser_wb/scripts/run_wb_persistent_watchdog.sh
```

Relevant paths:
- session log: `data/logs/wb_persistent_session.log`
- session state: `state/wb_persistent_session/latest.json`
- watchdog log: `data/logs/wb_persistent_watchdog.log`
- watchdog state: `state/wb_persistent_session/watchdog.json`

Default watchdog thresholds:
- `PARSER_WB_WATCHDOG_MAX_AGE_SECONDS=900`
- `PARSER_WB_WATCHDOG_BAD_HEARTBEATS=2`
- `PARSER_WB_WATCHDOG_RESTART_COOLDOWN_SECONDS=600`

The watchdog itself does not send WB requests and does not write cookies; it
only starts/restarts the persistent browser wrapper. Cookie promotion remains in
`scripts/wb_persistent_session.py` and still requires both SERP smoke and WB
HTML anti-bot smoke.

If WB blocks the host IP with `HTTP 498` on `www.wildberries.ru` or repeated
`HTTP 429` on `search.wb.ru`, do not store proxy credentials in the repository.
Set `PARSER_WB_PROXY_URL` in the scheduler environment or in the ignored local
file `config/runtime.env` with mode `600`, for example:

```bash
PARSER_WB_PROXY_URL=<proxy-url>
```

Do not commit or print this file. The wrapper scripts source `config/runtime.env`
before calling the parser, and the same variable is used by SERP requests and by
`wb_cookie_keeper.py` smoke/Playwright refresh.

`HTTP 498` is treated as an anti-bot/rate-limit status together with `HTTP 429`
for longer sleeps and the consecutive rate-limit stop guard.

If `serp` fails or finishes as an unacceptable partial, `sellers` is not
started. The wrapper retries `serp` after a cooldown for transient WB anomalies
such as `retryable_payload_anomaly: nested promo products=1`. Defaults:
`PARSER_WB_SERP_MAX_ATTEMPTS=2` and
`PARSER_WB_SERP_RETRY_SLEEP_SECONDS=3600`.

Inside a SERP run, repeated `retryable_payload_anomaly` responses are retried on
the same `query|page`. Current defaults: after 5 consecutive anomaly responses
SERP sleeps 600 seconds, recreates the HTTP session, reloads the cookie file,
optionally runs `scripts/wb_cookie_keeper.py smoke`, and retries the same page.
Each later cooldown in the same run grows by 600 seconds: 10, 20, 30 minutes,
and so on.

After the wrapper exits, it calls `scripts/notify_products_sellers_daily.py` to
send a Telegram summary to the `parser_wb` topic. The notification reports run
status, duration, query count, product rows from
`data/marts/serp/latest/products_daily.csv`, seller rows from
`data/marts/sellers/latest/sellers_daily.csv`, bridge rows from
`data/marts/sellers/latest/seller_query_product_bridge.csv`, warehouse status,
and the cron log path.

The same Telegram message includes a local-only health block. It reads existing
state files and CSV indexes; it does not call Wildberries and does not print
cookies, request headers, proxy credentials, or browser storage state. The block
summarizes WB API smoke, nightly preflight state, SERP latest page status counts
including HTTP `429`/`498`, latest publication run ids, latest run report, and
the persistent-browser/watchdog state. Notification errors are intentionally
ignored by the wrapper so Telegram problems do not change parser exit status.
Use `PARSER_WB_NOTIFY_DISABLED=1` to skip notification during manual tests.

### systemd timer note
- Create two services calling the same commands.
- Attach matching timers (`OnCalendar=daily` and `OnCalendar=*-*-01 00:00:00`).
- Keep the same working directory and env vars.

## 6. Collision Safety
Even with scheduler rules, keep lock enabled in config:
- `runtime.locking_enabled: true`

If a run crashes and lock remains stale, stale handling is controlled by:
- `runtime.lock_stale_seconds`

Empty, corrupt, or dead-owner `state/locks/pipeline.lock` files are recovered
immediately after verifying that no advisory lock is held and the metadata PID
is not alive. Do not remove a lock manually while a parser process is active or
the lock is held by the OS. Recovery is serialized through
`state/locks/pipeline.lock.guard` until the new `pipeline.lock` is created,
flocked, and fsynced with metadata.
