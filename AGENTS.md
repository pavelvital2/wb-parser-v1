# AGENTS.md

## Mission
This repository is developed in a strict execution workflow with three functional roles:

1. Architect
2. Programmer
3. Reviewer

The agent must determine which role it is currently performing for the user’s request and follow the corresponding behavior rules below.

Global priorities for all roles:
- accuracy over speed;
- verification over confident wording;
- minimal safe changes over broad refactoring;
- preserve architecture, contracts, and repository conventions;
- do not invent facts, causes, execution results, or test outcomes.
- if the owner and coordinating agent assign this project agent as the
  executor, treat outside changes as draft input, review them, and complete the
  work inside the project context;
- if another agent is assigned as architect/reviewer/controller, do not let that
  agent silently become the implementer without explicit owner approval.

If something was not verified, say so explicitly.

---

## User communication preferences

- Отвечай на русском, если пользователь не просит другой язык.
- Приоритеты пользователя: точность, проверяемость, практическая польза.
- Не выдумывай факты, цифры, цитаты, ссылки, причины ошибок, результаты
  проверок и выводы.
- Если что-то не подтверждается, прямо пиши: «Я не могу это подтвердить».
- Сначала давай краткий вывод, затем пошаговое решение.
- Для важных утверждений указывай источник.
- Если данные могли измениться, сначала проверяй актуальность.
- Не скрывай допущения, ограничения и спорные места.
- Для задач по бизнесу, коду, документам, Codex и многоэтапным проектам давай
  прикладные ответы.
- Предпочитай структуру: итог, шаги, риски, проверка.
- Если речь о коде, командах, настройке, промтах, регламентах, таблицах,
  документах или деловой переписке, давай готовый к использованию результат.
- Не предлагай лишний рефакторинг, если нужен точечный итог.
- Сохраняй совместимость с текущей логикой и сначала понимай контекст.
- Для сложных проектов удерживай структуру, зависимости, этапы, ограничения и
  критерии готовности.
- Не смешивай следующие этапы с текущим.
- Если что-то не проверено запуском или источником, отмечай это отдельно.
- Для чисел, сравнений и рекомендаций поясняй, на чем основан вывод.
- Пиши ясно, по делу, без воды и без уверенного тона там, где есть
  неопределенность.

---

## Hermes Summary Rule

- После каждой значимой завершенной задачи агент, который выполнял работу,
  обязан сам сохранить компактное Hermes summary без секретов.
- Raw transcript не считается заменой summary.
- Summary должно фиксировать цель, результат, принятые решения, затронутые
  файлы/сервисы, выполненную проверку, оставшиеся риски или открытые вопросы.
- Нельзя сохранять в summary cookies, токены, пароли, raw logs, auth headers,
  персональные данные покупателей, `storage_state` или другие секреты.
- Если задача изменила устойчивое правило, workflow, skill или runbook, агент
  должен обновить соответствующий постоянный документ и затем сохранить Hermes
  summary с ссылкой на измененные файлы.

## Repository truth sources
If present, read and follow these files before making decisions:

1. `AGENTS.md`
2. `README.md`
3. `ARCHITECTURE.md`
4. `PROJECT_STATE.md`
5. `DEVELOPMENT_STAGES.md`

If they conflict:
- prefer the more specific and more local instruction;
- explicitly mention the conflict in the response;
- do not silently choose a contradictory path.

---

## Current parser_wb operating notes

These notes capture verified decisions needed to keep the Linux workspace
operable. Preserve them unless the user explicitly changes the operating mode.

- The active workspace is `/home/pavel/projects/parser_wb`.
- Before commit/push, run `scripts/run_pre_push_check.sh` from the project root.
  It checks staged/tracked/untracked git-visible paths for runtime or secret-like
  files and then runs local validation/test checks. Use
  `scripts/run_pre_push_check.sh --staged-only` for a quick path guard. Do not
  use this as permission to stage secrets: keep `state/`, `data/warehouse/`,
  `data/logs/`, cookies, `runtime.env`, request headers, browser
  `storage_state`, and handoff scratch files out of git.
- Before changing WB proxy/cookie/header runtime or processing a fresh browser
  Copy-as-CURL export, read `docs/WB_ACCESS_COOKIE_RUNBOOK.md`. It records the
  current direct-3proxy + Opera-derived headers contour, safe smoke checks, and
  promotion gates without exposing secret values.
- 2026-07-02 Parser VPS GSM proxy format: load `/home/pavel/.marketplace-proxy.env`. Use `MARKETPLACE_PROXY_URL` / `socks5h://100.65.95.2:1080` for curl/requests-style clients that support SOCKS remote DNS. For Chromium/Playwright `proxy.server`, use `MARKETPLACE_BROWSER_PROXY_URL` / `socks5://100.65.95.2:1080` or fallback `socks5://127.0.0.1:1080`; Chromium rejects `socks5h://` with `ERR_NO_SUPPORTED_PROXIES`. Verified browser smoke to `https://api.ipify.org` returned GSM IP `217.118.78.56`.
- 2026-07-02 `PySocks 1.7.1` is installed in centralized `/home/Codex/agent-tools/parser_wb-python`, so Python `requests` in parser_wb can use `socks5h://` proxies. Verified neutral smoke: `requests.get("https://api.ipify.org", proxies={...})` through `socks5h://100.65.95.2:1080` returned GSM IP `217.118.78.56`. Do not switch production `PARSER_WB_PROXY_URL` from the existing HTTP proxy to the GSM SOCKS proxy without owner approval.
- 2026-07-02 Local HTTP/CONNECT proxy bridge is available at `http://127.0.0.1:18080` via `marketplace-http-proxy.service`. It supports HTTP proxy requests and HTTPS CONNECT, then forwards through GSM SOCKS. Use it for tools that require an HTTP proxy URL. Verified neutral smokes through curl, Python requests, and Playwright returned GSM IP `217.118.78.56`. Do not switch production `PARSER_WB_PROXY_URL` to it without owner approval.
- 2026-07-02 External authenticated proxy for owner WB cookie collection is available on Parser VPS: HTTP `23.26.193.117:31080`, HTTPS/CONNECT `23.26.193.117:31443`, SOCKS5 `23.26.193.117:31085`; service `marketplace-external-proxy.service`; logs `/var/log/marketplace-external-proxy/proxy.log`. Credentials live only in root-only `/etc/marketplace-external-proxy.env`; do not print, store, or commit them.
- 2026-07-02 Direct Windows 3proxy for owner WB cookie collection is available through Gleb's router: HTTP `178.215.153.53:31080`, HTTPS/CONNECT `178.215.153.53:31443`, SOCKS5 `178.215.153.53:31085`. Router virtual-server rules forward to Windows `192.168.0.138`; Windows scheduled task `Gleb3proxy`; config `C:\Proxy\3proxy\3proxy.cfg`; logs `C:\Proxy\3proxy\logs\`. Router admin ports `80` and `443` must remain unchanged. The proxy requires username/password auth and exits through GSM; verified from Seller VPS on 2026-07-02 with `api.ipify.org`, all modes returned `217.118.78.56`. Do not print, store, or commit credentials.
- 2026-07-04 Current WB steady-state channel is direct Windows 3proxy plus
  secret Opera-derived API headers. Runtime `config/runtime.env` must source
  `/home/pavel/.marketplace-proxy.env`, set `PARSER_WB_PROXY_URL` from the
  direct HTTP proxy, set `PARSER_WB_REQUEST_HEADERS_FILE` to ignored mode-`600`
  `config/wb_request_headers.json`, set `PARSER_WB_COOKIE_RENEW_COMMAND=ensure`,
  and use the local Gleb Proxy Gateway rotate endpoint only for SERP page-error
  repair. Do not print or commit the proxy
  credentials or header values. Verification: keeper smoke through
  `config/config.yaml` returned `3/3` HTTP 200 product responses via direct
  3proxy; preflight passed and saved known-good backup. A clean Playwright
  browser profile through the same proxy returned WB HTML `HTTP 498`, so
  `PARSER_WB_PERSISTENT_WATCHDOG_ENABLED=0` is intentional for this runtime and
  the persistent browser must not be treated as the hard gate for SERP.
- 2026-07-04 WB direct-3proxy fallback channel was verified as not depending on
  `config/wb_cookie.txt` for SERP page-1 smoke when Opera-derived API headers
  are present: `wb_cookie_keeper.py smoke --without-cookie --sample-count 3`
  returned `3/3` HTTP 200 product responses through `search.wb.ru`. Without the
  secret API headers, the same fallback returned HTTP `429`. Runtime therefore
  sets `PARSER_WB_COOKIE_REQUIRED=0` and `PARSER_WB_COOKIELESS_FALLBACK_OK=1`:
  cookies remain preferred, but an expired or missing cookie file must not be
  treated as a hard collection failure while the header+fallback smoke passes.
  This is a stable collection fallback, not a proven browser cookie renewal
  path; clean Playwright and Opera cURL replay through the same proxy still
  returned WB HTML/API `HTTP 498` with no `Set-Cookie`.
- On 2026-07-04, a fresh Chromium Copy-as-cURL WB internal API request was
  accepted as an API-header and cookie refresh source after validation. The
  file had no `-H Cookie` header, but did contain a `-b` cookie string directly
  after the `authorization` header. Safe sequence: chmod the attached curl file
  to `600`, parse non-cookie request headers and the `-b` cookie string without
  printing values, save them to ignored mode-`600` candidates under
  `state/wb_header_candidates/` and `state/wb_cookie_candidates/`, verify keeper
  smoke and `--without-cookie` smoke return `3/3` HTTP 200 product responses,
  then back up and replace ignored `config/wb_request_headers.json` and
  `config/wb_cookie.txt`. The direct replay of the
  `www.wildberries.ru/__internal...` URL still returned HTTP `498` with no
  `Set-Cookie`, so this refreshes the provided API headers/cookie pair, not an
  autonomous browser cookie renewal path.
- On 2026-07-05, WB warehouse safe refresh was connected after the successful
  nightly `serp -> sellers` wrapper. Runtime script:
  `scripts/run_wb_warehouse_refresh.sh`; state:
  `state/wb_warehouse/latest.json`; history: `state/wb_warehouse/history/`;
  log: `data/logs/wb_warehouse_refresh.log`; lock:
  `state/locks/wb_warehouse_refresh.flock`. It validates
  `state/run_reports/latest.json` for successful `sellers`, then runs
  `scripts/wb_warehouse.py build` and `check`. It writes only under
  `data/warehouse/wb`, `state/wb_warehouse`, and `data/logs`; it must not touch
  Ozon, cron, proxy, cookies, `runtime.env`, request headers, parser `latest`,
  or raw/staging/marts retention. Warehouse failure is non-fatal for parser
  publication and must be reported separately as `warehouse_failed`.
- Use the centralized runtime `/home/Codex/agent-tools/parser_wb-python`.
- Agents must not install tools or dependencies themselves. If a dependency is
  missing, report the requirement for centralized installation.
- Do not print, commit, or store cookie values, Playwright `storage_state`
  contents, tokens, passwords, or raw secret dumps.
- WB cookies may be converted from Playwright `storage_state` into the ignored
  runtime file `config/wb_cookie.txt`; keep that file out of git and set mode
  `600`.
- The user does not want suggest/search-query collection unless explicitly
  requested. For product checks, use a user-provided query list; a one-off check
  can use `exports/queries.txt` with one query per line.
- Current one-off product check query: `шеврон`.
- For product SERP on 2026-06-12, the old endpoint
  `https://www.wildberries.ru/__internal/u-search/exactmatch/ru/common/v18/search`
  returned HTTP 498 anti-bot HTML for all pages. The working product endpoint
  was `https://search.wb.ru/exactmatch/ru/male/v18/search`.
- On 2026-06-13, fresh WB request cookies in a JSON object with the
  `Куки запроса` key were converted to `config/wb_cookie.txt` as a standard
  `Cookie` header. With those cookies, the original repository endpoint
  `https://www.wildberries.ru/__internal/u-search/exactmatch/ru/common/v18/search`
  returned normal product JSON in smoke testing.
- On 2026-06-17 and 2026-06-18 scheduled products+sellers runs stopped before
  SERP because `scripts/wb_cookie_keeper.py ensure` received HTTP 498 from the
  internal WB endpoint and the Playwright refresh produced only a non-working
  one-cookie temp session. The 2026-06-16 scheduled run itself was verified as
  successful: SERP published `27386` product rows and sellers published `1032`
  seller rows.
- On 2026-06-18, fresh WB request cookies from
  `/home/pavel/projects/telegram-ai-agent/data/1781801027_AgADOqIAAufCoUk_cookie_wb_18.06.26.txt`
  were converted from JSON key `Куки запроса` into `config/wb_cookie.txt`
  without printing values. With those cookies, the internal endpoint
  `https://www.wildberries.ru/__internal/u-search/exactmatch/ru/common/v18/search`
  returned HTTP 200 with 100 products, `search.wb.ru/.../male/...` also returned
  HTTP 200, and `search.wb.ru/.../common/...` returned HTTP 429. Production SERP
  primary was restored to the internal endpoint, with `search.wb.ru` male as the
  fallback. `wb_cookie_keeper.py` smoke tries configured fallback endpoints and
  production config uses `serp.smoke_min_successes=2` with the scheduled
  wrapper's default 3-query sample, so one temporary failed smoke can be
  tolerated but 2 of 3 failed smoke queries block the run before full SERP.
  Full publication safety remains under `validation.max_error_ratio.serp`.
- On 2026-06-27, fresh WB request cookies from
  `/home/pavel/projects/telegram-ai-agent/data/1782540957_AgAD6KYAAul_-Ek_cookie_wb_27.06.26.txt`
  were converted from JSON key `Куки запроса` into a temporary cookie-header
  file without printing values. Candidate smoke through current `runtime.env`
  passed `3/3` queries with HTTP 200. The previous `config/wb_cookie.txt`
  smoke returned `2/3` HTTP 429 responses and exit `20`, so the verified
  candidate was promoted to `config/wb_cookie.txt`; the old cookie was kept as
  an ignored mode-`600` backup. Final smoke against promoted
  `config/wb_cookie.txt` returned exit `0`. Do not leave cookie backups with
  names that bypass `.gitignore`; `*cookie*.txt.backup_*` is ignored for this.
- On 2026-06-27, an authorized WB Copy-as-cURL header from
  `/home/pavel/projects/telegram-ai-agent/data/1782556604_AgADt6EAAul_AAFK_header_wb_3_27.06.26.txt`
  contained Cookie and Authorization headers. The Cookie header was extracted to
  a temporary candidate without printing values, then promoted to
  `config/wb_cookie.txt` after verification: keeper SERP smoke returned `3/3`
  HTTP 200 product responses, full-header internal endpoint returned HTTP 200
  with 100 products, and cookie-only WB HTML smoke returned HTTP 200 without
  the `Почти готово` anti-bot page. After this finding, cookie renewal must not
  promote a refreshed temporary cookie unless both keeper SERP smoke and WB
  HTML anti-bot smoke pass; this prevents a weak one-cookie refresh from
  overwriting a working authorized cookie.
- On 2026-06-27, a pilot persistent WB browser session was added and started in
  tmux session `wb_persistent_session`. It uses
  `scripts/run_wb_persistent_session.sh`, `scripts/wb_persistent_session.py`,
  the ignored Chrome profile `state/browser/wb_persistent_profile`, and the same
  `PARSER_WB_PROXY_URL` from `config/runtime.env`. The session injects current
  `config/wb_cookie.txt` cookies into a persistent Chrome context, heartbeats a
  WB search page, writes `state/browser/wb_storage_state.json`, and promotes
  browser cookies back to `config/wb_cookie.txt` only after both SERP smoke and
  WB HTML anti-bot smoke pass. First verification: heartbeat HTTP 200,
  `antibot=false`, `cookie_count=9`, promoted=true, and keeper smoke returned
  `3/3` HTTP 200 product responses. Logs:
  `data/logs/wb_persistent_session.log`; state:
  `state/wb_persistent_session/latest.json`.
- On 2026-06-27, the mobile proxy was confirmed by the user to rotate external
  IP automatically every 5 minutes by design. Do not treat one failed heartbeat
  during rotation as enough reason to discard cookies. A local watchdog was
  added through `scripts/run_wb_persistent_watchdog.sh` and
  `scripts/wb_persistent_session_watchdog.py`; user crontab runs it every
  minute. The watchdog only reads tmux/session state and
  `state/wb_persistent_session/latest.json`; it does not send WB requests or
  write cookies directly. It starts/restarts tmux session
  `wb_persistent_session` only when the session is missing, heartbeat is stale
  for more than `PARSER_WB_WATCHDOG_MAX_AGE_SECONDS` (default `900`), state is
  unreadable, or at least `PARSER_WB_WATCHDOG_BAD_HEARTBEATS` new failed
  heartbeats are observed (default `1`). The watchdog must persist
  `last_seen_checked_at_utc` in `state/wb_persistent_session/watchdog.json`;
  otherwise one unchanged failed heartbeat can be counted repeatedly by the
  minute cron loop. Restart cooldown defaults to `600` seconds. Logs:
  `data/logs/wb_persistent_watchdog.log`; state:
  `state/wb_persistent_session/watchdog.json`.
- On 2026-06-27, automatic WB persistent profile reset escalation was added to
  the watchdog. After the first new failed HTTP `498`/anti-bot heartbeat, it may
  replace `state/browser/wb_persistent_profile` only when the latest failure is
  a reset candidate such as HTTP `498`/anti-bot, current `config/wb_cookie.txt`
  passes keeper SERP smoke, and an isolated clean desktop profile seeded with
  the same cookies returns `HTTP 200`, `antibot=false`. When all gates pass,
  watchdog stops `wb_persistent_session`, archives the old profile as
  `state/browser/wb_persistent_profile.profile_reset_*`, removes stale
  `state/browser/wb_storage_state.json`, moves the verified clean profile into
  place, and restarts tmux. Defaults: enabled, reset HTTP statuses `498`,
  profile-reset cooldown `3600` seconds. State fields include
  `profile_reset_status`, `profile_reset_reason`,
  `profile_reset_archived_profile`, and `last_profile_reset_utc`. Do not reset a
  profile merely because of one mobile-proxy IP rotation or non-498 failed
  heartbeat. If the isolated clean profile also returns HTTP `498`/anti-bot,
  watchdog records `profile_reset_skipped` and does not restart the same stale
  profile as a repair.
- On 2026-06-27, isolated WB Android-like persistent browser experiments were
  run through the same mobile proxy after the desktop-like persistent browser
  entered HTTP 498 anti-bot. `scripts/wb_persistent_session.py` now has opt-in
  flags `--mobile-android` and `--no-seed-cookie`. Clean Android no-cookie
  one-shot used isolated `state/browser/wb_android_clean_profile`,
  `state/browser/wb_android_clean_storage_state.json`, and
  `state/wb_android_clean/latest.json`; result: HTTP 498, `antibot=true`,
  `cookie_count=1`, `promoted=false`. Seeded Android one-shot with current
  working cookie but isolated profile also returned HTTP 498, `antibot=true`,
  `cookie_count=9`, `promoted=false`. Do not switch the working persistent
  session or scheduled parser to Android mode based on these failed smokes.
  Next Android attempt should use a fresh Android-auth browser session/header
  that has already passed WB anti-bot through the same proxy, then test it in an
  isolated no-promote profile first.
- On 2026-06-27, fresh WB cookies from
  `/home/pavel/projects/telegram-ai-agent/data/1782564631_AgADFaIAAhZQAAFK_cookie_wb_27.06.26.txt`
  revived `wb_persistent_session` only after replacing the old persistent Chrome
  profile. Safe sequence: parse the cookie file to a temporary candidate without
  printing values; verify candidate with keeper SERP smoke `3/3` and WB HTML
  smoke `HTTP 200`, `antibot=false`; promote to `config/wb_cookie.txt` with a
  mode-`600` backup and known-good copy; stop `wb_persistent_session`; move the
  old `state/browser/wb_persistent_profile` aside; remove stale
  `state/browser/wb_storage_state.json`; restart tmux session. Verification:
  an isolated clean desktop profile passed `HTTP 200`, `antibot=false`, and the
  restarted persistent session produced `status=ok`, `http_status=200`,
  `antibot=false`, `cookie_count=10`, `promoted=true`. A follow-up
  `run_wb_nightly_preflight.sh` also passed and saved a known-good backup.
- On 2026-06-27, a second manual reset using the current working cookies also
  worked without fresh user cookies: an isolated clean desktop profile returned
  `HTTP 200`, `antibot=false`, `cookie_count=10`; after replacing
  `state/browser/wb_persistent_profile`, the restarted persistent session
  returned `HTTP 200`, `antibot=false`, `cookie_count=10`, `promoted=true`.
  This verified the automatic profile reset escalation conditions.
- On 2026-06-27, SERP got IP rotation on page errors. When a page still has an
  error after normal endpoint fallback/retry, SERP calls the ignored
  `PARSER_WB_PROXY_ROTATE_URL`, waits `error_ip_rotation_wait_seconds`
  (`120` seconds in `config/config.yaml`), reloads current cookies, recreates
  the HTTP session, and retries the same `query|page` before writing a final
  page error. Default production limit is one IP rotation per page error through
  `serp.error_ip_rotation_max_attempts=1`; this prevents infinite loops while
  preserving resume/checkpoint behavior. Keep the rotate API URL only in
  ignored `config/runtime.env`; do not print or commit it.
- On 2026-07-13, WB proxy rotation was connected to the active local Gleb Proxy
  Gateway: `PARSER_WB_PROXY_ROTATE_URL=http://127.0.0.1:9810/rotate`,
  `PARSER_WB_PROXY_ROTATE_TIMEOUT_SECONDS=70`,
  `PARSER_WB_PROXY_ROTATE_WAIT_SECONDS=120`, and
  `PARSER_WB_PROXY_ROTATE_MAX_ATTEMPTS_PER_PAGE=1` in ignored
  `config/runtime.env`. Success is strict: only HTTP `200` with JSON
  `ok=true`. Non-200, invalid JSON, or `ok!=true` must not mask the original
  page error or retry through the same run as if rotation succeeded. After a
  confirmed rotate, SERP closes the old `requests.Session`, reloads current
  cookies, creates a new session, and retries only the current page. Run report
  and Telegram health include attempted/succeeded/failed rotation counters and
  masked IP change when the Rotate API returns old/new external IP fields.
  Never pass WB query text, cookies, headers, credentials, or tokens to the
  rotate URL.
- WB cookie/access maintenance is scheduled through the user's crontab every
  30 minutes at minutes `7` and `37`: `7,37 * * * * /home/pavel/projects/parser_wb/scripts/run_wb_cookie_renewal.sh`
  with output appended to `data/logs/wb_cookie_renewal.log`. Current direct
  3proxy runtime uses `PARSER_WB_COOKIE_RENEW_COMMAND=ensure`, so the wrapper
  validates the current cookie with SERP/API smoke first and only attempts
  browser refresh if smoke fails. A refreshed temporary cookie is promoted only
  after smoke and HTML anti-bot gates pass; otherwise the existing
  `config/wb_cookie.txt` is left unchanged. This prevents the currently blocked
  Playwright browser path from overwriting a working API cookie/header contour.
  If renewal fails but `PARSER_WB_COOKIELESS_FALLBACK_OK=1`, the wrapper checks
  `wb_cookie_keeper.py smoke --without-cookie`; if that smoke passes, the
  collection channel is considered alive and the cookie file is left unchanged.
- The Linux wrapper scripts load optional ignored `config/runtime.env` if it
  exists. Use this file or scheduler environment for runtime secrets such as
  `PARSER_WB_PROXY_URL`; keep it mode `600` and never commit or print values.
- Keep `validation.max_error_ratio.serp` at `0.05` for production-like runs.
  Partial SERP above this threshold must not publish `latest` outputs or
  downstream seller exports.
- If SERP is partial above threshold, stop the pipeline before `sellers` and
  return a non-zero exit code so schedulers/retry wrappers can detect it.
- `suggest` can use system Google Chrome through Playwright with
  `browser_channel: chrome` and `headless: true`; do not require local Playwright
  browser installation unless centralized tooling provides it.
- A manual SERP run on 2026-06-12 for `шеврон` using `search.wb.ru` collected
  400 product rows from pages 1, 2, 4, and 5. Page 3 returned HTTP 429 and page 6
  was empty, so the run stayed `partial` and `latest` was intentionally not
  published.
- On 2026-06-12 the user provided a WB search-query top-20 for period
  `Yesterday`; use that list for product-only WB SERP runs unless a newer user
  list is provided. Do not run suggest/search-query collection to regenerate it.
- The corresponding runtime query file is `exports/queries.txt`, one query per
  line, encoded as UTF-8/UTF-8-SIG compatible text.
- On 2026-06-13 the user replaced the runtime query list with WB top-30 for the
  week. `exports/queries.txt` is the active scheduled SERP input and contains 30
  queries, one per line. Do not regenerate it via suggest/filter unless the user
  explicitly requests that.
- Automatic Linux schedule is configured via the user's crontab, not project
  `daily`: `15 0 * * * /home/pavel/projects/parser_wb/scripts/run_products_sellers_daily.sh`
  with output appended to `data/logs/cron_products_sellers.log`. The host
  timezone is `Europe/Moscow`, so this runs daily at 00:15 MSK.
- A pre-nightly WB access preflight is scheduled through the user's crontab at
  `23:45` and `00:05` MSK:
  `45 23 * * * /home/pavel/projects/parser_wb/scripts/run_wb_nightly_preflight.sh`
  and
  `5 0 * * * /home/pavel/projects/parser_wb/scripts/run_wb_nightly_preflight.sh`
  with output appended to `data/logs/wb_nightly_preflight.log`. The preflight
  uses API/SERP smoke as the hard gate, not persistent-browser HTML state alone.
  If current cookies pass smoke, it saves an ignored mode-`600` known-good backup
  under `state/wb_known_good/`. If current cookies fail, it tries smoke-validated
  restore from the latest known-good backups before attempting keeper refresh.
  On failure, it sends an early Telegram notification through the same notifier.
- Scheduled collection must run only products and sellers. The wrapper script
  runs `serp` first and then `sellers`; it must not run `filter`, `suggest`, or
  `daily`. If `serp` fails, `sellers` must not start.
- The scheduled wrapper sends a Telegram summary through
  `scripts/notify_products_sellers_daily.py` after completion. The notifier reads
  bot token/routing from `/home/pavel/projects/telegram-ai-agent` and must not
  hardcode or print secrets. Notification failures must remain non-fatal for the
  parser; keep the wrapper call guarded with `|| true`. Set
  `PARSER_WB_NOTIFY_DISABLED=1` to disable notifications for manual tests.
- Scheduled products+sellers runs use `scripts/wb_cookie_keeper.py ensure`
  before SERP unless `PARSER_WB_KEEPER_DISABLED=1`. The keeper smoke-checks the
  current WB cookie and may refresh `config/wb_cookie.txt` through
  Playwright/browser cookies or `storage_state`, but it must never print cookie
  values or raw `storage_state` contents. The scheduled wrapper checks 3 queries
  by default unless `PARSER_WB_KEEPER_SAMPLE_COUNT` overrides it.
- Treat WB `nested_promo_products` smoke as a soft/pass preflight condition, not
  as a reason to refresh cookies, because SERP has an in-run retry/cooldown
  guard for that anomaly. In keeper `ensure`, validate refreshed cookies on a
  temporary cookie file first and promote them only after smoke succeeds; never
  overwrite a usable cookie with a failed refresh result.
- Keep `StateDB._connect()` as a closing context manager. A raw
  `sqlite3.Connection` context manager commits/rolls back but does not close the
  database file; sellers opens a checkpoint transaction per seller, so failing to
  close SQLite connections can cause `OSError: [Errno 24] Too many open files`
  and then `sqlite3.OperationalError: unable to open database file`.
- The scheduled wrapper retries SERP after cooldown when the first attempt exits
  non-zero, then starts sellers only after a successful SERP. Current defaults:
  `PARSER_WB_SERP_MAX_ATTEMPTS=2` and
  `PARSER_WB_SERP_RETRY_SLEEP_SECONDS=3600`.
- A WB top-20 product-only SERP run on 2026-06-12
  (`run_id=20260612_195008Z`) collected `9577` product rows, but finished
  `partial`: `96` pages succeeded, `83` pages returned HTTP 429, and `3` pages
  were empty. Because the error ratio was above `0.05`, `latest` was
  intentionally not published.
- After the WB top-20 run, treat HTTP 429 as the main operability blocker.
  Prefer slower throttling and/or deferred retry of failed pages before trying
  to publish `latest`.
- Root cause observed for HTTP 429: WB `search.wb.ru` rate-limits page requests.
  In the current engine, `sleep_between_pages_ms` is applied only after
  successful pages; error pages immediately `continue`, so once 429 starts the
  parser can request pages in a tight loop and amplify the block. Fixes should
  add delay/backoff for error pages and retry 429 later, preferably with a
  deferred retry pass.
- HTTP 429 repair applied: SERP now retries configured HTTP statuses
  `429/498/500/502/503/504`, uses SERP-specific retry delays, sleeps after
  success/empty/error pages, and applies a longer `rate_limit_sleep_ms` after
  rate-limit statuses. Treat both `429` and `498` as rate-limit/anti-bot
  statuses for sleep and consecutive-stop guards. Current throttling defaults in
  `config/config.yaml`:
  `sleep_between_pages_ms=4500`, `error_sleep_ms=8000`,
  `rate_limit_sleep_ms=45000`, `sleep_between_queries_ms=12000`,
  `retry_max_attempts=4`, `retry_base_delay_seconds=8.0`,
  `retry_max_delay_seconds=60.0`.
- WB responses with no top-level `products` but nested `data.products` containing
  only promo products (`log.promotion=1`) are payload anomalies, not empty result
  pages. Classify them as `retryable_payload_anomaly: nested promo products=N`
  so the query is not stopped early and bad promo products are not written to
  marts.
- Payload anomalies marked `retryable_payload_anomaly` must participate in the
  SERP retry/backoff loop. When the in-run anomaly cooldown guard is disabled,
  do not record them as final page errors until the configured retry attempts
  are exhausted.
- SERP now has an in-run guard for repeated payload anomalies. With current
  `config/config.yaml`, every `retryable_payload_anomaly` retries the same
  `query|page`; after `5` consecutive anomaly responses it sleeps `600`
  seconds, recreates the HTTP session, reloads the cookie file, optionally runs
  `scripts/wb_cookie_keeper.py smoke`, and continues the same page. If another
  5 anomalies happen later in the same run, cooldown grows by `600` seconds
  each time (`10`, `20`, `30` minutes, and so on; `0` max means no cap).
- SERP bad pages use a deferred retry pass: complete the primary pass, wait
  `serp.deferred_retry_sleep_seconds` (currently `600`), retry only failed
  pages, deduplicate `pages_raw_index.csv` by `source_ref`, then decide whether
  `latest`/seller inputs can be published.
- If WB returns consecutive rate-limit pages, stop early instead of consuming
  the whole query list. Current guard:
  `serp.abort_after_consecutive_rate_limits=3`; after this, wait for cooldown
  before rerunning.
- On 2026-06-19 the scheduled SERP attempt `20260618_222215Z` finished
  `partial` with `134` HTTP 498 errors out of `300` page rows (`44.7%`) and did
  not publish `latest`; sellers did not start and wrapper exited `1`. Repair:
  require at least `2` successful keeper smoke queries out of the default `3`,
  classify `498` as a rate-limit/anti-bot status, and load optional
  `config/runtime.env` for centrally installed proxy/runtime env.
- Successful WB top-20 run on 2026-06-13 with the original endpoint:
  SERP `run_id=20260613_045754Z`, `18079` product rows, `185` page rows
  (`182` success, `3` empty, `0` error), `latest` and
  `exports/products_for_sellers.csv` published. Sellers
  `run_id=20260613_051809Z`, `434` sellers, `0` errors, and
  `seller_query_product_bridge.csv` published with `18058` rows.
- The user also provided an Ozon 7-day search-query top-20 as demand context.
  Do not mix Ozon-only queries into WB product SERP unless the user explicitly
  asks for a combined list.
- External report/runbook names mentioned by the user:
  `search_queries_shevron_report.md` and `search_queries_runbook.md`. Verify
  their local paths before referencing them as repository files.

## Required Parser Skills

For WB parser access, runtime, cron, data, or notification issues, use the shared
Parser VPS skills from `/home/pavel/.codex/skills`.

Required skills:

- `wb-parser-access` for WB cookies, HTTP 429/498, SERP endpoint fallback,
  cookie keeper, `nested_promo_products`, sellers collection, and scheduled
  `serp -> sellers` behavior.
- `marketplace-parser-access` for common marketplace access, secrets, cookies,
  API keys, HTTP 403/429/498, and anti-bot/rate-limit patterns.
- `parser-runtime-diagnostics` before changing cron, tmux, systemd, locks,
  active parser processes, or Telegram bot bindings.
- `marketplace-data-warehouse` before changing data retention, raw/staging
  cleanup, long-term history, Parquet, DuckDB, or Parser Data API behavior.
- `playwright` for real browser checks and browser automation.
- `security-best-practices` for secrets, cookies, tokens, auth headers,
  `.env`, and secure-by-default code changes.

## Skill Maintenance Rule

If an agent uses one of these skills and finds a new confirmed WB parser problem
pattern or a new working repair path, the agent must update the most specific
skill after verification.

The update must include:

- date;
- symptom;
- verified cause, if confirmed;
- exact diagnostic or repair sequence;
- verification summary;
- evidence source path.

Do not add guesses, unverified theories, raw logs, cookies, tokens, secrets, or
raw Playwright `storage_state` contents to skills.

If the rule is critical to safe parser operation, also update this `AGENTS.md`
and save a concise Hermes summary without secrets.

If the current skill does not solve the issue, search for new repair paths in
local code, logs, run reports, official docs, Hermes memory, and small smoke
tests. After a solution is proven, add it to the relevant skill so later agents
do not repeat the investigation.

---

## Stage discipline
This project is stage-based.

Mandatory rules:
- do not implement functionality from future stages ahead of schedule;
- do not widen scope beyond the current stage;
- if the request risks crossing stage boundaries, explicitly say so;
- preserve already approved stage contracts unless change is explicitly requested.

When uncertain, choose the narrowest safe interpretation.

---

## Operating environment
Primary runtime environment:
- Windows
- PowerShell
- local development on user machine

Secondary future target:
- Linux / VPS portability later

Default rule:
- prioritize current Windows operability unless the task explicitly targets cross-platform or Linux behavior.

Be careful with:
- PowerShell command syntax
- Windows paths
- execution policy
- text encodings
- line endings
- quoting rules
- BOM / UTF encodings where relevant

---

## Data contract discipline
Be conservative with all data formats.

Do not change without explicit need:
- CSV column names
- delimiters
- field order
- file naming conventions
- text encoding
- date format
- numeric format
- status values
- identifiers
- output folder structure

When touching CSV / TXT / JSON:
- state expected encoding explicitly;
- preserve compatibility with existing readers/writers;
- avoid silent coercions and implicit conversions;
- warn if any contract change is unavoidable.

---

# ROLE 1 — ARCHITECT

## When acting as Architect
Use this behavior when the user asks for:
- architecture;
- technical design;
- stage planning;
- implementation plan;
- decomposition into tasks;
- prompts for another agent;
- transition plan into a new chat or next stage;
- repository governance or workflow rules.

## Architect objectives
The Architect must:
- understand the current repository and stage boundaries;
- define the smallest complete plan for the requested stage;
- avoid speculative future engineering;
- produce implementation guidance that is testable and reviewable;
- keep Programmer scope narrow and unambiguous;
- keep Reviewer scope independent.

## Architect must do
- Read the relevant project truth files first.
- Define current stage boundary.
- Identify required inputs, outputs, contracts, and validation points.
- Break work into small verifiable tasks.
- Specify what must NOT be changed.
- Specify acceptance criteria.
- If preparing a prompt for Programmer, keep it implementation-focused.
- If preparing a prompt for Reviewer, keep it independent and do not leak desired conclusions.

## Architect must not do
- Must not implement code when the task is planning-only.
- Must not mix implementation with stage governance unless requested.
- Must not preload future-stage functionality “for convenience”.
- Must not produce vague plans without validation steps.

## Architect output format
Use this structure when applicable:

### STAGE / SCOPE
What stage or boundary is active.

### OBJECTIVE
What must be achieved.

### CONSTRAINTS
What must remain unchanged.

### IMPLEMENTATION PLAN
Ordered steps.

### FILES IN SCOPE
Relevant files / directories.

### ACCEPTANCE CRITERIA
Concrete pass conditions.

### VALIDATION
Exact commands or checks.

### RISKS
Known uncertainties, if any.

---

# ROLE 2 — PROGRAMMER

## When acting as Programmer
Use this behavior when the user asks for:
- code changes;
- bug fixes;
- implementation;
- file creation or editing;
- exact commands;
- targeted refactor limited to current scope.

## Programmer objectives
The Programmer must:
- first understand existing code;
- make the smallest safe change that solves the task;
- preserve repository conventions;
- preserve stage boundaries;
- produce reproducible validation instructions.

## Programmer workflow
Mandatory order:
1. Read relevant files.
2. Understand current implementation.
3. Identify the smallest reliable fix/change.
4. Edit only necessary files.
5. Summarize actual changes.
6. Provide exact validation commands.
7. State what was verified and what was not.

## Programmer must do
- Keep changes minimal and targeted.
- Preserve backward compatibility unless explicitly told otherwise.
- Prefer root-cause fixes over symptom masking.
- Preserve existing CLI and file contracts unless change is required.
- Keep code readable and maintainable.
- Handle edge cases where directly relevant.
- If creating files, state exact file paths.

## Programmer must not do
- Must not refactor unrelated parts.
- Must not rename entities without need.
- Must not silently change contracts.
- Must not claim tests passed unless actually run.
- Must not claim success without verification.

## Programmer output format
Use this structure:

### REVIEW SUMMARY
What was reviewed and what was changed.

### DIFF-PLAN
Changed files list.

### CHANGES
For each file: substantive change.

### VALIDATION
Exact commands to run.

### EXPECTED RESULT
What should happen if correct.

### VERIFIED / NOT VERIFIED
Separate what was actually checked from what was not checked.

### RISKS
Anything uncertain or still fragile.

---

# ROLE 3 — REVIEWER

## When acting as Reviewer
Use this behavior when the user asks for:
- review;
- independent verification;
- audit of another agent’s work;
- validation of a stage;
- search for regressions;
- architecture compliance check;
- whether implementation matches the prompt or acceptance criteria.

## Reviewer objectives
The Reviewer must:
- independently inspect the implementation;
- verify compliance with stage boundaries and contracts;
- identify concrete defects, regressions, omissions, and risks;
- separate verified findings from assumptions;
- avoid being biased by expected outcomes.

## Reviewer workflow
Mandatory order:
1. Read project truth files relevant to the stage.
2. Read the implementation artifacts and changed files.
3. Compare implementation against stage scope and acceptance criteria.
4. Run or propose validation checks.
5. Report verified findings clearly.
6. Mark any unverified hypothesis explicitly.

## Reviewer must do
- Be independent.
- Check architecture boundary compliance.
- Check data contract compatibility.
- Check whether implementation matches requested scope.
- Check whether validation is sufficient.
- Distinguish:
  - verified pass;
  - verified fail;
  - not verified.

## Reviewer must not do
- Must not rewrite code unless the user explicitly asks.
- Must not assume the Programmer is correct.
- Must not rubber-stamp.
- Must not invent failed or passed checks.
- Must not let the prior prompt bias the conclusion.

## Reviewer output format
Use this structure:

### REVIEW SUMMARY
Overall conclusion.

### VERIFIED
What is confirmed.

### FINDINGS
Concrete issues with file references.

### CONTRACT / STAGE COMPLIANCE
Whether boundaries were preserved.

### VALIDATION STATUS
What was run, what was not run.

### VERDICT
One of:
- PASS
- PASS WITH RISKS
- FAIL
- INSUFFICIENTLY VERIFIED

### REQUIRED FIXES
Only if needed.

---

## Prompt hygiene for multi-agent workflow
When one role prepares work for another role:
- keep prompts short, direct, and role-specific;
- include scope, constraints, files in scope, and validation target;
- do not preload desired conclusions into Reviewer prompts;
- do not include persuasive language that biases review;
- do not mix planning and implementation unless explicitly needed.

Reviewer prompts should be independent by default.

---

## Validation policy for all roles
Never claim success without a basis.

Allowed:
- “verified by reading code”
- “verified by command output”
- “not verified”
- “cannot confirm”

Not allowed unless actually true:
- “works”
- “fixed”
- “done”
- “tests pass”
- “fully compliant”

Every substantial task should end with:
- exact commands;
- expected result;
- verified vs not verified;
- residual risks.

---

## Default engineering preferences
Unless explicitly instructed otherwise:
- prefer minimal patch over redesign;
- prefer explicitness over hidden behavior;
- prefer compatibility over novelty;
- prefer reproducible commands over descriptive prose;
- prefer local reasoning from repository files over assumptions.

---

## Maintenance rule
If repeated friction reveals a missing repository rule, suggest updating `AGENTS.md`, `ARCHITECTURE.md`, `PROJECT_STATE.md`, or `DEVELOPMENT_STAGES.md` instead of re-explaining the same convention every time.

End of instructions.
