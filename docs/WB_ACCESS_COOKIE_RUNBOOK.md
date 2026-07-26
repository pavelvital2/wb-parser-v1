# WB Access And Cookie Runbook

## Итог

Текущий рабочий WB-канал на Parser VPS - это API/SERP контур через direct
Windows 3proxy и секретные Opera-derived request headers. Persistent browser
сейчас не является hard gate для сбора: через тот же proxy чистый browser может
получать WB HTML `HTTP 498`, пока API/SERP smoke остается рабочим.

Эта инструкция описывает, что проверять и как безопасно обновлять access inputs
без печати и коммита cookies, request headers, proxy credentials, tokens или
`storage_state`.

## Что Нельзя Делать

- Не печатать значения cookies, request headers, authorization, proxy URL с
  credentials, Telegram tokens или `storage_state`.
- Не коммитить `config/runtime.env`, `config/wb_cookie.txt`,
  `config/wb_request_headers.json`, backups/candidates, `state/`, `data/logs/`
  или `data/warehouse/`.
- Не запускать `suggest`, `filter` или full `daily` для проверки доступа.
- Не увеличивать concurrency и не убирать backoff при `429`/`498`.
- Не считать browser renewal доказанным автономным способом обновления cookies.
- Не продвигать новый cookie/header candidate без smoke-проверок.
- Не трогать Ozon в рамках WB access repair.

## Рабочий Контур

Runtime files, все ignored и локальные:

```text
config/runtime.env
config/wb_cookie.txt
config/wb_request_headers.json
state/wb_session_keeper/latest.json
state/wb_nightly_preflight/latest.json
state/wb_known_good/
data/logs/wb_cookie_renewal.log
data/logs/wb_nightly_preflight.log
data/logs/cron_products_sellers.log
```

Ожидаемая логика runtime:

- `config/runtime.env` source'ит `/home/pavel/.marketplace-proxy.env`.
- `PARSER_WB_PROXY_URL` указывает на текущий direct HTTP proxy из ignored env.
- `PARSER_WB_REQUEST_HEADERS_FILE` указывает на mode-`600`
  `config/wb_request_headers.json`.
- `PARSER_WB_COOKIE_RENEW_COMMAND=ensure`.
- `PARSER_WB_COOKIE_REQUIRED=0`.
- `PARSER_WB_COOKIELESS_FALLBACK_OK=1`.
- `PARSER_WB_PROXY_ROTATE_URL` указывает на локальный Rotate API без
  credentials: `http://127.0.0.1:9810/rotate`.
- `PARSER_WB_PROXY_ROTATE_TIMEOUT_SECONDS=70`.
- `PARSER_WB_PROXY_ROTATE_WAIT_SECONDS=120`.
- `PARSER_WB_PROXY_ROTATE_MAX_ATTEMPTS_PER_PAGE=1`.
- `PARSER_WB_PERSISTENT_WATCHDOG_ENABLED=0` допустим для текущего direct-proxy
  режима, если browser HTML blocked, а API/SERP smoke проходит.

Не вставляй значения этих переменных в git, чат, summary или docs.

## Быстрая Диагностика

Перед любым repair:

```bash
cd /home/pavel/projects/parser_wb

pgrep -af 'run_products|run_sellers|main.py|wb_cookie_keeper|wb_warehouse' || true

for f in \
  state/locks/products_sellers_daily.flock \
  state/locks/wb_cookie_renewal.flock \
  state/locks/wb_warehouse_refresh.flock \
  state/locks/pipeline.lock
do
  (flock -n 9 && echo "lock_free $f" || echo "lock_busy $f") 9>"$f"
done
```

Проверить локальное состояние без вывода секретов:

```bash
/home/Codex/agent-tools/parser_wb-python/bin/python -m json.tool state/wb_session_keeper/latest.json
/home/Codex/agent-tools/parser_wb-python/bin/python -m json.tool state/wb_nightly_preflight/latest.json
/home/Codex/agent-tools/parser_wb-python/bin/python -m json.tool state/wb_warehouse/latest.json
tail -n 80 data/logs/cron_products_sellers.log
tail -n 80 data/logs/wb_cookie_renewal.log
tail -n 80 data/logs/wb_nightly_preflight.log
```

Проверить текущий API/SERP доступ:

```bash
set -a
source config/runtime.env
set +a

/home/Codex/agent-tools/parser_wb-python/bin/python scripts/wb_cookie_keeper.py smoke \
  --config config/config.yaml \
  --cookie-file config/wb_cookie.txt \
  --sample-count 3
```

Проверить fallback без cookie, но с secret API headers:

```bash
set -a
source config/runtime.env
set +a

/home/Codex/agent-tools/parser_wb-python/bin/python scripts/wb_cookie_keeper.py smoke \
  --config config/config.yaml \
  --cookie-file config/wb_cookie.txt \
  --sample-count 3 \
  --without-cookie
```

Ожидаемый здоровый результат: smoke возвращает exit `0`, в state видно
достаточно успешных запросов. Если cookie-smoke падает, но `--without-cookie`
проходит, collection channel еще жив за счет headers+fallback; это не означает,
что cookies автономно обновляются.

## Как Читать Симптомы

`WB API smoke ok`

: Сбор можно считать доступным для SERP/API. Browser HTML может быть blocked,
  это не hard failure для текущего runtime.

`cookie smoke failed`, но `--without-cookie smoke ok`

: Cookie слабый или отсутствует, но headers+fallback still usable. Не
  перезаписывай cookie невалидным browser refresh. Проверь nightly report и
  renewal log.

`cookie smoke failed` и `--without-cookie smoke failed`

: Канал доступа сломан. Сначала проверяй `runtime.env`, headers file presence,
  proxy state и recent logs. Не запускай full SERP до восстановления smoke.

Browser `HTTP 498`, API/SERP smoke ok

: Browser channel blocked. Для текущего direct-proxy режима это диагностический
  сигнал, а не причина останавливать SERP.

Много `429`/`498` в `pages_raw_index.csv`

: Это rate-limit/anti-bot на сборе. Не повышай скорость. Смотри retry,
  deferred retry и IP rotation logs.

## Proxy Rotation

WB SERP вызывает local Rotate API только как реакцию на ошибку текущей страницы,
после штатных retry/fallback. Периодической ротации "на всякий случай" нет.

Контракт успешной ротации:

```text
GET http://127.0.0.1:9810/rotate
HTTP 200
JSON ok=true
```

Если endpoint вернул не `200`, invalid JSON или `ok` не `true`, WB не повторяет
эту страницу через тот же run как будто ремонт удался: ошибка остается ошибкой,
дальше работают обычные пороги partial/latest.

После успешного rotate SERP:

- ждет `PARSER_WB_PROXY_ROTATE_WAIT_SECONDS`;
- закрывает старую `requests.Session`;
- заново читает текущий cookie file;
- создает новую `requests.Session`;
- повторяет только текущую `query|page`;
- делает не больше одной ротации на страницу.

Rotate URL не должен содержать query, cookies, токены, credentials или значения
request headers. Старый/новый внешний IP в run report и Telegram health
показываются только в маскированном виде, если Rotate API вернул эти поля.

## Безопасное Обновление Из Copy-As-CURL

Используй этот путь только если владелец прислал свежий browser/API
Copy-as-cURL или cookie export.

1. Сохрани attachment в ignored location и выставь mode `600`.
2. Распарси non-cookie request headers и cookie string без печати значений.
3. Сохрани candidates под ignored `state/wb_header_candidates/` и
   `state/wb_cookie_candidates/`.
4. Запусти smoke с candidate cookie.
5. Запусти `--without-cookie` smoke с candidate headers.
6. Только если оба smoke проходят, сделай backup текущих runtime files и
   замени:

```text
config/wb_request_headers.json
config/wb_cookie.txt
```

7. После promotion повтори smoke на уже promoted files.

Нельзя продвигать файл, если:

- нет cookie/header material для runtime;
- smoke не проходит;
- direct replay WB HTML/API вернул `498` и нет успешного keeper smoke;
- candidate получен из browser session, который сам не прошел anti-bot.

## Renewal И Preflight

30-минутная maintenance job:

```text
scripts/run_wb_cookie_renewal.sh
```

Текущий режим - `ensure`, не blind browser renew. Он сначала проверяет текущий
канал через API/SERP smoke. Browser refresh допускается только после fail и не
должен перезаписывать рабочий cookie/header contour без успешных gates.

Pre-nightly checks:

```text
scripts/run_wb_nightly_preflight.sh
```

Preflight использует API/SERP smoke как hard gate перед ночным `serp ->
sellers`, сохраняет known-good backups и пытается восстановиться из них при
провале.

## Что Смотреть После Ночи

Основной Telegram report теперь содержит `Health` block:

- `WB API smoke`
- `Preflight`
- `SERP latest`
- `Latest`
- `Run report`
- `Proxy rotation`
- `Browser channel`
- `Warehouse`

Локальные источники:

```text
state/wb_session_keeper/latest.json
state/wb_nightly_preflight/latest.json
data/raw/serp/latest/pages_raw_index.csv
state/run_reports/latest.json
state/wb_persistent_session/watchdog.json
state/wb_persistent_session/latest.json
state/wb_warehouse/latest.json
```

## Перед Commit/Push

Всегда запускать:

```bash
scripts/run_pre_push_check.sh
```

Быстрая проверка staged paths:

```bash
scripts/run_pre_push_check.sh --staged-only
```

Если check падает на forbidden path, не обходи его через force add. Убери
runtime/secret file из staged или добавь корректное ignore-правило только если
файл действительно локальный и не должен попадать в git.
