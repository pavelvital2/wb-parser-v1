# WB Warehouse MVP

## Итог

WB warehouse - это первый read-only аналитический слой поверх уже собранных
данных `parser_wb`. Он не запускает парсер, не меняет `latest`, не удаляет
`raw/staging` и не трогает cookies/proxy.

Цель MVP: собрать существующую историю WB из `data/marts` и `state/run_reports`
в DuckDB + Parquet, чтобы агенты могли задавать аналитические вопросы по
товарам, продавцам, SEO-позициям и качеству запусков.

## Где лежит

```text
data/warehouse/wb/wb.duckdb
data/warehouse/wb/parquet/*.parquet
data/warehouse/wb/manifests/latest.json
scripts/wb_warehouse.py
```

## Что попадает в базу

- `product_snapshots` - товары из SERP marts.
- `query_positions` - view по позициям товаров в поисковых запросах.
- `seller_snapshots` - продавцы из sellers marts.
- `seller_daily_metrics` - view по метрикам продавцов.
- `product_seller_bridge` - связь запрос -> товар -> продавец.
- `serp_pages` - индекс страниц SERP.
- `run_reports` и `run_report_components` - качество запусков.

Исторические источники читаются из run-директорий. Каталоги `latest` намеренно
игнорируются, чтобы не дублировать уже загруженные run-данные.

Manifest:

```text
data/warehouse/wb/manifests/latest.json
```

В нем фиксируются время сборки, количество исходных файлов, row counts,
пути к DuckDB/Parquet и ограничения MVP.

## Команды

Dry-run без записи:

```bash
/home/Codex/agent-tools/parser_wb-python/bin/python scripts/wb_warehouse.py build --dry-run
```

Пересобрать warehouse из уже существующих данных:

```bash
/home/Codex/agent-tools/parser_wb-python/bin/python scripts/wb_warehouse.py build
```

Проверить counts и базовые выборки:

```bash
/home/Codex/agent-tools/parser_wb-python/bin/python scripts/wb_warehouse.py check
```

Read-only SQL:

```bash
/home/Codex/agent-tools/parser_wb-python/bin/python scripts/wb_warehouse.py sql \
  "select query, count(*) from query_positions group by query order by 2 desc limit 10"
```

## Parser Data API

Seller VPS agents должны читать warehouse через read-only Parser Data API, а не
копировать `data/warehouse` или сырые parser datasets:

```text
http://127.0.0.1:8787
```

Базовые read-only endpoints:

```text
GET /warehouse/wb/summary
GET /warehouse/wb/query-positions
GET /warehouse/wb/daily-changes
GET /warehouse/wb/top-movers
GET /warehouse/wb/seller-changes
GET /warehouse/wb/run-quality
```

Агрегированные endpoints для компактной аналитики:

```text
GET /warehouse/wb/aggregates/store-query-positions
GET /warehouse/wb/aggregates/visibility-gaps
GET /warehouse/wb/aggregates/query-coverage
GET /warehouse/wb/aggregates/competitors-top
GET /warehouse/wb/aggregates/top-movers
GET /warehouse/wb/aggregates/seo-visibility-candidates
GET /warehouse/wb/aggregates/promotion-visibility-candidates
GET /warehouse/wb/aggregates/market-summary
GET /warehouse/wb/aggregates/product-position-history
GET /warehouse/wb/aggregates/query-product-matrix
GET /warehouse/wb/aggregates/price-position-map
GET /warehouse/wb/aggregates/rating-review-gaps
GET /warehouse/wb/aggregates/brand-supplier-share
GET /warehouse/wb/aggregates/new-lost-top
GET /warehouse/wb/aggregates/serp-volatility
GET /warehouse/wb/aggregates/niche-opportunities
GET /warehouse/wb/aggregates/content-proxy-gaps
GET /warehouse/wb/aggregates/query-discovery
```

Правила API:

- API остается read-only и не пишет в warehouse.
- Если `supplier_id`, `product_id`/`nmID` или `brand` не переданы, агрегаты
  возвращают market-level аналитику.
- Parser Data API не содержит hardcoded ownership logic для конкретных
  магазинов или брендов.
- `total_quantity` - parser-visible stock из WB выдачи, не официальные остатки.
- SEO, promotion, content и niche endpoints являются parser-side shortlist;
  продажи, маржа, официальный stock, campaign state, спрос и эффективность
  рекламы должны присоединяться на Seller VPS из seller-side источников.

Safe refresh wrapper:

```bash
scripts/run_wb_warehouse_refresh.sh --dry-run
scripts/run_wb_warehouse_refresh.sh --check-only
scripts/run_wb_warehouse_refresh.sh
```

Wrapper проверяет `state/run_reports/latest.json` и запускает rebuild только
если latest report относится к успешному `sellers` run. Состояние и история:

```text
state/wb_warehouse/latest.json
state/wb_warehouse/history/*.json
data/logs/wb_warehouse_refresh.log
state/locks/wb_warehouse_refresh.flock
```

Ночной `scripts/run_products_sellers_daily.sh` вызывает wrapper в самом конце,
после успешной цепочки `serp -> sellers`. Ошибка warehouse refresh не меняет
статус успешного парсинга и не откатывает `latest`; она пишется в лог/state и
попадает отдельной строкой `Warehouse` в Telegram-отчет.

## Миграция исторических данных в regional warehouse

Официальный standalone path использует тот же warehouse wrapper, но отдельный
режим. Он не запускает SERP/sellers, не читает сеть и не меняет исходный
`data/warehouse/wb/wb.duckdb`, CSV/JSON или parser latest:

```bash
scripts/run_wb_warehouse_refresh.sh \
  --migrate-legacy-yaroslavl --dry-run

scripts/run_wb_warehouse_refresh.sh \
  --migrate-legacy-yaroslavl --apply

scripts/run_wb_warehouse_refresh.sh \
  --migrate-legacy-yaroslavl --check
```

Перед dry-run/apply команда non-blocking захватывает locks в порядке:

```text
products_sellers_daily.flock
pipeline.lock
wb_warehouse_refresh.flock
wb_collection_plan.flock
```

Source global DuckDB открывается через read-only `ATTACH`. Миграция выполняется
в отдельной staging DuckDB с production-ограничениями `memory_limit=1GiB`,
`threads=2` и private temp spill. Staging проверяется read-only до публикации.
При `--apply` только полностью проверенный staging-файл атомарно заменяет
`data/warehouse/wb_regional/wb_regional.duckdb`, после чего fsync выполняется
для каталога. Ошибка до replace оставляет прежний target неизменным.

Исторические строки получают:

```text
region_id=yaroslavl
region_name=Ярославль
displayed_region=Ярославль
region_provenance=legacy_global_assigned_yaroslavl
query_pack_id=legacy-global
query_pack_version=legacy
query_group=legacy-global
```

Повторный `--apply` без новых global rows возвращает `no_changes` и не
перепубликует target. Добавление новых legacy runs синхронизируется
инкрементально; изменение или исчезновение ранее импортированной строки
отклоняет всю staging-транзакцию. Фактические будущие региональные строки
сохраняют исходный `region_id/region_name` и не переклассифицируются.

Поле проверки `api_source_schema_compatible=true` подтверждает только наличие
и читаемость таблиц regional warehouse, предназначенных для будущего API
источника. Оно не означает, что текущий Parser Data API уже переключён с
global warehouse на regional warehouse.

## Правила безопасности

- Не удалять `raw/`, `staging/`, `marts/` на этом этапе.
- Не подключать автоматический retention, пока warehouse не поработает стабильно.
- Не считать failed/partial run полноценным аналитическим днем без проверки
  `run_reports`.
- Не добавлять cookies, proxy credentials, headers или `storage_state` в warehouse.
- Не менять cron, proxy, cookies, `runtime.env`, request headers или публикацию
  `latest`.
- Ozon вне scope этого MVP.
- Warehouse refresh не является quality gate для публикации parser `latest`.

## Следующий этап

После проверки MVP:

1. После нескольких стабильных safe refresh перейти к настоящему append с
   идемпотентными ключами.
2. Расширить Parser Data API read-only запросами к `wb.duckdb`.
3. Сделать первые отчеты: падения/рост позиций, новые конкуренты, сильные
   продавцы, изменение цены/рейтинга/отзывов.
4. Только после этого включать dry-run retention для старых `raw/staging`.
