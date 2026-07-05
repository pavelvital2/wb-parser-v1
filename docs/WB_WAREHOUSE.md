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
