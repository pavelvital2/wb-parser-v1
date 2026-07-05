# ТЗ: WB Warehouse MVP

Дата: 2026-07-05
Проект: `/home/pavel/projects/parser_wb`
Исполнитель: агент проекта `parser_wb`
Роль главного агента: архитектор/контролер/ревьюер, не исполнитель

## 1. Краткий итог

Нужно сделать безопасный MVP аналитического хранилища для уже имеющихся данных
Wildberries. Цель - получить минимальную базу для аналитики товаров, продавцов,
SEO-позиций и истории запусков, не ломая текущий рабочий парсер WB.

Работа должна выполняться агентом `parser_wb` в контексте проекта
`/home/pavel/projects/parser_wb`.

## 2. Зачем это нужно

Сейчас парсер собирает CSV/JSON-файлы. Это удобно для текущего запуска, но
плохо для долгой аналитики. Warehouse должен дать возможность отвечать на
вопросы:

- какие товары растут или падают в позициях;
- какие продавцы усиливаются;
- какие конкуренты появляются в топе;
- как меняются цены, отзывы, рейтинги и остатки;
- какие запросы дают видимость, а какие проседают;
- насколько качественно прошел ночной сбор.

## 3. Scope первого этапа

Только WB.

Источник данных:

```text
/home/pavel/projects/parser_wb/data
```

Первый этап должен использовать уже существующие данные:

```text
data/marts/serp/*/products_daily.csv
data/marts/sellers/*/sellers_daily.csv
data/marts/sellers/*/seller_query_product_bridge.csv
data/raw/serp/*/pages_raw_index.csv
state/run_reports/*.json
```

Ozon не трогать.

## 4. Строгие ограничения

Нельзя:

- запускать полный парсер;
- запускать массовый сбор WB;
- менять cron;
- менять proxy;
- менять cookies;
- менять `runtime.env`;
- менять request headers;
- менять логику публикации `latest`;
- удалять или перезаписывать `raw/`, `staging/`, `marts/`, `latest`;
- включать retention;
- трогать Ozon;
- печатать cookies, proxy credentials, request headers, tokens, `storage_state`;
- откатывать чужие изменения в dirty worktree.

Можно:

- создавать/править файлы, относящиеся к WB warehouse MVP;
- создавать новый каталог `data/warehouse/wb`;
- читать существующие CSV/JSON;
- строить DuckDB/Parquet базу из уже имеющихся данных;
- добавлять тесты и документацию.

## 5. Уже созданный черновик

Главный агент преждевременно создал черновые файлы. Считать их draft input, а
не готовой реализацией. Нужно проверить, доработать или заменить их по качеству
проекта.

Черновик:

```text
scripts/wb_warehouse.py
tests/test_wb_warehouse.py
docs/WB_WAREHOUSE.md
.gitignore: data/warehouse/
```

DuckDB уже установлен в централизованный runtime:

```text
/home/Codex/agent-tools/parser_wb-python
```

Проверка:

```bash
/home/Codex/agent-tools/parser_wb-python/bin/python - <<'PY'
import duckdb
print(duckdb.__version__)
PY
```

## 6. Ожидаемая структура результата

```text
data/warehouse/wb/
  wb.duckdb
  parquet/
    product_snapshots.parquet
    seller_snapshots.parquet
    product_seller_bridge.parquet
    serp_pages.parquet
    run_reports.parquet
    run_report_components.parquet
  manifests/
    latest.json
```

## 7. Минимальные таблицы / представления

Обязательный минимум:

```text
product_snapshots
seller_snapshots
product_seller_bridge
serp_pages
run_reports
run_report_components
```

Желательные read-only views:

```text
query_positions
seller_daily_metrics
daily_run_quality
```

## 8. Требования к реализации

Скрипт должен:

1. Работать из корня проекта `parser_wb`.
2. Иметь режим dry-run без записи.
3. Читать CSV с разделителем `;`.
4. Игнорировать каталоги `latest` при сборе истории, чтобы не дублировать уже
   имеющиеся run-директории.
5. Читать `state/run_reports/*.json`.
6. Строить DuckDB и Parquet.
7. Писать manifest с:
   - временем сборки;
   - количеством файлов;
   - количеством строк по таблицам;
   - путями к базе/Parquet;
   - ограничениями MVP.
8. Не удалять исходные данные.
9. Не менять текущий runtime парсера.
10. Давать команду `check`, которая показывает counts и простые sample queries.

## 9. Обязательные команды проверки

Перед изменениями проверить, что нет активного WB-сбора:

```bash
pgrep -af 'parser_wb|run_products|run_sellers|serp|sellers' || true
ls -la state/locks 2>/dev/null || true
```

Тесты:

```bash
/home/Codex/agent-tools/parser_wb-python/bin/python -m pytest tests/test_wb_warehouse.py -q
```

Dry-run:

```bash
/home/Codex/agent-tools/parser_wb-python/bin/python scripts/wb_warehouse.py build --dry-run
```

Build:

```bash
/home/Codex/agent-tools/parser_wb-python/bin/python scripts/wb_warehouse.py build
```

Check:

```bash
/home/Codex/agent-tools/parser_wb-python/bin/python scripts/wb_warehouse.py check
```

Пример SQL-проверки:

```bash
/home/Codex/agent-tools/parser_wb-python/bin/python scripts/wb_warehouse.py sql \
  "select query, count(*) from query_positions group by query order by 2 desc limit 10"
```

## 10. Критерии готовности

Готово только если:

- тест `tests/test_wb_warehouse.py` проходит;
- dry-run проходит;
- build проходит;
- check проходит;
- создан `data/warehouse/wb/wb.duckdb`;
- создан manifest `data/warehouse/wb/manifests/latest.json`;
- исходные `raw/staging/marts/latest` не удалены и не перезаписаны;
- cron/proxy/cookies/runtime.env/request headers не изменены;
- Ozon не затронут;
- агент дал отчет с row counts;
- Hermes summary сохранен без секретов.

## 11. Формат отчета агентом parser_wb

Отчет в топик должен быть таким:

```text
Итог:
Что сделано:
Файлы:
Проверки:
Row counts:
- product_snapshots:
- seller_snapshots:
- product_seller_bridge:
- serp_pages:
- run_reports:
Что не трогал:
Риски/ограничения:
Следующий шаг:
```

## 12. Что не является задачей этого этапа

Не делать сейчас:

- подключение ежедневного append к cron;
- retention/удаление старых raw/staging;
- Parser Data API endpoints для warehouse;
- отчеты для seller-агентов;
- Ozon warehouse;
- PostgreSQL;
- ClickHouse;
- сложную оптимизацию схемы.

Это следующие этапы после приемки MVP.

## 13. Напоминание по ролям

Павел отдельно указал: нужно строго следовать договоренностям. Если исполнитель
назначен как `parser_wb`, другой агент не должен самовольно делать его работу.
Если нужна смена роли или исполнителя - сначала согласовать с Павлом.
