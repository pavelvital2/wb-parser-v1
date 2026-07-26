# ТЗ: WB Warehouse Daily Append / Safe Refresh

Дата: 2026-07-05
Проект: `/home/pavel/projects/parser_wb`
Исполнитель: агент проекта `parser_wb`
Роль главного агента: архитектор/контролер/ревьюер, не исполнитель

## 1. Краткий итог

Нужно сделать следующий безопасный этап после WB Warehouse MVP: автоматическое
обновление WB warehouse после успешного ночного сбора `serp -> sellers`.

Цель: чтобы новые успешные данные WB попадали в аналитический слой регулярно, а
не только после ручного запуска `scripts/wb_warehouse.py build`.

Важно: этот этап не должен ломать рабочий ночной сбор. Если warehouse-обновление
упало, это не должно портить `latest`, `raw`, `staging`, `marts` и не должно
скрывать статус самого парсера.

## 2. Что уже есть

Принятый MVP:

```text
data/warehouse/wb/wb.duckdb
data/warehouse/wb/parquet/*.parquet
data/warehouse/wb/manifests/latest.json
scripts/wb_warehouse.py
tests/test_wb_warehouse.py
docs/WB_WAREHOUSE.md
```

Проверенные counts MVP на момент приемки:

```text
product_snapshots:       389949
seller_snapshots:        9929
product_seller_bridge:   257451
serp_pages:              4275
run_reports:             33
run_report_components:   33
```

Команды MVP:

```bash
/home/Codex/agent-tools/parser_wb-python/bin/python scripts/wb_warehouse.py build --dry-run
/home/Codex/agent-tools/parser_wb-python/bin/python scripts/wb_warehouse.py build
/home/Codex/agent-tools/parser_wb-python/bin/python scripts/wb_warehouse.py check
/home/Codex/agent-tools/parser_wb-python/bin/python scripts/wb_warehouse.py sql "..."
```

## 3. Задача этапа

Сделать безопасный механизм регулярного обновления warehouse после успешного
ночного запуска WB.

Минимально допустимый вариант для этого этапа:

```text
после успешного run_products_sellers_daily.sh
  -> проверить state/run_reports/latest.json
  -> убедиться, что sellers run success
  -> запустить warehouse refresh/build
  -> записать отдельный лог и manifest
  -> отправить в отчет строку о warehouse status
```

Технически допускается два подхода:

### Вариант A: safe rebuild после успешного run

Использовать текущий rebuild-подход:

```bash
scripts/wb_warehouse.py build
```

Плюсы:
- проще;
- меньше риск дублей;
- легче проверить;
- подходит для текущего объема данных.

Минусы:
- каждый раз пересобирает всю базу.

### Вариант B: настоящий append

Добавить отдельный append-режим:

```bash
scripts/wb_warehouse.py append --run-id <serp_run_id/sellers_run_id>
```

Плюсы:
- правильнее архитектурно для больших объемов.

Минусы:
- выше риск дублей и ошибок на первом внедрении;
- нужны ключи идемпотентности и больше тестов.

## 4. Рекомендация архитектора

Для этого этапа выбрать **Вариант A: safe rebuild после успешного run**.

Причина: сейчас важнее надежность и отсутствие поломки парсера, чем оптимизация.
Объем MVP уже проверен, rebuild работает быстро достаточно для первого
production-safe контура. Настоящий append можно сделать следующим этапом после
нескольких успешных ночей.

## 5. Строгие ограничения

Нельзя:

- менять proxy;
- менять cookies;
- менять `runtime.env`;
- менять request headers;
- менять WB endpoints;
- менять логику сбора SERP/sellers;
- менять публикацию `latest`;
- удалять `raw/`, `staging/`, `marts/`;
- включать retention;
- трогать Ozon;
- печатать secrets;
- считать warehouse success обязательным условием публикации `latest`;
- запускать полный сбор вручную без отдельного согласования.

Можно:

- добавить wrapper для warehouse refresh;
- добавить лог warehouse refresh;
- добавить state/report JSON для warehouse refresh;
- аккуратно подключить warehouse refresh после успешного nightly wrapper;
- добавить тесты;
- обновить docs/WB_WAREHOUSE.md;
- обновить это ТЗ/AGENTS только при необходимости.

## 6. Требования к безопасности

Warehouse refresh должен быть non-destructive:

- не трогать исходные CSV/JSON;
- писать только в `data/warehouse/wb`;
- писать лог в `data/logs/`;
- писать состояние в `state/wb_warehouse/`;
- использовать lock, чтобы два refresh не шли параллельно;
- иметь понятную ошибку, если warehouse refresh уже идет;
- не запускаться, если основной WB-сбор еще активен.

Если nightly parser success, но warehouse refresh failed:

- данные парсера считаются собранными;
- `latest` не откатывать;
- в Telegram/логах написать: `warehouse_failed`;
- сохранить ошибку в отдельный state/report;
- exit-код wrapper обсудить и выбрать безопасно.

Рекомендация: на первом этапе warehouse failure не должен ломать статус
успешного парсинга, но должен явно попадать в отчет.

## 7. Предлагаемые файлы

Новые или изменяемые файлы:

```text
scripts/run_wb_warehouse_refresh.sh
scripts/wb_warehouse.py
tests/test_wb_warehouse.py
docs/WB_WAREHOUSE.md
docs/tasks/WB_WAREHOUSE_DAILY_APPEND_TZ_20260705.md
```

Возможное состояние/логи:

```text
state/wb_warehouse/latest.json
state/wb_warehouse/history/*.json
data/logs/wb_warehouse_refresh.log
state/locks/wb_warehouse_refresh.flock
```

Файл ночного wrapper может быть изменен только аккуратно и минимально:

```text
scripts/run_products_sellers_daily.sh
```

Перед его изменением обязательно прочитать текущий файл и понять порядок:

```bash
sed -n '1,240p' scripts/run_products_sellers_daily.sh
```

## 8. Минимальный workflow

1. Проверить, что нет активного WB-сбора:

   ```bash
   pgrep -af 'run_products|run_sellers|scripts/run_.*serp|scripts/run_.*sellers' || true
   ls -la state/locks 2>/dev/null || true
   ```

2. Прочитать:

   ```bash
   sed -n '1,240p' AGENTS.md
   sed -n '1,240p' docs/WB_WAREHOUSE.md
   sed -n '1,240p' scripts/run_products_sellers_daily.sh
   python3 -m json.tool state/run_reports/latest.json | sed -n '1,220p'
   ```

3. Добавить safe wrapper `scripts/run_wb_warehouse_refresh.sh`.

4. Wrapper должен:
   - брать lock;
   - проверять `state/run_reports/latest.json`;
   - запускать `scripts/wb_warehouse.py build` только если latest report success;
   - запускать `scripts/wb_warehouse.py check`;
   - писать `state/wb_warehouse/latest.json`;
   - писать лог;
   - не печатать secrets.

5. Добавить тесты на:
   - dry-run/build/check сохраняют текущую работоспособность;
   - latest directories игнорируются;
   - wrapper не запускает build при failed latest report;
   - wrapper пишет state success/failure.

6. Подключение к `scripts/run_products_sellers_daily.sh` делать только после
   тестов wrapper. Подключение должно быть в самом конце, после успешной цепочки
   `serp -> sellers`.

## 9. Команды проверки

Обязательные:

```bash
/home/Codex/agent-tools/parser_wb-python/bin/python -m pytest tests/test_wb_warehouse.py -q
bash -n scripts/run_wb_warehouse_refresh.sh
bash -n scripts/run_products_sellers_daily.sh
/home/Codex/agent-tools/parser_wb-python/bin/python scripts/wb_warehouse.py build --dry-run
/home/Codex/agent-tools/parser_wb-python/bin/python scripts/wb_warehouse.py check
git diff --check
```

Если wrapper добавлен:

```bash
scripts/run_wb_warehouse_refresh.sh --dry-run
```

Если есть безопасный режим без изменения данных:

```bash
scripts/run_wb_warehouse_refresh.sh --check-only
```

Реальный refresh запускать только если:

- нет активного WB-сбора;
- latest run_report success;
- тесты прошли;
- dry-run/check прошли.

## 10. Критерии готовности

Готово только если:

- nightly parser logic не сломана;
- warehouse refresh имеет lock;
- есть state/report по warehouse refresh;
- есть лог;
- refresh запускается только после success report;
- тесты проходят;
- `bash -n` проходит;
- `git diff --check` проходит;
- `latest`, `raw/staging/marts`, cron/proxy/cookies/runtime.env/request headers
  не изменены вне согласованной минимальной вставки wrapper;
- Ozon не затронут;
- агент дал отчет.

## 11. Формат отчета агента

```text
Итог:
Что сделано:
Файлы:
Как подключено к ночному процессу:
Проверки:
Warehouse state:
Row counts до/после:
Что не трогал:
Риски/ограничения:
Следующий шаг:
Hermes summary:
```

## 12. Что НЕ делать в этом этапе

Не делать:

- retention cleanup;
- Parser Data API endpoints;
- seller-отчеты;
- Ozon warehouse;
- PostgreSQL/ClickHouse;
- сложный инкрементальный append с дедупликацией, если safe rebuild покрывает
  задачу;
- изменение качества сбора или антибот/proxy-логики.

## 13. Следующий этап после приемки

После нескольких успешных ночных refresh:

1. Parser Data API read-only endpoints к WB warehouse.
2. Первые seller-отчеты по позициям/конкурентам.
3. Только потом dry-run retention для старых `raw/staging`.
