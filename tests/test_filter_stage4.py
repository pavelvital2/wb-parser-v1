from __future__ import annotations

import csv
from pathlib import Path

from app.common.config import load_config
from app.common.run_context import RunContext, utc_now_iso
from app.common.state_db import StateDB
from app.filter.engine import FilterEngine


def _write_yaml(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def _make_test_config(tmp_path: Path) -> Path:
    config_yaml = f"""
project:
  name: test
  source_system: wildberries
  timezone: Europe/Moscow

paths:
  data_raw: {str((tmp_path / 'data' / 'raw')).replace('\\', '/')}
  data_staging: {str((tmp_path / 'data' / 'staging')).replace('\\', '/')}
  data_marts: {str((tmp_path / 'data' / 'marts')).replace('\\', '/')}
  logs: {str((tmp_path / 'data' / 'logs')).replace('\\', '/')}
  exports: {str((tmp_path / 'exports')).replace('\\', '/')}
  state_sqlite: {str((tmp_path / 'state' / 'sqlite' / 'state.sqlite')).replace('\\', '/')}
  checkpoints_dir: {str((tmp_path / 'state' / 'checkpoints')).replace('\\', '/')}

runtime:
  retry_max_attempts: 2
  retry_base_delay_seconds: 0.01
  retry_max_delay_seconds: 0.02
  http_timeout_seconds: 5
  dry_run: false
  debug: false

suggest:
  prefixes_file: config/prefixes.txt

filter:
  rules_file: config/query_rules.yaml
  wordstat_csv_files:
    - data/raw/wordstat/part1.csv
    - data/raw/wordstat/part2.csv
  wordstat_csv_glob: data/raw/wordstat/*.csv
  input_files:
    suggest_staging_csv: ""
  output_files:
    candidates_raw_csv: filter_candidates_raw.csv
    debug_scores_csv: debug_scores.csv
    top_queries_csv: top_queries.csv
    queries_txt: queries.txt

serp:
  input_files:
    queries_txt: exports/queries.txt
    top_queries_csv: data/marts/filter/latest/top_queries.csv

sellers:
  input_files:
    products_daily_csv: data/marts/serp/latest/products_daily.csv
"""

    rules_yaml = """
default:
  top_n: 10
  min_wordstat_volume: 0
  weights:
    suggest_position_sum: 1.0
    suggest_repeat_log: 10.0
    suggest_depth0_bonus: 3.0
    wordstat_log_scale: 20.0
    wordstat_weight: 0.5
    suggest_weight: 0.5
    parent_children_bonus: 2.0
    priority_pattern_bonus: 5.0
    required_term_bonus: 3.0
  suggest_position_weights:
    \"1\": 70
    \"2\": 55
    \"3\": 43
  dedupe:
    normalize_yo_to_e: true
    collapse_spaces: true
    token_replacements:
      шевроны: шеврон
      нашивки: нашивка
    canonical_token_mappings:
      патчи: патч
  filters:
    stop_words: []
    required_terms_any: []
    exclude_patterns: []
    priority_patterns: []
  parent_priority:
    enabled: true
    keep_parent_for_selected_child: true
    max_parent_hops: 2
  niche_rules:
    all:
      top_n: 10
      include_patterns: []
      exclude_patterns: []
      multipliers:
        total: 1.0
      canonical_token_mappings: {}
"""

    _write_yaml(tmp_path / "config" / "config.yaml", config_yaml)
    _write_yaml(tmp_path / "config" / "query_rules.yaml", rules_yaml)
    _write_yaml(tmp_path / "config" / "prefixes.txt", "шеврон\n")

    return tmp_path / "config" / "config.yaml"


def _make_engine(tmp_path: Path, run_id: str = "20260307_120000Z") -> FilterEngine:
    config_path = _make_test_config(tmp_path)
    config = load_config(str(config_path))
    db = StateDB(config.paths.SQLITE_DB)
    db.init_schema()
    ctx = RunContext(run_id=run_id, pipeline="filter", component="filter", started_at_utc=utc_now_iso())
    return FilterEngine(config=config, db=db, ctx=ctx)


def test_wordstat_loader_semicolon_bom_and_spaces(tmp_path: Path) -> None:
    engine = _make_engine(tmp_path)

    ws_dir = tmp_path / "data" / "raw" / "wordstat"
    ws_dir.mkdir(parents=True, exist_ok=True)
    (ws_dir / "part1.csv").write_text(
        "Запросы со словами;Число запросов;meta\nшевроны;12 345;...\n",
        encoding="utf-8-sig",
    )
    (ws_dir / "part2.csv").write_text(
        "патчи;2\u00a0500;...\n",
        encoding="utf-8-sig",
    )

    ws = engine._load_wordstat()
    assert ws.get("шеврон") == 12345
    assert ws.get("патч") == 2500


def test_canonical_and_dedupe_from_suggest_rows(tmp_path: Path) -> None:
    engine = _make_engine(tmp_path)
    rows = [
        {"suggestion": "шевроны", "position": "1", "depth": "0", "typed_query": "шевроны"},
        {"suggestion": "шеврон", "position": "2", "depth": "1", "typed_query": "шеврон"},
    ]
    stats = engine._build_query_stats(rows)
    engine._classify_niches(stats)
    engine._apply_canonical_queries(stats)

    assert len(stats) == 1
    key = next(iter(stats.keys()))
    assert key == "шеврон"
    assert stats[key].canonical_query == "шеврон"
    assert stats[key].count == 2


def test_filter_builds_top_and_debug_outputs(tmp_path: Path) -> None:
    run_id = "20260307_130000Z"
    engine = _make_engine(tmp_path, run_id=run_id)

    ws_dir = tmp_path / "data" / "raw" / "wordstat"
    ws_dir.mkdir(parents=True, exist_ok=True)
    (ws_dir / "part1.csv").write_text("шеврон;245000;...\nшеврон мвд;42000;...\n", encoding="utf-8-sig")

    suggest_path = tmp_path / "data" / "staging" / "suggest" / run_id / "suggest_alpha_staging.csv"
    suggest_path.parent.mkdir(parents=True, exist_ok=True)
    with suggest_path.open("w", encoding="utf-8", newline="") as f:
        w = csv.writer(f, delimiter=";")
        w.writerow([
            "run_id", "component", "collected_at_utc", "source_system", "source_type", "source_ref",
            "status", "error_message", "base_prefix", "typed_query", "letter", "depth", "position",
            "list_size", "suggestion", "suggestion_lc", "is_empty_suggestion"
        ])
        w.writerow([run_id, "suggest", utc_now_iso(), "wildberries", "wb", "x", "success", "", "шеврон", "шеврон", "seed", 0, 1, 3, "шеврон", "шеврон", 0])
        w.writerow([run_id, "suggest", utc_now_iso(), "wildberries", "wb", "x", "success", "", "шеврон", "шеврон", "seed", 0, 2, 3, "шеврон мвд", "шеврон мвд", 0])

    result = engine.run()
    assert int(result["items_ok"]) >= 1

    config = engine.config
    debug_path = config.paths.output_path(layer="staging", component="filter", run_id=run_id, filename="debug_scores.csv")
    top_path = config.paths.output_path(layer="marts", component="filter", run_id=run_id, filename="top_queries.csv")
    queries_path = config.paths.output_path(layer="marts", component="filter", run_id=run_id, filename="queries.txt")

    assert debug_path.exists()
    assert top_path.exists()
    assert queries_path.exists()

    with debug_path.open("r", encoding="utf-8") as f:
        rows = list(csv.DictReader(f, delimiter=";"))
    assert rows
    assert "normalized_query" in rows[0]
    assert "canonical_query" in rows[0]
    assert "passes_filters" in rows[0]
    assert "parent_hops_used" in rows[0]
    assert "source_typed_queries_count" in rows[0]

    with top_path.open("r", encoding="utf-8") as f:
        top_rows = list(csv.DictReader(f, delimiter=";"))
    assert top_rows
    assert "query" in top_rows[0]
