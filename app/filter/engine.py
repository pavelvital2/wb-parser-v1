from __future__ import annotations

import csv
import math
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

from app.common.config import AppConfig
from app.common.constants import COMPONENT_FILTER, COMPONENT_SUGGEST
from app.common.csv_io import write_csv_rows
from app.common.exceptions import CriticalPipelineError
from app.common.logging_setup import get_logger
from app.common.run_context import RunContext, utc_now_iso
from app.common.state_db import StateDB


_HEADER_QUERY_HINT = "запрос"
_HEADER_VOLUME_HINT = "число"


@dataclass(slots=True)
class QueryStats:
    normalized_query: str
    canonical_query: str
    source_query: str
    niche: str = "all"
    niche_multiplier_total: float = 1.0

    count: int = 0
    source_rows: int = 0
    source_typed_queries_count: int = 0
    min_position: int = 10**9
    suggest_position_sum: float = 0.0
    depth0_hits: int = 0
    wordstat_volume: int = 0

    parent_query: str = ""
    children_count: int = 0
    parent_selected_flag: int = 0
    parent_hops_used: int = 0

    matched_required_terms: list[str] = field(default_factory=list)
    matched_priority_patterns: list[str] = field(default_factory=list)

    score_suggest: float = 0.0
    score_wordstat: float = 0.0
    score_parent: float = 0.0
    score_priority: float = 0.0
    hybrid_score: float = 0.0

    passes_filters: bool = True
    exclude_reason: str = ""
    is_selected: bool = False
    selected_reason: str = ""
    rank_in_niche: int = 0

    _typed_queries_seen: set[str] = field(default_factory=set, repr=False)


class FilterEngine:
    def __init__(self, config: AppConfig, db: StateDB, ctx: RunContext) -> None:
        self.config = config
        self.db = db
        self.ctx = ctx
        self.logger = get_logger("filter")
        self.rules = self._load_rules()
        self.default_rules = self.rules["default"]

    def run(self) -> dict[str, int | str]:
        self._checkpoint("collect", "all", "start")
        suggest_rows = self._load_suggest_rows()

        self._checkpoint("score", "all", "start")
        stats = self._build_query_stats(suggest_rows)
        self._classify_niches(stats)
        self._apply_canonical_queries(stats)
        self._attach_wordstat(stats)
        self._attach_parent_relations(stats)
        self._score(stats)

        self._checkpoint("select", "all", "start")
        selected = self._select_queries(stats)

        self._checkpoint("export", "all", "start")
        outputs = self._write_outputs(stats, selected)

        return {
            "items_ok": len(selected),
            "items_error": 0,
            "non_critical_errors": 0,
            "note": f"selected={len(selected)} candidates={len(stats)}",
            **outputs,
        }

    def _checkpoint(self, stage: str, group: str, meta: str) -> None:
        key = f"{stage}|{group}"
        value = f"{meta}|{utc_now_iso()}"
        self.db.save_checkpoint(
            component=COMPONENT_FILTER,
            checkpoint_key=key,
            checkpoint_value=value,
            updated_at_utc=utc_now_iso(),
        )

    def _load_rules(self) -> dict[str, Any]:
        rules_file = self.config.raw.get("filter", {}).get("rules_file", "config/query_rules.yaml")
        path = Path(rules_file)
        if not path.is_absolute():
            path = self.config.project_root / path
        if not path.exists():
            raise CriticalPipelineError(f"Filter rules file not found: {path}")

        data = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
        if "default" not in data:
            raise CriticalPipelineError("query_rules.yaml must contain 'default' section")
        return data

    def _resolve_suggest_input(self) -> Path:
        explicit = str(self.config.raw.get("filter", {}).get("input_files", {}).get("suggest_staging_csv", "")).strip()
        if explicit:
            path = Path(explicit)
            if not path.is_absolute():
                path = self.config.project_root / path
            if path.exists():
                return path

        run_local = self.config.paths.output_path(
            layer="staging",
            component=COMPONENT_SUGGEST,
            run_id=self.ctx.run_id,
            filename="suggest_alpha_staging.csv",
        )
        if run_local.exists():
            return run_local

        latest = self.config.paths.latest_output_path(
            layer="staging",
            component=COMPONENT_SUGGEST,
            filename="suggest_alpha_staging.csv",
        )
        if latest is not None:
            return latest

        raise CriticalPipelineError("No suggest staging input found for filter")

    def _load_suggest_rows(self) -> list[dict[str, str]]:
        path = self._resolve_suggest_input()
        rows: list[dict[str, str]] = []
        with path.open("r", encoding="utf-8-sig") as f:
            reader = csv.DictReader(f, delimiter=";")
            for row in reader:
                status = (row.get("status") or "").strip().lower()
                suggestion = (row.get("suggestion") or "").strip()
                if status != "success" or not suggestion:
                    continue
                rows.append(row)

        if not rows:
            raise CriticalPipelineError(f"Filter input has no success suggestions: {path}")
        return rows

    def _normalize_query(self, value: str) -> str:
        dedupe = self.default_rules.get("dedupe", {})
        s = (value or "").strip().lower().replace('"', "")
        if dedupe.get("normalize_yo_to_e", True):
            s = s.replace("ё", "е")
        if dedupe.get("collapse_spaces", True):
            s = re.sub(r"\s+", " ", s)
        token_replacements = dedupe.get("token_replacements", {})
        tokens = [token_replacements.get(tok, tok) for tok in s.split(" ") if tok]
        return " ".join(tokens).strip()

    def _canonicalize_query(self, normalized_query: str, niche: str) -> str:
        dedupe = self.default_rules.get("dedupe", {})
        base_map = dedupe.get("canonical_token_mappings", {})
        niche_map = self.default_rules.get("niche_rules", {}).get(niche, {}).get("canonical_token_mappings", {})
        tokens = []
        for tok in normalized_query.split(" "):
            mapped = niche_map.get(tok, base_map.get(tok, tok))
            tokens.append(mapped)
        return " ".join(tokens).strip()

    def _parse_int(self, value: str) -> int:
        cleaned = (value or "").replace("\u00a0", " ").replace('"', "").replace("'", "")
        cleaned = cleaned.replace(" ", "").strip()
        if not cleaned or not cleaned.isdigit():
            return 0
        return int(cleaned)

    def _build_query_stats(self, rows: list[dict[str, str]]) -> dict[str, QueryStats]:
        pos_weights = self.default_rules.get("suggest_position_weights", {})
        stats: dict[str, QueryStats] = {}

        for row in rows:
            source_query = (row.get("suggestion") or "").strip()
            normalized = self._normalize_query(source_query)
            if not normalized:
                continue

            cur = stats.get(normalized)
            if cur is None:
                cur = QueryStats(
                    normalized_query=normalized,
                    canonical_query=normalized,
                    source_query=source_query,
                )
                stats[normalized] = cur

            pos = self._parse_int(row.get("position", "0"))
            depth = self._parse_int(row.get("depth", "0"))
            typed_query = self._normalize_query(row.get("typed_query", ""))

            cur.count += 1
            cur.source_rows += 1
            if typed_query:
                cur._typed_queries_seen.add(typed_query)
            if pos > 0:
                cur.min_position = min(cur.min_position, pos)
            cur.suggest_position_sum += float(pos_weights.get(str(pos), 0.0))
            if depth == 0:
                cur.depth0_hits += 1

        for cur in stats.values():
            cur.source_typed_queries_count = len(cur._typed_queries_seen)

        return stats

    def _compile_patterns(self, patterns: list[str]) -> list[re.Pattern[str]]:
        return [re.compile(p, flags=re.IGNORECASE | re.UNICODE) for p in patterns if p]

    def _classify_niche_for_query(self, normalized_query: str) -> str:
        niche_rules = self.default_rules.get("niche_rules", {"all": {}})
        for niche_name, rule in niche_rules.items():
            inc = self._compile_patterns(rule.get("include_patterns", []))
            exc = self._compile_patterns(rule.get("exclude_patterns", []))
            if exc and any(rx.search(normalized_query) for rx in exc):
                continue
            if not inc or any(rx.search(normalized_query) for rx in inc):
                return niche_name
        return "all"

    def _classify_niches(self, stats: dict[str, QueryStats]) -> None:
        for cur in stats.values():
            cur.niche = self._classify_niche_for_query(cur.normalized_query)

    def _apply_canonical_queries(self, stats: dict[str, QueryStats]) -> None:
        for cur in stats.values():
            cur.canonical_query = self._canonicalize_query(cur.normalized_query, cur.niche)

    def _resolve_wordstat_paths(self) -> list[Path]:
        cfg = self.config.raw.get("filter", {})
        candidates: list[Path] = []

        files_cfg = cfg.get("wordstat_csv_files", [])
        if isinstance(files_cfg, list):
            for value in files_cfg:
                p = Path(str(value))
                if not p.is_absolute():
                    p = self.config.project_root / p
                if p.exists() and p.is_file():
                    candidates.append(p)

        single = str(cfg.get("wordstat_csv", "")).strip()
        if single:
            p = Path(single)
            if not p.is_absolute():
                p = self.config.project_root / p
            if p.exists() and p.is_file():
                candidates.append(p)

        glob_pattern = str(cfg.get("wordstat_csv_glob", "")).strip()
        if glob_pattern:
            gp = Path(glob_pattern)
            if gp.is_absolute():
                root = gp.parent
                pattern = gp.name
            else:
                root = (self.config.project_root / gp.parent).resolve()
                pattern = gp.name
            if root.exists():
                candidates.extend(sorted(root.glob(pattern)))

        unique: list[Path] = []
        seen: set[str] = set()
        for p in candidates:
            key = str(p.resolve())
            if key not in seen:
                seen.add(key)
                unique.append(p.resolve())
        return unique

    def _load_wordstat(self) -> dict[str, int]:
        paths = self._resolve_wordstat_paths()
        if not paths:
            self.logger.warning("wordstat_missing", extra={"source_ref": "no files configured/found"})
            return {}

        out: dict[str, int] = {}
        for path in paths:
            with path.open("r", encoding="utf-8-sig", newline="") as f:
                reader = csv.reader(f, delimiter=";")
                for row in reader:
                    if len(row) < 2:
                        continue
                    raw_query = (row[0] or "").strip()
                    raw_volume = row[1] if len(row) > 1 else ""
                    if not raw_query:
                        continue

                    if _HEADER_QUERY_HINT in raw_query.lower() and _HEADER_VOLUME_HINT in (raw_volume or "").lower():
                        continue

                    volume = self._parse_int(raw_volume)
                    if volume <= 0:
                        continue

                    normalized = self._normalize_query(raw_query)
                    if not normalized:
                        continue
                    niche = self._classify_niche_for_query(normalized)
                    canonical = self._canonicalize_query(normalized, niche)
                    out[canonical] = out.get(canonical, 0) + volume

        if not out:
            self.logger.warning("wordstat_empty_after_parse", extra={"source_ref": ",".join(str(p) for p in paths)})
        return out

    def _attach_wordstat(self, stats: dict[str, QueryStats]) -> None:
        wordstat_map = self._load_wordstat()
        for cur in stats.values():
            cur.wordstat_volume = int(wordstat_map.get(cur.canonical_query, 0))

    def _attach_parent_relations(self, stats: dict[str, QueryStats]) -> None:
        for query, cur in stats.items():
            parent = query.rsplit(" ", 1)[0] if " " in query else ""
            if parent and parent in stats:
                cur.parent_query = parent
                stats[parent].children_count += 1

    def _required_term_match(self, normalized_query: str, terms: list[str]) -> list[str]:
        tokens = normalized_query.split(" ")
        token_set = set(tokens)
        matched: list[str] = []
        for term in terms:
            raw_term = (term or "").strip()
            if not raw_term:
                continue

            if raw_term.lower().startswith("re:"):
                pattern = raw_term[3:].strip()
                if pattern and re.search(pattern, normalized_query, flags=re.IGNORECASE | re.UNICODE):
                    matched.append(raw_term)
                continue

            rule = self._normalize_query(raw_term)
            if not rule:
                continue

            if " " in rule:
                phrase_pattern = re.compile(rf"(?<!\w){re.escape(rule)}(?!\w)", flags=re.IGNORECASE | re.UNICODE)
                if phrase_pattern.search(normalized_query):
                    matched.append(raw_term)
            elif rule in token_set:
                matched.append(raw_term)
        return matched

    def _score(self, stats: dict[str, QueryStats]) -> None:
        weights = self.default_rules.get("weights", {})
        filters = self.default_rules.get("filters", {})
        stop_words = {self._normalize_query(w) for w in filters.get("stop_words", []) if w}
        required_terms = [str(v) for v in filters.get("required_terms_any", []) if str(v).strip()]
        exclude_patterns = self._compile_patterns(filters.get("exclude_patterns", []))
        priority_patterns = [p for p in filters.get("priority_patterns", []) if p]
        priority_patterns_rx = [(p, re.compile(p, flags=re.IGNORECASE | re.UNICODE)) for p in priority_patterns]

        min_wordstat = int(self.default_rules.get("min_wordstat_volume", 0))
        w_suggest = float(weights.get("suggest_weight", 0.45))
        w_wordstat = float(weights.get("wordstat_weight", 0.45))

        for cur in stats.values():
            tokens = [tok for tok in cur.normalized_query.split(" ") if tok]
            if any(tok in stop_words for tok in tokens):
                cur.passes_filters = False
                cur.exclude_reason = "stop_word"
            elif exclude_patterns and any(rx.search(cur.normalized_query) for rx in exclude_patterns):
                cur.passes_filters = False
                cur.exclude_reason = "exclude_pattern"
            elif cur.wordstat_volume < min_wordstat:
                cur.passes_filters = False
                cur.exclude_reason = "wordstat_below_min"

            matched_required = self._required_term_match(cur.normalized_query, required_terms)
            cur.matched_required_terms = matched_required
            if required_terms and not matched_required:
                cur.passes_filters = False
                cur.exclude_reason = cur.exclude_reason or "required_terms_miss"

            matched_priority: list[str] = []
            for pattern, rx in priority_patterns_rx:
                if rx.search(cur.normalized_query):
                    matched_priority.append(pattern)
            cur.matched_priority_patterns = matched_priority

            suggest_component = (
                float(weights.get("suggest_position_sum", 1.0)) * cur.suggest_position_sum
                + float(weights.get("suggest_repeat_log", 18.0)) * math.log1p(cur.count)
                + float(weights.get("suggest_depth0_bonus", 8.0)) * cur.depth0_hits
            )
            wordstat_component = float(weights.get("wordstat_log_scale", 50.0)) * math.log1p(cur.wordstat_volume)
            parent_component = float(weights.get("parent_children_bonus", 6.0)) * cur.children_count
            priority_component = (
                float(weights.get("priority_pattern_bonus", 20.0)) * len(cur.matched_priority_patterns)
                + float(weights.get("required_term_bonus", 10.0)) * len(cur.matched_required_terms)
            )

            cur.score_suggest = suggest_component
            cur.score_wordstat = wordstat_component
            cur.score_parent = parent_component
            cur.score_priority = priority_component

            niche_rule = self.default_rules.get("niche_rules", {}).get(cur.niche, {})
            cur.niche_multiplier_total = float(niche_rule.get("multipliers", {}).get("total", 1.0))

            cur.hybrid_score = (
                (w_suggest * suggest_component)
                + (w_wordstat * wordstat_component)
                + parent_component
                + priority_component
            ) * cur.niche_multiplier_total

    def _parent_chain(self, query: str, max_hops: int, stats: dict[str, QueryStats]) -> list[tuple[str, int]]:
        chain: list[tuple[str, int]] = []
        current = query
        for hop in range(1, max_hops + 1):
            if " " not in current:
                break
            current = current.rsplit(" ", 1)[0]
            if current in stats:
                chain.append((current, hop))
            else:
                break
        return chain

    def _select_queries(self, stats: dict[str, QueryStats]) -> list[QueryStats]:
        niche_rules = self.default_rules.get("niche_rules", {"all": {}})
        default_top_n = int(self.default_rules.get("top_n", 50))

        pp = self.default_rules.get("parent_priority", {})
        parent_enabled = bool(pp.get("enabled", True))
        keep_parent = bool(pp.get("keep_parent_for_selected_child", True))
        max_hops = max(1, int(pp.get("max_parent_hops", 1)))

        selected: list[QueryStats] = []
        for niche_name, niche_rule in niche_rules.items():
            top_n = int(niche_rule.get("top_n", default_top_n))
            pool = [s for s in stats.values() if s.niche == niche_name and s.passes_filters]
            pool.sort(key=lambda x: (x.hybrid_score, x.count, x.wordstat_volume), reverse=True)
            chosen = list(pool[:top_n])
            chosen_keys = {c.normalized_query for c in chosen}

            if parent_enabled and keep_parent:
                for child in list(chosen):
                    for parent_query, hops in self._parent_chain(child.normalized_query, max_hops, stats):
                        parent = stats[parent_query]
                        if not parent.passes_filters:
                            continue
                        if parent.normalized_query not in chosen_keys:
                            parent.is_selected = True
                            parent.selected_reason = "parent_of_selected"
                            parent.parent_selected_flag = 1
                            parent.parent_hops_used = hops
                            chosen.append(parent)
                            chosen_keys.add(parent.normalized_query)
                        else:
                            existing = next((x for x in chosen if x.normalized_query == parent.normalized_query), None)
                            if existing is not None and existing.parent_hops_used == 0:
                                existing.parent_hops_used = hops
                                existing.parent_selected_flag = 1

            chosen.sort(key=lambda x: (x.hybrid_score, x.count, x.wordstat_volume), reverse=True)
            for rank, item in enumerate(chosen, start=1):
                item.is_selected = True
                if not item.selected_reason:
                    item.selected_reason = "top_n"
                item.rank_in_niche = rank
                selected.append(item)

        selected_unique: dict[str, QueryStats] = {}
        for item in selected:
            prev = selected_unique.get(item.normalized_query)
            if prev is None or item.hybrid_score > prev.hybrid_score:
                selected_unique[item.normalized_query] = item

        for cur in stats.values():
            if not cur.is_selected and cur.passes_filters:
                cur.exclude_reason = cur.exclude_reason or "below_top_n"
            if not cur.is_selected and not cur.exclude_reason:
                cur.exclude_reason = "filtered_out"

        out = list(selected_unique.values())
        out.sort(key=lambda x: (x.hybrid_score, x.count, x.wordstat_volume), reverse=True)
        return out

    def _write_outputs(self, stats: dict[str, QueryStats], selected: list[QueryStats]) -> dict[str, str]:
        filter_cfg = self.config.raw.get("filter", {})
        out_cfg = filter_cfg.get("output_files", {})

        raw_name = str(out_cfg.get("candidates_raw_csv", "filter_candidates_raw.csv"))
        debug_name = str(out_cfg.get("debug_scores_csv", "debug_scores.csv"))
        top_name = str(out_cfg.get("top_queries_csv", "top_queries.csv"))
        queries_name = str(out_cfg.get("queries_txt", "queries.txt"))

        raw_path = self.config.paths.output_path(layer="raw", component=COMPONENT_FILTER, run_id=self.ctx.run_id, filename=raw_name)
        debug_path = self.config.paths.output_path(layer="staging", component=COMPONENT_FILTER, run_id=self.ctx.run_id, filename=debug_name)
        top_path = self.config.paths.output_path(layer="marts", component=COMPONENT_FILTER, run_id=self.ctx.run_id, filename=top_name)

        raw_fields = [
            "run_id", "component", "collected_at_utc", "normalized_query", "canonical_query", "source_query", "niche",
            "count", "source_rows", "source_typed_queries_count", "min_position", "suggest_position_sum", "depth0_hits",
            "wordstat_volume", "parent_query", "children_count"
        ]
        debug_fields = [
            "run_id", "component", "normalized_query", "canonical_query", "source_query", "niche",
            "niche_multiplier_total", "is_selected", "selected_reason", "exclude_reason", "rank_in_niche", "passes_filters",
            "hybrid_score", "score_suggest", "score_wordstat", "score_parent", "score_priority", "wordstat_volume",
            "count", "min_position", "source_rows", "source_typed_queries_count", "depth0_hits",
            "matched_required_terms", "matched_priority_patterns", "parent_query", "children_count",
            "parent_hops_used", "parent_selected_flag"
        ]
        top_fields = [
            "run_id", "component", "rank", "query", "query_group", "niche", "normalized_query", "canonical_query", "source_query",
            "hybrid_score", "score_suggest", "score_wordstat", "wordstat_volume", "count", "selected_reason"
        ]

        ts = utc_now_iso()
        raw_rows: list[dict[str, object]] = []
        debug_rows: list[dict[str, object]] = []
        for cur in stats.values():
            raw_rows.append(
                {
                    "run_id": self.ctx.run_id,
                    "component": COMPONENT_FILTER,
                    "collected_at_utc": ts,
                    "normalized_query": cur.normalized_query,
                    "canonical_query": cur.canonical_query,
                    "source_query": cur.source_query,
                    "niche": cur.niche,
                    "count": cur.count,
                    "source_rows": cur.source_rows,
                    "source_typed_queries_count": cur.source_typed_queries_count,
                    "min_position": 0 if cur.min_position == 10**9 else cur.min_position,
                    "suggest_position_sum": round(cur.suggest_position_sum, 6),
                    "depth0_hits": cur.depth0_hits,
                    "wordstat_volume": cur.wordstat_volume,
                    "parent_query": cur.parent_query,
                    "children_count": cur.children_count,
                }
            )
            debug_rows.append(
                {
                    "run_id": self.ctx.run_id,
                    "component": COMPONENT_FILTER,
                    "normalized_query": cur.normalized_query,
                    "canonical_query": cur.canonical_query,
                    "source_query": cur.source_query,
                    "niche": cur.niche,
                    "niche_multiplier_total": round(cur.niche_multiplier_total, 6),
                    "is_selected": 1 if cur.is_selected else 0,
                    "selected_reason": cur.selected_reason,
                    "exclude_reason": cur.exclude_reason,
                    "rank_in_niche": cur.rank_in_niche,
                    "passes_filters": 1 if cur.passes_filters else 0,
                    "hybrid_score": round(cur.hybrid_score, 6),
                    "score_suggest": round(cur.score_suggest, 6),
                    "score_wordstat": round(cur.score_wordstat, 6),
                    "score_parent": round(cur.score_parent, 6),
                    "score_priority": round(cur.score_priority, 6),
                    "wordstat_volume": cur.wordstat_volume,
                    "count": cur.count,
                    "min_position": 0 if cur.min_position == 10**9 else cur.min_position,
                    "source_rows": cur.source_rows,
                    "source_typed_queries_count": cur.source_typed_queries_count,
                    "depth0_hits": cur.depth0_hits,
                    "matched_required_terms": "|".join(cur.matched_required_terms),
                    "matched_priority_patterns": "|".join(cur.matched_priority_patterns),
                    "parent_query": cur.parent_query,
                    "children_count": cur.children_count,
                    "parent_hops_used": cur.parent_hops_used,
                    "parent_selected_flag": cur.parent_selected_flag,
                }
            )

        top_rows: list[dict[str, object]] = []
        for rank, cur in enumerate(selected, start=1):
            top_rows.append(
                {
                    "run_id": self.ctx.run_id,
                    "component": COMPONENT_FILTER,
                    "rank": rank,
                    "query": cur.normalized_query,
                    "query_group": cur.niche,
                    "niche": cur.niche,
                    "normalized_query": cur.normalized_query,
                    "canonical_query": cur.canonical_query,
                    "source_query": cur.source_query,
                    "hybrid_score": round(cur.hybrid_score, 6),
                    "score_suggest": round(cur.score_suggest, 6),
                    "score_wordstat": round(cur.score_wordstat, 6),
                    "wordstat_volume": cur.wordstat_volume,
                    "count": cur.count,
                    "selected_reason": cur.selected_reason,
                }
            )

        write_csv_rows(raw_path, raw_rows, raw_fields)
        write_csv_rows(debug_path, debug_rows, debug_fields)
        write_csv_rows(top_path, top_rows, top_fields)

        queries_lines = [cur.normalized_query for cur in selected]
        run_queries_path = self.config.paths.output_path(
            layer="marts",
            component=COMPONENT_FILTER,
            run_id=self.ctx.run_id,
            filename=queries_name,
        )
        run_queries_path.write_text("\n".join(queries_lines) + ("\n" if queries_lines else ""), encoding="utf-8")

        export_queries = self.config.paths.EXPORTS_DIR / queries_name
        export_queries.parent.mkdir(parents=True, exist_ok=True)
        export_queries.write_text("\n".join(queries_lines) + ("\n" if queries_lines else ""), encoding="utf-8")

        latest_raw = self.config.paths.publish_latest_output(layer="raw", component=COMPONENT_FILTER, source_path=raw_path, filename=raw_name)
        latest_debug = self.config.paths.publish_latest_output(layer="staging", component=COMPONENT_FILTER, source_path=debug_path, filename=debug_name)
        latest_top = self.config.paths.publish_latest_output(layer="marts", component=COMPONENT_FILTER, source_path=top_path, filename=top_name)
        latest_queries = self.config.paths.publish_latest_output(layer="marts", component=COMPONENT_FILTER, source_path=run_queries_path, filename=queries_name)

        return {
            "raw_candidates_path": str(raw_path),
            "debug_scores_path": str(debug_path),
            "top_queries_path": str(top_path),
            "queries_txt_path": str(export_queries),
            "latest_raw_candidates_path": str(latest_raw),
            "latest_debug_scores_path": str(latest_debug),
            "latest_top_queries_path": str(latest_top),
            "latest_queries_txt_path": str(latest_queries),
        }

