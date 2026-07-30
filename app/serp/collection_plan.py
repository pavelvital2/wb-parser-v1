from __future__ import annotations

import hashlib
import json
import os
import re
import stat
import tempfile
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path, PurePosixPath
from typing import Any, Mapping


QUERY_PACK_SCHEMA_VERSION = "wb_query_pack_v1"
REGION_REGISTRY_SCHEMA_VERSION = "wb_region_registry_v1"
COLLECTION_PLAN_SCHEMA_VERSION = "wb_collection_plan_v1"
BOUNDED_COLLECTION_PLAN_SCHEMA_VERSION = "wb_collection_plan_v2"
EFFECTIVE_PLAN_SCHEMA_VERSION = "wb_effective_collection_plan_v1"
RESUMABLE_EFFECTIVE_PLAN_SCHEMA_VERSION = "wb_effective_collection_plan_v2"
BOUNDED_RESUMABLE_EFFECTIVE_PLAN_SCHEMA_VERSION = "wb_effective_collection_plan_v3"
PROVENANCE_SCHEMA_VERSION = "wb_query_pack_provenance_v1"

PAGE_SIZE = 100
SUPPORTED_DEPTHS = frozenset(range(PAGE_SIZE, 1001, PAGE_SIZE))

_ID_RE = re.compile(r"^[a-z0-9][a-z0-9_-]*$")
_VERSION_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]*$")
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_WB_DEST_ID_RE = re.compile(r"^[+-]?[0-9]{1,16}$")
_RFC3339_UTC_RE = re.compile(
    r"^[0-9]{4}-[0-9]{2}-[0-9]{2}T"
    r"[0-9]{2}:[0-9]{2}:[0-9]{2}"
    r"(?:\.[0-9]{1,6})?(?:Z|\+00:00)$"
)


class CollectionPlanValidationError(ValueError):
    pass


@dataclass(frozen=True, slots=True)
class QueryCategory:
    category_id: str
    name: str
    enabled: bool


@dataclass(frozen=True, slots=True)
class QueryDefinition:
    query_id: str
    category_id: str
    text: str
    enabled: bool


@dataclass(frozen=True, slots=True)
class QueryPack:
    source_path: Path
    source_sha256: str
    query_pack_id: str
    version: str
    enabled: bool
    categories: tuple[QueryCategory, ...]
    queries: tuple[QueryDefinition, ...]


@dataclass(frozen=True, slots=True)
class RegionDefinition:
    region_id: str
    region_name: str
    enabled: bool
    resolver: str
    latitude: str
    longitude: str
    address_label: str
    dest_id: None
    dest_resolved_at_utc: None
    dest_resolution_source: None
    dest_resolution_status: str


@dataclass(frozen=True, slots=True)
class RegionRegistry:
    source_path: Path
    source_sha256: str
    regions: tuple[RegionDefinition, ...]


@dataclass(frozen=True, slots=True)
class CollectionQuality:
    expected_queries_per_region: int
    expected_pages_per_query: int
    max_page_errors: int
    require_constant_egress: bool
    require_distinct_destinations: bool


@dataclass(frozen=True, slots=True)
class CollectionRuntimeWindow:
    mode: str
    scheduled_start_msk: str
    new_run_start_grace_seconds: int
    max_invocation_runtime_seconds: int
    absolute_cutoff_msk: str
    minimum_resume_window_seconds: int
    finalization_reserve_seconds: int


@dataclass(frozen=True, slots=True)
class CollectionPlan:
    source_path: Path
    source_sha256: str
    collection_plan_id: str
    enabled: bool
    query_pack_file: str
    query_ids: tuple[str, ...]
    region_set: tuple[str, ...]
    depth: int
    schedule_id: str
    publication_mode: str
    sellers_mode: str
    proxy_rotation_mode: str
    quality: CollectionQuality
    runtime_window: CollectionRuntimeWindow | None = None


@dataclass(frozen=True, slots=True)
class CollectionPlanBundle:
    project_root: Path
    query_pack: QueryPack
    region_registry: RegionRegistry
    collection_plan: CollectionPlan

    @property
    def query_pack_sha256(self) -> str:
        return self.query_pack.source_sha256

    @property
    def collection_plan_sha256(self) -> str:
        return self.collection_plan.source_sha256

    @property
    def region_registry_sha256(self) -> str:
        return self.region_registry.source_sha256

    @property
    def enabled_queries(self) -> tuple[QueryDefinition, ...]:
        if not self.collection_plan.enabled:
            return ()
        by_id = {query.query_id: query for query in self.query_pack.queries}
        return tuple(
            by_id[query_id]
            for query_id in self.collection_plan.query_ids
            if by_id[query_id].enabled
        )

    @property
    def enabled_regions(self) -> tuple[RegionDefinition, ...]:
        if not self.collection_plan.enabled:
            return ()
        by_id = {region.region_id: region for region in self.region_registry.regions}
        return tuple(
            by_id[region_id]
            for region_id in self.collection_plan.region_set
            if by_id[region_id].enabled
        )


@dataclass(frozen=True, slots=True)
class ResolvedDestination:
    region_id: str
    dest_id_observed: str
    dest_resolved_at_utc: str
    dest_resolution_source: str = "wb_geo_xinfo"
    dest_resolution_status: str = "resolved_not_sent"


@dataclass(frozen=True, slots=True)
class EffectiveEndpointPolicy:
    selection_mode: str
    endpoint_ids: tuple[str, ...]
    pinned_endpoint_id: str


def normalize_query_text(value: str) -> str:
    return " ".join((value or "").strip().split())


def exact_file_sha256(path: Path | str) -> str:
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def _reject_json_constant(value: str) -> None:
    raise CollectionPlanValidationError(f"invalid JSON numeric constant: {value}")


def _unique_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise CollectionPlanValidationError(f"duplicate JSON key: {key}")
        result[key] = value
    return result


def _load_json_document(path: Path | str) -> tuple[Path, dict[str, Any], str]:
    source_path = Path(path).resolve()
    try:
        raw_bytes = source_path.read_bytes()
    except OSError as exc:
        raise CollectionPlanValidationError(f"cannot read JSON document {source_path}: {exc}") from exc

    try:
        text = raw_bytes.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise CollectionPlanValidationError(f"JSON document must be UTF-8: {source_path}") from exc

    try:
        payload = json.loads(
            text,
            object_pairs_hook=_unique_object,
            parse_constant=_reject_json_constant,
        )
    except CollectionPlanValidationError:
        raise
    except json.JSONDecodeError as exc:
        raise CollectionPlanValidationError(f"invalid JSON document {source_path}: {exc}") from exc

    if not isinstance(payload, dict):
        raise CollectionPlanValidationError(f"JSON document root must be an object: {source_path}")

    return source_path, payload, hashlib.sha256(raw_bytes).hexdigest()


def _require_keys(
    value: Mapping[str, Any],
    *,
    required: set[str],
    field: str,
    optional: set[str] | None = None,
) -> None:
    allowed = required | (optional or set())
    missing = sorted(required - set(value))
    extra = sorted(set(value) - allowed)
    if missing:
        raise CollectionPlanValidationError(f"{field} missing keys: {', '.join(missing)}")
    if extra:
        raise CollectionPlanValidationError(f"{field} has unknown keys: {', '.join(extra)}")


def _require_object(value: Any, *, field: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise CollectionPlanValidationError(f"{field} must be an object")
    return value


def _require_list(value: Any, *, field: str, non_empty: bool = True) -> list[Any]:
    if not isinstance(value, list):
        raise CollectionPlanValidationError(f"{field} must be an array")
    if non_empty and not value:
        raise CollectionPlanValidationError(f"{field} must not be empty")
    return value


def _require_string(value: Any, *, field: str, non_empty: bool = True) -> str:
    if not isinstance(value, str):
        raise CollectionPlanValidationError(f"{field} must be a string")
    if value != value.strip():
        raise CollectionPlanValidationError(f"{field} must not have surrounding whitespace")
    if non_empty and not value:
        raise CollectionPlanValidationError(f"{field} must not be empty")
    return value


def _require_id(value: Any, *, field: str) -> str:
    identifier = _require_string(value, field=field)
    if not _ID_RE.fullmatch(identifier):
        raise CollectionPlanValidationError(f"{field} must match {_ID_RE.pattern}")
    return identifier


def _require_version(value: Any, *, field: str) -> str:
    version = _require_string(value, field=field)
    if not _VERSION_RE.fullmatch(version):
        raise CollectionPlanValidationError(f"{field} must match {_VERSION_RE.pattern}")
    return version


def _require_bool(value: Any, *, field: str) -> bool:
    if type(value) is not bool:
        raise CollectionPlanValidationError(f"{field} must be a boolean")
    return value


def _require_int(
    value: Any,
    *,
    field: str,
    minimum: int | None = None,
    maximum: int | None = None,
) -> int:
    if type(value) is not int:
        raise CollectionPlanValidationError(f"{field} must be an integer")
    if minimum is not None and value < minimum:
        raise CollectionPlanValidationError(f"{field} must be >= {minimum}")
    if maximum is not None and value > maximum:
        raise CollectionPlanValidationError(f"{field} must be <= {maximum}")
    return value


def _require_sha256(value: Any, *, field: str) -> str:
    digest = _require_string(value, field=field)
    if not _SHA256_RE.fullmatch(digest):
        raise CollectionPlanValidationError(f"{field} must be a lowercase SHA-256 digest")
    return digest


def _require_unique_ids(values: list[Any], *, field: str) -> tuple[str, ...]:
    result: list[str] = []
    seen: set[str] = set()
    for index, value in enumerate(values):
        identifier = _require_id(value, field=f"{field}[{index}]")
        if identifier in seen:
            raise CollectionPlanValidationError(f"{field} contains duplicate ID: {identifier}")
        seen.add(identifier)
        result.append(identifier)
    return tuple(result)


def _require_decimal_coordinate(
    value: Any,
    *,
    field: str,
    minimum: Decimal,
    maximum: Decimal,
) -> str:
    text = _require_string(value, field=field)
    try:
        number = Decimal(text)
    except InvalidOperation as exc:
        raise CollectionPlanValidationError(f"{field} must be a decimal string") from exc
    if not number.is_finite() or number < minimum or number > maximum:
        raise CollectionPlanValidationError(f"{field} must be between {minimum} and {maximum}")
    return text


def _validate_schema_version(payload: Mapping[str, Any], *, expected: str, field: str) -> None:
    actual = _require_string(payload.get("schema_version"), field=f"{field}.schema_version")
    if actual != expected:
        raise CollectionPlanValidationError(
            f"{field}.schema_version must be {expected}, got {actual}"
        )


def load_query_pack(path: Path | str) -> QueryPack:
    source_path, payload, source_sha256 = _load_json_document(path)
    _require_keys(
        payload,
        required={
            "schema_version",
            "query_pack_id",
            "version",
            "enabled",
            "categories",
            "queries",
        },
        field="query_pack",
    )
    _validate_schema_version(payload, expected=QUERY_PACK_SCHEMA_VERSION, field="query_pack")

    query_pack_id = _require_id(payload["query_pack_id"], field="query_pack.query_pack_id")
    version = _require_version(payload["version"], field="query_pack.version")
    enabled = _require_bool(payload["enabled"], field="query_pack.enabled")

    category_items = _require_list(payload["categories"], field="query_pack.categories")
    categories: list[QueryCategory] = []
    category_ids: set[str] = set()
    for index, raw_category in enumerate(category_items):
        field = f"query_pack.categories[{index}]"
        category = _require_object(raw_category, field=field)
        _require_keys(
            category,
            required={"category_id", "name", "enabled"},
            field=field,
        )
        category_id = _require_id(category["category_id"], field=f"{field}.category_id")
        if category_id in category_ids:
            raise CollectionPlanValidationError(
                f"query_pack.categories contains duplicate ID: {category_id}"
            )
        category_ids.add(category_id)
        categories.append(
            QueryCategory(
                category_id=category_id,
                name=_require_string(category["name"], field=f"{field}.name"),
                enabled=_require_bool(category["enabled"], field=f"{field}.enabled"),
            )
        )

    query_items = _require_list(payload["queries"], field="query_pack.queries")
    queries: list[QueryDefinition] = []
    query_ids: set[str] = set()
    normalized_texts: set[str] = set()
    categories_by_id = {category.category_id: category for category in categories}
    for index, raw_query in enumerate(query_items):
        field = f"query_pack.queries[{index}]"
        query = _require_object(raw_query, field=field)
        _require_keys(
            query,
            required={"query_id", "category_id", "text", "enabled"},
            field=field,
        )
        query_id = _require_id(query["query_id"], field=f"{field}.query_id")
        if query_id in query_ids:
            raise CollectionPlanValidationError(f"query_pack.queries contains duplicate ID: {query_id}")
        query_ids.add(query_id)

        category_id = _require_id(query["category_id"], field=f"{field}.category_id")
        category = categories_by_id.get(category_id)
        if category is None:
            raise CollectionPlanValidationError(
                f"{field}.category_id references unknown category: {category_id}"
            )

        text = _require_string(query["text"], field=f"{field}.text")
        normalized_text = normalize_query_text(text)
        if text != normalized_text:
            raise CollectionPlanValidationError(f"{field}.text must already be normalized")
        dedupe_key = normalized_text.casefold().replace("ё", "е")
        if dedupe_key in normalized_texts:
            raise CollectionPlanValidationError(
                f"query_pack.queries contains duplicate normalized text: {text}"
            )
        normalized_texts.add(dedupe_key)

        query_enabled = _require_bool(query["enabled"], field=f"{field}.enabled")
        if query_enabled and not category.enabled:
            raise CollectionPlanValidationError(
                f"{field} is enabled but category {category_id} is disabled"
            )
        queries.append(
            QueryDefinition(
                query_id=query_id,
                category_id=category_id,
                text=text,
                enabled=query_enabled,
            )
        )

    return QueryPack(
        source_path=source_path,
        source_sha256=source_sha256,
        query_pack_id=query_pack_id,
        version=version,
        enabled=enabled,
        categories=tuple(categories),
        queries=tuple(queries),
    )


def load_region_registry(path: Path | str) -> RegionRegistry:
    source_path, payload, source_sha256 = _load_json_document(path)
    _require_keys(
        payload,
        required={"schema_version", "regions"},
        field="region_registry",
    )
    _validate_schema_version(
        payload,
        expected=REGION_REGISTRY_SCHEMA_VERSION,
        field="region_registry",
    )

    region_items = _require_list(payload["regions"], field="region_registry.regions")
    regions: list[RegionDefinition] = []
    region_ids: set[str] = set()
    region_names: set[str] = set()
    for index, raw_region in enumerate(region_items):
        field = f"region_registry.regions[{index}]"
        region = _require_object(raw_region, field=field)
        _require_keys(
            region,
            required={
                "region_id",
                "region_name",
                "enabled",
                "resolver",
                "latitude",
                "longitude",
                "address_label",
                "dest_id",
                "dest_resolved_at_utc",
                "dest_resolution_source",
                "dest_resolution_status",
            },
            field=field,
        )

        region_id = _require_id(region["region_id"], field=f"{field}.region_id")
        if region_id in region_ids:
            raise CollectionPlanValidationError(
                f"region_registry.regions contains duplicate ID: {region_id}"
            )
        region_ids.add(region_id)

        region_name = _require_string(region["region_name"], field=f"{field}.region_name")
        region_name_key = region_name.casefold()
        if region_name_key in region_names:
            raise CollectionPlanValidationError(
                f"region_registry.regions contains duplicate name: {region_name}"
            )
        region_names.add(region_name_key)

        resolver = _require_string(region["resolver"], field=f"{field}.resolver")
        if resolver != "wb_geo_xinfo":
            raise CollectionPlanValidationError(
                f"{field}.resolver must be wb_geo_xinfo"
            )

        if region["dest_id"] is not None:
            raise CollectionPlanValidationError(
                f"{field}.dest_id must remain null in versioned Stage 1 config"
            )
        if region["dest_resolved_at_utc"] is not None:
            raise CollectionPlanValidationError(
                f"{field}.dest_resolved_at_utc must remain null in versioned Stage 1 config"
            )
        if region["dest_resolution_source"] is not None:
            raise CollectionPlanValidationError(
                f"{field}.dest_resolution_source must remain null in versioned Stage 1 config"
            )
        resolution_status = _require_string(
            region["dest_resolution_status"],
            field=f"{field}.dest_resolution_status",
        )
        if resolution_status != "unresolved":
            raise CollectionPlanValidationError(
                f"{field}.dest_resolution_status must be unresolved in versioned Stage 1 config"
            )

        regions.append(
            RegionDefinition(
                region_id=region_id,
                region_name=region_name,
                enabled=_require_bool(region["enabled"], field=f"{field}.enabled"),
                resolver=resolver,
                latitude=_require_decimal_coordinate(
                    region["latitude"],
                    field=f"{field}.latitude",
                    minimum=Decimal("-90"),
                    maximum=Decimal("90"),
                ),
                longitude=_require_decimal_coordinate(
                    region["longitude"],
                    field=f"{field}.longitude",
                    minimum=Decimal("-180"),
                    maximum=Decimal("180"),
                ),
                address_label=_require_string(
                    region["address_label"],
                    field=f"{field}.address_label",
                ),
                dest_id=None,
                dest_resolved_at_utc=None,
                dest_resolution_source=None,
                dest_resolution_status=resolution_status,
            )
        )

    return RegionRegistry(
        source_path=source_path,
        source_sha256=source_sha256,
        regions=tuple(regions),
    )


def _load_quality(value: Any) -> CollectionQuality:
    quality = _require_object(value, field="collection_plan.quality")
    _require_keys(
        quality,
        required={
            "expected_queries_per_region",
            "expected_pages_per_query",
            "max_page_errors",
            "require_constant_egress",
            "require_distinct_destinations",
        },
        field="collection_plan.quality",
    )
    return CollectionQuality(
        expected_queries_per_region=_require_int(
            quality["expected_queries_per_region"],
            field="collection_plan.quality.expected_queries_per_region",
            minimum=1,
        ),
        expected_pages_per_query=_require_int(
            quality["expected_pages_per_query"],
            field="collection_plan.quality.expected_pages_per_query",
            minimum=1,
        ),
        max_page_errors=_require_int(
            quality["max_page_errors"],
            field="collection_plan.quality.max_page_errors",
            minimum=0,
        ),
        require_constant_egress=_require_bool(
            quality["require_constant_egress"],
            field="collection_plan.quality.require_constant_egress",
        ),
        require_distinct_destinations=_require_bool(
            quality["require_distinct_destinations"],
            field="collection_plan.quality.require_distinct_destinations",
        ),
    )


def _require_hhmm(value: Any, *, field: str) -> str:
    text = _require_string(value, field=field)
    if not re.fullmatch(r"(?:[01][0-9]|2[0-3]):[0-5][0-9]", text):
        raise CollectionPlanValidationError(f"{field} must use HH:MM")
    return text


def _load_runtime_window(value: Any) -> CollectionRuntimeWindow:
    field = "collection_plan.runtime_window"
    window = _require_object(value, field=field)
    _require_keys(
        window,
        required={
            "mode",
            "scheduled_start_msk",
            "new_run_start_grace_seconds",
            "max_invocation_runtime_seconds",
            "absolute_cutoff_msk",
            "minimum_resume_window_seconds",
            "finalization_reserve_seconds",
        },
        field=field,
    )
    mode = _require_string(window["mode"], field=f"{field}.mode")
    if mode != "bounded_resumable":
        raise CollectionPlanValidationError(
            f"{field}.mode must be bounded_resumable"
        )
    scheduled_start_msk = _require_hhmm(
        window["scheduled_start_msk"],
        field=f"{field}.scheduled_start_msk",
    )
    absolute_cutoff_msk = _require_hhmm(
        window["absolute_cutoff_msk"],
        field=f"{field}.absolute_cutoff_msk",
    )
    if absolute_cutoff_msk <= scheduled_start_msk:
        raise CollectionPlanValidationError(
            f"{field}.absolute_cutoff_msk must be after scheduled_start_msk"
        )
    finalization_reserve_seconds = _require_int(
        window["finalization_reserve_seconds"],
        field=f"{field}.finalization_reserve_seconds",
        minimum=5,
        maximum=900,
    )
    minimum_resume_window_seconds = _require_int(
        window["minimum_resume_window_seconds"],
        field=f"{field}.minimum_resume_window_seconds",
        minimum=300,
        maximum=7200,
    )
    max_invocation_runtime_seconds = _require_int(
        window["max_invocation_runtime_seconds"],
        field=f"{field}.max_invocation_runtime_seconds",
        minimum=minimum_resume_window_seconds,
        maximum=43200,
    )
    if minimum_resume_window_seconds <= finalization_reserve_seconds:
        raise CollectionPlanValidationError(
            f"{field}.minimum_resume_window_seconds must exceed finalization reserve"
        )
    return CollectionRuntimeWindow(
        mode=mode,
        scheduled_start_msk=scheduled_start_msk,
        new_run_start_grace_seconds=_require_int(
            window["new_run_start_grace_seconds"],
            field=f"{field}.new_run_start_grace_seconds",
            minimum=0,
            maximum=43200,
        ),
        max_invocation_runtime_seconds=max_invocation_runtime_seconds,
        absolute_cutoff_msk=absolute_cutoff_msk,
        minimum_resume_window_seconds=minimum_resume_window_seconds,
        finalization_reserve_seconds=finalization_reserve_seconds,
    )


def load_collection_plan(path: Path | str) -> CollectionPlan:
    source_path, payload, source_sha256 = _load_json_document(path)
    schema_version = _require_string(
        payload.get("schema_version"),
        field="collection_plan.schema_version",
    )
    if schema_version not in {
        COLLECTION_PLAN_SCHEMA_VERSION,
        BOUNDED_COLLECTION_PLAN_SCHEMA_VERSION,
    }:
        raise CollectionPlanValidationError(
            "collection_plan.schema_version is unsupported"
        )
    optional = (
        {"runtime_window"}
        if schema_version == BOUNDED_COLLECTION_PLAN_SCHEMA_VERSION
        else set()
    )
    _require_keys(
        payload,
        required={
            "schema_version",
            "collection_plan_id",
            "enabled",
            "query_pack_file",
            "query_ids",
            "region_set",
            "depth",
            "schedule_id",
            "publication_mode",
            "sellers_mode",
            "proxy_rotation_mode",
            "quality",
        },
        field="collection_plan",
        optional=optional,
    )
    if schema_version == BOUNDED_COLLECTION_PLAN_SCHEMA_VERSION:
        if "runtime_window" not in payload:
            raise CollectionPlanValidationError(
                "collection_plan.runtime_window is required for schema v2"
            )
        runtime_window = _load_runtime_window(payload["runtime_window"])
    else:
        runtime_window = None

    collection_plan_id = _require_id(
        payload["collection_plan_id"],
        field="collection_plan.collection_plan_id",
    )
    query_pack_file = _require_string(
        payload["query_pack_file"],
        field="collection_plan.query_pack_file",
    )
    query_pack_path = PurePosixPath(query_pack_file)
    if (
        query_pack_path.is_absolute()
        or "\\" in query_pack_file
        or "." in query_pack_path.parts
        or ".." in query_pack_path.parts
    ):
        raise CollectionPlanValidationError(
            "collection_plan.query_pack_file must be a safe project-relative POSIX path"
        )
    expected_prefix = ("config", "wb", "query_packs")
    if query_pack_path.parts[:3] != expected_prefix:
        raise CollectionPlanValidationError(
            "collection_plan.query_pack_file must be under config/wb/query_packs"
        )

    query_ids = _require_unique_ids(
        _require_list(payload["query_ids"], field="collection_plan.query_ids"),
        field="collection_plan.query_ids",
    )
    region_set = _require_unique_ids(
        _require_list(payload["region_set"], field="collection_plan.region_set"),
        field="collection_plan.region_set",
    )

    depth = _require_int(payload["depth"], field="collection_plan.depth", minimum=1)
    if depth not in SUPPORTED_DEPTHS:
        raise CollectionPlanValidationError(
            f"collection_plan.depth must be one of {sorted(SUPPORTED_DEPTHS)}"
        )

    publication_mode = _require_string(
        payload["publication_mode"],
        field="collection_plan.publication_mode",
    )
    if publication_mode != "none":
        raise CollectionPlanValidationError("collection_plan.publication_mode must be none")

    sellers_mode = _require_string(
        payload["sellers_mode"],
        field="collection_plan.sellers_mode",
    )
    if sellers_mode != "disabled":
        raise CollectionPlanValidationError("collection_plan.sellers_mode must be disabled")

    proxy_rotation_mode = _require_string(
        payload["proxy_rotation_mode"],
        field="collection_plan.proxy_rotation_mode",
    )
    if proxy_rotation_mode != "disabled":
        raise CollectionPlanValidationError(
            "collection_plan.proxy_rotation_mode must be disabled"
        )

    quality = _load_quality(payload["quality"])
    if quality.expected_queries_per_region != len(query_ids):
        raise CollectionPlanValidationError(
            "collection_plan.quality.expected_queries_per_region must match query_ids count"
        )
    if quality.expected_pages_per_query != depth // PAGE_SIZE:
        raise CollectionPlanValidationError(
            "collection_plan.quality.expected_pages_per_query must match depth/page_size"
        )
    if quality.max_page_errors != 0:
        raise CollectionPlanValidationError(
            "collection_plan.quality.max_page_errors must be 0 in Stage 1"
        )
    if not quality.require_constant_egress or not quality.require_distinct_destinations:
        raise CollectionPlanValidationError(
            "collection_plan quality must require constant egress and distinct destinations"
        )

    return CollectionPlan(
        source_path=source_path,
        source_sha256=source_sha256,
        collection_plan_id=collection_plan_id,
        enabled=_require_bool(payload["enabled"], field="collection_plan.enabled"),
        query_pack_file=query_pack_file,
        query_ids=query_ids,
        region_set=region_set,
        depth=depth,
        schedule_id=_require_id(payload["schedule_id"], field="collection_plan.schedule_id"),
        publication_mode=publication_mode,
        sellers_mode=sellers_mode,
        proxy_rotation_mode=proxy_rotation_mode,
        quality=quality,
        runtime_window=runtime_window,
    )


def _require_regular_project_file(
    path: Path | str,
    *,
    project_root: Path,
    field: str,
    exact_path: Path | None = None,
    allowed_root: Path | None = None,
    direct_child: bool = False,
) -> Path:
    root = project_root.resolve()
    candidate = Path(path)
    if not candidate.is_absolute():
        candidate = root / candidate
    lexical_path = Path(os.path.abspath(candidate))

    try:
        relative_path = lexical_path.relative_to(root)
    except ValueError as exc:
        raise CollectionPlanValidationError(
            f"{field} must be inside project root"
        ) from exc

    if exact_path is not None and lexical_path != exact_path:
        raise CollectionPlanValidationError(
            f"{field} must be exactly {exact_path.relative_to(root).as_posix()}"
        )

    if allowed_root is not None:
        try:
            allowed_relative = lexical_path.relative_to(allowed_root)
        except ValueError as exc:
            raise CollectionPlanValidationError(
                f"{field} must be inside {allowed_root.relative_to(root).as_posix()}"
            ) from exc
        if direct_child and len(allowed_relative.parts) != 1:
            raise CollectionPlanValidationError(
                f"{field} must be a direct child of "
                f"{allowed_root.relative_to(root).as_posix()}"
            )

    current = root
    for part in relative_path.parts:
        current /= part
        if current.is_symlink():
            raise CollectionPlanValidationError(f"{field} must not use symlinks")

    try:
        mode = lexical_path.stat().st_mode
    except OSError as exc:
        raise CollectionPlanValidationError(
            f"{field} must be a readable regular file: {exc}"
        ) from exc
    if not stat.S_ISREG(mode):
        raise CollectionPlanValidationError(f"{field} must be a regular file")
    if lexical_path.suffix != ".json":
        raise CollectionPlanValidationError(f"{field} must be a JSON file")
    return lexical_path


def _resolve_project_file(project_root: Path, relative_path: str) -> Path:
    root = project_root.resolve()
    return _require_regular_project_file(
        relative_path,
        project_root=root,
        field="collection_plan.query_pack_file",
        allowed_root=root / "config/wb/query_packs",
    )


def _validate_bundle_references(bundle: CollectionPlanBundle) -> None:
    plan = bundle.collection_plan
    pack = bundle.query_pack
    registry = bundle.region_registry

    if plan.source_path.stem != plan.collection_plan_id:
        raise CollectionPlanValidationError(
            "collection plan filename must match collection_plan_id"
        )
    if pack.source_path.parent.name != pack.query_pack_id:
        raise CollectionPlanValidationError(
            "query pack parent directory must match query_pack_id"
        )
    if pack.source_path.stem != pack.version:
        raise CollectionPlanValidationError("query pack filename must match version")

    query_by_id = {query.query_id: query for query in pack.queries}
    category_by_id = {category.category_id: category for category in pack.categories}
    unknown_queries = [query_id for query_id in plan.query_ids if query_id not in query_by_id]
    if unknown_queries:
        raise CollectionPlanValidationError(
            f"collection plan references unknown query IDs: {', '.join(unknown_queries)}"
        )

    region_by_id = {region.region_id: region for region in registry.regions}
    unknown_regions = [region_id for region_id in plan.region_set if region_id not in region_by_id]
    if unknown_regions:
        raise CollectionPlanValidationError(
            f"collection plan references unknown region IDs: {', '.join(unknown_regions)}"
        )

    if not plan.enabled:
        return
    if not pack.enabled:
        raise CollectionPlanValidationError(
            "enabled collection plan cannot reference a disabled query pack"
        )

    disabled_queries = [
        query_id for query_id in plan.query_ids if not query_by_id[query_id].enabled
    ]
    if disabled_queries:
        raise CollectionPlanValidationError(
            f"enabled collection plan references disabled queries: {', '.join(disabled_queries)}"
        )

    disabled_categories = [
        query_by_id[query_id].category_id
        for query_id in plan.query_ids
        if not category_by_id[query_by_id[query_id].category_id].enabled
    ]
    if disabled_categories:
        raise CollectionPlanValidationError(
            "enabled collection plan references disabled categories: "
            + ", ".join(sorted(set(disabled_categories)))
        )

    disabled_regions = [
        region_id for region_id in plan.region_set if not region_by_id[region_id].enabled
    ]
    if disabled_regions:
        raise CollectionPlanValidationError(
            f"enabled collection plan references disabled regions: {', '.join(disabled_regions)}"
        )


def load_collection_plan_bundle(
    *,
    project_root: Path | str,
    plan_path: Path | str,
    region_registry_path: Path | str,
    provenance_path: Path | str | None = None,
) -> CollectionPlanBundle:
    root = Path(project_root).resolve()
    plan_source = _require_regular_project_file(
        plan_path,
        project_root=root,
        field="plan_path",
        allowed_root=root / "config/wb/collection_plans",
        direct_child=True,
    )
    registry_source = _require_regular_project_file(
        region_registry_path,
        project_root=root,
        field="region_registry_path",
        exact_path=root / "config/wb/regions.json",
    )
    plan = load_collection_plan(plan_source)
    registry = load_region_registry(registry_source)
    pack_path = _resolve_project_file(root, plan.query_pack_file)
    pack = load_query_pack(pack_path)

    bundle = CollectionPlanBundle(
        project_root=root,
        query_pack=pack,
        region_registry=registry,
        collection_plan=plan,
    )
    _validate_bundle_references(bundle)

    if provenance_path is not None:
        register_query_pack_provenance(
            provenance_path=provenance_path,
            query_pack=pack,
            project_root=root,
        )
    return bundle


def _load_provenance_payload(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {
            "schema_version": PROVENANCE_SCHEMA_VERSION,
            "query_packs": [],
        }

    _, payload, _ = _load_json_document(path)
    _require_keys(
        payload,
        required={"schema_version", "query_packs"},
        field="provenance",
    )
    _validate_schema_version(
        payload,
        expected=PROVENANCE_SCHEMA_VERSION,
        field="provenance",
    )

    items = _require_list(payload["query_packs"], field="provenance.query_packs", non_empty=False)
    seen: set[tuple[str, str]] = set()
    normalized_items: list[dict[str, str]] = []
    for index, raw_item in enumerate(items):
        field = f"provenance.query_packs[{index}]"
        item = _require_object(raw_item, field=field)
        _require_keys(
            item,
            required={"query_pack_id", "version", "query_pack_sha256"},
            field=field,
        )
        key = (
            _require_id(item["query_pack_id"], field=f"{field}.query_pack_id"),
            _require_version(item["version"], field=f"{field}.version"),
        )
        if key in seen:
            raise CollectionPlanValidationError(
                f"provenance contains duplicate query pack identity: {key[0]}@{key[1]}"
            )
        seen.add(key)
        normalized_items.append(
            {
                "query_pack_id": key[0],
                "version": key[1],
                "query_pack_sha256": _require_sha256(
                    item["query_pack_sha256"],
                    field=f"{field}.query_pack_sha256",
                ),
            }
        )

    return {
        "schema_version": PROVENANCE_SCHEMA_VERSION,
        "query_packs": normalized_items,
    }


def _ensure_durable_provenance_parent(
    path: Path,
    *,
    project_root: Path,
) -> None:
    expected = (
        project_root
        / "state/wb_collection_plans/provenance/query_pack_versions.json"
    )
    lexical = Path(os.path.abspath(path))
    if lexical != expected:
        raise CollectionPlanValidationError(
            "provenance_path must be the exact scoped provenance path"
        )
    current = project_root
    for part in lexical.relative_to(project_root).parts[:-1]:
        current /= part
        if current.is_symlink():
            raise CollectionPlanValidationError(
                "provenance_path must not use symlink components"
            )
        if not current.exists():
            parent = current.parent
            current.mkdir()
            for directory in (parent, current):
                fd = os.open(directory, os.O_RDONLY)
                try:
                    os.fsync(fd)
                finally:
                    os.close(fd)
        elif not current.is_dir():
            raise CollectionPlanValidationError(
                "provenance_path parent must be a directory"
            )
    if lexical.is_symlink() or (lexical.exists() and not lexical.is_file()):
        raise CollectionPlanValidationError(
            "provenance target must be a regular non-symlink file"
        )


def _atomic_write_json(
    path: Path,
    payload: Mapping[str, Any],
    *,
    project_root: Path,
) -> None:
    _ensure_durable_provenance_parent(path, project_root=project_root)
    text = json.dumps(
        payload,
        ensure_ascii=False,
        indent=2,
        sort_keys=True,
        allow_nan=False,
    ) + "\n"

    temp_path: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w",
            encoding="utf-8",
            newline="\n",
            prefix=f".{path.name}.",
            suffix=".tmp",
            dir=path.parent,
            delete=False,
        ) as handle:
            temp_path = Path(handle.name)
            handle.write(text)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temp_path, path)
        temp_path = None
        directory_fd = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
    finally:
        if temp_path is not None:
            temp_path.unlink(missing_ok=True)


def register_query_pack_provenance(
    *,
    provenance_path: Path | str,
    query_pack: QueryPack,
    project_root: Path | str,
) -> bool:
    """Record immutable pack identity.

    Stage 2 must call this while holding its plan-specific exclusion lock.
    Returns True for a new record and False for an identical existing record.
    """

    root = Path(project_root).resolve()
    path = Path(os.path.abspath(provenance_path))
    _ensure_durable_provenance_parent(path, project_root=root)
    payload = _load_provenance_payload(path)
    key = (query_pack.query_pack_id, query_pack.version)
    query_pack_sha256 = _require_sha256(
        query_pack.source_sha256,
        field="query_pack.source_sha256",
    )

    for item in payload["query_packs"]:
        if (item["query_pack_id"], item["version"]) != key:
            continue
        if item["query_pack_sha256"] != query_pack_sha256:
            raise CollectionPlanValidationError(
                "query pack provenance mismatch for "
                f"{query_pack.query_pack_id}@{query_pack.version}: "
                "the same identity was already used with a different content hash"
            )
        return False

    payload["query_packs"].append(
        {
            "query_pack_id": query_pack.query_pack_id,
            "version": query_pack.version,
            "query_pack_sha256": query_pack_sha256,
        }
    )
    payload["query_packs"].sort(
        key=lambda item: (item["query_pack_id"], item["version"])
    )
    _atomic_write_json(path, payload, project_root=root)
    return True


def _validate_utc_timestamp(value: Any, *, field: str) -> str:
    text = _require_string(value, field=field)
    if not _RFC3339_UTC_RE.fullmatch(text):
        raise CollectionPlanValidationError(
            f"{field} must be a strict RFC 3339 UTC timestamp"
        )
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError as exc:
        raise CollectionPlanValidationError(
            f"{field} must be a strict RFC 3339 UTC timestamp"
        ) from exc
    if parsed.tzinfo is None or parsed.utcoffset() != timezone.utc.utcoffset(parsed):
        raise CollectionPlanValidationError(f"{field} must use UTC")
    return text


def _require_wb_dest_id(value: Any, *, field: str) -> str:
    if not isinstance(value, str) or not _WB_DEST_ID_RE.fullmatch(value):
        raise CollectionPlanValidationError(
            f"{field} must match [+-]?[0-9]{{1,16}}"
        )
    return value


def build_effective_plan_snapshot(
    bundle: CollectionPlanBundle,
    *,
    resolved_destinations: Mapping[str, ResolvedDestination],
    page_size: int,
    endpoint_policy: EffectiveEndpointPolicy,
    transport_fingerprint: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Build the secret-free Stage 2 snapshot in memory without writing it."""

    plan = bundle.collection_plan
    if not plan.enabled:
        raise CollectionPlanValidationError(
            "cannot build an effective snapshot for a disabled collection plan"
        )
    _validate_bundle_references(bundle)

    expected_regions = set(plan.region_set)
    provided_regions = set(resolved_destinations)
    if provided_regions != expected_regions:
        missing = sorted(expected_regions - provided_regions)
        extra = sorted(provided_regions - expected_regions)
        raise CollectionPlanValidationError(
            f"resolved destination set mismatch; missing={missing} extra={extra}"
        )

    page_size = _require_int(page_size, field="effective_plan.page_size", minimum=1)
    if page_size != 100 or plan.depth % page_size != 0:
        raise CollectionPlanValidationError(
            "effective plan page_size must be 100 and divide depth exactly"
        )
    endpoint_ids = _require_unique_ids(
        list(endpoint_policy.endpoint_ids),
        field="effective_plan.endpoint_policy.endpoint_ids",
    )
    if endpoint_policy.selection_mode != "ordered_fallbacks":
        raise CollectionPlanValidationError(
            "effective endpoint selection_mode must be ordered_fallbacks"
        )
    pinned_endpoint_id = _require_id(
        endpoint_policy.pinned_endpoint_id,
        field="effective_plan.endpoint_policy.pinned_endpoint_id",
    )
    if pinned_endpoint_id not in endpoint_ids:
        raise CollectionPlanValidationError(
            "effective endpoint pinned_endpoint_id must be present in endpoint_ids"
        )

    query_by_id = {query.query_id: query for query in bundle.query_pack.queries}
    region_by_id = {
        region.region_id: region for region in bundle.region_registry.regions
    }
    regions: list[dict[str, Any]] = []
    for region_id in plan.region_set:
        resolution = resolved_destinations[region_id]
        if resolution.region_id != region_id:
            raise CollectionPlanValidationError(
                f"resolved destination key does not match region_id: {region_id}"
            )
        if resolution.dest_resolution_source != "wb_geo_xinfo":
            raise CollectionPlanValidationError(
                f"resolved destination source must be wb_geo_xinfo: {region_id}"
            )
        if resolution.dest_resolution_status != "resolved_not_sent":
            raise CollectionPlanValidationError(
                f"effective snapshot destination status must be resolved_not_sent: {region_id}"
            )
        regions.append(
            {
                "region_id": region_id,
                "region_name": region_by_id[region_id].region_name,
                "resolver": region_by_id[region_id].resolver,
                "latitude": region_by_id[region_id].latitude,
                "longitude": region_by_id[region_id].longitude,
                "address_label": region_by_id[region_id].address_label,
                "dest_id_observed": _require_wb_dest_id(
                    resolution.dest_id_observed,
                    field=f"resolved_destinations.{region_id}.dest_id_observed",
                ),
                "dest_resolved_at_utc": _validate_utc_timestamp(
                    resolution.dest_resolved_at_utc,
                    field=f"resolved_destinations.{region_id}.dest_resolved_at_utc",
                ),
                "dest_resolution_source": resolution.dest_resolution_source,
                "dest_resolution_status": resolution.dest_resolution_status,
            }
        )

    snapshot: dict[str, Any] = {
        "schema_version": (
            BOUNDED_RESUMABLE_EFFECTIVE_PLAN_SCHEMA_VERSION
            if transport_fingerprint is not None and plan.runtime_window is not None
            else (
                RESUMABLE_EFFECTIVE_PLAN_SCHEMA_VERSION
                if transport_fingerprint is not None
                else EFFECTIVE_PLAN_SCHEMA_VERSION
            )
        ),
        "collection_plan_id": plan.collection_plan_id,
        "query_pack_id": bundle.query_pack.query_pack_id,
        "query_pack_version": bundle.query_pack.version,
        "query_pack_sha256": bundle.query_pack_sha256,
        "collection_plan_sha256": bundle.collection_plan_sha256,
        "region_registry_sha256": bundle.region_registry_sha256,
        "queries": [
            {
                "query_id": query_by_id[query_id].query_id,
                "category_id": query_by_id[query_id].category_id,
                "text": query_by_id[query_id].text,
            }
            for query_id in plan.query_ids
        ],
        "regions": regions,
        "depth": plan.depth,
        "page_size": page_size,
        "endpoint_policy": {
            "selection_mode": endpoint_policy.selection_mode,
            "endpoint_ids": list(endpoint_ids),
            "pinned_endpoint_id": pinned_endpoint_id,
        },
        "schedule_id": plan.schedule_id,
        "publication_mode": plan.publication_mode,
        "sellers_mode": plan.sellers_mode,
        "proxy_rotation_mode": plan.proxy_rotation_mode,
        "quality": {
            "expected_queries_per_region": plan.quality.expected_queries_per_region,
            "expected_pages_per_query": plan.quality.expected_pages_per_query,
            "max_page_errors": plan.quality.max_page_errors,
            "require_constant_egress": plan.quality.require_constant_egress,
            "require_distinct_destinations": plan.quality.require_distinct_destinations,
        },
    }
    if plan.runtime_window is not None:
        snapshot["runtime_window"] = {
            "mode": plan.runtime_window.mode,
            "scheduled_start_msk": plan.runtime_window.scheduled_start_msk,
            "new_run_start_grace_seconds": plan.runtime_window.new_run_start_grace_seconds,
            "max_invocation_runtime_seconds": plan.runtime_window.max_invocation_runtime_seconds,
            "absolute_cutoff_msk": plan.runtime_window.absolute_cutoff_msk,
            "minimum_resume_window_seconds": plan.runtime_window.minimum_resume_window_seconds,
            "finalization_reserve_seconds": plan.runtime_window.finalization_reserve_seconds,
        }
    if transport_fingerprint is not None:
        snapshot["transport_fingerprint"] = dict(transport_fingerprint)
    _validate_effective_plan_snapshot(snapshot)
    return snapshot


def _validate_effective_plan_snapshot(snapshot: Mapping[str, Any]) -> None:
    schema_version = snapshot.get("schema_version")
    if schema_version not in {
        EFFECTIVE_PLAN_SCHEMA_VERSION,
        RESUMABLE_EFFECTIVE_PLAN_SCHEMA_VERSION,
        BOUNDED_RESUMABLE_EFFECTIVE_PLAN_SCHEMA_VERSION,
    }:
        raise CollectionPlanValidationError(
            "effective_plan.schema_version is unsupported"
        )
    required = {
        "schema_version",
        "collection_plan_id",
        "query_pack_id",
        "query_pack_version",
        "query_pack_sha256",
        "collection_plan_sha256",
        "region_registry_sha256",
        "queries",
        "regions",
        "depth",
        "page_size",
        "endpoint_policy",
        "schedule_id",
        "publication_mode",
        "sellers_mode",
        "proxy_rotation_mode",
        "quality",
    }
    if schema_version in {
        RESUMABLE_EFFECTIVE_PLAN_SCHEMA_VERSION,
        BOUNDED_RESUMABLE_EFFECTIVE_PLAN_SCHEMA_VERSION,
    }:
        required.add("transport_fingerprint")
    if schema_version == BOUNDED_RESUMABLE_EFFECTIVE_PLAN_SCHEMA_VERSION:
        required.add("runtime_window")
    _require_keys(
        snapshot,
        required=required,
        field="effective_plan",
    )
    if schema_version == BOUNDED_RESUMABLE_EFFECTIVE_PLAN_SCHEMA_VERSION:
        _load_runtime_window(snapshot["runtime_window"])
    if schema_version in {
        RESUMABLE_EFFECTIVE_PLAN_SCHEMA_VERSION,
        BOUNDED_RESUMABLE_EFFECTIVE_PLAN_SCHEMA_VERSION,
    }:
        fingerprint = _require_object(
            snapshot["transport_fingerprint"],
            field="effective_plan.transport_fingerprint",
        )
        _require_keys(
            fingerprint,
            required={
                "schema_version",
                "ordered_endpoint_urls_sha256",
                "request_params_sha256",
                "proxy_route_sha256",
                "fingerprint_sha256",
            },
            field="effective_plan.transport_fingerprint",
        )
        if fingerprint["schema_version"] != "wb_transport_fingerprint_v1":
            raise CollectionPlanValidationError(
                "effective_plan.transport_fingerprint schema is unsupported"
            )
        for name in (
            "ordered_endpoint_urls_sha256",
            "request_params_sha256",
            "proxy_route_sha256",
            "fingerprint_sha256",
        ):
            _require_sha256(
                fingerprint[name],
                field=f"effective_plan.transport_fingerprint.{name}",
            )
    _require_id(snapshot["collection_plan_id"], field="effective_plan.collection_plan_id")
    _require_id(snapshot["query_pack_id"], field="effective_plan.query_pack_id")
    _require_version(snapshot["query_pack_version"], field="effective_plan.query_pack_version")
    for name in (
        "query_pack_sha256",
        "collection_plan_sha256",
        "region_registry_sha256",
    ):
        _require_sha256(snapshot[name], field=f"effective_plan.{name}")

    query_items = _require_list(snapshot["queries"], field="effective_plan.queries")
    query_ids: set[str] = set()
    for index, raw_query in enumerate(query_items):
        field = f"effective_plan.queries[{index}]"
        query = _require_object(raw_query, field=field)
        _require_keys(query, required={"query_id", "category_id", "text"}, field=field)
        query_id = _require_id(query["query_id"], field=f"{field}.query_id")
        if query_id in query_ids:
            raise CollectionPlanValidationError(f"{field}.query_id is duplicated")
        query_ids.add(query_id)
        _require_id(query["category_id"], field=f"{field}.category_id")
        text = _require_string(query["text"], field=f"{field}.text")
        if text != normalize_query_text(text):
            raise CollectionPlanValidationError(f"{field}.text must already be normalized")

    region_items = _require_list(snapshot["regions"], field="effective_plan.regions")
    region_ids: set[str] = set()
    destination_ids: list[str] = []
    for index, raw_region in enumerate(region_items):
        field = f"effective_plan.regions[{index}]"
        region = _require_object(raw_region, field=field)
        _require_keys(
            region,
            required={
                "region_id",
                "region_name",
                "resolver",
                "latitude",
                "longitude",
                "address_label",
                "dest_id_observed",
                "dest_resolved_at_utc",
                "dest_resolution_source",
                "dest_resolution_status",
            },
            field=field,
        )
        region_id = _require_id(region["region_id"], field=f"{field}.region_id")
        if region_id in region_ids:
            raise CollectionPlanValidationError(f"{field}.region_id is duplicated")
        region_ids.add(region_id)
        _require_string(region["region_name"], field=f"{field}.region_name")
        if _require_string(region["resolver"], field=f"{field}.resolver") != "wb_geo_xinfo":
            raise CollectionPlanValidationError(f"{field}.resolver must be wb_geo_xinfo")
        _require_decimal_coordinate(
            region["latitude"],
            field=f"{field}.latitude",
            minimum=Decimal("-90"),
            maximum=Decimal("90"),
        )
        _require_decimal_coordinate(
            region["longitude"],
            field=f"{field}.longitude",
            minimum=Decimal("-180"),
            maximum=Decimal("180"),
        )
        _require_string(region["address_label"], field=f"{field}.address_label")
        destination_ids.append(
            _require_wb_dest_id(
                region["dest_id_observed"],
                field=f"{field}.dest_id_observed",
            )
        )
        _validate_utc_timestamp(
            region["dest_resolved_at_utc"],
            field=f"{field}.dest_resolved_at_utc",
        )
        if (
            _require_string(
                region["dest_resolution_source"],
                field=f"{field}.dest_resolution_source",
            )
            != "wb_geo_xinfo"
        ):
            raise CollectionPlanValidationError(
                f"{field}.dest_resolution_source must be wb_geo_xinfo"
            )
        if (
            _require_string(
                region["dest_resolution_status"],
                field=f"{field}.dest_resolution_status",
            )
            != "resolved_not_sent"
        ):
            raise CollectionPlanValidationError(
                f"{field}.dest_resolution_status must be resolved_not_sent"
            )

    depth = _require_int(snapshot["depth"], field="effective_plan.depth", minimum=1)
    if depth not in SUPPORTED_DEPTHS:
        raise CollectionPlanValidationError(
            f"effective_plan.depth must be one of {sorted(SUPPORTED_DEPTHS)}"
        )
    page_size = _require_int(
        snapshot["page_size"],
        field="effective_plan.page_size",
        minimum=1,
    )
    if page_size != 100 or depth % page_size != 0:
        raise CollectionPlanValidationError(
            "effective_plan.page_size must be 100 and divide depth exactly"
        )

    endpoint_policy = _require_object(
        snapshot["endpoint_policy"],
        field="effective_plan.endpoint_policy",
    )
    _require_keys(
        endpoint_policy,
        required={"selection_mode", "endpoint_ids", "pinned_endpoint_id"},
        field="effective_plan.endpoint_policy",
    )
    if (
        _require_string(
            endpoint_policy["selection_mode"],
            field="effective_plan.endpoint_policy.selection_mode",
        )
        != "ordered_fallbacks"
    ):
        raise CollectionPlanValidationError(
            "effective_plan.endpoint_policy.selection_mode must be ordered_fallbacks"
        )
    endpoint_ids = _require_unique_ids(
        _require_list(
            endpoint_policy["endpoint_ids"],
            field="effective_plan.endpoint_policy.endpoint_ids",
        ),
        field="effective_plan.endpoint_policy.endpoint_ids",
    )
    pinned_endpoint_id = _require_id(
        endpoint_policy["pinned_endpoint_id"],
        field="effective_plan.endpoint_policy.pinned_endpoint_id",
    )
    if pinned_endpoint_id not in endpoint_ids:
        raise CollectionPlanValidationError(
            "effective_plan.endpoint_policy.pinned_endpoint_id must be present in endpoint_ids"
        )

    _require_id(snapshot["schedule_id"], field="effective_plan.schedule_id")
    if snapshot["publication_mode"] != "none":
        raise CollectionPlanValidationError("effective_plan.publication_mode must be none")
    if snapshot["sellers_mode"] != "disabled":
        raise CollectionPlanValidationError("effective_plan.sellers_mode must be disabled")
    if snapshot["proxy_rotation_mode"] != "disabled":
        raise CollectionPlanValidationError(
            "effective_plan.proxy_rotation_mode must be disabled"
        )
    quality = _load_quality(snapshot["quality"])
    if quality.expected_queries_per_region != len(query_items):
        raise CollectionPlanValidationError(
            "effective_plan.quality.expected_queries_per_region must match queries count"
        )
    if quality.expected_pages_per_query != depth // page_size:
        raise CollectionPlanValidationError(
            "effective_plan.quality.expected_pages_per_query must match depth/page_size"
        )
    if quality.max_page_errors != 0:
        raise CollectionPlanValidationError(
            "effective_plan.quality.max_page_errors must be 0"
        )
    if not quality.require_constant_egress or not quality.require_distinct_destinations:
        raise CollectionPlanValidationError(
            "effective_plan quality must require constant egress and distinct destinations"
        )
    if len(set(destination_ids)) != len(destination_ids):
        raise CollectionPlanValidationError(
            "effective_plan requires distinct dest_id_observed values"
        )


def canonical_effective_plan_bytes(snapshot: Mapping[str, Any]) -> bytes:
    _validate_effective_plan_snapshot(snapshot)
    return json.dumps(
        snapshot,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")


def canonical_effective_plan_sha256(snapshot: Mapping[str, Any]) -> str:
    return hashlib.sha256(canonical_effective_plan_bytes(snapshot)).hexdigest()
