#!/usr/bin/env python3
from __future__ import annotations

import argparse
import base64
import binascii
import hashlib
import json
import os
import re
import stat
import sys
import uuid
from collections import OrderedDict
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path
from typing import Any, Callable
from urllib.parse import quote, urlsplit
from zoneinfo import ZoneInfo

import requests
import yaml

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.common.proxy_required import (
    build_requests_session,
    proxy_route_from_url,
    require_marketplace_proxy,
)
from app.common.durable_atomic import durable_atomic_replace
from app.common.nightly_attestation import integrity_gate as attestation_integrity_gate


EXIT_OK = 0
EXIT_SMOKE_FAILED = 20
EXIT_REFRESH_FAILED = 21
COORDINATOR_LOCK_DIRECTORY = Path("/run/lock/parser-nightly-coordinator")

OK_KINDS = {"top_products", "nested_products", "nested_promo_products"}
AUTHORIZATION_POLICIES = {"if_present", "optional", "required"}
DEFAULT_AUTHORIZATION_HORIZON_PLAN = (
    "config/wb/collection_plans/shevron-four-regions-top1000-v2.json"
)
MAX_PINNED_SECRET_BYTES = 1024 * 1024
MSK = ZoneInfo("Europe/Moscow")
BROWSER_PROFILE_RELATIVE_PATH = Path("state/browser/wb_cookie_renewal_profile")
BROWSER_COOLDOWN_RELATIVE_PATH = Path(
    "state/wb_session_keeper/browser_refresh_cooldown.json"
)
BROWSER_COOKIE_CANDIDATE_RELATIVE_DIR = Path("state/wb_cookie_candidates")
BROWSER_COOKIE_BACKUP_RELATIVE_DIR = Path("state/wb_known_good")
BROWSER_REFRESH_COOLDOWN_SECONDS = 1800
BROWSER_TIMEOUT_MIN_MS = 1000
BROWSER_TIMEOUT_MAX_MS = 60000
BROWSER_SETTLE_MAX_MS = 15000
BROWSER_RATE_LIMIT_STATUSES = {429, 498}
BROWSER_COOLDOWN_SCHEMA = "wb_browser_refresh_cooldown_v1"


class AccessContractError(RuntimeError):
    def __init__(self, code: str, *, evidence: dict[str, Any] | None = None) -> None:
        self.code = code
        self.evidence = dict(evidence or {})
        super().__init__(code)


class PinnedFile:
    def __init__(
        self,
        *,
        path: Path,
        fd: int,
        payload: bytes,
        relative_path: str,
        exact_mode: int,
    ) -> None:
        self.path = path
        self.fd = fd
        self.payload = payload
        self.relative_path = relative_path
        self.exact_mode = exact_mode
        self.sha256 = hashlib.sha256(payload).hexdigest()
        self._identity = self._stat_identity(os.fstat(fd))

    @staticmethod
    def _stat_identity(info: os.stat_result) -> tuple[int, int, int, int, int, int]:
        return (
            info.st_dev,
            info.st_ino,
            info.st_uid,
            stat.S_IMODE(info.st_mode),
            info.st_nlink,
            info.st_size,
        )

    def verify(self) -> None:
        try:
            before = os.fstat(self.fd)
            payload = os.pread(self.fd, len(self.payload) + 1, 0)
            after = os.fstat(self.fd)
            current = os.lstat(self.path)
        except OSError as exc:
            raise AccessContractError("access_source_changed") from exc
        if (
            self._stat_identity(before) != self._identity
            or self._stat_identity(after) != self._identity
            or self._stat_identity(current) != self._identity
            or payload != self.payload
            or hashlib.sha256(payload).hexdigest() != self.sha256
        ):
            raise AccessContractError("access_source_changed")

    def close(self) -> None:
        if self.fd >= 0:
            os.close(self.fd)
            self.fd = -1


class RequestHeadersSource:
    def __init__(self, *, pinned: PinnedFile, headers: dict[str, str], source_kind: str) -> None:
        self.pinned = pinned
        self.headers = headers
        self.source_kind = source_kind

    def evidence(self) -> dict[str, Any]:
        return {
            "source": self.source_kind,
            "sha256": self.pinned.sha256,
            "headers_count": len(self.headers),
        }

    def verify(self) -> None:
        self.pinned.verify()

    def close(self) -> None:
        self.pinned.close()


def _require_host_lease_after_cutover() -> Any | None:
    if not os.path.lexists(COORDINATOR_LOCK_DIRECTORY):
        return None
    from app.common.nightly_coordinator import (
        require_official_live_entry_lease,
    )

    return require_official_live_entry_lease(environment=os.environ)


def _publication_integrity_gate(args: argparse.Namespace | None = None) -> Callable[[], None]:
    explicit = getattr(args, "_publication_integrity_gate", None) if args is not None else None
    if explicit is not None:
        return explicit
    return attestation_integrity_gate(PROJECT_ROOT, os.environ)


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def resolve_path(value: str | Path, *, root: Path | None = None) -> Path:
    effective_root = root or PROJECT_ROOT
    path = Path(value)
    if not path.is_absolute():
        path = effective_root / path
    return path


def load_config(config_path: Path, *, inject_request_headers: bool = True) -> dict[str, Any]:
    with config_path.open("r", encoding="utf-8-sig") as f:
        data = yaml.safe_load(f) or {}
    if not isinstance(data, dict):
        raise RuntimeError("config is not a YAML object")
    if inject_request_headers:
        inject_runtime_request_headers(data, config_path.parent.parent)
    return data


def resolve_cookie_path(config: dict[str, Any], explicit: str = "") -> Path:
    if explicit:
        return resolve_path(explicit)

    serp = config.get("serp") if isinstance(config.get("serp"), dict) else {}
    env_name = str(serp.get("wb_cookie_file_env") or "WB_COOKIE_FILE").strip()
    env_value = os.getenv(env_name, "").strip()
    if env_value:
        return resolve_path(env_value)

    cookie_file = str(serp.get("wb_cookie_file") or "").strip()
    if not cookie_file:
        raise RuntimeError(f"cookie file is not configured; set {env_name} or serp.wb_cookie_file")
    return resolve_path(cookie_file)


def resolve_serp_base_urls(config: dict[str, Any]) -> list[str]:
    serp = config.get("serp") if isinstance(config.get("serp"), dict) else {}
    urls: list[str] = []

    def add_url(value: Any) -> None:
        url = str(value or "").strip()
        if url and url not in urls:
            urls.append(url)

    add_url(serp.get("base_url"))
    fallback_urls = serp.get("fallback_base_urls")
    if isinstance(fallback_urls, list):
        for url in fallback_urls:
            add_url(url)

    if not urls:
        raise RuntimeError("serp.base_url is not configured")
    return urls


def resolve_proxy_url(config: dict[str, Any]) -> str:
    return require_marketplace_proxy(config).url


def requests_proxies(proxy_url: str) -> dict[str, str]:
    return proxy_route_from_url(proxy_url).requests_proxies


def _coerce_request_headers(raw_headers: Any) -> dict[str, str]:
    if not isinstance(raw_headers, dict):
        return {}
    headers: dict[str, str] = {}
    for name, value in raw_headers.items():
        header_name = str(name or "").strip()
        if not header_name or value is None:
            continue
        if header_name.lower() == "cookie":
            continue
        headers[header_name] = str(value)
    return headers


def inject_runtime_request_headers(config: dict[str, Any], project_root: Path = PROJECT_ROOT) -> None:
    serp = config.get("serp") if isinstance(config.get("serp"), dict) else {}
    env_name = str(serp.get("request_headers_file_env") or "PARSER_WB_REQUEST_HEADERS_FILE").strip()
    headers_file = os.getenv(env_name, "").strip() if env_name else ""
    headers_file = headers_file or str(serp.get("request_headers_file") or "").strip()
    if not headers_file:
        return

    path = resolve_path(headers_file, root=project_root)
    if not path.exists():
        raise RuntimeError(f"request headers file not found: {path}")
    payload = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    if isinstance(payload, dict) and isinstance(payload.get("headers"), dict):
        payload = payload["headers"]
    headers = _coerce_request_headers(serp.get("request_headers"))
    headers.update(_coerce_request_headers(payload))
    serp["request_headers"] = headers


def request_headers_from_config(config: dict[str, Any]) -> dict[str, str]:
    serp = config.get("serp") if isinstance(config.get("serp"), dict) else {}
    return _coerce_request_headers(serp.get("request_headers"))


def _project_lexical_path(value: str | Path, *, project_root: Path) -> Path:
    path = Path(value)
    if ".." in path.parts:
        raise AccessContractError("access_source_path_traversal")
    if not path.is_absolute():
        path = project_root / path
    lexical = Path(os.path.abspath(os.fspath(path)))
    try:
        lexical.relative_to(project_root)
    except ValueError as exc:
        raise AccessContractError("access_source_outside_project") from exc
    return lexical


def _reject_symlink_components(project_root: Path, path: Path) -> None:
    relative = path.relative_to(project_root)
    current = project_root
    for part in relative.parts:
        current = current / part
        try:
            info = os.lstat(current)
        except OSError as exc:
            raise AccessContractError("access_source_unavailable") from exc
        if stat.S_ISLNK(info.st_mode):
            raise AccessContractError("access_source_symlink")
        if current != path and not stat.S_ISDIR(info.st_mode):
            raise AccessContractError("access_source_parent_unsafe")


def _open_pinned_file(
    value: str | Path,
    *,
    project_root: Path,
    allowed_root: Path,
    exact_mode: int,
) -> PinnedFile:
    root = Path(os.path.abspath(os.fspath(project_root)))
    path = _project_lexical_path(value, project_root=root)
    allowed = Path(os.path.abspath(os.fspath(allowed_root)))
    try:
        path.relative_to(allowed)
    except ValueError as exc:
        raise AccessContractError("access_source_scope_invalid") from exc
    _reject_symlink_components(root, path)

    flags = os.O_RDONLY | os.O_CLOEXEC
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    try:
        fd = os.open(path, flags)
    except OSError as exc:
        raise AccessContractError("access_source_unavailable") from exc
    try:
        info = os.fstat(fd)
        if (
            not stat.S_ISREG(info.st_mode)
            or info.st_uid != os.geteuid()
            or stat.S_IMODE(info.st_mode) != exact_mode
            or info.st_nlink != 1
            or info.st_size <= 0
            or info.st_size > MAX_PINNED_SECRET_BYTES
        ):
            raise AccessContractError("access_source_metadata_unsafe")
        payload = os.pread(fd, info.st_size + 1, 0)
        if len(payload) != info.st_size:
            raise AccessContractError("access_source_changed")
        pinned = PinnedFile(
            path=path,
            fd=fd,
            payload=payload,
            relative_path=path.relative_to(root).as_posix(),
            exact_mode=exact_mode,
        )
        pinned.verify()
        return pinned
    except Exception:
        os.close(fd)
        raise


def _json_object_without_duplicates(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise AccessContractError("request_headers_json_duplicate_key")
        result[key] = value
    return result


def _parse_request_headers(payload: bytes) -> dict[str, str]:
    try:
        decoded = payload.decode("utf-8")
        document = json.loads(decoded, object_pairs_hook=_json_object_without_duplicates)
    except AccessContractError:
        raise
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise AccessContractError("request_headers_json_invalid") from exc
    if isinstance(document, dict) and isinstance(document.get("headers"), dict):
        document = document["headers"]
    if not isinstance(document, dict) or not document or len(document) > 128:
        raise AccessContractError("request_headers_schema_invalid")

    headers: dict[str, str] = {}
    normalized_names: set[str] = set()
    for name, value in document.items():
        if not isinstance(name, str) or not isinstance(value, str):
            raise AccessContractError("request_headers_schema_invalid")
        if not re.fullmatch(r"[!#$%&'*+.^_`|~0-9A-Za-z-]+", name):
            raise AccessContractError("request_headers_name_invalid")
        normalized = name.lower()
        if normalized in normalized_names or normalized == "cookie":
            raise AccessContractError("request_headers_name_invalid")
        if not value or any(ord(character) < 32 or ord(character) == 127 for character in value):
            raise AccessContractError("request_headers_value_invalid")
        normalized_names.add(normalized)
        headers[name] = value
    return headers


def _configured_request_headers_path(config: dict[str, Any]) -> str:
    serp = config.get("serp") if isinstance(config.get("serp"), dict) else {}
    env_name = str(serp.get("request_headers_file_env") or "PARSER_WB_REQUEST_HEADERS_FILE").strip()
    env_value = os.getenv(env_name, "").strip() if env_name else ""
    return env_value or str(serp.get("request_headers_file") or "").strip()


def load_request_headers_source(
    config: dict[str, Any],
    args: argparse.Namespace,
    *,
    project_root: Path | None = None,
) -> RequestHeadersSource | None:
    root = Path(project_root or PROJECT_ROOT)
    explicit = str(getattr(args, "request_headers_file", "") or "").strip()
    policy = str(getattr(args, "authorization_policy", "optional") or "optional").strip()
    if policy not in AUTHORIZATION_POLICIES:
        raise AccessContractError("authorization_policy_invalid")
    if explicit:
        if policy not in {"if_present", "required"}:
            raise AccessContractError("candidate_headers_require_authorization")
        source_value = explicit
        allowed_root = root / "state/wb_header_candidates"
        source_kind = "candidate"
    elif policy in {"if_present", "required"}:
        source_value = _configured_request_headers_path(config)
        if not source_value:
            raise AccessContractError("request_headers_source_missing")
        path = _project_lexical_path(source_value, project_root=root)
        expected = root / "config/wb_request_headers.json"
        if path != expected:
            raise AccessContractError("request_headers_source_not_approved")
        allowed_root = root / "config"
        source_kind = "production"
    else:
        return None

    pinned = _open_pinned_file(
        source_value,
        project_root=root,
        allowed_root=allowed_root,
        exact_mode=0o600,
    )
    try:
        return RequestHeadersSource(
            pinned=pinned,
            headers=_parse_request_headers(pinned.payload),
            source_kind=source_kind,
        )
    except Exception:
        pinned.close()
        raise


def _authorization_header(headers: dict[str, str]) -> str:
    matches = [value for name, value in headers.items() if name.lower() == "authorization"]
    if len(matches) > 1:
        raise AccessContractError("authorization_header_ambiguous")
    return matches[0] if matches else ""


def _decode_jwt_payload(token: str) -> dict[str, Any]:
    segments = token.split(".")
    if len(segments) != 3 or any(not segment for segment in segments):
        raise AccessContractError("authorization_not_jwt")
    encoded = segments[1]
    padding = "=" * (-len(encoded) % 4)
    try:
        payload = base64.b64decode(
            (encoded + padding).encode("ascii"),
            altchars=b"-_",
            validate=True,
        )
        if len(payload) > 16 * 1024:
            raise AccessContractError("authorization_claims_invalid")
        document = json.loads(
            payload.decode("utf-8"),
            object_pairs_hook=_json_object_without_duplicates,
        )
    except AccessContractError as exc:
        raise AccessContractError("authorization_claims_invalid") from exc
    except (UnicodeEncodeError, UnicodeDecodeError, binascii.Error, json.JSONDecodeError) as exc:
        raise AccessContractError("authorization_claims_invalid") from exc
    if not isinstance(document, dict):
        raise AccessContractError("authorization_claims_invalid")
    return document


def _jwt_timestamp(claims: dict[str, Any], name: str) -> int:
    value = claims.get(name)
    if isinstance(value, bool) or not isinstance(value, int) or value < 0 or value > 253402300799:
        raise AccessContractError(f"authorization_{name}_invalid")
    return value


def _timestamp_iso(value: int) -> str:
    return datetime.fromtimestamp(value, tz=timezone.utc).replace(microsecond=0).isoformat()


def authorization_horizon_from_plan(
    plan_file: str,
    *,
    now_utc: datetime,
    project_root: Path | None = None,
) -> tuple[datetime, dict[str, Any]]:
    from app.serp.collection_plan import load_collection_plan

    root = Path(project_root or PROJECT_ROOT)
    if now_utc.tzinfo is None or now_utc.utcoffset() is None:
        raise AccessContractError("authorization_clock_invalid")
    pinned = _open_pinned_file(
        plan_file,
        project_root=root,
        allowed_root=root / "config/wb/collection_plans",
        exact_mode=0o644,
    )
    try:
        plan = load_collection_plan(pinned.path)
        pinned.verify()
        if plan.source_sha256 != pinned.sha256 or not plan.enabled or plan.runtime_window is None:
            raise AccessContractError("authorization_horizon_plan_invalid")
        window = plan.runtime_window
        scheduled_hour, scheduled_minute = (int(part) for part in window.scheduled_start_msk.split(":", 1))
        cutoff_hour, cutoff_minute = (int(part) for part in window.absolute_cutoff_msk.split(":", 1))
        local_now = now_utc.astimezone(MSK)
        scheduled_today = datetime.combine(
            local_now.date(),
            time(scheduled_hour, scheduled_minute),
            tzinfo=MSK,
        )
        cutoff_today = datetime.combine(
            local_now.date(),
            time(cutoff_hour, cutoff_minute),
            tzinfo=MSK,
        )
        horizon_date: date = local_now.date()
        if local_now >= cutoff_today:
            horizon_date += timedelta(days=1)
        elif local_now >= scheduled_today:
            horizon_date = local_now.date()
        horizon = datetime.combine(
            horizon_date,
            time(cutoff_hour, cutoff_minute),
            tzinfo=MSK,
        ).astimezone(timezone.utc)
        return horizon, {
            "horizon_source": "collection_plan_runtime_window",
            "plan_sha256": pinned.sha256,
            "required_until_utc": horizon.replace(microsecond=0).isoformat(),
        }
    finally:
        pinned.close()


def validate_authorization_temporal_contract(
    headers: dict[str, str],
    *,
    policy: str,
    now_utc: datetime,
    required_until_utc: datetime | None,
    source_evidence: dict[str, Any] | None = None,
) -> dict[str, Any]:
    if policy not in AUTHORIZATION_POLICIES:
        raise AccessContractError("authorization_policy_invalid")
    evidence: dict[str, Any] = {
        "policy": policy,
        "status": "invalid",
        "source": dict(source_evidence or {}),
    }
    authorization = _authorization_header(headers)
    if policy == "optional":
        evidence["status"] = "present_not_validated_optional" if authorization else "not_present_optional"
        return evidence
    if not authorization:
        if policy == "if_present":
            evidence["status"] = "not_present_allowed"
            return evidence
        raise AccessContractError("authorization_missing", evidence=evidence)
    scheme, separator, token = authorization.partition(" ")
    if scheme.lower() != "bearer" or separator != " " or not token or token != token.strip():
        raise AccessContractError("authorization_bearer_invalid", evidence=evidence)
    try:
        claims = _decode_jwt_payload(token)
        iat = _jwt_timestamp(claims, "iat")
        nbf = _jwt_timestamp(claims, "nbf")
        exp = _jwt_timestamp(claims, "exp")
    except AccessContractError as exc:
        raise AccessContractError(exc.code, evidence=evidence) from exc
    if iat >= exp or nbf >= exp:
        raise AccessContractError("authorization_claim_order_invalid", evidence=evidence)

    now = now_utc.astimezone(timezone.utc).replace(microsecond=0)
    now_timestamp = int(now.timestamp())
    evidence.update(
        {
            "iat_utc": _timestamp_iso(iat),
            "nbf_utc": _timestamp_iso(nbf),
            "exp_utc": _timestamp_iso(exp),
            "ttl_seconds": exp - now_timestamp,
            "required_until_utc": (
                required_until_utc.astimezone(timezone.utc).replace(microsecond=0).isoformat()
                if required_until_utc is not None
                else None
            ),
        }
    )
    if now_timestamp < nbf:
        raise AccessContractError("authorization_not_yet_valid", evidence=evidence)
    if now_timestamp < iat:
        raise AccessContractError("authorization_iat_in_future", evidence=evidence)
    if now_timestamp >= exp:
        raise AccessContractError("authorization_expired", evidence=evidence)
    if required_until_utc is not None:
        required_timestamp = int(required_until_utc.astimezone(timezone.utc).timestamp())
        if exp <= required_timestamp:
            raise AccessContractError("authorization_horizon_not_covered", evidence=evidence)
    evidence["status"] = "valid"
    return evidence


def authorization_contract_for_smoke(
    headers: dict[str, str],
    source: RequestHeadersSource | None,
    args: argparse.Namespace,
    *,
    now_utc: datetime | None = None,
    project_root: Path | None = None,
) -> dict[str, Any]:
    policy = str(getattr(args, "authorization_policy", "optional") or "optional").strip()
    required_until: datetime | None = None
    horizon_evidence: dict[str, Any] = {}
    now = (now_utc or datetime.now(timezone.utc)).astimezone(timezone.utc)
    if policy in {"if_present", "required"}:
        horizon_plan = str(
            getattr(args, "authorization_horizon_plan_file", "")
            or DEFAULT_AUTHORIZATION_HORIZON_PLAN
        ).strip()
        required_until, horizon_evidence = authorization_horizon_from_plan(
            horizon_plan,
            now_utc=now,
            project_root=project_root,
        )
    source_evidence = source.evidence() if source is not None else {"source": "inline_config"}
    source_evidence.update(horizon_evidence)
    return validate_authorization_temporal_contract(
        headers,
        policy=policy,
        now_utc=now,
        required_until_utc=required_until,
        source_evidence=source_evidence,
    )


def playwright_proxy_config(proxy_url: str) -> dict[str, str]:
    return proxy_route_from_url(proxy_url, browser=True).playwright_proxy()


def marketplace_get(
    config: dict[str, Any],
    url: str,
    **kwargs: Any,
) -> requests.Response:
    with build_requests_session(require_marketplace_proxy(config)) as session:
        return session.get(url, **kwargs)


def resolve_smoke_min_successes(config: dict[str, Any], args: argparse.Namespace, total_queries: int) -> int:
    explicit = int(getattr(args, "min_successes", 0) or 0)
    if explicit > 0:
        return min(explicit, total_queries)

    serp = config.get("serp") if isinstance(config.get("serp"), dict) else {}
    configured = serp.get("smoke_min_successes")
    if configured is not None:
        try:
            return min(max(1, int(configured)), total_queries)
        except (TypeError, ValueError):
            pass

    return total_queries


def read_cookie_value(cookie_path: Path) -> str:
    if not cookie_path.exists():
        raise RuntimeError(f"cookie file not found: {cookie_path}")
    value = cookie_path.read_text(encoding="utf-8").strip()
    if not value:
        raise RuntimeError(f"cookie file is empty: {cookie_path}")
    return value


def cookie_required(config: dict[str, Any]) -> bool:
    serp = config.get("serp") if isinstance(config.get("serp"), dict) else {}
    env_name = str(serp.get("cookie_required_env") or "PARSER_WB_COOKIE_REQUIRED").strip()
    if env_name:
        env_value = os.getenv(env_name, "").strip().lower()
        if env_value:
            return env_value not in {"0", "false", "no", "off"}
    if "cookie_required" in serp:
        return bool(serp.get("cookie_required"))
    return True


def read_cookie_value_for_smoke(config: dict[str, Any], args: argparse.Namespace, cookie_path: Path) -> str:
    if getattr(args, "without_cookie", False):
        return ""
    try:
        return read_cookie_value(cookie_path)
    except RuntimeError:
        if cookie_required(config):
            raise
        return ""


def load_candidate_cookie_source(
    args: argparse.Namespace,
    *,
    project_root: Path | None = None,
) -> PinnedFile | None:
    generated = bool(getattr(args, "_generated_cookie_candidate", False))
    if (
        not generated
        and not str(getattr(args, "request_headers_file", "") or "").strip()
    ):
        return None
    if getattr(args, "without_cookie", False):
        return None
    cookie_file = str(getattr(args, "cookie_file", "") or "").strip()
    if not cookie_file:
        raise AccessContractError("candidate_cookie_missing")
    root = Path(project_root or PROJECT_ROOT)
    return _open_pinned_file(
        cookie_file,
        project_root=root,
        allowed_root=root / "state/wb_cookie_candidates",
        exact_mode=0o600,
    )


def write_cookie_value(
    cookie_path: Path,
    cookie_value: str,
    *,
    integrity_gate: Callable[[], None] | None = None,
    require_absent: bool = False,
) -> None:
    payload = cookie_value.strip()
    if not payload or "\n" in payload or "\r" in payload:
        raise AccessContractError("cookie_candidate_invalid")
    cookie_path.parent.mkdir(parents=True, exist_ok=True)
    durable_atomic_replace(
        cookie_path,
        (payload + "\n").encode("utf-8"),
        mode=0o600,
        require_absent=require_absent,
        integrity_gate=integrity_gate or _publication_integrity_gate(),
    )


def load_queries(config: dict[str, Any], explicit_query: str, sample_count: int) -> list[str]:
    if explicit_query.strip():
        return [explicit_query.strip()]

    serp = config.get("serp") if isinstance(config.get("serp"), dict) else {}
    input_files = serp.get("input_files") if isinstance(serp.get("input_files"), dict) else {}
    queries_txt = str(input_files.get("queries_txt") or "exports/queries.txt").strip()
    query_path = resolve_path(queries_txt)
    if not query_path.exists():
        return ["шеврон"]

    queries: list[str] = []
    for line in query_path.read_text(encoding="utf-8-sig").splitlines():
        query = " ".join(line.strip().split())
        if query:
            queries.append(query)
        if len(queries) >= sample_count:
            break
    return queries or ["шеврон"]


def classify_payload(payload: Any) -> tuple[str, int, str]:
    if not isinstance(payload, dict):
        return "json_not_object", 0, ""

    products = payload.get("products")
    if isinstance(products, list) and products:
        first = products[0] if isinstance(products[0], dict) else {}
        sample = str(first.get("name", ""))[:120] if isinstance(first, dict) else ""
        return "top_products", len(products), sample

    nested_data = payload.get("data") if isinstance(payload.get("data"), dict) else {}
    nested_products = nested_data.get("products") if isinstance(nested_data, dict) else None
    if isinstance(nested_products, list) and nested_products:
        product_dicts = [p for p in nested_products if isinstance(p, dict)]
        promo_count = 0
        for product in product_dicts:
            log = product.get("log")
            if isinstance(log, dict) and log.get("promotion") == 1:
                promo_count += 1
        sample = str(product_dicts[0].get("name", ""))[:120] if product_dicts else ""
        if product_dicts and promo_count == len(product_dicts):
            return "nested_promo_products", len(product_dicts), sample
        return "nested_products", len(product_dicts), sample

    return "empty_or_unknown", 0, ",".join(list(payload.keys())[:8])


def write_state(path_value: str, data: dict[str, Any]) -> None:
    if not path_value:
        return
    path = resolve_path(path_value)
    path.parent.mkdir(parents=True, exist_ok=True)
    durable_atomic_replace(
        path,
        (json.dumps(data, ensure_ascii=False, indent=2) + "\n").encode("utf-8"),
        mode=0o600,
        integrity_gate=_publication_integrity_gate(),
    )


def _strict_utc_timestamp(value: Any) -> datetime:
    if not isinstance(value, str) or not value.endswith("+00:00"):
        raise AccessContractError("browser_refresh_cooldown_invalid")
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError as exc:
        raise AccessContractError("browser_refresh_cooldown_invalid") from exc
    if parsed.tzinfo is None or parsed.utcoffset() != timedelta(0):
        raise AccessContractError("browser_refresh_cooldown_invalid")
    return parsed.astimezone(timezone.utc)


def _ensure_private_runtime_directory(path: Path) -> None:
    root = Path(os.path.abspath(os.fspath(PROJECT_ROOT)))
    expected_roots = {
        root / BROWSER_PROFILE_RELATIVE_PATH,
        root / BROWSER_COOKIE_CANDIDATE_RELATIVE_DIR,
        root / BROWSER_COOKIE_BACKUP_RELATIVE_DIR,
        root / BROWSER_COOLDOWN_RELATIVE_PATH.parent,
    }
    path = Path(os.path.abspath(os.fspath(path)))
    if path not in expected_roots:
        raise AccessContractError("browser_runtime_path_invalid")
    relative = path.relative_to(root)
    current = root
    for part in relative.parts:
        current /= part
        try:
            info = os.lstat(current)
        except FileNotFoundError:
            os.mkdir(current, mode=0o700)
            info = os.lstat(current)
        except OSError as exc:
            raise AccessContractError("browser_runtime_path_unavailable") from exc
        if (
            not stat.S_ISDIR(info.st_mode)
            or stat.S_ISLNK(info.st_mode)
            or info.st_uid != os.geteuid()
            or info.st_mode & 0o002
        ):
            raise AccessContractError("browser_runtime_path_unsafe")
        if current == path and stat.S_IMODE(info.st_mode) != 0o700:
            try:
                os.chmod(current, 0o700, follow_symlinks=False)
            except OSError as exc:
                raise AccessContractError("browser_runtime_path_unsafe") from exc
            if stat.S_IMODE(os.lstat(current).st_mode) != 0o700:
                raise AccessContractError("browser_runtime_path_unsafe")


def browser_profile_path(args: argparse.Namespace) -> Path:
    configured = str(
        getattr(args, "browser_profile_dir", "")
        or BROWSER_PROFILE_RELATIVE_PATH.as_posix()
    )
    path = resolve_path(configured)
    expected = resolve_path(BROWSER_PROFILE_RELATIVE_PATH)
    if path != expected:
        raise AccessContractError("browser_profile_path_not_approved")
    _ensure_private_runtime_directory(path)
    return path


def browser_cooldown_path() -> Path:
    path = resolve_path(BROWSER_COOLDOWN_RELATIVE_PATH)
    _ensure_private_runtime_directory(path.parent)
    return path


def browser_refresh_cooldown_active(
    *,
    now_utc: datetime | None = None,
) -> dict[str, Any] | None:
    path = browser_cooldown_path()
    if not path.exists():
        return None
    pinned = _open_pinned_file(
        path,
        project_root=PROJECT_ROOT,
        allowed_root=path.parent,
        exact_mode=0o600,
    )
    try:
        try:
            payload = json.loads(pinned.payload.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise AccessContractError("browser_refresh_cooldown_invalid") from exc
        if (
            not isinstance(payload, dict)
            or payload.get("schema_version") != BROWSER_COOLDOWN_SCHEMA
            or payload.get("status") not in {"active", "cleared"}
            or payload.get("reason") not in {
                "browser_http_429",
                "browser_http_498",
                "browser_timeout",
                "candidate_api_rate_limited",
                "none",
            }
        ):
            raise AccessContractError("browser_refresh_cooldown_invalid")
        pinned.verify()
        if payload["status"] == "cleared":
            return None
        next_allowed = _strict_utc_timestamp(payload.get("next_allowed_at_utc"))
        now = (now_utc or datetime.now(timezone.utc)).astimezone(timezone.utc)
        return payload if now < next_allowed else None
    finally:
        pinned.close()


def record_browser_refresh_cooldown(
    reason: str,
    *,
    now_utc: datetime | None = None,
) -> dict[str, Any]:
    if reason not in {
        "browser_http_429",
        "browser_http_498",
        "browser_timeout",
        "candidate_api_rate_limited",
    }:
        raise AccessContractError("browser_refresh_cooldown_reason_invalid")
    now = (now_utc or datetime.now(timezone.utc)).astimezone(timezone.utc).replace(
        microsecond=0
    )
    payload = {
        "schema_version": BROWSER_COOLDOWN_SCHEMA,
        "status": "active",
        "reason": reason,
        "checked_at_utc": now.isoformat(),
        "next_allowed_at_utc": (
            now + timedelta(seconds=BROWSER_REFRESH_COOLDOWN_SECONDS)
        ).isoformat(),
        "cooldown_seconds": BROWSER_REFRESH_COOLDOWN_SECONDS,
    }
    write_state(str(browser_cooldown_path()), payload)
    return payload


def clear_browser_refresh_cooldown(*, now_utc: datetime | None = None) -> None:
    now = (now_utc or datetime.now(timezone.utc)).astimezone(timezone.utc).replace(
        microsecond=0
    )
    write_state(
        str(browser_cooldown_path()),
        {
            "schema_version": BROWSER_COOLDOWN_SCHEMA,
            "status": "cleared",
            "reason": "none",
            "checked_at_utc": now.isoformat(),
            "next_allowed_at_utc": None,
            "cooldown_seconds": BROWSER_REFRESH_COOLDOWN_SECONDS,
        },
    )


def smoke(config: dict[str, Any], args: argparse.Namespace, *, emit: bool = True) -> bool:
    source: RequestHeadersSource | None = None
    candidate_cookie: PinnedFile | None = None
    try:
        source = load_request_headers_source(config, args)
        candidate_cookie = load_candidate_cookie_source(args)
        serp = config.get("serp") if isinstance(config.get("serp"), dict) else {}
        cookie_path = resolve_cookie_path(config, args.cookie_file)
        if candidate_cookie is not None:
            candidate_cookie.verify()
            try:
                candidate_cookie_value = candidate_cookie.payload.decode("utf-8").strip()
            except UnicodeDecodeError as exc:
                raise AccessContractError("candidate_cookie_invalid") from exc
            if (
                not candidate_cookie_value
                or "\n" in candidate_cookie_value
                or "\r" in candidate_cookie_value
            ):
                raise AccessContractError("candidate_cookie_invalid")
            cookie_value = "" if bool(args.without_cookie) else candidate_cookie_value
        else:
            cookie_value = read_cookie_value_for_smoke(config, args, cookie_path)
        queries = load_queries(config, args.query, max(1, int(args.sample_count)))
        page = int(args.page)
        base_urls = resolve_serp_base_urls(config)
        min_successes = resolve_smoke_min_successes(config, args, len(queries))
        if (
            str(getattr(args, "request_headers_file", "") or "").strip()
            or bool(getattr(args, "_generated_cookie_candidate", False))
        ) and (
            str(getattr(args, "query", "") or "").strip()
            or int(args.sample_count) != 3
            or int(args.min_successes) != 3
            or len(queries) != 3
            or min_successes != 3
        ):
            raise AccessContractError("candidate_smoke_requires_exact_3_of_3")

        headers = {
            "user-agent": str(serp.get("user_agent") or "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"),
            "x-requested-with": str(serp.get("x_requested_with") or "XMLHttpRequest"),
            "accept": "application/json, text/plain, */*",
        }
        headers.update(source.headers if source is not None else request_headers_from_config(config))
        authorization_evidence = authorization_contract_for_smoke(headers, source, args)
        setattr(args, "_authorization_evidence", authorization_evidence)
        # Route validation remains before every marketplace request. Temporal
        # authorization validation intentionally happens first, while offline.
        require_marketplace_proxy(config)
        if cookie_value:
            headers["cookie"] = cookie_value
        referer_base = str(
            serp.get("referer_base")
            or "https://www.wildberries.ru/catalog/0/search.aspx?search="
        )
        request_params = serp.get("request_params") if isinstance(serp.get("request_params"), dict) else {}
        timeout = int(config.get("runtime", {}).get("http_timeout_seconds", 45))
        results: list[dict[str, Any]] = []
        successes = 0
        attempts = 0
        terminal_reason = ""
        for query in queries:
            params = dict(request_params)
            params["query"] = query
            params["page"] = str(page)
            req_headers = dict(headers)
            req_headers["referer"] = f"{referer_base}{quote(query)}"

            result: dict[str, Any] | None = None
            query_ok = False
            for base_url in base_urls:
                attempts += 1
                result = {
                    "query": query,
                    "page": page,
                    "endpoint": base_url,
                    "checked_at_utc": utc_now_iso(),
                }
                try:
                    if source is not None:
                        source.verify()
                    if candidate_cookie is not None:
                        candidate_cookie.verify()
                    response = marketplace_get(
                        config,
                        base_url,
                        params=params,
                        headers=req_headers,
                        timeout=timeout,
                    )
                    result["http_status"] = response.status_code
                    if response.status_code != 200:
                        result["kind"] = "http_error"
                        result["products_count"] = 0
                        result["sample"] = response.text[:120].replace("\n", " ")
                        if (
                            bool(getattr(args, "_terminal_on_access_failure", False))
                            and response.status_code in BROWSER_RATE_LIMIT_STATUSES
                        ):
                            terminal_reason = f"http_{response.status_code}"
                            break
                    else:
                        try:
                            payload = response.json()
                        except Exception as exc:
                            result["kind"] = "json_decode_failed"
                            result["products_count"] = 0
                            result["sample"] = exc.__class__.__name__
                        else:
                            kind, count, sample = classify_payload(payload)
                            result["kind"] = kind
                            result["products_count"] = count
                            result["sample"] = sample
                            if kind in OK_KINDS:
                                query_ok = True
                                break
                except AccessContractError:
                    raise
                except requests.Timeout:
                    result["http_status"] = 0
                    result["kind"] = "request_failed"
                    result["products_count"] = 0
                    result["sample"] = "Timeout"
                    if bool(getattr(args, "_terminal_on_access_failure", False)):
                        terminal_reason = "timeout"
                        break
                except Exception as exc:
                    result["http_status"] = 0
                    result["kind"] = "request_failed"
                    result["products_count"] = 0
                    result["sample"] = exc.__class__.__name__

            if query_ok:
                successes += 1
            if result is None:
                result = {
                    "query": query,
                    "page": page,
                    "endpoint": "",
                    "checked_at_utc": utc_now_iso(),
                    "http_status": 0,
                    "kind": "request_failed",
                    "products_count": 0,
                    "sample": "no endpoint checked",
                }
            results.append(result)
            if emit:
                print(
                    "smoke",
                    f"query={query!r}",
                    f"page={page}",
                    f"http={result.get('http_status')}",
                    f"kind={result.get('kind')}",
                    f"products={result.get('products_count')}",
                    f"sample={result.get('sample')}",
                )
            if terminal_reason:
                break

        ok = successes >= min_successes
        setattr(args, "_smoke_attempts", attempts)
        setattr(args, "_smoke_terminal_reason", terminal_reason)
        if source is not None:
            source.verify()
        if candidate_cookie is not None:
            candidate_cookie.verify()
        write_state(
            args.state_json,
            {
                "status": "ok" if ok else "failed",
                "checked_at_utc": utc_now_iso(),
                "min_successes": min_successes,
                "successes": successes,
                "authorization": authorization_evidence,
                "cookie_sent": bool(cookie_value),
                "candidate_cookie": (
                    {
                        "sha256": candidate_cookie.sha256,
                    }
                    if candidate_cookie is not None
                    else None
                ),
                "results": results,
            },
        )
        return ok
    finally:
        if source is not None:
            source.close()
        if candidate_cookie is not None:
            candidate_cookie.close()


def html_access_smoke(config: dict[str, Any], args: argparse.Namespace, cookie_path: Path, *, emit: bool = True) -> bool:
    if os.getenv("PARSER_WB_REFRESH_HTML_SMOKE_DISABLED", "").strip() == "1":
        return True

    serp = config.get("serp") if isinstance(config.get("serp"), dict) else {}
    if serp.get("refresh_html_smoke_enabled") is False:
        return True
    require_marketplace_proxy(config)

    query = load_queries(config, getattr(args, "query", ""), 1)[0]
    url_template = str(
        serp.get("html_smoke_url") or "https://www.wildberries.ru/catalog/0/search.aspx?search={query}"
    )
    url = url_template.replace("{query}", quote(query))
    cookie_value = read_cookie_value(cookie_path)
    timeout = int(config.get("runtime", {}).get("http_timeout_seconds", 45))
    headers = {
        "user-agent": str(serp.get("user_agent") or "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"),
        "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    }
    headers.update(request_headers_from_config(config))
    headers["cookie"] = cookie_value
    try:
        response = marketplace_get(
            config,
            url,
            headers=headers,
            timeout=timeout,
            allow_redirects=True,
        )
        text = response.text[:5000]
    except Exception as exc:
        if emit:
            print(
                f"html_smoke failed: {exc.__class__.__name__}",
                file=sys.stderr,
            )
        return False

    antibot = (
        "__wbaas/challenges/antibot" in text
        or "Почти готово" in text
        or "ÐÐ¾ÑÑÐ¸" in text
    )
    ok = response.status_code == 200 and not antibot
    if emit:
        print(f"html_smoke http={response.status_code} antibot={str(antibot).lower()} ok={str(ok).lower()}")
    return ok


def storage_state_default() -> str:
    return os.getenv("PARSER_WB_STORAGE_STATE_FILE") or os.getenv("WB_STORAGE_STATE_FILE") or "state/browser/wb_storage_state.json"


def _browser_search_url(config: dict[str, Any], args: argparse.Namespace) -> str:
    query = load_queries(config, getattr(args, "query", ""), 1)[0]
    template = str(
        getattr(args, "refresh_url", "")
        or "https://www.wildberries.ru/catalog/0/search.aspx?search={query}"
    )
    if "{query}" not in template:
        raise AccessContractError("browser_refresh_url_invalid")
    url = template.replace("{query}", quote(query))
    parsed = urlsplit(url)
    if (
        parsed.scheme != "https"
        or parsed.hostname not in {"wildberries.ru", "www.wildberries.ru"}
        or parsed.username is not None
        or parsed.password is not None
    ):
        raise AccessContractError("browser_refresh_url_invalid")
    return url


def _browser_antibot(title: str, html: str) -> bool:
    return (
        "__wbaas/challenges/antibot" in html
        or "Почти готово" in title
        or "Почти готово" in html
        or "ÐÐ¾ÑÑÐ¸" in title
        or "ÐÐ¾ÑÑÐ¸" in html
    )


def _browser_failure_reason(exc: Exception) -> str:
    name = exc.__class__.__name__
    return "browser_timeout" if "timeout" in name.lower() else "browser_error"


def _is_wb_cookie_domain(value: str) -> bool:
    domain = value.strip().lower().lstrip(".")
    return domain in {"wildberries.ru", "wb.ru"} or domain.endswith(
        (".wildberries.ru", ".wb.ru")
    )


def refresh(config: dict[str, Any], args: argparse.Namespace) -> bool:
    if not bool(getattr(args, "_allow_candidate_write", False)):
        raise AccessContractError("browser_refresh_candidate_contract_required")
    if not bool(getattr(args, "headed", False)) or not os.getenv("DISPLAY", "").strip():
        raise AccessContractError("browser_headed_xvfb_required")
    browser_channel = str(getattr(args, "browser_channel", "") or "chrome").strip()
    if browser_channel != "chrome":
        raise AccessContractError("browser_channel_not_approved")
    timeout_ms = int(getattr(args, "timeout_ms", 0) or 0)
    wait_ms = int(getattr(args, "wait_ms", 0) or 0)
    if not BROWSER_TIMEOUT_MIN_MS <= timeout_ms <= BROWSER_TIMEOUT_MAX_MS:
        raise AccessContractError("browser_timeout_budget_invalid")
    if not 0 <= wait_ms <= BROWSER_SETTLE_MAX_MS:
        raise AccessContractError("browser_settle_budget_invalid")

    proxy_config = require_marketplace_proxy(config, browser=True).playwright_proxy()
    profile_dir = browser_profile_path(args)
    url = _browser_search_url(config, args)
    try:
        from playwright.sync_api import sync_playwright
    except Exception as exc:
        print("refresh failed: browser_runtime_unavailable", file=sys.stderr)
        setattr(args, "_browser_failure_reason", "browser_runtime_unavailable")
        return False

    cookie_path = resolve_cookie_path(config, args.cookie_file)
    candidate_dir = resolve_path(BROWSER_COOKIE_CANDIDATE_RELATIVE_DIR)
    if (
        cookie_path.parent != candidate_dir
        or not re.fullmatch(r"wb_cookie\.browser_[A-Za-z0-9_-]+\.txt", cookie_path.name)
    ):
        raise AccessContractError("browser_cookie_candidate_path_invalid")
    _ensure_private_runtime_directory(candidate_dir)
    response_status = 0
    antibot = False
    cookies: list[dict[str, Any]] = []
    try:
        with sync_playwright() as p:
            context = p.chromium.launch_persistent_context(
                user_data_dir=str(profile_dir),
                channel="chrome",
                headless=False,
                proxy=proxy_config,
            )
            try:
                page = context.pages[0] if context.pages else context.new_page()
                response = page.goto(
                    url,
                    wait_until="domcontentloaded",
                    timeout=timeout_ms,
                )
                response_status = (
                    response.status
                    if response is not None
                    and isinstance(response.status, int)
                    and not isinstance(response.status, bool)
                    else 0
                )
                page.wait_for_timeout(wait_ms)
                title = page.title()[:160]
                html = page.content()[:10000]
                antibot = _browser_antibot(title, html)
                cookies = context.cookies(
                    ["https://www.wildberries.ru", "https://search.wb.ru"]
                )
            finally:
                context.close()
    except Exception as exc:
        reason = _browser_failure_reason(exc)
        setattr(args, "_browser_failure_reason", reason)
        setattr(
            args,
            "_browser_evidence",
            {
                "status": "failed",
                "reason": reason,
                "http_status": 0,
                "antibot": False,
                "headed": True,
                "browser_channel": "chrome",
                "headers_mode": "browser_native",
                "profile": BROWSER_PROFILE_RELATIVE_PATH.as_posix(),
            },
        )
        print(f"refresh failed: {reason}", file=sys.stderr)
        return False

    if response_status in BROWSER_RATE_LIMIT_STATUSES:
        reason = f"browser_http_{response_status}"
    elif response_status != 200:
        reason = "browser_http_unusable"
    elif antibot:
        reason = "browser_antibot"
    else:
        reason = ""
    if reason:
        setattr(args, "_browser_failure_reason", reason)
        setattr(
            args,
            "_browser_evidence",
            {
                "status": "failed",
                "reason": reason,
                "http_status": response_status,
                "antibot": antibot,
                "headed": True,
                "browser_channel": "chrome",
                "headers_mode": "browser_native",
                "profile": BROWSER_PROFILE_RELATIVE_PATH.as_posix(),
            },
        )
        print(f"refresh failed: {reason}", file=sys.stderr)
        return False

    cookie_pairs: OrderedDict[str, str] = OrderedDict()
    for cookie in cookies:
        domain = str(cookie.get("domain") or "")
        name = str(cookie.get("name") or "")
        value = str(cookie.get("value") or "")
        if not name:
            continue
        if not _is_wb_cookie_domain(domain):
            continue
        cookie_pairs[name] = value

    if not cookie_pairs:
        setattr(args, "_browser_failure_reason", "browser_cookie_missing")
        print("refresh failed: browser_cookie_missing", file=sys.stderr)
        return False

    write_cookie_value(
        cookie_path,
        "; ".join(f"{name}={value}" for name, value in cookie_pairs.items()),
        integrity_gate=_publication_integrity_gate(args),
        require_absent=True,
    )
    evidence = {
        "status": "passed",
        "reason": "browser_non_antibot_passed",
        "http_status": response_status,
        "antibot": False,
        "headed": True,
        "browser_channel": "chrome",
        "headers_mode": "browser_native",
        "profile": BROWSER_PROFILE_RELATIVE_PATH.as_posix(),
        "cookie_count": len(cookie_pairs),
    }
    setattr(args, "_browser_failure_reason", "")
    setattr(args, "_browser_evidence", evidence)
    print(f"refresh candidate ready: cookie_count={len(cookie_pairs)}")
    return True


def _namespace_copy(args: argparse.Namespace, **overrides: Any) -> argparse.Namespace:
    values = vars(args).copy()
    values.update(overrides)
    return argparse.Namespace(**values)


def _temp_path(path: Path, suffix: str) -> Path:
    return path.with_name(f"{path.name}.{suffix}.tmp")


def _browser_candidate_path() -> tuple[str, Path]:
    candidate_dir = resolve_path(BROWSER_COOKIE_CANDIDATE_RELATIVE_DIR)
    _ensure_private_runtime_directory(candidate_dir)
    attempt_id = (
        datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
        + "-"
        + uuid.uuid4().hex[:12]
    )
    return attempt_id, candidate_dir / f"wb_cookie.browser_{attempt_id}.txt"


def _promote_browser_cookie_candidate(
    cookie_path: Path,
    candidate_path: Path,
    *,
    args: argparse.Namespace,
) -> dict[str, Any]:
    expected_cookie_path = resolve_path("config/wb_cookie.txt")
    if cookie_path != expected_cookie_path:
        raise AccessContractError("production_cookie_path_not_approved")
    gate = _publication_integrity_gate(args)
    candidate = _open_pinned_file(
        candidate_path,
        project_root=PROJECT_ROOT,
        allowed_root=resolve_path(BROWSER_COOKIE_CANDIDATE_RELATIVE_DIR),
        exact_mode=0o600,
    )
    current: PinnedFile | None = None
    backup_path: Path | None = None
    try:
        try:
            candidate_text = candidate.payload.decode("utf-8").strip()
        except UnicodeDecodeError as exc:
            raise AccessContractError("cookie_candidate_invalid") from exc
        if not candidate_text or "\n" in candidate_text or "\r" in candidate_text:
            raise AccessContractError("cookie_candidate_invalid")

        if cookie_path.exists():
            current = _open_pinned_file(
                cookie_path,
                project_root=PROJECT_ROOT,
                allowed_root=resolve_path("config"),
                exact_mode=0o600,
            )
            backup_dir = resolve_path(BROWSER_COOKIE_BACKUP_RELATIVE_DIR)
            _ensure_private_runtime_directory(backup_dir)
            backup_name = (
                "wb_cookie.browser_backup_"
                + datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
                + "_"
                + current.sha256[:12]
                + "_"
                + uuid.uuid4().hex[:8]
                + ".txt"
            )
            backup_path = backup_dir / backup_name
            durable_atomic_replace(
                backup_path,
                current.payload,
                mode=0o600,
                require_absent=True,
                integrity_gate=gate,
                source_integrity_gate=current.verify,
            )
            current.verify()

        candidate.verify()
        durable_atomic_replace(
            cookie_path,
            candidate.payload,
            mode=0o600,
            integrity_gate=gate,
            source_integrity_gate=candidate.verify,
        )
        promoted = _open_pinned_file(
            cookie_path,
            project_root=PROJECT_ROOT,
            allowed_root=resolve_path("config"),
            exact_mode=0o600,
        )
        try:
            if promoted.sha256 != candidate.sha256:
                raise AccessContractError("cookie_promotion_verification_failed")
        finally:
            promoted.close()
        return {
            "candidate_sha256": candidate.sha256,
            "previous_sha256": current.sha256 if current is not None else None,
            "backup_path": (
                backup_path.relative_to(PROJECT_ROOT).as_posix()
                if backup_path is not None
                else None
            ),
        }
    finally:
        if current is not None:
            current.close()
        candidate.close()


def refresh_and_promote(config: dict[str, Any], args: argparse.Namespace) -> bool:
    cookie_path = resolve_cookie_path(config, args.cookie_file)
    state_json = str(
        resolve_path(args.state_json)
        if getattr(args, "state_json", "")
        else PROJECT_ROOT / "state/wb_session_keeper/latest.json"
    )
    cooldown = browser_refresh_cooldown_active()
    if cooldown is not None:
        write_state(
            state_json,
            {
                "status": "cooldown",
                "checked_at_utc": utc_now_iso(),
                "failure_reason": "browser_refresh_cooldown_active",
                "cooldown": cooldown,
            },
        )
        print("browser refresh skipped: cooldown active", file=sys.stderr)
        return False

    attempt_id, candidate_cookie = _browser_candidate_path()
    refresh_args = _namespace_copy(
        args,
        cookie_file=str(candidate_cookie),
        state_json=state_json,
        browser_profile_dir=(
            getattr(args, "browser_profile_dir", "")
            or BROWSER_PROFILE_RELATIVE_PATH.as_posix()
        ),
        _allow_candidate_write=True,
    )
    try:
        if not refresh(config, refresh_args):
            reason = str(
                getattr(refresh_args, "_browser_failure_reason", "")
                or "browser_refresh_failed"
            )
            if reason in {"browser_http_429", "browser_http_498", "browser_timeout"}:
                cooldown = record_browser_refresh_cooldown(reason)
            else:
                cooldown = None
            write_state(
                state_json,
                {
                    "status": "failed",
                    "checked_at_utc": utc_now_iso(),
                    "attempt_id": attempt_id,
                    "failure_reason": reason,
                    "browser": getattr(refresh_args, "_browser_evidence", {}),
                    "cooldown": cooldown,
                    "production_cookie_changed": False,
                    "storage_state_changed": False,
                },
            )
            return False

        smoke_args = _namespace_copy(
            args,
            cookie_file=str(candidate_cookie),
            state_json=state_json,
            sample_count=3,
            min_successes=3,
            without_cookie=False,
            request_headers_file="",
            authorization_policy="if_present",
            authorization_horizon_plan_file=(
                getattr(args, "authorization_horizon_plan_file", "")
                or DEFAULT_AUTHORIZATION_HORIZON_PLAN
            ),
            _generated_cookie_candidate=True,
            _terminal_on_access_failure=True,
        )
        if not smoke(config, smoke_args):
            terminal = str(getattr(smoke_args, "_smoke_terminal_reason", "") or "")
            cooldown = None
            if terminal in {"http_429", "http_498", "timeout"}:
                cooldown = record_browser_refresh_cooldown(
                    "candidate_api_rate_limited"
                )
            write_state(
                state_json,
                {
                    "status": "failed",
                    "checked_at_utc": utc_now_iso(),
                    "attempt_id": attempt_id,
                    "failure_reason": "candidate_api_smoke_failed",
                    "browser": getattr(refresh_args, "_browser_evidence", {}),
                    "api_smoke": {
                        "required_successes": 3,
                        "attempts": int(getattr(smoke_args, "_smoke_attempts", 0)),
                        "terminal_reason": terminal or None,
                    },
                    "cooldown": cooldown,
                    "production_cookie_changed": False,
                    "storage_state_changed": False,
                },
            )
            print("refresh API smoke failed; production cookie unchanged", file=sys.stderr)
            return False

        promotion = _promote_browser_cookie_candidate(
            cookie_path,
            candidate_cookie,
            args=args,
        )
        clear_browser_refresh_cooldown()
        write_state(
            state_json,
            {
                "status": "promoted",
                "checked_at_utc": utc_now_iso(),
                "attempt_id": attempt_id,
                "browser": getattr(refresh_args, "_browser_evidence", {}),
                "api_smoke": {
                    "required_successes": 3,
                    "successes": 3,
                    "attempts": int(getattr(smoke_args, "_smoke_attempts", 0)),
                    "authorization": getattr(
                        smoke_args, "_authorization_evidence", {}
                    ),
                },
                "promotion": promotion,
                "production_cookie_changed": True,
                "storage_state_changed": False,
            },
        )
        print("refresh promoted: browser and exact API gates passed")
        return True
    finally:
        try:
            if candidate_cookie.exists():
                candidate_cookie.unlink()
        except OSError:
            pass


def renew(config: dict[str, Any], args: argparse.Namespace) -> bool:
    print("proactive cookie refresh started", file=sys.stderr)
    return refresh_and_promote(config, args)


def ensure(config: dict[str, Any], args: argparse.Namespace) -> bool:
    if smoke(config, args):
        return True

    print("smoke failed; trying cookie refresh", file=sys.stderr)
    return refresh_and_promote(config, args)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="WB cookie/session keeper without printing secret values.")
    parser.add_argument("command", choices=["smoke", "refresh", "renew", "ensure"])
    parser.add_argument("--config", default="config/config.yaml")
    parser.add_argument("--cookie-file", default="")
    parser.add_argument("--state-json", default="state/wb_session_keeper/latest.json")
    parser.add_argument("--query", default="")
    parser.add_argument("--sample-count", type=int, default=1)
    parser.add_argument("--min-successes", type=int, default=0)
    parser.add_argument("--page", type=int, default=1)
    parser.add_argument(
        "--refresh-url",
        default="https://www.wildberries.ru/catalog/0/search.aspx?search={query}",
    )
    parser.add_argument("--storage-state", default="")
    parser.add_argument("--storage-state-out", default="")
    parser.add_argument("--require-storage-state", action="store_true")
    parser.add_argument("--browser-channel", default="")
    parser.add_argument(
        "--browser-profile-dir",
        default=BROWSER_PROFILE_RELATIVE_PATH.as_posix(),
    )
    parser.add_argument("--headed", action="store_true")
    parser.add_argument("--no-headless", action="store_true")
    parser.add_argument("--wait-ms", type=int, default=3000)
    parser.add_argument("--timeout-ms", type=int, default=45000)
    parser.add_argument("--without-cookie", action="store_true")
    parser.add_argument("--request-headers-file", default="")
    parser.add_argument(
        "--authorization-policy",
        choices=sorted(AUTHORIZATION_POLICIES),
        default="optional",
    )
    parser.add_argument(
        "--authorization-horizon-plan-file",
        default=DEFAULT_AUTHORIZATION_HORIZON_PLAN,
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    _require_host_lease_after_cutover()
    args = build_parser().parse_args(argv)
    config_path = resolve_path(args.config)
    if args.request_headers_file and args.command != "smoke":
        print("candidate request headers are supported only by smoke", file=sys.stderr)
        return EXIT_SMOKE_FAILED
    config = load_config(
        config_path,
        inject_request_headers=not bool(args.request_headers_file),
    )

    try:
        if args.command == "smoke":
            return EXIT_OK if smoke(config, args) else EXIT_SMOKE_FAILED
        if args.command == "refresh":
            return EXIT_OK if refresh_and_promote(config, args) else EXIT_REFRESH_FAILED
        if args.command == "renew":
            return EXIT_OK if renew(config, args) else EXIT_REFRESH_FAILED
        return EXIT_OK if ensure(config, args) else EXIT_SMOKE_FAILED
    except AccessContractError as exc:
        write_state(
            args.state_json,
            {
                "status": "failed",
                "checked_at_utc": utc_now_iso(),
                "failure_reason": exc.code,
                "authorization": exc.evidence,
            },
        )
        print(f"{args.command} failed: {exc.code}", file=sys.stderr)
        return EXIT_SMOKE_FAILED if args.command != "refresh" else EXIT_REFRESH_FAILED
    except Exception as exc:
        print(f"{args.command} failed: {exc.__class__.__name__}: {exc}", file=sys.stderr)
        return EXIT_SMOKE_FAILED if args.command != "refresh" else EXIT_REFRESH_FAILED


if __name__ == "__main__":
    raise SystemExit(main())
