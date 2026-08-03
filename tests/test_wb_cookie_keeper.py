from __future__ import annotations

import argparse
import base64
import hashlib
import importlib.util
import json
import shutil
import sys
from datetime import datetime, timezone
from pathlib import Path
from types import ModuleType, SimpleNamespace

import pytest


def _enable_proxy(monkeypatch, url: str = "http://proxy.local:3128") -> None:
    monkeypatch.setenv("PARSER_WB_RUNTIME_ENV_LOADED", "1")
    monkeypatch.setenv("PARSER_WB_RUNTIME_ENV_SHA256", "a" * 64)
    monkeypatch.setenv("PARSER_WB_PROXY_URL", url)


def _load_keeper():
    path = Path(__file__).resolve().parents[1] / "scripts" / "wb_cookie_keeper.py"
    spec = importlib.util.spec_from_file_location("wb_cookie_keeper", path)
    assert spec is not None
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


def _jwt(*, iat: int, nbf: int, exp: int) -> str:
    def encode(payload: dict[str, object]) -> str:
        raw = json.dumps(payload, separators=(",", ":")).encode("utf-8")
        return base64.urlsafe_b64encode(raw).rstrip(b"=").decode("ascii")

    return f"{encode({'alg': 'none', 'typ': 'JWT'})}.{encode({'iat': iat, 'nbf': nbf, 'exp': exp})}.sig"


def _authorization_args(**overrides) -> argparse.Namespace:
    values = {
        "authorization_policy": "required",
        "authorization_horizon_plan_file": "config/wb/collection_plans/shevron-four-regions-top1000-v2.json",
        "request_headers_file": "",
    }
    values.update(overrides)
    return argparse.Namespace(**values)


def test_nested_promo_products_is_preflight_ok() -> None:
    keeper = _load_keeper()
    assert "nested_promo_products" in keeper.OK_KINDS


def test_proxy_helpers_use_env_and_prepare_requests_and_playwright(monkeypatch) -> None:
    keeper = _load_keeper()
    _enable_proxy(monkeypatch)
    monkeypatch.setenv("TEST_WB_PROXY_URL", "http://user:pa%24%24@proxy.local:3128")
    config = {"serp": {"proxy_url_env": "TEST_WB_PROXY_URL", "proxy_url": "http://ignored.local:8080"}}

    proxy_url = keeper.resolve_proxy_url(config)

    assert proxy_url == "http://user:pa%24%24@proxy.local:3128"
    assert keeper.requests_proxies(proxy_url) == {"http": proxy_url, "https": proxy_url}
    assert keeper.playwright_proxy_config(proxy_url) == {
        "server": "http://proxy.local:3128",
        "username": "user",
        "password": "pa$$",
    }


def test_runtime_request_headers_file_merges_without_cookie(tmp_path: Path, monkeypatch) -> None:
    keeper = _load_keeper()
    headers_path = tmp_path / "headers.json"
    headers_path.write_text(
        '{"authorization":"Bearer runtime","deviceid":"device-1","cookie":"stale=1"}\n',
        encoding="utf-8",
    )
    monkeypatch.setenv("TEST_WB_HEADERS_FILE", str(headers_path))
    config = {
        "serp": {
            "request_headers_file_env": "TEST_WB_HEADERS_FILE",
            "request_headers": {"x-queryid": "base"},
        }
    }

    keeper.inject_runtime_request_headers(config, tmp_path)

    assert keeper.request_headers_from_config(config) == {
        "x-queryid": "base",
        "authorization": "Bearer runtime",
        "deviceid": "device-1",
    }


def test_authorization_temporal_contract_accepts_valid_ttl_and_redacts_token() -> None:
    keeper = _load_keeper()
    now = datetime(2026, 8, 3, 0, 0, tzinfo=timezone.utc)
    now_timestamp = int(now.timestamp())
    token = _jwt(
        iat=now_timestamp - 3600,
        nbf=now_timestamp - 3600,
        exp=now_timestamp + 86400,
    )

    evidence = keeper.validate_authorization_temporal_contract(
        {"Authorization": f"Bearer {token}"},
        policy="required",
        now_utc=now,
        required_until_utc=datetime(2026, 8, 3, 20, 0, tzinfo=timezone.utc),
        source_evidence={"sha256": "a" * 64},
    )

    assert evidence["status"] == "valid"
    assert evidence["ttl_seconds"] > 0
    assert set(evidence) == {
        "policy",
        "status",
        "source",
        "iat_utc",
        "nbf_utc",
        "exp_utc",
        "ttl_seconds",
        "required_until_utc",
    }
    assert token not in json.dumps(evidence)


@pytest.mark.parametrize(
    ("authorization", "expected_code"),
    [
        ("Bearer not-a-jwt", "authorization_not_jwt"),
        ("Basic value", "authorization_bearer_invalid"),
    ],
)
def test_authorization_temporal_contract_rejects_malformed_without_secret(
    authorization: str,
    expected_code: str,
) -> None:
    keeper = _load_keeper()
    with pytest.raises(keeper.AccessContractError) as captured:
        keeper.validate_authorization_temporal_contract(
            {"authorization": authorization},
            policy="required",
            now_utc=datetime(2026, 8, 3, tzinfo=timezone.utc),
            required_until_utc=datetime(2026, 8, 3, 20, tzinfo=timezone.utc),
        )
    assert captured.value.code == expected_code
    assert authorization not in str(captured.value)
    assert authorization not in json.dumps(captured.value.evidence)


def test_authorization_temporal_contract_rejects_expired_and_future_nbf() -> None:
    keeper = _load_keeper()
    now = datetime(2026, 8, 3, 12, 0, tzinfo=timezone.utc)
    now_timestamp = int(now.timestamp())
    expired = _jwt(iat=now_timestamp - 200, nbf=now_timestamp - 200, exp=now_timestamp)
    future = _jwt(iat=now_timestamp + 10, nbf=now_timestamp + 10, exp=now_timestamp + 1000)

    with pytest.raises(keeper.AccessContractError, match="authorization_expired"):
        keeper.validate_authorization_temporal_contract(
            {"authorization": f"Bearer {expired}"},
            policy="required",
            now_utc=now,
            required_until_utc=now,
        )
    with pytest.raises(keeper.AccessContractError, match="authorization_not_yet_valid"):
        keeper.validate_authorization_temporal_contract(
            {"authorization": f"Bearer {future}"},
            policy="required",
            now_utc=now,
            required_until_utc=now,
        )


def test_authorization_temporal_contract_rejects_insufficient_horizon() -> None:
    keeper = _load_keeper()
    now = datetime(2026, 8, 3, 0, 0, tzinfo=timezone.utc)
    now_timestamp = int(now.timestamp())
    token = _jwt(iat=now_timestamp - 10, nbf=now_timestamp - 10, exp=now_timestamp + 3600)
    with pytest.raises(keeper.AccessContractError, match="authorization_horizon_not_covered"):
        keeper.validate_authorization_temporal_contract(
            {"authorization": f"Bearer {token}"},
            policy="required",
            now_utc=now,
            required_until_utc=now.replace(hour=20),
        )


def test_authorization_missing_is_allowed_only_by_optional_policy() -> None:
    keeper = _load_keeper()
    now = datetime(2026, 8, 3, tzinfo=timezone.utc)
    assert keeper.validate_authorization_temporal_contract(
        {}, policy="optional", now_utc=now, required_until_utc=None
    )["status"] == "not_present_optional"
    with pytest.raises(keeper.AccessContractError, match="authorization_missing"):
        keeper.validate_authorization_temporal_contract(
            {}, policy="required", now_utc=now, required_until_utc=now
        )


def test_if_present_allows_missing_and_validates_present_authorization() -> None:
    keeper = _load_keeper()
    now = datetime(2026, 8, 3, 0, 0, tzinfo=timezone.utc)
    now_timestamp = int(now.timestamp())
    valid = _jwt(
        iat=now_timestamp - 3600,
        nbf=now_timestamp - 3600,
        exp=now_timestamp + 86400,
    )

    missing = keeper.validate_authorization_temporal_contract(
        {},
        policy="if_present",
        now_utc=now,
        required_until_utc=now.replace(hour=20),
    )
    present = keeper.validate_authorization_temporal_contract(
        {"authorization": f"Bearer {valid}"},
        policy="if_present",
        now_utc=now,
        required_until_utc=now.replace(hour=20),
    )

    assert missing["status"] == "not_present_allowed"
    assert present["status"] == "valid"
    assert valid not in json.dumps(present)


def test_if_present_rejects_expired_authorization_without_secret_leak() -> None:
    keeper = _load_keeper()
    now = datetime(2026, 8, 3, 12, 0, tzinfo=timezone.utc)
    now_timestamp = int(now.timestamp())
    expired = _jwt(
        iat=now_timestamp - 3600,
        nbf=now_timestamp - 3600,
        exp=now_timestamp,
    )

    with pytest.raises(keeper.AccessContractError) as captured:
        keeper.validate_authorization_temporal_contract(
            {"authorization": f"Bearer {expired}"},
            policy="if_present",
            now_utc=now,
            required_until_utc=now.replace(hour=20),
        )

    assert captured.value.code == "authorization_expired"
    assert expired not in str(captured.value)
    assert expired not in json.dumps(captured.value.evidence)


def test_authorization_horizon_comes_from_reviewed_plan_cutoff() -> None:
    keeper = _load_keeper()
    project_root = Path(__file__).resolve().parents[1]
    plan = keeper.DEFAULT_AUTHORIZATION_HORIZON_PLAN

    before_schedule, evidence_before = keeper.authorization_horizon_from_plan(
        plan,
        now_utc=datetime(2026, 8, 2, 21, 5, tzinfo=timezone.utc),
        project_root=project_root,
    )
    after_cutoff, evidence_after = keeper.authorization_horizon_from_plan(
        plan,
        now_utc=datetime(2026, 8, 3, 20, 45, tzinfo=timezone.utc),
        project_root=project_root,
    )

    assert before_schedule == datetime(2026, 8, 3, 20, 0, tzinfo=timezone.utc)
    assert after_cutoff == datetime(2026, 8, 4, 20, 0, tzinfo=timezone.utc)
    assert evidence_before["horizon_source"] == "collection_plan_runtime_window"
    assert evidence_before["plan_sha256"] == evidence_after["plan_sha256"]


def test_candidate_request_headers_require_scoped_0600_regular_file(
    tmp_path: Path,
) -> None:
    keeper = _load_keeper()
    candidate_dir = tmp_path / "state/wb_header_candidates"
    candidate_dir.mkdir(parents=True)
    candidate = candidate_dir / "candidate.json"
    candidate.write_text('{"authorization":"Bearer value","deviceid":"d"}\n', encoding="utf-8")
    candidate.chmod(0o600)
    args = _authorization_args(request_headers_file=str(candidate))

    source = keeper.load_request_headers_source({}, args, project_root=tmp_path)
    assert source is not None
    try:
        assert source.evidence() == {
            "source": "candidate",
            "sha256": source.pinned.sha256,
            "headers_count": 2,
        }
    finally:
        source.close()

    candidate.chmod(0o644)
    with pytest.raises(keeper.AccessContractError, match="access_source_metadata_unsafe"):
        keeper.load_request_headers_source({}, args, project_root=tmp_path)


def test_candidate_request_headers_reject_outside_and_symlink(tmp_path: Path) -> None:
    keeper = _load_keeper()
    candidate_dir = tmp_path / "state/wb_header_candidates"
    candidate_dir.mkdir(parents=True)
    outside = tmp_path / "outside.json"
    outside.write_text('{"authorization":"Bearer value"}\n', encoding="utf-8")
    outside.chmod(0o600)
    args = _authorization_args(request_headers_file=str(outside))
    with pytest.raises(keeper.AccessContractError, match="access_source_scope_invalid"):
        keeper.load_request_headers_source({}, args, project_root=tmp_path)

    args.request_headers_file = "state/wb_header_candidates/../wb_header_candidates/candidate.json"
    with pytest.raises(keeper.AccessContractError, match="access_source_path_traversal"):
        keeper.load_request_headers_source({}, args, project_root=tmp_path)

    link = candidate_dir / "candidate.json"
    link.symlink_to(outside)
    args.request_headers_file = str(link)
    with pytest.raises(keeper.AccessContractError, match="access_source_symlink"):
        keeper.load_request_headers_source({}, args, project_root=tmp_path)


def test_candidate_cookie_requires_scoped_0600_file(tmp_path: Path) -> None:
    keeper = _load_keeper()
    cookie_dir = tmp_path / "state/wb_cookie_candidates"
    cookie_dir.mkdir(parents=True)
    cookie = cookie_dir / "candidate.txt"
    cookie.write_text("cookie=1\n", encoding="utf-8")
    cookie.chmod(0o600)
    args = argparse.Namespace(
        request_headers_file="state/wb_header_candidates/candidate.json",
        cookie_file=str(cookie),
        without_cookie=False,
    )
    source = keeper.load_candidate_cookie_source(args, project_root=tmp_path)
    assert source is not None
    source.close()

    cookie.chmod(0o664)
    with pytest.raises(keeper.AccessContractError, match="access_source_metadata_unsafe"):
        keeper.load_candidate_cookie_source(args, project_root=tmp_path)

def test_candidate_request_headers_detect_change_after_load(tmp_path: Path) -> None:
    keeper = _load_keeper()
    candidate_dir = tmp_path / "state/wb_header_candidates"
    candidate_dir.mkdir(parents=True)
    candidate = candidate_dir / "candidate.json"
    candidate.write_text('{"authorization":"Bearer first"}\n', encoding="utf-8")
    candidate.chmod(0o600)
    source = keeper.load_request_headers_source(
        {},
        _authorization_args(request_headers_file=str(candidate)),
        project_root=tmp_path,
    )
    assert source is not None
    try:
        candidate.write_text('{"authorization":"Bearer other"}\n', encoding="utf-8")
        candidate.chmod(0o600)
        with pytest.raises(keeper.AccessContractError, match="access_source_changed"):
            source.verify()
    finally:
        source.close()


def test_expired_candidate_authorization_fails_before_network(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    keeper = _load_keeper()
    monkeypatch.setattr(keeper, "PROJECT_ROOT", tmp_path)
    plan_dir = tmp_path / "config/wb/collection_plans"
    plan_dir.mkdir(parents=True)
    plan = plan_dir / "shevron-four-regions-top1000-v2.json"
    shutil.copyfile(
        Path(__file__).resolve().parents[1]
        / "config/wb/collection_plans/shevron-four-regions-top1000-v2.json",
        plan,
    )
    plan.chmod(0o644)
    now_timestamp = 1_775_000_000
    token = _jwt(iat=now_timestamp - 1000, nbf=now_timestamp - 1000, exp=now_timestamp - 1)
    headers_dir = tmp_path / "state/wb_header_candidates"
    headers_dir.mkdir(parents=True)
    headers = headers_dir / "candidate.json"
    headers.write_text(json.dumps({"authorization": f"Bearer {token}"}), encoding="utf-8")
    headers.chmod(0o600)
    cookie_dir = tmp_path / "state/wb_cookie_candidates"
    cookie_dir.mkdir(parents=True)
    cookie = cookie_dir / "candidate.txt"
    cookie.write_text("cookie=1\n", encoding="utf-8")
    cookie.chmod(0o600)
    queries = tmp_path / "exports/queries.txt"
    queries.parent.mkdir(parents=True)
    queries.write_text("q1\nq2\nq3\n", encoding="utf-8")
    calls = 0

    def forbidden_network(*_args, **_kwargs):
        nonlocal calls
        calls += 1
        raise AssertionError("network must not be called")

    monkeypatch.setattr(keeper, "marketplace_get", forbidden_network)
    monkeypatch.setattr(
        keeper,
        "datetime",
        type(
            "FixedDateTime",
            (datetime,),
            {"now": classmethod(lambda cls, tz=None: datetime.fromtimestamp(now_timestamp, tz=timezone.utc))},
        ),
    )
    args = argparse.Namespace(
        cookie_file=str(cookie),
        request_headers_file=str(headers),
        authorization_policy="required",
        authorization_horizon_plan_file=str(plan),
        state_json="",
        query="",
        sample_count=3,
        min_successes=3,
        page=1,
        without_cookie=False,
    )
    config = {
        "runtime": {"http_timeout_seconds": 5},
        "serp": {
            "base_url": "https://example.invalid/search",
            "proxy_url": "http://proxy.invalid:3128",
            "input_files": {"queries_txt": str(queries)},
            "request_params": {},
        },
    }
    with pytest.raises(keeper.AccessContractError, match="authorization_expired"):
        keeper.smoke(config, args, emit=False)
    assert calls == 0


@pytest.mark.parametrize(
    ("authorization_policy", "with_authorization", "expected_status"),
    [
        ("required", True, "valid"),
        ("if_present", False, "not_present_allowed"),
    ],
)
def test_candidate_headers_cookie_and_cookieless_exact_three_of_three_smoke(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    authorization_policy: str,
    with_authorization: bool,
    expected_status: str,
) -> None:
    keeper = _load_keeper()
    monkeypatch.setattr(keeper, "PROJECT_ROOT", tmp_path)
    plan_dir = tmp_path / "config/wb/collection_plans"
    plan_dir.mkdir(parents=True)
    plan = plan_dir / "shevron-four-regions-top1000-v2.json"
    shutil.copyfile(
        Path(__file__).resolve().parents[1]
        / "config/wb/collection_plans/shevron-four-regions-top1000-v2.json",
        plan,
    )
    plan.chmod(0o644)
    now = datetime(2026, 8, 3, 0, 0, tzinfo=timezone.utc)
    now_timestamp = int(now.timestamp())
    token = _jwt(
        iat=now_timestamp - 3600,
        nbf=now_timestamp - 3600,
        exp=now_timestamp + 172800,
    )
    headers_dir = tmp_path / "state/wb_header_candidates"
    headers_dir.mkdir(parents=True)
    headers = headers_dir / "candidate.json"
    header_payload = {"deviceid": "device"}
    if with_authorization:
        header_payload["authorization"] = f"Bearer {token}"
    headers.write_text(json.dumps(header_payload), encoding="utf-8")
    headers.chmod(0o600)
    cookie_dir = tmp_path / "state/wb_cookie_candidates"
    cookie_dir.mkdir(parents=True)
    cookie = cookie_dir / "candidate.txt"
    cookie.write_text("cookie=1\n", encoding="utf-8")
    cookie.chmod(0o600)
    queries = tmp_path / "exports/queries.txt"
    queries.parent.mkdir(parents=True)
    queries.write_text("q1\nq2\nq3\n", encoding="utf-8")
    monkeypatch.setattr(
        keeper,
        "datetime",
        type(
            "FixedDateTime",
            (datetime,),
            {"now": classmethod(lambda cls, tz=None: now)},
        ),
    )
    calls: list[dict[str, str]] = []

    class Response:
        status_code = 200
        text = ""

        @staticmethod
        def json():
            return {"products": [{"id": 1}]}

    def fake_get(_config, _url, **kwargs):
        calls.append(dict(kwargs["headers"]))
        return Response()

    monkeypatch.setattr(keeper, "marketplace_get", fake_get)
    monkeypatch.setattr(keeper, "require_marketplace_proxy", lambda _config: object())
    state = tmp_path / "state/smoke.json"
    args = argparse.Namespace(
        cookie_file=str(cookie),
        request_headers_file=str(headers),
        authorization_policy=authorization_policy,
        authorization_horizon_plan_file=str(plan),
        state_json=str(state),
        query="",
        sample_count=3,
        min_successes=3,
        page=1,
        without_cookie=False,
    )
    config = {
        "runtime": {"http_timeout_seconds": 5},
        "serp": {
            "base_url": "https://example.invalid/search",
            "proxy_url": "http://proxy.invalid:3128",
            "input_files": {"queries_txt": str(queries)},
            "request_params": {},
        },
    }

    assert keeper.smoke(config, args, emit=False) is True
    assert len(calls) == 3
    if with_authorization:
        assert all(call["authorization"] == f"Bearer {token}" for call in calls)
    else:
        assert all("authorization" not in call for call in calls)
    assert all(call["cookie"] == "cookie=1" for call in calls)
    payload = json.loads(state.read_text(encoding="utf-8"))
    assert payload["successes"] == 3
    assert payload["cookie_sent"] is True
    assert payload["authorization"]["status"] == expected_status
    assert payload["authorization"]["source"]["source"] == "candidate"
    assert payload["candidate_cookie"]["sha256"] == hashlib.sha256(cookie.read_bytes()).hexdigest()
    assert token not in state.read_text(encoding="utf-8")

    calls.clear()
    args.without_cookie = True
    args.state_json = str(tmp_path / "state/smoke_without_cookie.json")
    assert keeper.smoke(config, args, emit=False) is True
    assert len(calls) == 3
    assert all("cookie" not in call for call in calls)
    cookieless_payload = json.loads(Path(args.state_json).read_text(encoding="utf-8"))
    assert cookieless_payload["successes"] == 3
    assert cookieless_payload["cookie_sent"] is False
    assert cookieless_payload["authorization"]["status"] == expected_status
    assert cookieless_payload["candidate_cookie"] is None
    assert token not in Path(args.state_json).read_text(encoding="utf-8")


def test_ensure_keeps_existing_cookie_when_refresh_smoke_fails(tmp_path: Path, monkeypatch) -> None:
    keeper = _load_keeper()
    cookie_path = tmp_path / "wb_cookie.txt"
    cookie_path.write_text("old_cookie=1\n", encoding="utf-8")

    args = argparse.Namespace(
        cookie_file=str(cookie_path),
        state_json=str(tmp_path / "state.json"),
        storage_state="",
        storage_state_out="",
    )

    def fake_smoke(config, smoke_args, emit=True):
        return Path(smoke_args.cookie_file) != cookie_path and False

    def fake_refresh(config, refresh_args):
        Path(refresh_args.cookie_file).write_text("new_cookie=1\n", encoding="utf-8")
        Path(refresh_args.storage_state_out).write_text('{"cookies":[]}\n', encoding="utf-8")
        return True

    monkeypatch.setattr(keeper, "smoke", fake_smoke)
    monkeypatch.setattr(keeper, "refresh", fake_refresh)
    monkeypatch.setattr(keeper, "html_access_smoke", lambda config, smoke_args, cookie_path, emit=True: True)

    assert keeper.ensure({}, args) is False
    assert cookie_path.read_text(encoding="utf-8") == "old_cookie=1\n"


def test_renew_promotes_temp_cookie_after_smoke_success(tmp_path: Path, monkeypatch) -> None:
    keeper = _load_keeper()
    cookie_path = tmp_path / "wb_cookie.txt"
    cookie_path.write_text("old_cookie=1\n", encoding="utf-8")
    storage_state = tmp_path / "storage_state.json"
    storage_state.write_text('{"cookies":[]}\n', encoding="utf-8")

    args = argparse.Namespace(
        cookie_file=str(cookie_path),
        state_json=str(tmp_path / "state.json"),
        storage_state=str(storage_state),
        storage_state_out="",
    )

    def fake_refresh(config, refresh_args):
        Path(refresh_args.cookie_file).write_text("new_cookie=1\n", encoding="utf-8")
        Path(refresh_args.storage_state_out).write_text('{"cookies":[{"name":"ok"}]}\n', encoding="utf-8")
        return True

    def fake_smoke(config, smoke_args, emit=True):
        return Path(smoke_args.cookie_file).read_text(encoding="utf-8") == "new_cookie=1\n"

    monkeypatch.setattr(keeper, "refresh", fake_refresh)
    monkeypatch.setattr(keeper, "smoke", fake_smoke)
    monkeypatch.setattr(keeper, "html_access_smoke", lambda config, smoke_args, cookie_path, emit=True: True)

    assert keeper.renew({}, args) is True
    assert cookie_path.read_text(encoding="utf-8") == "new_cookie=1\n"
    assert storage_state.read_text(encoding="utf-8") == '{"cookies":[{"name":"ok"}]}\n'


def test_renew_keeps_existing_cookie_when_html_smoke_fails(tmp_path: Path, monkeypatch) -> None:
    keeper = _load_keeper()
    cookie_path = tmp_path / "wb_cookie.txt"
    cookie_path.write_text("old_cookie=1\n", encoding="utf-8")
    storage_state = tmp_path / "storage_state.json"
    storage_state.write_text('{"cookies":[]}\n', encoding="utf-8")

    args = argparse.Namespace(
        cookie_file=str(cookie_path),
        state_json=str(tmp_path / "state.json"),
        storage_state=str(storage_state),
        storage_state_out="",
    )

    def fake_refresh(config, refresh_args):
        Path(refresh_args.cookie_file).write_text("new_cookie=1\n", encoding="utf-8")
        Path(refresh_args.storage_state_out).write_text('{"cookies":[{"name":"weak"}]}\n', encoding="utf-8")
        return True

    monkeypatch.setattr(keeper, "refresh", fake_refresh)
    monkeypatch.setattr(keeper, "smoke", lambda config, smoke_args, emit=True: True)
    monkeypatch.setattr(keeper, "html_access_smoke", lambda config, smoke_args, cookie_path, emit=True: False)

    assert keeper.renew({}, args) is False
    assert cookie_path.read_text(encoding="utf-8") == "old_cookie=1\n"
    assert storage_state.read_text(encoding="utf-8") == '{"cookies":[]}\n'


def test_smoke_uses_fallback_urls_and_min_successes(tmp_path: Path, monkeypatch) -> None:
    keeper = _load_keeper()
    _enable_proxy(monkeypatch)
    cookie_path = tmp_path / "wb_cookie.txt"
    cookie_path.write_text("cookie=1\n", encoding="utf-8")
    queries_path = tmp_path / "exports" / "queries.txt"
    queries_path.parent.mkdir(parents=True)
    queries_path.write_text("q1\nq2\n", encoding="utf-8")

    config = {
        "runtime": {"http_timeout_seconds": 5},
        "serp": {
            "wb_cookie_file": str(cookie_path),
            "base_url": "https://internal.example/search",
            "fallback_base_urls": ["https://fallback.example/search"],
            "smoke_min_successes": 1,
            "proxy_url": "http://proxy.local:3128",
            "input_files": {"queries_txt": str(queries_path)},
            "request_params": {},
            "request_headers": {
                "authorization": "Bearer token",
                "deviceid": "device-1",
                "cookie": "stale=1",
            },
        },
    }
    args = argparse.Namespace(
        cookie_file=str(cookie_path),
        state_json=str(tmp_path / "state.json"),
        query="",
        sample_count=2,
        min_successes=0,
        page=1,
    )

    class Response:
        def __init__(self, status_code: int, payload=None, text: str = "") -> None:
            self.status_code = status_code
            self._payload = payload
            self.text = text

        def json(self):
            if self._payload is None:
                raise ValueError("no json")
            return self._payload

    calls: list[tuple[str, str, dict[str, str] | None, dict[str, str]]] = []

    def fake_get(session, url, *, params, headers, timeout):
        calls.append((url, params["query"], dict(session.proxies), headers))
        if url == "https://fallback.example/search" and params["query"] == "q1":
            return Response(200, {"products": [{"name": "ok"}]})
        return Response(498, text="blocked")

    monkeypatch.setattr(keeper.requests.Session, "get", fake_get)

    assert keeper.smoke(config, args, emit=False) is True
    expected_proxy = {"http": "http://proxy.local:3128", "https": "http://proxy.local:3128"}
    assert [(url, query, proxy) for url, query, proxy, _headers in calls] == [
        ("https://internal.example/search", "q1", expected_proxy),
        ("https://fallback.example/search", "q1", expected_proxy),
        ("https://internal.example/search", "q2", expected_proxy),
        ("https://fallback.example/search", "q2", expected_proxy),
    ]
    assert all(headers["authorization"] == "Bearer token" for *_prefix, headers in calls)
    assert all(headers["deviceid"] == "device-1" for *_prefix, headers in calls)
    assert all(headers["cookie"] == "cookie=1" for *_prefix, headers in calls)


def test_smoke_can_check_fallback_without_cookie(tmp_path: Path, monkeypatch) -> None:
    keeper = _load_keeper()
    _enable_proxy(monkeypatch)
    cookie_path = tmp_path / "wb_cookie.txt"
    cookie_path.write_text("cookie=1\n", encoding="utf-8")

    config = {
        "runtime": {"http_timeout_seconds": 5},
        "serp": {
            "wb_cookie_file": str(cookie_path),
            "base_url": "https://fallback.example/search",
            "input_files": {"queries_txt": str(tmp_path / "missing.txt")},
            "request_params": {},
            "request_headers": {"authorization": "Bearer token"},
        },
    }
    args = argparse.Namespace(
        cookie_file=str(cookie_path),
        state_json=str(tmp_path / "state.json"),
        query="q1",
        sample_count=1,
        min_successes=1,
        page=1,
        without_cookie=True,
    )

    class Response:
        status_code = 200
        text = ""

        def json(self):
            return {"products": [{"name": "ok"}]}

    seen_headers: list[dict[str, str]] = []

    def fake_get(_session, url, *, params, headers, timeout):
        seen_headers.append(headers)
        return Response()

    monkeypatch.setattr(keeper.requests.Session, "get", fake_get)

    assert keeper.smoke(config, args, emit=False) is True
    assert seen_headers
    assert "cookie" not in seen_headers[0]
    assert seen_headers[0]["authorization"] == "Bearer token"


def test_smoke_without_proxy_fails_before_requests_call(
    tmp_path: Path,
    monkeypatch,
) -> None:
    keeper = _load_keeper()
    cookie_path = tmp_path / "wb_cookie.txt"
    cookie_path.write_text("cookie=1\n", encoding="utf-8")
    config = {
        "runtime": {"http_timeout_seconds": 5},
        "serp": {
            "wb_cookie_file": str(cookie_path),
            "base_url": "https://search.example.test",
            "request_params": {},
        },
    }
    args = argparse.Namespace(
        cookie_file=str(cookie_path),
        state_json="",
        query="q1",
        sample_count=1,
        min_successes=1,
        page=1,
    )
    calls = 0

    def forbidden_get(*_args, **_kwargs):
        nonlocal calls
        calls += 1
        raise AssertionError("network must not be called")

    monkeypatch.setenv("PARSER_WB_RUNTIME_ENV_LOADED", "1")
    monkeypatch.setenv("PARSER_WB_RUNTIME_ENV_SHA256", "a" * 64)
    monkeypatch.delenv("PARSER_WB_PROXY_URL", raising=False)
    monkeypatch.setattr(keeper.requests.Session, "get", forbidden_get)

    with pytest.raises(Exception, match="marketplace_proxy_env_missing"):
        keeper.smoke(config, args, emit=False)
    assert calls == 0


def _refresh_args(tmp_path: Path) -> SimpleNamespace:
    return SimpleNamespace(
        cookie_file=str(tmp_path / "wb_cookie.txt"),
        storage_state=str(tmp_path / "missing-storage-state.json"),
        storage_state_out=str(tmp_path / "storage-state-out.json"),
        state_json="",
        browser_channel="chrome",
        no_headless=False,
        headed=False,
        require_storage_state=False,
        refresh_url="https://www.wildberries.ru/",
        timeout_ms=1000,
        wait_ms=0,
    )


def test_refresh_without_proxy_fails_before_playwright_context(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    keeper = _load_keeper()
    calls = 0
    sync_api = ModuleType("playwright.sync_api")

    def forbidden_playwright():
        nonlocal calls
        calls += 1
        raise AssertionError("browser must not start")

    sync_api.sync_playwright = forbidden_playwright  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "playwright.sync_api", sync_api)
    monkeypatch.setenv("PARSER_WB_RUNTIME_ENV_LOADED", "1")
    monkeypatch.setenv("PARSER_WB_RUNTIME_ENV_SHA256", "a" * 64)
    monkeypatch.delenv("PARSER_WB_PROXY_URL", raising=False)

    with pytest.raises(Exception, match="marketplace_proxy_env_missing"):
        keeper.refresh(
            {"serp": {"proxy_url_env": "PARSER_WB_PROXY_URL"}},
            _refresh_args(tmp_path),
        )

    assert calls == 0


def test_refresh_passes_explicit_proxy_to_browser(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    keeper = _load_keeper()
    _enable_proxy(
        monkeypatch,
        "http://user:test-only@proxy.example.test:8080",
    )
    captured: dict[str, object] = {}
    sync_api = ModuleType("playwright.sync_api")

    class Chromium:
        @staticmethod
        def launch(**kwargs):
            captured.update(kwargs)
            raise RuntimeError("stop after launch contract check")

    class Playwright:
        chromium = Chromium()

    class ContextManager:
        def __enter__(self):
            return Playwright()

        def __exit__(self, *_args):
            return False

    sync_api.sync_playwright = lambda: ContextManager()  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "playwright.sync_api", sync_api)

    assert (
        keeper.refresh(
            {"serp": {"proxy_url_env": "PARSER_WB_PROXY_URL"}},
            _refresh_args(tmp_path),
        )
        is False
    )
    assert captured["proxy"] == {
        "server": "http://proxy.example.test:8080",
        "username": "user",
        "password": "test-only",
    }
