from __future__ import annotations

import json
import os
import shutil
import subprocess
from pathlib import Path

import pytest


PROJECT_ROOT = Path(__file__).resolve().parents[1]
PYTHON_BIN = Path("/home/Codex/agent-tools/parser_wb-python/bin/python")


def _copy_launcher(tmp_path: Path, launcher_name: str) -> Path:
    project = tmp_path / "project"
    scripts = project / "scripts"
    config = project / "config"
    scripts.mkdir(parents=True)
    config.mkdir(parents=True)
    for relative in (
        "scripts/wb_runtime_env.sh",
        "scripts/wb_runtime_env.py",
        "app/__init__.py",
        "app/common/__init__.py",
        "app/common/durable_atomic.py",
        "app/common/exceptions.py",
        "app/common/nightly_coordinator.py",
        "app/common/runtime_env.py",
    ):
        source = PROJECT_ROOT / relative
        target = project / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source, target)
        if relative == "scripts/wb_runtime_env.py":
            target.write_text(
                target.read_text(encoding="utf-8").replace(
                    "require_official_live_entry_lease(environment=os.environ)",
                    "None  # isolated fixture: lock-v3 is covered separately",
                ),
                encoding="utf-8",
            )
    launcher = scripts / launcher_name
    launcher.write_text(
        (PROJECT_ROOT / f"scripts/{launcher_name}")
        .read_text(encoding="utf-8")
        .replace(
            "/run/lock/parser-nightly-coordinator",
            str(tmp_path / "isolated-coordinator-lock-not-present"),
        ),
        encoding="utf-8",
    )
    launcher.chmod(0o755)
    return project


def _write_fake_main(project: Path, capture_path: Path) -> None:
    (project / "main.py").write_text(
        "import json, os, sys\n"
        "payload = {\n"
        "  'runtime_loaded': os.getenv('PARSER_WB_RUNTIME_ENV_LOADED'),\n"
        "  'runtime_sha256_present': len(os.getenv('PARSER_WB_RUNTIME_ENV_SHA256', '')) == 64,\n"
        "  'proxy_present': bool(os.getenv('PARSER_WB_PROXY_URL')),\n"
        "  'headers_present': bool(os.getenv('PARSER_WB_REQUEST_HEADERS_FILE')),\n"
        "  'cookie_required_present': 'PARSER_WB_COOKIE_REQUIRED' in os.environ,\n"
        "  'argv': sys.argv[1:],\n"
        "}\n"
        f"open({str(capture_path)!r}, 'w', encoding='utf-8').write(json.dumps(payload))\n",
        encoding="utf-8",
    )


def _write_fake_keeper(project: Path, capture_path: Path) -> None:
    (project / "scripts/wb_cookie_keeper.py").write_text(
        "import json, os, sys\n"
        "payload = {\n"
        "  'runtime_loaded': os.getenv('PARSER_WB_RUNTIME_ENV_LOADED'),\n"
        "  'runtime_sha256_present': len(os.getenv('PARSER_WB_RUNTIME_ENV_SHA256', '')) == 64,\n"
        "  'argv': sys.argv[1:],\n"
        "}\n"
        f"open({str(capture_path)!r}, 'w', encoding='utf-8').write(json.dumps(payload))\n",
        encoding="utf-8",
    )


def _write_runtime_env(project: Path) -> Path:
    runtime_env = project / "config/runtime.env"
    runtime_env.write_text(
        "PARSER_WB_PROXY_URL=http://proxy.example.test:8080\n"
        "PARSER_WB_REQUEST_HEADERS_FILE=config/wb_request_headers.json\n"
        "PARSER_WB_COOKIE_REQUIRED=0\n",
        encoding="utf-8",
    )
    runtime_env.chmod(0o600)
    headers = project / "config/wb_request_headers.json"
    headers.write_text("{}\n", encoding="utf-8")
    headers.chmod(0o600)
    cookie = project / "config/wb_cookie.txt"
    cookie.write_text("test-cookie\n", encoding="utf-8")
    cookie.chmod(0o600)
    return runtime_env


def test_guarded_launcher_sources_runtime_before_python(
    tmp_path: Path,
) -> None:
    project = _copy_launcher(
        tmp_path,
        "run_wb_guarded_regional_pilot.sh",
    )
    capture = tmp_path / "capture.json"
    _write_fake_main(project, capture)
    _write_runtime_env(project)

    result = subprocess.run(
        [str(project / "scripts/run_wb_guarded_regional_pilot.sh")],
        cwd=project,
        text=True,
        capture_output=True,
        check=False,
    )

    assert result.returncode == 0
    payload = json.loads(capture.read_text(encoding="utf-8"))
    assert payload["runtime_loaded"] == "1"
    assert payload["runtime_sha256_present"] is True
    assert payload["proxy_present"] is True
    assert payload["headers_present"] is True
    assert payload["cookie_required_present"] is True
    assert payload["argv"][-2:] == ["--no-publish", "--guarded-pilot"]


def test_collection_plan_launcher_sources_runtime_and_forwards_plan(
    tmp_path: Path,
) -> None:
    project = _copy_launcher(tmp_path, "run_wb_collection_plan.sh")
    capture = tmp_path / "capture.json"
    scripts = project / "scripts"
    (scripts / "run_wb_collection_plan.py").write_text(
        "import json, os, sys\n"
        "payload = {\n"
        "  'runtime_loaded': os.getenv('PARSER_WB_RUNTIME_ENV_LOADED'),\n"
        "  'runtime_sha256_present': len(os.getenv('PARSER_WB_RUNTIME_ENV_SHA256', '')) == 64,\n"
        "  'proxy_present': bool(os.getenv('PARSER_WB_PROXY_URL')),\n"
        "  'argv': sys.argv[1:],\n"
        "}\n"
        f"open({str(capture)!r}, 'w', encoding='utf-8').write(json.dumps(payload))\n",
        encoding="utf-8",
    )
    _write_runtime_env(project)

    result = subprocess.run(
        [
            str(scripts / "run_wb_collection_plan.sh"),
            "--config",
            "config/config.yaml",
            "--plan-file",
            "config/wb/collection_plans/test.json",
            "--no-publish",
        ],
        cwd=project,
        text=True,
        capture_output=True,
        check=False,
    )

    assert result.returncode == 0
    payload = json.loads(capture.read_text(encoding="utf-8"))
    assert payload["runtime_loaded"] == "1"
    assert payload["runtime_sha256_present"] is True
    assert payload["proxy_present"] is True
    assert payload["argv"] == [
        "--config",
        "config/config.yaml",
        "--plan-file",
        "config/wb/collection_plans/test.json",
        "--no-publish",
    ]


def test_collection_plan_launcher_rejects_guarded_pilot_before_python(
    tmp_path: Path,
) -> None:
    project = _copy_launcher(tmp_path, "run_wb_collection_plan.sh")
    capture = tmp_path / "python-started"
    (project / "scripts/run_wb_collection_plan.py").write_text(
        f"from pathlib import Path\nPath({str(capture)!r}).write_text('started')\n",
        encoding="utf-8",
    )

    result = subprocess.run(
        [
            str(project / "scripts/run_wb_collection_plan.sh"),
            "--guarded-pilot",
        ],
        cwd=project,
        text=True,
        capture_output=True,
        check=False,
    )

    assert result.returncode == 2
    assert not capture.exists()
    assert "run_wb_guarded_regional_pilot.sh" in result.stderr
    assert "Traceback" not in result.stderr


@pytest.mark.parametrize("runtime_state", ["missing", "symlink", "unreadable"])
def test_guarded_launcher_rejects_unsafe_runtime_before_python(
    tmp_path: Path,
    runtime_state: str,
) -> None:
    project = _copy_launcher(
        tmp_path,
        "run_wb_guarded_regional_pilot.sh",
    )
    capture = tmp_path / "capture.json"
    _write_fake_main(project, capture)
    runtime_env = project / "config/runtime.env"
    if runtime_state == "symlink":
        target = tmp_path / "external-runtime.env"
        target.write_text(
            "PARSER_WB_PROXY_URL=http://proxy.example.test:8080\n",
            encoding="utf-8",
        )
        runtime_env.symlink_to(target)
    elif runtime_state == "unreadable":
        runtime_env.write_text(
            "PARSER_WB_PROXY_URL=http://proxy.example.test:8080\n",
            encoding="utf-8",
        )
        runtime_env.chmod(0)

    result = subprocess.run(
        [str(project / "scripts/run_wb_guarded_regional_pilot.sh")],
        cwd=project,
        text=True,
        capture_output=True,
        check=False,
    )

    if runtime_state == "unreadable" and result.returncode == 0:
        pytest.skip("current user can read mode-000 test files")
    assert result.returncode != 0
    assert not capture.exists()
    assert "proxy.example.test" not in result.stdout + result.stderr


def test_live_component_launcher_sources_runtime_and_forwards_target(
    tmp_path: Path,
) -> None:
    project = _copy_launcher(tmp_path, "run_wb_live_component.sh")
    capture = tmp_path / "capture.json"
    _write_fake_main(project, capture)
    _write_runtime_env(project)

    result = subprocess.run(
        [
            str(project / "scripts/run_wb_live_component.sh"),
            "sellers",
            "--job-id",
            "test",
        ],
        cwd=project,
        text=True,
        capture_output=True,
        check=False,
    )

    assert result.returncode == 0
    payload = json.loads(capture.read_text(encoding="utf-8"))
    assert payload["runtime_loaded"] == "1"
    assert payload["argv"][-4:] == [
        "run",
        "sellers",
        "--job-id",
        "test",
    ]


def test_access_launcher_sources_runtime_and_forwards_keeper_target(
    tmp_path: Path,
) -> None:
    project = _copy_launcher(tmp_path, "run_wb_access_tool.sh")
    capture = tmp_path / "capture.json"
    _write_fake_keeper(project, capture)
    _write_runtime_env(project)

    result = subprocess.run(
        [
            str(project / "scripts/run_wb_access_tool.sh"),
            "smoke",
            "--sample-count",
            "1",
        ],
        cwd=project,
        text=True,
        capture_output=True,
        check=False,
    )

    assert result.returncode == 0
    payload = json.loads(capture.read_text(encoding="utf-8"))
    assert payload["runtime_loaded"] == "1"
    assert payload["runtime_sha256_present"] is True
    assert payload["argv"] == ["smoke", "--sample-count", "1"]


def test_all_marketplace_wrappers_use_required_runtime_loader() -> None:
    wrapper_names = (
        "run_products_sellers_daily.sh",
        "run_wb_cookie_renewal.sh",
        "run_wb_nightly_preflight.sh",
        "run_wb_persistent_session.sh",
        "run_wb_persistent_watchdog.sh",
        "run_wb_guarded_regional_pilot.sh",
        "run_wb_collection_plan.sh",
        "run_wb_live_component.sh",
        "run_wb_access_tool.sh",
    )
    for name in wrapper_names:
        source = (PROJECT_ROOT / "scripts" / name).read_text(encoding="utf-8")
        assert 'source "$RUNTIME_LOADER"' in source
        assert "wb_load_required_runtime_env" in source


def test_nightly_preflight_enforces_if_present_authorization_horizon() -> None:
    source = (PROJECT_ROOT / "scripts/run_wb_nightly_preflight.sh").read_text(encoding="utf-8")
    assert "--authorization-policy if_present" in source
    assert "--authorization-policy required" not in source
    assert '--authorization-horizon-plan-file "$AUTHORIZATION_HORIZON_PLAN"' in source
    assert (
        'AUTHORIZATION_HORIZON_PLAN="$PROJECT_DIR/config/wb/collection_plans/'
        'shevron-four-regions-top1000-v2.json"'
    ) in source


def test_cookie_maintenance_uses_bounded_headed_xvfb_persistent_profile() -> None:
    renewal = (PROJECT_ROOT / "scripts/run_wb_cookie_renewal.sh").read_text(
        encoding="utf-8"
    )
    preflight = (PROJECT_ROOT / "scripts/run_wb_nightly_preflight.sh").read_text(
        encoding="utf-8"
    )
    legacy = (PROJECT_ROOT / "scripts/run_products_sellers_daily.sh").read_text(
        encoding="utf-8"
    )

    assert 'XVFB_RUN="/usr/bin/xvfb-run"' in renewal
    assert 'TIMEOUT_BIN="/usr/bin/timeout"' in renewal
    assert "BROWSER_INVOCATION_TIMEOUT_SECONDS=600" in renewal
    assert '"$TIMEOUT_BIN" --signal=TERM --kill-after=10s' in renewal
    assert '"$XVFB_RUN" --auto-servernum' in renewal
    assert "--headed" in renewal
    assert "--browser-channel chrome" in renewal
    assert '--browser-profile-dir "$BROWSER_PROFILE_DIR"' in renewal
    assert "--authorization-policy if_present" in renewal
    assert 'XVFB_RUN="/usr/bin/xvfb-run"' in preflight
    assert 'TIMEOUT_BIN="/usr/bin/timeout"' in preflight
    assert "BROWSER_INVOCATION_TIMEOUT_SECONDS=600" in preflight
    assert '"$TIMEOUT_BIN" --signal=TERM --kill-after=10s' in preflight
    assert '"$XVFB_RUN" --auto-servernum' in preflight
    assert "--headed" in preflight
    assert '--browser-profile-dir "$BROWSER_PROFILE_DIR"' in preflight
    assert "xvfb-run" not in legacy


@pytest.mark.parametrize(
    "script_name",
    ["wb_cookie_keeper.py", "wb_persistent_session.py"],
)
def test_real_script_path_bootstraps_project_imports_from_external_cwd(
    tmp_path: Path,
    script_name: str,
) -> None:
    result = subprocess.run(
        [
            str(PYTHON_BIN),
            "-B",
            "-c",
            (
                "import runpy; "
                f"runpy.run_path({str(PROJECT_ROOT / 'scripts' / script_name)!r}, "
                "run_name='import_only')"
            ),
        ],
        cwd=tmp_path,
        text=True,
        capture_output=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr
    assert "ModuleNotFoundError" not in result.stderr
    assert "No module named 'app'" not in result.stderr


def test_real_keeper_launcher_imports_then_fails_closed_before_network(
    tmp_path: Path,
) -> None:
    project = tmp_path / "project"
    scripts = project / "scripts"
    common = project / "app/common"
    config = project / "config"
    scripts.mkdir(parents=True)
    common.mkdir(parents=True)
    config.mkdir(parents=True)
    for relative in (
        "scripts/wb_runtime_env.sh",
        "scripts/wb_runtime_env.py",
        "scripts/run_wb_access_tool.sh",
        "scripts/wb_cookie_keeper.py",
        "app/__init__.py",
        "app/common/__init__.py",
        "app/common/durable_atomic.py",
        "app/common/exceptions.py",
            "app/common/nightly_coordinator.py",
            "app/common/nightly_attestation.py",
            "app/common/proxy_required.py",
        "app/common/runtime_env.py",
    ):
        source = PROJECT_ROOT / relative
        target = project / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source, target)
        if relative == "scripts/wb_runtime_env.py":
            target.write_text(
                target.read_text(encoding="utf-8").replace(
                    "require_official_live_entry_lease(environment=os.environ)",
                    "None  # isolated fixture: lock-v3 is covered separately",
                ),
                encoding="utf-8",
            )
        elif relative == "scripts/wb_cookie_keeper.py":
            target.write_text(
                target.read_text(encoding="utf-8").replace(
                    "/run/lock/parser-nightly-coordinator",
                    str(tmp_path / "isolated-coordinator-lock-not-present"),
                ),
                encoding="utf-8",
            )
    launcher = scripts / "run_wb_access_tool.sh"
    launcher.write_text(
        launcher.read_text(encoding="utf-8").replace(
            "/run/lock/parser-nightly-coordinator",
            str(tmp_path / "isolated-coordinator-lock-not-present"),
        ),
        encoding="utf-8",
    )
    (scripts / "run_wb_access_tool.sh").chmod(0o755)
    (config / "runtime.env").write_text(
        "PARSER_WB_COOKIE_REQUIRED=0\n",
        encoding="utf-8",
    )
    (config / "runtime.env").chmod(0o600)
    (config / "config.yaml").write_text(
        "runtime:\n"
        "  http_timeout_seconds: 1\n"
        "serp:\n"
        "  proxy_url_env: PARSER_WB_PROXY_URL\n"
        "  wb_cookie_file: config/wb_cookie.txt\n"
        "  base_url: http://127.0.0.1:9/forbidden\n"
        "  request_params: {}\n",
        encoding="utf-8",
    )
    (config / "wb_cookie.txt").write_text("test_cookie=1\n", encoding="utf-8")
    (config / "wb_cookie.txt").chmod(0o600)
    network_sentinel = tmp_path / "network-called"
    hook_dir = tmp_path / "hook"
    hook_dir.mkdir()
    (hook_dir / "sitecustomize.py").write_text(
        "from pathlib import Path\n"
        "import requests.sessions\n"
        f"_sentinel = Path({str(network_sentinel)!r})\n"
        "def _forbidden(*args, **kwargs):\n"
        "    _sentinel.write_text('called', encoding='utf-8')\n"
        "    raise AssertionError('network forbidden')\n"
        "requests.sessions.Session.request = _forbidden\n",
        encoding="utf-8",
    )
    env = os.environ.copy()
    env["PYTHONPATH"] = str(hook_dir)
    env.pop("PARSER_WB_PROXY_URL", None)

    result = subprocess.run(
        [
            str(scripts / "run_wb_access_tool.sh"),
            "smoke",
            "--config",
            str(config / "config.yaml"),
            "--cookie-file",
            str(config / "wb_cookie.txt"),
            "--query",
            "offline-test",
            "--sample-count",
            "1",
        ],
        cwd=project,
        env=env,
        text=True,
        capture_output=True,
        check=False,
    )

    assert result.returncode == 20
    assert "MarketplaceProxyError" in result.stderr
    assert "marketplace_proxy_env_missing" in result.stderr
    assert "ModuleNotFoundError" not in result.stderr
    assert not network_sentinel.exists()
