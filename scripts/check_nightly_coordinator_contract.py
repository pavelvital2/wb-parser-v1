#!/home/Codex/agent-tools/parser_wb-python/bin/python
from __future__ import annotations

import hashlib
import json
import os
import stat
import sys
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.common.nightly_attestation import (
    MANIFEST_SCHEMA_VERSION,
    MANIFEST_RELATIVE_PATH,
    verify_input_manifest,
)

DEPLOYED_PROJECT_ROOT = Path("/home/pavel/projects/parser_wb")
ADAPTER = PROJECT_ROOT / "scripts/run_wb_four_region_nightly.sh"
PLAN = (
    PROJECT_ROOT
    / "config/wb/collection_plans/shevron-four-regions-top1000-v2.json"
)
REGISTRY = PROJECT_ROOT / "config/wb/regions.json"
QUERY_PACK = (
    PROJECT_ROOT
    / "config/wb/query_packs/shevron-core/2026-07-26.1.json"
)
TRACKED_CONFIG = PROJECT_ROOT / "config/config.yaml"
DIRECT_ATTESTATION_ROOTS = (
    PROJECT_ROOT / MANIFEST_RELATIVE_PATH,
    PROJECT_ROOT / "app/common/nightly_attestation.py",
    PROJECT_ROOT / "app/common/nightly_coordinator.py",
    PROJECT_ROOT / "app/common/runtime_env.py",
    PROJECT_ROOT / "app/common/durable_atomic.py",
    PROJECT_ROOT / "app/common/state_db.py",
    PROJECT_ROOT / "app/webui/app.py",
    PROJECT_ROOT / "app/webui/services.py",
    PROJECT_ROOT / "scripts/check_nightly_coordinator_contract.py",
    PROJECT_ROOT / "scripts/marketplace_lock_v3_supervisor.py",
    PROJECT_ROOT / "scripts/wb_nightly_coordinator_adapter.py",
    PROJECT_ROOT / "scripts/wb_runtime_env.py",
    PROJECT_ROOT / "scripts/wb_runtime_env.sh",
    PROJECT_ROOT / "scripts/run_wb_four_region_nightly.py",
    ADAPTER,
    PLAN,
    REGISTRY,
    QUERY_PACK,
    TRACKED_CONFIG,
)
RESULT_SCHEMA = "marketplace_parser_result_v3"
LOCK_CONTRACT = "marketplace_collection_lock_v3"
QUARANTINE_CONTRACT = "marketplace_collection_quarantine_v1"
CHECK_SCHEMA = "parser_coordinator_contract_check_v2"
QUARANTINE_PATH = (
    "/var/lib/parser-nightly-coordinator/unsafe-cleanup-quarantine.json"
)
FOUR_REGIONS = (
    "moscow",
    "rostov-on-don",
    "novosibirsk",
    "kazan",
)
OFFICIAL_ENTRYPOINTS = (
    "run_products_sellers_daily.sh",
    "run_wb_collection_plan.sh",
    "run_wb_guarded_regional_pilot.sh",
    "run_wb_live_component.sh",
    "run_wb_cookie_renewal.sh",
    "run_wb_nightly_preflight.sh",
    "run_wb_access_tool.sh",
    "run_wb_warehouse_refresh.sh",
)
COORDINATOR_DISABLED_ENTRYPOINTS = (
    "run_wb_persistent_session.sh",
    "run_wb_persistent_watchdog.sh",
)
DIRECT_PYTHON_ENTRYPOINTS = (
    "run_wb_collection_plan.py",
    "run_wb_four_region_nightly.py",
    "wb_cookie_keeper.py",
    "wb_nightly_preflight.py",
    "wb_persistent_session.py",
    "wb_persistent_session_watchdog.py",
    "wb_warehouse.py",
)


class CheckError(RuntimeError):
    pass


def _sha256(encoded: bytes) -> str:
    return hashlib.sha256(encoded).hexdigest()


def _command_sha256(command: tuple[str, ...]) -> str:
    return _sha256(
        json.dumps(
            list(command),
            ensure_ascii=False,
            separators=(",", ":"),
        ).encode("utf-8")
    )


def _read_safe(path: Path, *, executable: bool = False, mode: int = 0o644) -> bytes:
    if not path.is_absolute():
        raise CheckError("unsafe input path")
    current = Path(path.anchor)
    for part in path.parts[1:-1]:
        current /= part
        if stat.S_ISLNK(current.lstat().st_mode):
            raise CheckError("symlink ancestor")
    flags = os.O_RDONLY | os.O_CLOEXEC
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    fd = os.open(path, flags)
    try:
        info = os.fstat(fd)
        expected_mode = 0o755 if executable else mode
        if (
            not stat.S_ISREG(info.st_mode)
            or info.st_uid != os.geteuid()
            or stat.S_IMODE(info.st_mode) != expected_mode
            or info.st_mode & 0o022
            or info.st_nlink != 1
            or info.st_size <= 0
            or info.st_size > 32 * 1024 * 1024
        ):
            raise CheckError("unsafe input file")
        encoded = b""
        remaining = info.st_size
        while remaining:
            chunk = os.read(fd, min(65536, remaining))
            if not chunk:
                raise CheckError("input changed while reading")
            encoded += chunk
            remaining -= len(chunk)
        if os.read(fd, 1):
            raise CheckError("input grew while reading")
    finally:
        os.close(fd)
    return encoded


def _json_object(encoded: bytes, field: str) -> dict[str, Any]:
    try:
        value = json.loads(encoded.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise CheckError(f"{field} is invalid") from exc
    if not isinstance(value, dict):
        raise CheckError(f"{field} is invalid")
    return value


def _configured_command(stage: str) -> tuple[str, ...]:
    base = (
        str(DEPLOYED_PROJECT_ROOT / "scripts/run_wb_four_region_nightly.sh"),
        "--plan-file",
        "config/wb/collection_plans/shevron-four-regions-top1000-v2.json",
        "--no-publish",
    )
    if stage == "wb_initial":
        return base
    if stage == "wb_resume":
        return (*base, "--resume-run-id", "{resume_ref}")
    raise CheckError("unsupported stage")


def _validate_sources() -> list[dict[str, str]]:
    if (
        stat.S_IMODE(ADAPTER.stat().st_mode) != 0o755
        or stat.S_IMODE(PLAN.stat().st_mode) != 0o644
    ):
        raise CheckError("production target mode mismatch")
    adapter_source = _read_safe(ADAPTER, executable=True).decode("utf-8")
    if (
        "wb_nightly_coordinator_adapter.py" not in adapter_source
        or "four-region" not in adapter_source
        or "run_wb_collection_plan.sh" in adapter_source
    ):
        raise CheckError("adapter target is invalid")
    plan_bytes = _read_safe(PLAN)
    registry_bytes = _read_safe(REGISTRY)
    pack_bytes = _read_safe(QUERY_PACK)
    plan = _json_object(plan_bytes, "plan")
    registry = _json_object(registry_bytes, "registry")
    pack = _json_object(pack_bytes, "query pack")
    cli_source = _read_safe(PROJECT_ROOT / "app/common/cli.py").decode("utf-8-sig")
    inner_source = _read_safe(
        PROJECT_ROOT / "scripts/run_wb_four_region_nightly.py",
        executable=True,
    ).decode("utf-8")
    collection_source = _read_safe(
        PROJECT_ROOT / "scripts/run_wb_collection_plan.py",
        executable=True,
    ).decode("utf-8")
    manifest = _json_object(
        _read_safe(PROJECT_ROOT / MANIFEST_RELATIVE_PATH),
        "input manifest",
    )
    runtime_shell_source = _read_safe(
        PROJECT_ROOT / "scripts/wb_runtime_env.sh",
        executable=True,
    ).decode("utf-8")
    runtime_python_source = _read_safe(
        PROJECT_ROOT / "scripts/wb_runtime_env.py",
    ).decode("utf-8")
    runtime_contract_source = _read_safe(
        PROJECT_ROOT / "app/common/runtime_env.py",
    ).decode("utf-8")
    attestation_source = _read_safe(
        PROJECT_ROOT / "app/common/nightly_attestation.py",
    ).decode("utf-8")
    durable_source = _read_safe(
        PROJECT_ROOT / "app/common/durable_atomic.py",
    ).decode("utf-8")
    paths_source = _read_safe(
        PROJECT_ROOT / "app/common/paths.py",
    ).decode("utf-8-sig")
    run_report_source = _read_safe(
        PROJECT_ROOT / "app/common/run_report.py",
    ).decode("utf-8-sig")
    filter_source = _read_safe(
        PROJECT_ROOT / "app/filter/engine.py",
    ).decode("utf-8-sig")
    serp_source = _read_safe(
        PROJECT_ROOT / "app/serp/engine.py",
    ).decode("utf-8-sig")
    legacy_warehouse_source = _read_safe(
        PROJECT_ROOT / "scripts/wb_warehouse.py",
        executable=True,
    ).decode("utf-8")
    warehouse_refresh_source = _read_safe(
        PROJECT_ROOT / "scripts/run_wb_warehouse_refresh.sh",
        executable=True,
    ).decode("utf-8")
    coordinator_source = _read_safe(
        PROJECT_ROOT / "app/common/nightly_coordinator.py",
    ).decode("utf-8")
    scoped_source = _read_safe(
        PROJECT_ROOT / "app/serp/collection_plan_runner.py",
    ).decode("utf-8")
    downstream_source = _read_safe(
        PROJECT_ROOT / "app/serp/four_region_nightly.py",
    ).decode("utf-8")
    warehouse_source = _read_safe(
        PROJECT_ROOT / "app/warehouse/wb_regional.py",
    ).decode("utf-8")
    webui_app_source = _read_safe(
        PROJECT_ROOT / "app/webui/app.py",
    ).decode("utf-8-sig")
    webui_services_source = _read_safe(
        PROJECT_ROOT / "app/webui/services.py",
    ).decode("utf-8-sig")
    if (
        manifest.get("schema_version") != MANIFEST_SCHEMA_VERSION
        or "requirements.txt" not in manifest.get("files", [])
        or not isinstance(manifest.get("python_runtime"), dict)
        or not isinstance(
            manifest.get("python_runtime", {}).get("dependencies"),
            dict,
        )
    ):
        raise CheckError("runtime attestation manifest contract mismatch")
    if (
        not _read_safe(
            PROJECT_ROOT / "scripts/check_nightly_coordinator_contract.py",
            executable=True,
        ).startswith(
            b"#!/home/Codex/agent-tools/parser_wb-python/bin/python\n"
        )
        or
        'source "$runtime_env_file"' in runtime_shell_source
        or ". \"$runtime_env_file\"" in runtime_shell_source
        or "wb_runtime_env.py" not in runtime_shell_source
        or "require_official_live_entry_lease(environment=os.environ)"
        not in runtime_python_source
        or "load_strict_runtime_environment" not in runtime_python_source
        or "subprocess" in runtime_contract_source
        or "runtime_cookie_path_invalid" not in runtime_contract_source
        or "APPROVED_PYTHON_BIN" not in attestation_source
        or "APPROVED_SITE_PACKAGES" not in attestation_source
        or "coordinator_python_runtime_mismatch" not in attestation_source
    ):
        raise CheckError("runtime loading/attestation contract mismatch")
    if (
        "os.O_NOFOLLOW" not in durable_source
        or "os.fsync(temp_fd)" not in durable_source
        or "os.fsync(directory_fd)" not in durable_source
        or "integrity_gate()" not in durable_source
        or "require_absent=True" not in coordinator_source
        or "integrity_gate=integrity_gate" not in coordinator_source
        or "integrity_gate=integrity_gate" not in scoped_source
        or "integrity_gate=lease.integrity_gate" not in downstream_source
        or warehouse_source.count("integrity_gate()") < 2
        or "integrity_gate=input_integrity_gate" not in downstream_source
        or "durable_atomic_copy(" not in paths_source
        or "shutil.copy" in paths_source
        or run_report_source.count("durable_atomic_replace(") != 2
        or ".write_text(" in run_report_source
        or "publish_output_copy(" not in filter_source
        or serp_source.count("publish_output_copy(") < 2
        or legacy_warehouse_source.count("durable_atomic_replace(") != 2
        or "latest.write_text(" in legacy_warehouse_source
        or warehouse_refresh_source.count("durable_atomic_replace(") != 2
        or "latest.write_text(" in warehouse_refresh_source
        or "history.write_text(" in warehouse_refresh_source
    ):
        raise CheckError("durable publication contract mismatch")
    if (
        "db.init_schema()" in webui_app_source
        or webui_services_source.count(
            "require_official_live_entry_lease()"
        )
        < 2
        or webui_services_source.count(
            "integrity_gate=integrity_gate(self.config.project_root)"
        )
        < 2
        or "run_wb_live_component.sh" not in webui_services_source
        or "sys.executable" in webui_services_source
    ):
        raise CheckError("WebUI maintenance lease contract mismatch")
    doctor_source = cli_source[
        cli_source.index("def cmd_doctor"):
        cli_source.index("\ndef cmd_runs")
    ]
    runs_source = cli_source[
        cli_source.index("def cmd_runs"):
        cli_source.index("\ndef cmd_cleanup")
    ]
    if (
        "init_schema" in doctor_source
        or "init_schema" in runs_source
        or "list_runs_read_only" not in runs_source
        or "schema_snapshot_read_only" not in cli_source
    ):
        raise CheckError("read-only CLI state contract mismatch")
    if (
        "require_official_live_entry_lease(environment=os.environ)"
        not in cli_source
        or '"cleanup", "collection-plan", "run"' not in cli_source
        or "require_official_live_entry_lease(environment=os.environ)"
        not in inner_source
        or "require_official_live_entry_lease(environment=os.environ)"
        not in collection_source
    ):
        raise CheckError("direct live entry lock contract mismatch")
    for name in DIRECT_PYTHON_ENTRYPOINTS:
        source = _read_safe(
            PROJECT_ROOT / "scripts" / name,
            executable=True,
        ).decode("utf-8")
        expected_guard = (
            "_require_host_lease_after_cutover()"
            if name in {"wb_cookie_keeper.py", "wb_warehouse.py"}
            else "require_official_live_entry_lease(environment=os.environ)"
        )
        if expected_guard not in source:
            raise CheckError("direct Python entrypoint contract mismatch")
    if (
        plan.get("schema_version") != "wb_collection_plan_v2"
        or plan.get("collection_plan_id")
        != "shevron-four-regions-top1000-v2"
        or plan.get("enabled") is not False
        or plan.get("query_pack_file")
        != "config/wb/query_packs/shevron-core/2026-07-26.1.json"
        or plan.get("region_set") != list(FOUR_REGIONS)
        or plan.get("depth") != 1000
        or plan.get("publication_mode") != "none"
        or plan.get("sellers_mode") != "disabled"
        or plan.get("proxy_rotation_mode") != "disabled"
    ):
        raise CheckError("plan contract mismatch")
    queries = pack.get("queries")
    if (
        pack.get("query_pack_id") != "shevron-core"
        or pack.get("version") != "2026-07-26.1"
        or pack.get("enabled") is not True
        or not isinstance(queries, list)
        or len(queries) != 30
        or any(
            not isinstance(item, dict)
            or item.get("enabled") is not True
            or not isinstance(item.get("query_id"), str)
            or not isinstance(item.get("text"), str)
            for item in queries
        )
        or len({item["query_id"] for item in queries}) != 30
    ):
        raise CheckError("query pack contract mismatch")
    region_entries = {
        item.get("region_id"): item
        for item in registry.get("regions", [])
        if isinstance(item, dict)
    }
    if set(FOUR_REGIONS) - set(region_entries) or any(
        region_entries[region_id].get("enabled") is not False
        or region_entries[region_id].get("resolver") != "wb_geo_xinfo"
        for region_id in FOUR_REGIONS
    ):
        raise CheckError("region registry contract mismatch")
    official_sources: list[tuple[Path, bytes]] = []
    for name in OFFICIAL_ENTRYPOINTS:
        path = PROJECT_ROOT / "scripts" / name
        source_bytes = _read_safe(
            path,
            executable=True,
            mode=0o755,
        )
        source = source_bytes.decode("utf-8")
        bootstrap = (
            'if [[ -e "$COORDINATOR_LOCK_DIR" '
            '|| -L "$COORDINATOR_LOCK_DIR" ]]; then'
        )
        bootstrap_index = source.find(bootstrap)
        exec_index = source.find(
            'exec "$PYTHON_BIN" "$COORDINATOR_ADAPTER" passthrough -- "$0" "$@"'
        )
        if (
            bootstrap_index < 0
            or exec_index <= bootstrap_index
            or source.find(
                '"$PYTHON_BIN" "$COORDINATOR_ADAPTER" entry-check'
            )
            <= exec_index
            or source.find(
                'COORDINATOR_LOCK_DIR="/run/lock/parser-nightly-coordinator"'
            )
            < 0
        ):
            raise CheckError("official entrypoint contract mismatch")
        for unsafe_before_lock in (
            'source "$RUNTIME_LOADER"',
            "wb_load_required_runtime_env",
        ):
            index = source.find(unsafe_before_lock)
            if 0 <= index < exec_index:
                raise CheckError("official entrypoint lock order mismatch")
        official_sources.append((path, source_bytes))
    disabled_sources: list[tuple[Path, bytes]] = []
    for name in COORDINATOR_DISABLED_ENTRYPOINTS:
        path = PROJECT_ROOT / "scripts" / name
        source_bytes = _read_safe(path, executable=True, mode=0o755)
        source = source_bytes.decode("utf-8")
        refusal = (
            'if [[ -e "$COORDINATOR_LOCK_DIR" '
            '|| -L "$COORDINATOR_LOCK_DIR" ]]; then'
        )
        refusal_index = source.find(refusal)
        if (
            refusal_index < 0
            or source.find(
                'COORDINATOR_LOCK_DIR="/run/lock/parser-nightly-coordinator"'
            )
            < 0
        ):
            raise CheckError("disabled entrypoint contract mismatch")
        for unsafe_before_refusal in (
            "mkdir -p",
            'source "$RUNTIME_LOADER"',
            "wb_load_required_runtime_env",
        ):
            index = source.find(unsafe_before_refusal)
            if 0 <= index < refusal_index:
                raise CheckError("disabled entrypoint lock order mismatch")
        disabled_sources.append((path, source_bytes))

    verify_input_manifest(PROJECT_ROOT)
    tracked_inputs = []
    for path in DIRECT_ATTESTATION_ROOTS:
        executable = path.suffix in {".py", ".sh"} and os.access(path, os.X_OK)
        tracked_inputs.append(
            (path, _read_safe(path, executable=executable))
        )
    return [
        {"path": str(path), "sha256": _sha256(encoded)}
        for path, encoded in tracked_inputs
    ]


def main() -> int:
    try:
        if os.getenv("MARKETPLACE_COORDINATOR_CONTRACT_CHECK") != "1":
            raise CheckError("contract check mode is required")
        stage = os.environ["MARKETPLACE_COORDINATOR_CHECK_STAGE"]
        phase = os.environ["MARKETPLACE_COORDINATOR_CHECK_PHASE"]
        expected_phase = {
            "wb_initial": "initial",
            "wb_resume": "resume",
        }.get(stage)
        if phase != expected_phase:
            raise CheckError("stage phase mismatch")
        if (
            os.getenv("MARKETPLACE_COORDINATOR_EXPECTED_RESULT_SCHEMA")
            != RESULT_SCHEMA
            or os.getenv("MARKETPLACE_COORDINATOR_EXPECTED_LOCK_CONTRACT")
            != LOCK_CONTRACT
            or os.getenv(
                "MARKETPLACE_COORDINATOR_EXPECTED_QUARANTINE_CONTRACT"
            )
            != QUARANTINE_CONTRACT
            or os.getenv("MARKETPLACE_COORDINATOR_QUARANTINE_MARKER_PATH")
            != QUARANTINE_PATH
        ):
            raise CheckError("expected contract mismatch")
        command_sha256 = _command_sha256(_configured_command(stage))
        if (
            os.getenv("MARKETPLACE_COORDINATOR_CHECK_COMMAND_SHA256")
            != command_sha256
        ):
            raise CheckError("configured command hash mismatch")
        adapter_bytes = _read_safe(ADAPTER, executable=True)
        inputs = _validate_sources()
        payload = {
            "schema_version": CHECK_SCHEMA,
            "ok": True,
            "parser": "wb",
            "stage": stage,
            "phase": phase,
            "result_schema_version": RESULT_SCHEMA,
            "lock_contract_version": LOCK_CONTRACT,
            "quarantine_contract_version": QUARANTINE_CONTRACT,
            "quarantine_marker_path": QUARANTINE_PATH,
            "official_entrypoints_quarantine_checked": True,
            "network_used": False,
            "adapter_executable": str(ADAPTER),
            "adapter_sha256": _sha256(adapter_bytes),
            "adapter_command_sha256": command_sha256,
            "inputs_verified": True,
            "input_files": inputs,
        }
    except (CheckError, KeyError, OSError, RuntimeError):
        print(
            json.dumps(
                {
                    "ok": False,
                    "reason_code": "wb_coordinator_contract_check_failed",
                },
                sort_keys=True,
            ),
            file=sys.stderr,
        )
        return 2
    print(json.dumps(payload, sort_keys=True, separators=(",", ":")))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
