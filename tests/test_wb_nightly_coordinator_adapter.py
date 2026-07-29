from __future__ import annotations

import fcntl
import hashlib
import json
import os
import py_compile
import re
import shutil
import signal
import stat
import subprocess
import sys
import time
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace

import pytest

from app.common import cli as wb_cli
from app.common import durable_atomic
from app.common import nightly_coordinator as contract
from app.common import nightly_attestation as attestation
from app.common import runtime_env
from app.serp.collection_plan_runner import CollectionPlanRunError, DeadlineGuard
from scripts import check_nightly_coordinator_contract as checker
from scripts import marketplace_lock_v3_supervisor as supervisor
from scripts import run_wb_four_region_nightly as pipeline_launcher
from scripts import wb_nightly_coordinator_adapter as adapter


PROJECT_ROOT = Path(__file__).resolve().parents[1]
PLAN_ARGUMENT = (
    "config/wb/collection_plans/shevron-four-regions-top1000-v2.json"
)
MATRIX_ARGUMENT = (
    "config/wb/execution_matrices/four-region-nightly-v1.json"
)
COORDINATOR_ARGUMENTS = [
    "--matrix-file",
    MATRIX_ARGUMENT,
    "--no-publish",
]
RUN_REF = "20260727_001500Z"


@pytest.fixture(autouse=True)
def _isolate_pipeline_launcher_host_lock(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        pipeline_launcher,
        "require_official_live_entry_lease",
        lambda **_kwargs: None,
    )
    monkeypatch.setattr(
        adapter,
        "capture_attested_environment",
        lambda _root, environment: dict(environment),
    )
    monkeypatch.setattr(
        adapter,
        "verify_attested_environment",
        lambda _root, _environment: None,
    )
    monkeypatch.setattr(
        adapter,
        "verify_input_manifest",
        lambda _root: "a" * 64,
    )


class ExitCalled(RuntimeError):
    def __init__(self, code: int) -> None:
        self.code = code
        super().__init__(str(code))


def _cleanup_debt_gate(project_root: Path):
    def gate() -> None:
        return None

    setattr(gate, "_wb_cleanup_debt_enabled", True)
    setattr(
        gate,
        "_wb_cleanup_debt_project_root",
        project_root.resolve(strict=True),
    )
    setattr(gate, "_wb_cleanup_debt_validate_lease", lambda: None)
    return gate


class FakeLease:
    def __init__(
        self,
        *,
        invocation: contract.CoordinatorInvocation | None,
        validation_fd: int,
    ) -> None:
        self.invocation = invocation
        self.validation_fd = validation_fd
        self.guard_fd = os.dup(validation_fd)
        validation_path = Path(os.readlink(f"/proc/self/fd/{validation_fd}"))
        self.policy = SimpleNamespace(
            guard_path=validation_path,
            validation_path=validation_path,
        )
        self.quarantine_marker_path = Path("/tmp/missing-wb-quarantine")
        self.exited = False
        self.assertions = 0

    @property
    def pass_fds(self) -> tuple[int, ...]:
        return tuple(sorted((self.guard_fd, self.validation_fd)))

    def assert_held(self) -> None:
        os.fstat(self.validation_fd)
        self.assertions += 1

    def __exit__(self, *_args: object) -> None:
        try:
            os.close(self.guard_fd)
        except OSError:
            pass
        self.exited = True


def _lock_policy(tmp_path: Path) -> contract.HostLockPolicy:
    directory = tmp_path / "locks"
    directory.mkdir(mode=0o755)
    guard = directory / "marketplace-collection.guard.flock"
    validation = directory / "marketplace-collection.validation.flock"
    for path in (guard, validation):
        path.touch(mode=0o660)
        path.chmod(0o660)
    return contract.HostLockPolicy(
        directory=directory,
        guard_path=guard,
        validation_path=validation,
        directory_uid=os.geteuid(),
        file_uid=os.geteuid(),
        file_gid=os.getegid(),
    )


def _invocation(
    tmp_path: Path,
    *,
    stage: str = "wb_initial",
    resume_ref: str = "",
) -> contract.CoordinatorInvocation:
    result_dir = tmp_path / "results"
    result_dir.mkdir(mode=0o750, exist_ok=True)
    return contract.CoordinatorInvocation(
        result_path=result_dir / f"nightly-20260727-abcdef.{stage}.1.json",
        coordinator_run_id="nightly-20260727-abcdef",
        schedule_date="2026-07-27",
        stage=stage,
        phase=contract.PHASE_BY_STAGE[stage],
        attempt=1,
        invocation_id="invoke-abcdef",
        resume_ref=resume_ref,
        deadline_utc=datetime.now(UTC) + timedelta(minutes=30),
        quarantine_marker_path=tmp_path / "quarantine.json",
    )


def _coordinator_environment(
    invocation: contract.CoordinatorInvocation,
    *,
    policy: contract.HostLockPolicy,
    validation_fd: int,
) -> dict[str, str]:
    return {
        "MARKETPLACE_COORDINATOR_RESULT_CONTRACT": contract.RESULT_SCHEMA_VERSION,
        "MARKETPLACE_COORDINATOR_RESULT_FILE": str(invocation.result_path),
        "MARKETPLACE_COORDINATOR_RUN_ID": invocation.coordinator_run_id,
        "MARKETPLACE_COORDINATOR_SCHEDULE_DATE": invocation.schedule_date,
        "MARKETPLACE_COORDINATOR_STAGE": invocation.stage,
        "MARKETPLACE_COORDINATOR_ATTEMPT": str(invocation.attempt),
        "MARKETPLACE_COORDINATOR_INVOCATION_ID": invocation.invocation_id,
        "MARKETPLACE_COORDINATOR_RESUME_REF": invocation.resume_ref,
        "MARKETPLACE_COORDINATOR_DEADLINE_UTC": contract.utc_iso(
            invocation.deadline_utc
        ),
        "MARKETPLACE_COLLECTION_LOCK_CONTRACT": contract.LOCK_CONTRACT_VERSION,
        "MARKETPLACE_COLLECTION_GUARD_PATH": str(policy.guard_path),
        "MARKETPLACE_COLLECTION_VALIDATION_PATH": str(policy.validation_path),
        "MARKETPLACE_COLLECTION_VALIDATION_OWNER_PID": str(os.getpid()),
        "MARKETPLACE_COLLECTION_VALIDATION_FD": str(validation_fd),
        "MARKETPLACE_COLLECTION_QUARANTINE_CONTRACT": (
            contract.QUARANTINE_CONTRACT_VERSION
        ),
        "MARKETPLACE_COLLECTION_QUARANTINE_MARKER_PATH": str(
            invocation.quarantine_marker_path
        ),
    }


def _write_marker(
    path: Path,
    invocation: contract.CoordinatorInvocation,
) -> None:
    path.parent.mkdir(mode=0o755, exist_ok=True)
    payload = {
        "schema_version": contract.QUARANTINE_CONTRACT_VERSION,
        "schedule_date": invocation.schedule_date,
        "coordinator_run_id": invocation.coordinator_run_id,
        "stage": invocation.stage,
        "attempt": invocation.attempt,
        "invocation_id": invocation.invocation_id,
        "state_generation": 1,
        "state_sha256": "a" * 64,
        "armed_at_utc": "2026-07-27T00:14:00Z",
        "reason_code": "child_activity_in_progress",
        "marker_sha256": "",
    }
    payload["marker_sha256"] = contract._marker_digest(payload)
    path.write_text(
        json.dumps(payload, sort_keys=True, separators=(",", ":")),
        encoding="utf-8",
    )
    path.chmod(0o440)


def _command_digest(stage: str) -> str:
    command = [
        str(
            checker.DEPLOYED_PROJECT_ROOT
            / "scripts/run_wb_four_region_nightly.sh"
        ),
        "--matrix-file",
        MATRIX_ARGUMENT,
        "--no-publish",
    ]
    if stage == "wb_resume":
        command.extend(["--resume-run-id", "{resume_ref}"])
    return hashlib.sha256(
        json.dumps(command, ensure_ascii=False, separators=(",", ":")).encode()
    ).hexdigest()


def _checker_environment(stage: str) -> dict[str, str]:
    return {
        "MARKETPLACE_COORDINATOR_CONTRACT_CHECK": "1",
        "MARKETPLACE_COORDINATOR_CHECK_STAGE": stage,
        "MARKETPLACE_COORDINATOR_CHECK_PHASE": (
            "initial" if stage == "wb_initial" else "resume"
        ),
        "MARKETPLACE_COORDINATOR_CHECK_COMMAND_SHA256": _command_digest(stage),
        "MARKETPLACE_COORDINATOR_EXPECTED_RESULT_SCHEMA": (
            contract.RESULT_SCHEMA_VERSION
        ),
        "MARKETPLACE_COORDINATOR_EXPECTED_LOCK_CONTRACT": (
            contract.LOCK_CONTRACT_VERSION
        ),
        "MARKETPLACE_COORDINATOR_EXPECTED_QUARANTINE_CONTRACT": (
            contract.QUARANTINE_CONTRACT_VERSION
        ),
        "MARKETPLACE_COORDINATOR_QUARANTINE_MARKER_PATH": (
            str(contract.QUARANTINE_MARKER_PATH)
        ),
    }


def _remove_group_world_write(root: Path) -> None:
    for path in (root, *root.rglob("*")):
        info = path.lstat()
        if stat.S_ISLNK(info.st_mode):
            continue
        path.chmod(stat.S_IMODE(info.st_mode) & ~0o022)


@pytest.mark.parametrize(
    ("stage", "phase"),
    (("wb_initial", "initial"), ("wb_resume", "resume")),
)
def test_checker_dry_run_is_exact_and_no_network(
    stage: str,
    phase: str,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setattr(os, "environ", _checker_environment(stage))
    monkeypatch.setattr(
        checker,
        "_read_safe",
        lambda path, **_kwargs: path.read_bytes(),
    )
    monkeypatch.setattr(
        checker,
        "verify_input_manifest",
        lambda _root: "a" * 64,
    )

    def network_forbidden(*_args: object, **_kwargs: object) -> None:
        raise AssertionError("contract checker must not use network")

    monkeypatch.setattr("socket.socket", network_forbidden)
    assert checker.main() == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["schema_version"] == contract.CHECK_SCHEMA_VERSION
    assert payload["stage"] == stage
    assert payload["phase"] == phase
    assert payload["network_used"] is False
    assert payload["official_entrypoints_quarantine_checked"] is True
    input_paths = {item["path"] for item in payload["input_files"]}
    assert (
        str(PROJECT_ROOT / "scripts/wb_nightly_coordinator_adapter.py")
        in input_paths
    )
    assert (
        str(PROJECT_ROOT / "app/common/nightly_coordinator.py")
        in input_paths
    )
    assert len(input_paths) <= 32
    manifest = json.loads(
        (
            PROJECT_ROOT
            / "config/wb/nightly_coordinator_adapter_inputs.json"
        ).read_text(encoding="utf-8")
    )
    for name in (
        *checker.OFFICIAL_ENTRYPOINTS,
        *checker.COORDINATOR_DISABLED_ENTRYPOINTS,
    ):
        assert f"scripts/{name}" in manifest["files"]


@pytest.mark.parametrize("stage", ("wb_initial", "wb_resume"))
def test_exact_checker_command_uses_approved_python_with_restricted_path(
    stage: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    project = tmp_path / "project"
    for relative in ("app", "scripts", "config"):
        shutil.copytree(PROJECT_ROOT / relative, project / relative)
    shutil.copy2(PROJECT_ROOT / "main.py", project / "main.py")
    shutil.copy2(PROJECT_ROOT / "requirements.txt", project / "requirements.txt")
    dependencies = tmp_path / "site-packages"
    dependencies.mkdir()
    (dependencies / "approved.py").write_text(
        "VALUE = 1\n",
        encoding="utf-8",
    )
    attestation_source = project / "app/common/nightly_attestation.py"
    source = attestation_source.read_text(encoding="utf-8")
    source = re.sub(
        r'APPROVED_SITE_PACKAGES = Path\(\n'
        r'\s*"/home/Codex/agent-tools/parser_wb-python/lib/python3\.14/'
        r'site-packages"\n'
        r'\)',
        f'APPROVED_SITE_PACKAGES = Path({str(dependencies)!r})',
        source,
        count=1,
    )
    attestation_source.write_text(source, encoding="utf-8")
    _remove_group_world_write(project)
    _remove_group_world_write(dependencies)
    monkeypatch.setattr(
        attestation,
        "APPROVED_SITE_PACKAGES",
        dependencies,
    )
    manifest_path = project / attestation.MANIFEST_RELATIVE_PATH
    manifest_path.write_text(
        json.dumps(
            attestation.build_input_manifest(project),
            sort_keys=True,
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )
    manifest_path.chmod(0o644)

    command = project / "scripts/check_nightly_coordinator_contract.py"
    assert command.read_bytes().splitlines()[0] == (
        b"#!/home/Codex/agent-tools/parser_wb-python/bin/python -B"
    )
    environment = _checker_environment(stage)
    environment.update(
        {
            "PATH": "/usr/bin:/bin",
            "HOME": os.environ.get("HOME", "/home/pavel"),
        }
    )
    completed = subprocess.run(
        [str(command)],
        cwd=project,
        env=environment,
        text=True,
        capture_output=True,
        timeout=120,
        check=False,
    )
    assert completed.returncode == 0, completed.stderr
    payload = json.loads(completed.stdout)
    assert payload["ok"] is True
    assert payload["stage"] == stage
    assert payload["network_used"] is False

    rejected_environment = dict(environment)
    rejected_environment[
        "MARKETPLACE_COORDINATOR_CHECK_COMMAND_SHA256"
    ] = "0" * 64
    rejected = subprocess.run(
        [str(command)],
        cwd=project,
        env=rejected_environment,
        text=True,
        capture_output=True,
        timeout=120,
        check=False,
    )
    assert rejected.returncode == 2
    assert "Traceback" not in rejected.stderr


def test_all_official_shell_entrants_gate_before_runtime_and_preserve_fds() -> None:
    adapter_targets = {
        path.name for path in adapter.OFFICIAL_PASSTHROUGH_TARGETS
    }
    assert adapter_targets == set(checker.OFFICIAL_ENTRYPOINTS)
    for name in checker.OFFICIAL_ENTRYPOINTS:
        source = (PROJECT_ROOT / "scripts" / name).read_text(encoding="utf-8")
        adapter_exec = source.index(
            'exec "$PYTHON_BIN" "$COORDINATOR_ADAPTER" passthrough -- "$0" "$@"'
        )
        entry_check = source.index(
            '"$PYTHON_BIN" "$COORDINATOR_ADAPTER" entry-check'
        )
        runtime_indices = [
            index
            for token in ('source "$RUNTIME_LOADER"', "wb_load_required_runtime_env")
            if (index := source.find(token)) >= 0
        ]
        assert adapter_exec < entry_check
        assert source.index("export PYTHONDONTWRITEBYTECODE=1") < adapter_exec
        assert all(entry_check < index for index in runtime_indices)
        assert re.search(r"exec [0-9]+>", source) is None
        assert re.search(r"flock (?:-n |-u )?[0-9]+(?:\\s|$)", source) is None


def test_persistent_entrants_stop_before_runtime_or_state_after_cutover() -> None:
    for name in checker.COORDINATOR_DISABLED_ENTRYPOINTS:
        source = (PROJECT_ROOT / "scripts" / name).read_text(encoding="utf-8")
        refusal = source.index(
            'if [[ -e "$COORDINATOR_LOCK_DIR" '
            '|| -L "$COORDINATOR_LOCK_DIR" ]]; then'
        )
        for token in (
            "mkdir -p",
            'source "$RUNTIME_LOADER"',
            "wb_load_required_runtime_env",
        ):
            index = source.find(token)
            assert index < 0 or refusal < index


def test_direct_python_entrants_require_full_host_lease() -> None:
    for name in checker.DIRECT_PYTHON_ENTRYPOINTS:
        source = (PROJECT_ROOT / "scripts" / name).read_text(
            encoding="utf-8"
        )
        expected = (
            "_require_host_lease_after_cutover()"
            if name in {"wb_cookie_keeper.py", "wb_warehouse.py"}
            else "require_official_live_entry_lease(environment=os.environ)"
        )
        assert expected in source


def test_checker_rejects_command_hash_mismatch(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    environment = _checker_environment("wb_initial")
    environment["MARKETPLACE_COORDINATOR_CHECK_COMMAND_SHA256"] = "0" * 64
    monkeypatch.setattr(os, "environ", environment)
    assert checker.main() == 2
    assert json.loads(capsys.readouterr().err) == {
        "ok": False,
        "reason_code": "wb_coordinator_contract_check_failed",
    }


def test_standalone_lease_uses_guard_then_validation_and_refuses_marker(
    tmp_path: Path,
) -> None:
    policy = _lock_policy(tmp_path)
    marker = tmp_path / "quarantine" / "unsafe.json"
    marker.parent.mkdir(mode=0o755)
    lease = contract.acquire_marketplace_collection_lease(
        environment={},
        policy=policy,
        quarantine_marker_path=marker,
    )
    try:
        lease.assert_held()
        guard_probe = os.open(policy.guard_path, os.O_RDWR)
        validation_probe = os.open(policy.validation_path, os.O_RDWR)
        try:
            with pytest.raises(BlockingIOError):
                fcntl.flock(guard_probe, fcntl.LOCK_EX | fcntl.LOCK_NB)
            with pytest.raises(BlockingIOError):
                fcntl.flock(validation_probe, fcntl.LOCK_EX | fcntl.LOCK_NB)
        finally:
            os.close(guard_probe)
            os.close(validation_probe)
    finally:
        lease.__exit__()

    invocation = _invocation(tmp_path)
    _write_marker(marker, invocation)
    with pytest.raises(
        contract.NightlyCoordinatorContractError,
        match="unsafe_cleanup_quarantine_active",
    ):
        contract.acquire_marketplace_collection_lease(
            environment={},
            policy=policy,
            quarantine_marker_path=marker,
        )


def test_descendant_lease_is_required_after_secure_layout_exists(
    tmp_path: Path,
) -> None:
    policy = _lock_policy(tmp_path)
    with pytest.raises(
        contract.NightlyCoordinatorContractError,
        match="official_live_entry_requires_lock_v3",
    ) as raised:
        contract.require_official_live_entry_lease(
            environment={},
            policy=policy,
        )
    assert raised.value.outcome == "deferred"

    lease = contract.acquire_marketplace_collection_lease(
        environment={},
        policy=policy,
        quarantine_marker_path=tmp_path / "missing-quarantine.json",
    )
    try:
        environment = {
            "PARSER_WB_LOCK_V3_WRAPPED": "1",
            **contract.descendant_lease_environment(lease),
        }
        assert (
            contract.require_official_live_entry_lease(
                environment=environment,
                policy=policy,
            )
            == lease.validation_fd
        )
    finally:
        lease.__exit__()


def test_forged_wrapped_flag_cannot_authorize_live_entry(
    tmp_path: Path,
) -> None:
    policy = _lock_policy(tmp_path)
    with pytest.raises(
        contract.NightlyCoordinatorContractError,
        match="official_live_entry_requires_lock_v3",
    ):
        contract.require_official_live_entry_lease(
            environment={"PARSER_WB_LOCK_V3_WRAPPED": "1"},
            policy=policy,
        )


def test_direct_cli_refuses_before_config_or_network(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    lock_dir = tmp_path / "locks"
    lock_dir.mkdir()
    error = contract.NightlyCoordinatorContractError(
        "official_live_entry_requires_lock_v3",
        outcome="deferred",
    )
    monkeypatch.setattr(
        wb_cli,
        "_COORDINATOR_LOCK_DIRECTORY",
        lock_dir,
    )
    monkeypatch.setattr(
        contract,
        "require_official_live_entry_lease",
        lambda **_kwargs: (_ for _ in ()).throw(error),
    )
    monkeypatch.setattr(
        wb_cli,
        "load_config",
        lambda *_args, **_kwargs: pytest.fail("config must not load"),
    )
    monkeypatch.setattr(sys, "argv", ["main.py", "run", "serp"])
    assert wb_cli.main() == 75
    assert capsys.readouterr().err == (
        "WB live entry refused by host lock contract\n"
    )


def test_direct_cleanup_refuses_before_config(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    lock_dir = tmp_path / "locks"
    lock_dir.mkdir()
    monkeypatch.setattr(wb_cli, "_COORDINATOR_LOCK_DIRECTORY", lock_dir)
    monkeypatch.setattr(
        contract,
        "require_official_live_entry_lease",
        lambda **_kwargs: (_ for _ in ()).throw(
            contract.NightlyCoordinatorContractError(
                "official_live_entry_requires_lock_v3",
                outcome="deferred",
            )
        ),
    )
    monkeypatch.setattr(
        wb_cli,
        "cmd_cleanup",
        lambda *_args, **_kwargs: pytest.fail("cleanup must not start"),
    )
    monkeypatch.setattr(sys, "argv", ["main.py", "cleanup", "--apply"])
    assert wb_cli.main() == 75


def test_coordinator_inherited_validation_fd_and_marker_are_verified(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    policy = _lock_policy(tmp_path)
    marker_path = tmp_path / "quarantine" / "unsafe.json"
    invocation = replace(_invocation(tmp_path), quarantine_marker_path=marker_path)
    _write_marker(marker_path, invocation)
    monkeypatch.setattr(contract, "GUARD_LOCK_PATH", policy.guard_path)
    monkeypatch.setattr(contract, "VALIDATION_LOCK_PATH", policy.validation_path)
    monkeypatch.setattr(contract, "RESULT_DIRECTORY", invocation.result_path.parent)
    monkeypatch.setattr(contract, "QUARANTINE_MARKER_PATH", marker_path)
    guard_fd = os.open(policy.guard_path, os.O_RDWR)
    validation_fd = os.open(policy.validation_path, os.O_RDWR)
    fcntl.flock(guard_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    fcntl.flock(validation_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    try:
        environment = _coordinator_environment(
            invocation,
            policy=policy,
            validation_fd=validation_fd,
        )
        lease = contract.acquire_marketplace_collection_lease(
            environment=environment,
            policy=policy,
        )
        lease.assert_held()
        lease.__exit__()
        os.fstat(validation_fd)
        invalid_owner = dict(environment)
        invalid_owner["MARKETPLACE_COLLECTION_VALIDATION_OWNER_PID"] = "1"
        with pytest.raises(
            contract.NightlyCoordinatorContractError,
            match="coordinator_validation_(?:owner|lease)_invalid",
        ):
            contract.acquire_marketplace_collection_lease(
                environment=invalid_owner,
                policy=policy,
            )
    finally:
        fcntl.flock(validation_fd, fcntl.LOCK_UN)
        fcntl.flock(guard_fd, fcntl.LOCK_UN)
        os.close(validation_fd)
        os.close(guard_fd)


def test_malformed_and_symlink_quarantine_fail_closed(
    tmp_path: Path,
) -> None:
    policy = _lock_policy(tmp_path)
    quarantine = tmp_path / "quarantine"
    quarantine.mkdir(mode=0o755)
    marker = quarantine / "unsafe.json"
    marker.write_text("{}", encoding="utf-8")
    marker.chmod(0o440)
    with pytest.raises(contract.NightlyCoordinatorContractError):
        contract.acquire_marketplace_collection_lease(
            environment={},
            policy=policy,
            quarantine_marker_path=marker,
        )
    marker.unlink()
    outside = tmp_path / "outside.json"
    outside.write_text("{}", encoding="utf-8")
    outside.chmod(0o440)
    marker.symlink_to(outside)
    with pytest.raises(contract.NightlyCoordinatorContractError):
        contract.acquire_marketplace_collection_lease(
            environment={},
            policy=policy,
            quarantine_marker_path=marker,
        )


def test_terminal_result_is_exact_atomic_mode_0440(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    invocation = _invocation(tmp_path)
    monkeypatch.setattr(contract, "RESULT_DIRECTORY", invocation.result_path.parent)
    started = datetime(2026, 7, 27, 0, 15, tzinfo=UTC)
    exit_code = contract.write_terminal_result(
        invocation=invocation,
        outcome="checkpoint",
        run_ref=RUN_REF,
        resume_ref=RUN_REF,
        reason_code="checkpoint_saved",
        started_at_utc=started,
        finished_at_utc=started + timedelta(minutes=1),
        report_refs=("state/run_reports/latest.json",),
    )
    assert exit_code == 76
    assert stat.S_IMODE(invocation.result_path.stat().st_mode) == 0o440
    payload = json.loads(invocation.result_path.read_text(encoding="utf-8"))
    assert set(payload) == {
        "schema_version",
        "lock_contract_version",
        "parser",
        "phase",
        "coordinator_run_id",
        "schedule_date",
        "attempt",
        "invocation_id",
        "outcome",
        "terminal_exit_code",
        "run_ref",
        "started_at_utc",
        "finished_at_utc",
        "resources_released",
        "validation_lease_held_until_exit",
        "resume_required",
        "resume_ref",
        "reason_code",
        "report_refs",
    }
    assert payload["validation_lease_held_until_exit"] is True
    with pytest.raises(
        contract.NightlyCoordinatorContractError,
        match="coordinator_result_already_exists",
    ):
        contract.write_terminal_result(
            invocation=invocation,
            outcome="success",
            run_ref=RUN_REF,
            resume_ref="",
            reason_code="completed",
            started_at_utc=started,
            finished_at_utc=started,
        )


def test_terminal_result_checks_integrity_inside_atomic_writer(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    invocation = _invocation(tmp_path)
    monkeypatch.setattr(
        contract,
        "RESULT_DIRECTORY",
        invocation.result_path.parent,
    )
    sentinel = tmp_path / "attested"
    sentinel.write_bytes(b"approved")

    def mutate_before_gate(event: str, _path: Path) -> None:
        if event == "before_integrity_check":
            sentinel.write_bytes(b"changed")

    def gate() -> None:
        if sentinel.read_bytes() != b"approved":
            raise contract.NightlyCoordinatorContractError(
                "coordinator_input_changed",
                outcome="hard_failure",
            )

    with pytest.raises(
        contract.NightlyCoordinatorContractError,
        match="coordinator_input_changed",
    ):
        contract.write_terminal_result(
            invocation=invocation,
            outcome="success",
            run_ref=RUN_REF,
            resume_ref="",
            reason_code="completed",
            started_at_utc=datetime.now(UTC),
            finished_at_utc=datetime.now(UTC),
            integrity_gate=gate,
            write_event_hook=mutate_before_gate,
        )
    assert not invocation.result_path.exists()
    assert not list(
        invocation.result_path.parent.glob(
            f".{invocation.result_path.name}.*.tmp"
        )
    )


@pytest.mark.parametrize(
    "report_ref",
    (
        "/state/run_reports/latest.json",
        "state/run_reports/../../secret.json",
        "state/other/report.json",
        "state/run_reports/not-json.txt",
    ),
)
def test_terminal_result_rejects_unsafe_report_refs(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    report_ref: str,
) -> None:
    invocation = _invocation(tmp_path)
    monkeypatch.setattr(contract, "RESULT_DIRECTORY", invocation.result_path.parent)
    with pytest.raises(
        contract.NightlyCoordinatorContractError,
        match="coordinator_result_report_refs_invalid",
    ):
        contract.write_terminal_result(
            invocation=invocation,
            outcome="success",
            run_ref=RUN_REF,
            resume_ref="",
            reason_code="completed",
            started_at_utc=datetime.now(UTC),
            finished_at_utc=datetime.now(UTC),
            report_refs=(report_ref,),
        )
    assert not invocation.result_path.exists()


def test_four_region_command_is_exact_and_has_no_legacy_fallback() -> None:
    adapter._validate_four_region_command(
        ["--plan-file", PLAN_ARGUMENT, "--no-publish"],
        invocation=None,
    )
    adapter._validate_four_region_command(
        [
            "--config",
            "config/config.yaml",
            "--plan-file",
            PLAN_ARGUMENT,
            "--no-publish",
            "--resume-run-id",
            RUN_REF,
        ],
        invocation=None,
    )
    with pytest.raises(
        contract.NightlyCoordinatorContractError,
        match="four_region_command_invalid",
    ):
        adapter._validate_four_region_command(
            [
                "--plan-file",
                (
                    "config/wb/collection_plans/"
                    "shevron-moscow-rostov-top1000-v1.json"
                ),
                "--no-publish",
            ],
            invocation=None,
        )


def test_coordinator_command_must_match_exact_phase(
    tmp_path: Path,
) -> None:
    initial = _invocation(tmp_path)
    adapter._validate_four_region_command(
        COORDINATOR_ARGUMENTS,
        invocation=initial,
    )
    resume = _invocation(tmp_path, stage="wb_resume", resume_ref=RUN_REF)
    adapter._validate_four_region_command(
        [
            *COORDINATOR_ARGUMENTS,
            "--resume-run-id",
            RUN_REF,
        ],
        invocation=resume,
    )
    with pytest.raises(
        contract.NightlyCoordinatorContractError,
        match="coordinator_command_invalid",
    ):
        adapter._validate_four_region_command(
            COORDINATOR_ARGUMENTS,
            invocation=resume,
        )


def test_adapter_passes_validation_fd_to_collection_descendant(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    validation = tmp_path / "validation.flock"
    validation.touch()
    validation_fd = os.open(validation, os.O_RDWR)
    lease = FakeLease(invocation=None, validation_fd=validation_fd)
    child = tmp_path / "child.py"
    child.write_text(
        "\n".join(
            (
                "import json, os, sys",
                "os.fstat(int(os.environ['TEST_VALIDATION_FD']))",
                "assert os.environ['PYTHONDONTWRITEBYTECODE'] == '1'",
                "payload = {",
                f"  'schema_version': {contract.ADAPTER_STATUS_SCHEMA_VERSION!r},",
                "  'outcome': 'success',",
                f"  'run_ref': {RUN_REF!r},",
                "  'resume_ref': '',",
                "  'reason_code': 'completed',",
                "  'report_refs': [],",
                "}",
                "os.write(int(os.environ['PARSER_WB_ADAPTER_STATUS_FD']), "
                "json.dumps(payload).encode())",
                "sys.exit(0)",
            )
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(
        adapter,
        "acquire_marketplace_collection_lease",
        lambda: lease,
    )
    monkeypatch.setattr(adapter, "PYTHON_BIN", Path(sys.executable))
    monkeypatch.setattr(adapter, "FOUR_REGION_SCRIPT", child)
    monkeypatch.setattr(
        adapter,
        "_utc_now",
        lambda: datetime(2026, 7, 27, 0, 15, tzinfo=UTC),
    )
    monkeypatch.setattr(
        adapter,
        "load_required_runtime_environment",
        lambda **_kwargs: {"TEST_VALIDATION_FD": str(validation_fd)},
    )
    try:
        assert (
            adapter._run_four_region(
                ["--plan-file", PLAN_ARGUMENT, "--no-publish"]
            )
            == 0
        )
        assert lease.assertions >= 2
        assert lease.exited is True
    finally:
        os.close(validation_fd)


def test_child_exit_must_match_status_before_result_commit(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    validation = tmp_path / "validation.flock"
    validation.touch()
    validation_fd = os.open(validation, os.O_RDWR)
    invocation = _invocation(tmp_path)
    lease = FakeLease(invocation=invocation, validation_fd=validation_fd)
    child = tmp_path / "child.py"
    child.write_text(
        "\n".join(
            (
                "import json, os, sys",
                "payload = {",
                f"  'schema_version': {contract.ADAPTER_STATUS_SCHEMA_VERSION!r},",
                "  'outcome': 'success',",
                f"  'run_ref': {RUN_REF!r},",
                "  'resume_ref': '',",
                "  'reason_code': 'completed',",
                "  'report_refs': [],",
                "}",
                "os.write(int(os.environ['PARSER_WB_ADAPTER_STATUS_FD']), "
                "json.dumps(payload).encode())",
                "sys.exit(2)",
            )
        ),
        encoding="utf-8",
    )
    captured: dict[str, object] = {}
    monkeypatch.setattr(
        adapter,
        "acquire_marketplace_collection_lease",
        lambda: lease,
    )
    monkeypatch.setattr(adapter, "PYTHON_BIN", Path(sys.executable))
    monkeypatch.setattr(adapter, "FOUR_REGION_SCRIPT", child)
    monkeypatch.setattr(
        adapter,
        "_utc_now",
        lambda: datetime(2026, 7, 27, 0, 15, tzinfo=UTC),
    )
    monkeypatch.setattr(
        adapter,
        "load_required_runtime_environment",
        lambda **_kwargs: {},
    )

    def fake_write(**kwargs: object) -> int:
        captured.update(kwargs)
        return 2

    monkeypatch.setattr(adapter, "write_terminal_result", fake_write)
    monkeypatch.setattr(
        adapter.os,
        "_exit",
        lambda code: (_ for _ in ()).throw(ExitCalled(code)),
    )
    try:
        with pytest.raises(ExitCalled) as raised:
            adapter._run_four_region(
                COORDINATOR_ARGUMENTS
            )
        assert raised.value.code == 2
        assert captured["outcome"] == "hard_failure"
        assert captured["reason_code"] == "child_exit_contract_mismatch"
    finally:
        os.close(validation_fd)


def test_status_run_ref_cannot_override_invocation(
    tmp_path: Path,
) -> None:
    invocation = _invocation(tmp_path)
    read_fd, write_fd = os.pipe()
    try:
        os.write(
            write_fd,
            json.dumps(
                {
                    "schema_version": contract.ADAPTER_STATUS_SCHEMA_VERSION,
                    "outcome": "success",
                    "run_ref": "20260727_001501Z",
                    "resume_ref": "",
                    "reason_code": "completed",
                    "report_refs": [],
                }
            ).encode(),
        )
        os.close(write_fd)
        write_fd = -1
        assert (
            adapter._read_status(
                read_fd,
                expected_run_ref=RUN_REF,
                invocation=invocation,
            )
            is None
        )
    finally:
        os.close(read_fd)
        if write_fd >= 0:
            os.close(write_fd)


def test_relative_passthrough_target_is_canonical_and_allowlisted() -> None:
    expected = PROJECT_ROOT / "scripts/run_wb_collection_plan.sh"
    assert (
        adapter._canonical_passthrough_target(
            "scripts/run_wb_collection_plan.sh"
        )
        == expected
    )
    with pytest.raises(
        contract.NightlyCoordinatorContractError,
        match="passthrough_target_invalid",
    ):
        adapter._canonical_passthrough_target("../parser_ozon/run.sh")


def test_deadline_is_rechecked_immediately_before_spawn(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    validation = tmp_path / "validation.flock"
    validation.touch()
    validation_fd = os.open(validation, os.O_RDWR)
    invocation = replace(
        _invocation(tmp_path),
        deadline_utc=datetime(2026, 7, 27, 0, 16, tzinfo=UTC),
    )
    lease = FakeLease(invocation=invocation, validation_fd=validation_fd)
    times = iter(
        (
            datetime(2026, 7, 27, 0, 15, tzinfo=UTC),
            datetime(2026, 7, 27, 0, 15, tzinfo=UTC),
            datetime(2026, 7, 27, 0, 16, tzinfo=UTC),
            datetime(2026, 7, 27, 0, 16, tzinfo=UTC),
        )
    )
    monkeypatch.setattr(adapter, "_utc_now", lambda: next(times))
    monkeypatch.setattr(
        adapter,
        "acquire_marketplace_collection_lease",
        lambda: lease,
    )
    monkeypatch.setattr(
        adapter,
        "load_required_runtime_environment",
        lambda **_kwargs: {},
    )
    monkeypatch.setattr(
        adapter.subprocess,
        "Popen",
        lambda *_args, **_kwargs: pytest.fail("child must not spawn"),
    )
    captured: dict[str, object] = {}
    monkeypatch.setattr(
        adapter,
        "write_terminal_result",
        lambda **kwargs: captured.update(kwargs) or 75,
    )
    monkeypatch.setattr(
        adapter.os,
        "_exit",
        lambda code: (_ for _ in ()).throw(ExitCalled(code)),
    )
    try:
        with pytest.raises(ExitCalled) as raised:
            adapter._run_four_region(
                COORDINATOR_ARGUMENTS
            )
        assert raised.value.code == 75
        assert captured["reason_code"] == (
            "absolute_deadline_reached_before_spawn"
        )
    finally:
        os.close(validation_fd)


def test_supervisor_waits_for_surviving_descendant(
    tmp_path: Path,
) -> None:
    policy = _lock_policy(tmp_path)
    lease = contract.acquire_marketplace_collection_lease(
        environment={},
        policy=policy,
        quarantine_marker_path=tmp_path / "missing.json",
    )
    leader = tmp_path / "leader.py"
    leader.write_text(
        "import subprocess\n"
        "subprocess.Popen(['/bin/sleep', '0.45'])\n",
        encoding="utf-8",
    )
    environment = {
        **os.environ,
        **contract.descendant_lease_environment(lease),
    }
    environment["PARSER_WB_SUPERVISOR_PASS_FDS"] = ",".join(
        str(value) for value in lease.pass_fds
    )
    started = time.monotonic()
    try:
        completed = subprocess.run(
            [
                sys.executable,
                str(adapter.SUPERVISOR),
                "--",
                sys.executable,
                str(leader),
            ],
            env=environment,
            close_fds=True,
            pass_fds=lease.pass_fds,
            check=False,
        )
        assert completed.returncode == 0
        assert time.monotonic() - started >= 0.35
        lease.assert_held()
    finally:
        lease.__exit__()


def test_proc_stat_parser_accepts_comm_with_spaces_and_parentheses() -> None:
    assert supervisor._parse_proc_stat(
        "4321 (worker (x) with spaces) "
        "S 123 456 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 987654",
        expected_pid=4321,
    ) == supervisor.ProcessStat(
        ppid=123,
        pgrp=456,
        state="S",
        starttime=987654,
    )


def test_supervisor_does_not_signal_reused_pid_or_process_group(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    supervisor_pid = 50
    leader = supervisor.ProcessIdentity(pid=100, starttime=1000)
    descendant = supervisor.ProcessIdentity(pid=101, starttime=1001)
    initial = {
        100: supervisor.ProcessStat(50, 100, "S", 1000),
        101: supervisor.ProcessStat(100, 100, "S", 1001),
        200: supervisor.ProcessStat(1, 100, "S", 2000),
    }
    owned = supervisor._refresh_owned(
        {leader},
        supervisor_pid=supervisor_pid,
        baseline_children=set(),
        table=initial,
    )
    assert owned == {leader, descendant}

    reused = {
        100: supervisor.ProcessStat(1, 100, "S", 9000),
        101: supervisor.ProcessStat(50, 777, "S", 1001),
        200: supervisor.ProcessStat(1, 100, "S", 2000),
    }
    owned = supervisor._refresh_owned(
        owned,
        supervisor_pid=supervisor_pid,
        baseline_children=set(),
        table=reused,
    )
    assert owned == {descendant}

    identities = {
        100: supervisor.ProcessIdentity(100, 9000),
        101: descendant,
        200: supervisor.ProcessIdentity(200, 2000),
    }
    signaled: list[tuple[int, int]] = []
    monkeypatch.setattr(
        supervisor,
        "_current_identity",
        lambda pid: identities.get(pid),
    )
    monkeypatch.setattr(supervisor.os, "pidfd_open", lambda pid, _flags: pid + 500)
    monkeypatch.setattr(
        supervisor.signal,
        "pidfd_send_signal",
        lambda pidfd, signum, _siginfo, _flags: signaled.append(
            (pidfd, signum)
        ),
    )
    monkeypatch.setattr(supervisor.os, "close", lambda _fd: None)
    monkeypatch.setattr(
        supervisor.os,
        "kill",
        lambda _pid, _signum: pytest.fail("numeric PID signal is forbidden"),
    )
    assert supervisor._signal_identity(descendant, signal.SIGTERM) is True
    assert signaled == [(601, signal.SIGTERM)]
    assert supervisor._signal_identity(leader, signal.SIGTERM) is False
    assert signaled == [(601, signal.SIGTERM)]


def test_supervisor_requires_pidfd_before_child_spawn(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(supervisor.sys, "argv", ["supervisor", "--", "child"])
    monkeypatch.setattr(supervisor, "_inherited_fds", lambda: (3, 4))
    monkeypatch.setattr(
        supervisor.ctypes,
        "CDLL",
        lambda *_args, **_kwargs: SimpleNamespace(
            prctl=lambda *_args: 0
        ),
    )
    monkeypatch.setattr(supervisor.os, "pidfd_open", None)
    monkeypatch.setattr(
        supervisor.subprocess,
        "Popen",
        lambda *_args, **_kwargs: pytest.fail(
            "child must not spawn without pidfd"
        ),
    )
    assert supervisor.main() == 2


def test_supervisor_pidfd_send_capability_fails_before_spawn(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pid = os.getpid()
    identity = supervisor.ProcessIdentity(pid=pid, starttime=456)
    monkeypatch.setattr(
        supervisor,
        "_current_identity",
        lambda current_pid: identity if current_pid == pid else None,
    )
    monkeypatch.setattr(
        supervisor.os,
        "pidfd_open",
        lambda _pid, _flags: 999,
    )
    monkeypatch.setattr(supervisor.os, "close", lambda _fd: None)
    monkeypatch.setattr(
        supervisor.signal,
        "pidfd_send_signal",
        lambda *_args: (_ for _ in ()).throw(OSError("denied")),
    )
    with pytest.raises(
        supervisor.PidfdCapabilityError,
        match="capability check failed",
    ):
        supervisor._require_pidfd_capability()


def test_supervisor_pidfd_failure_never_signals_unpinned_pid(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    identity = supervisor.ProcessIdentity(pid=123, starttime=456)
    monkeypatch.setattr(
        supervisor,
        "_current_identity",
        lambda _pid: identity,
    )
    monkeypatch.setattr(
        supervisor.os,
        "pidfd_open",
        lambda _pid, _flags: (_ for _ in ()).throw(OSError("denied")),
    )
    monkeypatch.setattr(
        supervisor.os,
        "kill",
        lambda _pid, _signum: pytest.fail("numeric PID signal is forbidden"),
    )
    with pytest.raises(supervisor.PidfdSignalError, match="pidfd open"):
        supervisor._signal_identity(identity, signal.SIGTERM)

    identities = iter((identity, None))
    monkeypatch.setattr(
        supervisor,
        "_current_identity",
        lambda _pid: next(identities),
    )
    assert supervisor._signal_identity(identity, signal.SIGTERM) is False

    monkeypatch.setattr(
        supervisor,
        "_current_identity",
        lambda _pid: identity,
    )
    monkeypatch.setattr(
        supervisor.os,
        "pidfd_open",
        lambda _pid, _flags: 999,
    )
    monkeypatch.setattr(supervisor.os, "close", lambda _fd: None)
    monkeypatch.setattr(
        supervisor.signal,
        "pidfd_send_signal",
        lambda *_args: (_ for _ in ()).throw(OSError("denied")),
    )
    with pytest.raises(supervisor.PidfdSignalError, match="pidfd signal"):
        supervisor._signal_identity(identity, signal.SIGTERM)


def test_supervisor_retains_lease_until_descendant_exits_after_pidfd_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    identity = supervisor.ProcessIdentity(pid=123, starttime=456)
    refreshes = iter(({identity}, {identity}, set()))
    monkeypatch.setattr(
        supervisor,
        "_refresh_owned",
        lambda *_args, **_kwargs: next(refreshes),
    )
    monkeypatch.setattr(
        supervisor,
        "_signal_identity",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            supervisor.PidfdSignalError("pidfd signal failed")
        ),
    )
    sleeps: list[float] = []
    monkeypatch.setattr(supervisor.time, "sleep", sleeps.append)
    assert supervisor._terminate_owned(
        {identity},
        supervisor_pid=50,
        baseline_children=set(),
    ) is False
    assert len(sleeps) == 2


def test_supervisor_waits_for_detached_adopted_descendant_with_complex_comm(
    tmp_path: Path,
) -> None:
    policy = _lock_policy(tmp_path)
    lease = contract.acquire_marketplace_collection_lease(
        environment={},
        policy=policy,
        quarantine_marker_path=tmp_path / "missing.json",
    )
    detached = tmp_path / "detached.py"
    detached.write_text(
        "import time\n"
        "open('/proc/self/comm', 'w').write('worker (x) ok\\n')\n"
        "time.sleep(0.55)\n",
        encoding="utf-8",
    )
    leader = tmp_path / "leader.py"
    leader.write_text(
        "import subprocess, sys\n"
        "subprocess.Popen("
        "[sys.executable, sys.argv[1]], start_new_session=True)\n",
        encoding="utf-8",
    )
    environment = {
        **os.environ,
        **contract.descendant_lease_environment(lease),
    }
    environment["PARSER_WB_SUPERVISOR_PASS_FDS"] = ",".join(
        str(value) for value in lease.pass_fds
    )
    started = time.monotonic()
    try:
        completed = subprocess.run(
            [
                sys.executable,
                str(adapter.SUPERVISOR),
                "--",
                sys.executable,
                str(leader),
                str(detached),
            ],
            env=environment,
            close_fds=True,
            pass_fds=lease.pass_fds,
            check=False,
        )
        assert completed.returncode == 0
        assert time.monotonic() - started >= 0.45
        lease.assert_held()
    finally:
        lease.__exit__()


def test_supervisor_retains_locks_after_adapter_parent_death(
    tmp_path: Path,
) -> None:
    policy = _lock_policy(tmp_path)
    bootstrap = tmp_path / "bootstrap.py"
    ready = tmp_path / "ready"
    bootstrap.write_text(
        "import fcntl, os, subprocess, sys\n"
        "guard=os.open(sys.argv[1], os.O_RDWR)\n"
        "validation=os.open(sys.argv[2], os.O_RDWR)\n"
        "fcntl.flock(guard, fcntl.LOCK_EX)\n"
        "fcntl.flock(validation, fcntl.LOCK_EX)\n"
        "env=os.environ.copy()\n"
        "env['PARSER_WB_LOCK_V3_GUARD_FD']=str(guard)\n"
        "env['PARSER_WB_LOCK_V3_VALIDATION_FD']=str(validation)\n"
        "env['PARSER_WB_SUPERVISOR_PASS_FDS']=f'{guard},{validation}'\n"
        "child=subprocess.Popen("
        "[sys.executable, sys.argv[3], '--', '/bin/sleep', '0.55'], "
        "env=env, pass_fds=(guard, validation), start_new_session=True)\n"
        "open(sys.argv[4], 'w').write(str(child.pid))\n",
        encoding="utf-8",
    )
    completed = subprocess.run(
        [
            sys.executable,
            str(bootstrap),
            str(policy.guard_path),
            str(policy.validation_path),
            str(adapter.SUPERVISOR),
            str(ready),
        ],
        check=False,
    )
    assert completed.returncode == 0
    assert ready.is_file()
    guard_probe = os.open(policy.guard_path, os.O_RDWR)
    validation_probe = os.open(policy.validation_path, os.O_RDWR)
    try:
        with pytest.raises(BlockingIOError):
            fcntl.flock(guard_probe, fcntl.LOCK_EX | fcntl.LOCK_NB)
        with pytest.raises(BlockingIOError):
            fcntl.flock(
                validation_probe,
                fcntl.LOCK_EX | fcntl.LOCK_NB,
            )
        deadline = time.monotonic() + 3
        while time.monotonic() < deadline:
            try:
                fcntl.flock(
                    guard_probe,
                    fcntl.LOCK_EX | fcntl.LOCK_NB,
                )
                fcntl.flock(
                    validation_probe,
                    fcntl.LOCK_EX | fcntl.LOCK_NB,
                )
                break
            except BlockingIOError:
                time.sleep(0.05)
        else:
            pytest.fail("orphan supervisor did not release finished lease")
    finally:
        os.close(guard_probe)
        os.close(validation_probe)


def test_lease_release_never_uses_lock_un() -> None:
    source = (
        PROJECT_ROOT / "app/common/nightly_coordinator.py"
    ).read_text(encoding="utf-8")
    exit_source = source[
        source.index("def __exit__"):source.index(
            "\ndef _safe_id",
            source.index("def __exit__"),
        )
    ]
    assert "LOCK_UN" not in exit_source


def test_input_manifest_detects_omission_and_mutation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    for relative in ("app", "scripts", "config"):
        shutil.copytree(PROJECT_ROOT / relative, tmp_path / relative)
    shutil.copy2(PROJECT_ROOT / "main.py", tmp_path / "main.py")
    shutil.copy2(PROJECT_ROOT / "requirements.txt", tmp_path / "requirements.txt")
    dependencies = tmp_path / "site-packages"
    dependencies.mkdir()
    (dependencies / "approved.py").write_text("VALUE = 1\n", encoding="utf-8")
    _remove_group_world_write(tmp_path)
    monkeypatch.setattr(
        attestation,
        "APPROVED_SITE_PACKAGES",
        dependencies,
    )
    manifest_path = tmp_path / attestation.MANIFEST_RELATIVE_PATH
    manifest_path.write_text(
        json.dumps(
            attestation.build_input_manifest(tmp_path),
            ensure_ascii=False,
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )
    manifest_path.chmod(0o644)
    assert len(attestation.verify_input_manifest(tmp_path)) == 64
    target = tmp_path / "app/common/cleanup.py"
    target.write_bytes(target.read_bytes() + b"\n")
    with pytest.raises(
        contract.NightlyCoordinatorContractError,
        match="coordinator_input_manifest_hash_mismatch",
    ):
        attestation.verify_input_manifest(tmp_path)


def test_runtime_input_fingerprint_binds_secret_files_without_values(
    tmp_path: Path,
) -> None:
    config_dir = tmp_path / "config"
    config_dir.mkdir()
    runtime = config_dir / "runtime.env"
    headers = config_dir / "wb_request_headers.json"
    cookie = config_dir / "wb_cookie.txt"
    runtime.write_text(
        "PARSER_WB_PROXY_URL=test-route.invalid\n",
        encoding="utf-8",
    )
    headers.write_text(
        '{"x-test-header":"test-value"}\n',
        encoding="utf-8",
    )
    cookie.write_text("test-cookie-value\n", encoding="utf-8")
    for path in (runtime, headers, cookie):
        path.chmod(0o600)
    loaded = runtime_env.load_strict_runtime_environment(
        project_root=tmp_path,
        base_environment={
            "PARSER_WB_PROXY_URL": "http://test-route.invalid",
            "PARSER_WB_REQUEST_HEADERS_FILE": str(headers),
            "PARSER_WB_COOKIE_REQUIRED": "1",
        },
    )
    environment = loaded.environment
    first = attestation.runtime_input_sha256(tmp_path, environment)
    headers.write_text('{"x-test-header":"changed"}\n', encoding="utf-8")
    headers.chmod(0o600)
    second = attestation.runtime_input_sha256(tmp_path, environment)
    assert first != second
    assert len(first) == len(second) == 64


def test_runtime_attestation_rejects_substitute_cookie_path(
    tmp_path: Path,
) -> None:
    config_dir = tmp_path / "config"
    config_dir.mkdir()
    runtime = config_dir / "runtime.env"
    headers = config_dir / "wb_request_headers.json"
    cookie = config_dir / "wb_cookie.txt"
    substitute = config_dir / "substitute_cookie.txt"
    runtime.write_text(
        "PARSER_WB_REQUEST_HEADERS_FILE="
        f"{headers.as_posix()}\n",
        encoding="utf-8",
    )
    headers.write_text("{}\n", encoding="utf-8")
    cookie.write_text("approved\n", encoding="utf-8")
    substitute.write_text("substitute\n", encoding="utf-8")
    for path in (runtime, headers, cookie, substitute):
        path.chmod(0o600)
    with pytest.raises(
        runtime_env.RuntimeEnvironmentError,
        match="runtime_cookie_path_invalid",
    ):
        runtime_env.load_strict_runtime_environment(
            project_root=tmp_path,
            base_environment={
                "WB_COOKIE_FILE": str(substitute),
            },
        )
    substitute.unlink()
    substitute.symlink_to(cookie)
    with pytest.raises(
        runtime_env.RuntimeEnvironmentError,
        match="runtime_cookie_path_invalid",
    ):
        runtime_env.load_strict_runtime_environment(
            project_root=tmp_path,
            base_environment={
                "WB_COOKIE_FILE": str(substitute),
            },
        )


def test_dependency_tree_drift_invalidates_pinned_manifest(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    project = tmp_path / "project"
    for relative in ("app", "scripts", "config"):
        shutil.copytree(PROJECT_ROOT / relative, project / relative)
    shutil.copy2(PROJECT_ROOT / "main.py", project / "main.py")
    shutil.copy2(PROJECT_ROOT / "requirements.txt", project / "requirements.txt")
    dependencies = tmp_path / "site-packages"
    dependencies.mkdir()
    dependency = dependencies / "approved.py"
    dependency.write_text("VALUE = 1\n", encoding="utf-8")
    _remove_group_world_write(project)
    _remove_group_world_write(dependencies)
    monkeypatch.setattr(
        attestation,
        "APPROVED_SITE_PACKAGES",
        dependencies,
    )
    manifest_path = project / attestation.MANIFEST_RELATIVE_PATH
    manifest_path.write_text(
        json.dumps(
            attestation.build_input_manifest(project),
            sort_keys=True,
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )
    manifest_path.chmod(0o644)
    assert len(attestation.verify_input_manifest(project)) == 64
    dependency.chmod(0o664)
    with pytest.raises(
        contract.NightlyCoordinatorContractError,
        match="coordinator_python_dependencies_unsafe",
    ):
        attestation.verify_input_manifest(project)
    dependency.chmod(0o644)
    tracked_module = project / "app/common/paths.py"
    tracked_module.chmod(0o664)
    with pytest.raises(
        contract.NightlyCoordinatorContractError,
        match="coordinator_input_metadata_invalid",
    ):
        attestation.verify_input_manifest(project)
    tracked_module.chmod(0o644)
    dependency.write_text("VALUE = 2\n", encoding="utf-8")
    with pytest.raises(
        contract.NightlyCoordinatorContractError,
        match="coordinator_python_runtime_mismatch",
    ):
        attestation.verify_input_manifest(project)


@pytest.mark.parametrize("operation", ("add", "remove", "change"))
def test_dependency_attestation_pins_timestamp_valid_bytecode(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    operation: str,
) -> None:
    project = tmp_path / "project"
    for relative in ("app", "scripts", "config"):
        shutil.copytree(PROJECT_ROOT / relative, project / relative)
    shutil.copy2(PROJECT_ROOT / "main.py", project / "main.py")
    shutil.copy2(PROJECT_ROOT / "requirements.txt", project / "requirements.txt")
    dependencies = tmp_path / "site-packages"
    dependencies.mkdir()
    source = dependencies / "approved.py"
    source.write_text("VALUE = 1\n", encoding="utf-8")
    bytecode = dependencies / "__pycache__/approved.pyc"
    bytecode.parent.mkdir()
    py_compile.compile(str(source), cfile=str(bytecode), doraise=True)
    _remove_group_world_write(project)
    _remove_group_world_write(dependencies)
    monkeypatch.setattr(
        attestation,
        "APPROVED_SITE_PACKAGES",
        dependencies,
    )
    manifest_path = project / attestation.MANIFEST_RELATIVE_PATH
    manifest_path.write_text(
        json.dumps(
            attestation.build_input_manifest(project),
            sort_keys=True,
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )
    manifest_path.chmod(0o644)
    assert len(attestation.verify_input_manifest(project)) == 64
    if operation == "add":
        injected_source = tmp_path / "injected.py"
        injected_source.write_text("VALUE = 2\n", encoding="utf-8")
        py_compile.compile(
            str(injected_source),
            cfile=str(bytecode.parent / "injected.pyc"),
            doraise=True,
        )
    elif operation == "remove":
        bytecode.unlink()
    else:
        payload = bytearray(bytecode.read_bytes())
        payload[-1] ^= 1
        bytecode.write_bytes(payload)
    _remove_group_world_write(dependencies)
    with pytest.raises(
        contract.NightlyCoordinatorContractError,
        match="coordinator_python_runtime_mismatch",
    ):
        attestation.verify_input_manifest(project)


def test_strict_runtime_loader_rejects_shell_expression_without_spawn(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = tmp_path / "config"
    config.mkdir()
    runtime = config / "runtime.env"
    cookie = config / "wb_cookie.txt"
    runtime.write_text(
        "PARSER_WB_PROXY_URL=$(touch should-not-run)\n",
        encoding="utf-8",
    )
    cookie.write_text("cookie\n", encoding="utf-8")
    runtime.chmod(0o600)
    cookie.chmod(0o600)
    monkeypatch.setattr(
        subprocess,
        "Popen",
        lambda *_args, **_kwargs: pytest.fail(
            "strict runtime loading must not spawn"
        ),
    )
    with pytest.raises(
        runtime_env.RuntimeEnvironmentError,
        match="runtime_env_value_invalid",
    ):
        runtime_env.load_strict_runtime_environment(
            project_root=tmp_path,
            base_environment={},
        )


def test_durable_atomic_writer_refuses_symlink_race_and_partial_write(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    target = tmp_path / "latest.json"
    outside = tmp_path / "outside.json"
    outside.write_bytes(b"outside\n")
    target.symlink_to(outside)
    with pytest.raises(durable_atomic.DurableAtomicWriteError):
        durable_atomic.durable_atomic_replace(target, b"new\n")
    assert outside.read_bytes() == b"outside\n"
    target.unlink()
    target.write_bytes(b"old\n")

    def race(event: str, path: Path) -> None:
        if event == "before_target_recheck":
            path.write_bytes(b"racer\n")

    with pytest.raises(
        durable_atomic.DurableAtomicWriteError,
        match="changed before commit",
    ):
        durable_atomic.durable_atomic_replace(
            target,
            b"new\n",
            event_hook=race,
        )
    assert target.read_bytes() == b"racer\n"

    original_fsync = durable_atomic.os.fsync
    failed = False

    def fail_first_fsync(fd: int) -> None:
        nonlocal failed
        if not failed:
            failed = True
            raise OSError("injected fsync failure")
        original_fsync(fd)

    target.write_bytes(b"stable\n")
    monkeypatch.setattr(durable_atomic.os, "fsync", fail_first_fsync)
    with pytest.raises(OSError, match="injected fsync failure"):
        durable_atomic.durable_atomic_replace(target, b"partial\n")
    assert target.read_bytes() == b"stable\n"
    assert not list(tmp_path.glob(".latest.json.*.tmp"))


def test_durable_atomic_writer_rejects_ancestor_exchange_and_hardlinks(
    tmp_path: Path,
) -> None:
    parent = tmp_path / "publication"
    parent.mkdir()
    target = parent / "latest.json"
    target.write_bytes(b"old\n")
    outside = tmp_path / "outside"
    outside.mkdir()
    detached = tmp_path / "detached"

    def exchange_ancestor(event: str, _path: Path) -> None:
        if event == "before_target_recheck":
            parent.rename(detached)
            parent.symlink_to(outside, target_is_directory=True)

    with pytest.raises(
        durable_atomic.DurableAtomicWriteError,
        match="parent changed",
    ):
        durable_atomic.durable_atomic_replace(
            target,
            b"new\n",
            event_hook=exchange_ancestor,
        )
    assert not (outside / "latest.json").exists()
    assert (detached / "latest.json").read_bytes() == b"old\n"

    parent.unlink()
    detached.rename(parent)
    linked_target = parent / "linked.json"
    linked_alias = parent / "linked-alias.json"
    linked_target.write_bytes(b"linked\n")
    os.link(linked_target, linked_alias)
    with pytest.raises(
        durable_atomic.DurableAtomicWriteError,
        match="target is unsafe",
    ):
        durable_atomic.durable_atomic_replace(
            linked_target,
            b"replacement\n",
        )
    assert linked_alias.read_bytes() == b"linked\n"

    source = parent / "source.csv"
    source_alias = parent / "source-alias.csv"
    destination = parent / "destination.csv"
    source.write_bytes(b"source\n")
    os.link(source, source_alias)
    with pytest.raises(
        durable_atomic.DurableAtomicWriteError,
        match="target is unsafe",
    ):
        durable_atomic.durable_atomic_copy(source, destination)
    assert not destination.exists()


def test_durable_publication_attests_immediately_before_commit(
    tmp_path: Path,
) -> None:
    target = tmp_path / "latest.json"
    target.write_bytes(b"stable\n")
    gate_calls = 0

    def changed_input() -> None:
        nonlocal gate_calls
        gate_calls += 1
        raise contract.NightlyCoordinatorContractError(
            "coordinator_attested_input_changed",
            outcome="hard_failure",
        )

    with pytest.raises(
        contract.NightlyCoordinatorContractError,
        match="coordinator_attested_input_changed",
    ):
        durable_atomic.durable_atomic_replace(
            target,
            b"untrusted\n",
            integrity_gate=changed_input,
        )
    assert gate_calls == 1
    assert target.read_bytes() == b"stable\n"
    assert not list(tmp_path.glob(".latest.json.*.tmp"))


def test_durable_writer_pins_temp_and_target_identity_through_commit(
    tmp_path: Path,
) -> None:
    target = tmp_path / "latest.json"
    target.write_bytes(b"trusted\n")

    def substitute_temp(event: str, _path: Path) -> None:
        if event != "before_commit":
            return
        temp = next(tmp_path.glob(".latest.json.*.tmp"))
        temp.unlink()
        temp.write_bytes(b"substitute\n")

    with pytest.raises(
        durable_atomic.DurableAtomicWriteError,
        match="atomic file",
    ):
        durable_atomic.durable_atomic_replace(
            target,
            b"candidate\n",
            event_hook=substitute_temp,
        )
    assert target.read_bytes() == b"trusted\n"

    def mutate_committed_leaf(event: str, path: Path) -> None:
        if event == "after_rename":
            path.unlink()
            path.write_bytes(b"late-substitute\n")

    with pytest.raises(durable_atomic.DurableAtomicWriteError):
        durable_atomic.durable_atomic_replace(
            target,
            b"candidate\n",
            event_hook=mutate_committed_leaf,
        )
    assert target.read_bytes() == b"late-substitute\n"
    assert target.stat().st_nlink == 1


def test_durable_writer_cas_rollback_and_cleanup_debt(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    target = tmp_path / "latest.json"
    target.write_bytes(b"trusted\n")

    def second_writer(event: str, path: Path) -> None:
        if event == "after_rename":
            replacement = tmp_path / "second-writer.json"
            replacement.write_bytes(b"newer\n")
            os.replace(replacement, path)

    with pytest.raises(durable_atomic.DurableAtomicWriteError):
        durable_atomic.durable_atomic_replace(
            target,
            b"candidate\n",
            event_hook=second_writer,
        )
    assert target.read_bytes() == b"newer\n"

    target.write_bytes(b"trusted\n")

    def swap_after_proof(event: str, path: Path) -> None:
        if event == "publication_proved":
            replacement = tmp_path / "post-proof.json"
            replacement.write_bytes(b"newest\n")
            os.replace(replacement, path)

    with pytest.raises(
        durable_atomic.DurableAtomicWriteError,
        match="changed after proof",
    ):
        durable_atomic.durable_atomic_replace(
            target,
            b"candidate\n",
            event_hook=swap_after_proof,
        )
    assert target.read_bytes() == b"newest\n"

    target.write_bytes(b"trusted\n")
    original_unlink = durable_atomic.os.unlink

    def fail_backup_unlink(
        path: str,
        *,
        dir_fd: int | None = None,
    ) -> None:
        if path.endswith(".rollback"):
            raise OSError("cleanup unavailable")
        original_unlink(path, dir_fd=dir_fd)

    monkeypatch.setattr(durable_atomic.os, "unlink", fail_backup_unlink)
    caplog.set_level("WARNING")
    result = durable_atomic.durable_atomic_replace(target, b"durable\n")
    assert result.sha256 == hashlib.sha256(b"durable\n").hexdigest()
    assert result.cleanup_debt_count == 1
    assert target.read_bytes() == b"durable\n"
    assert "cleanup debt" in caplog.text


def test_durable_writer_cleanup_fsync_failure_is_not_publication_failure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    target = tmp_path / "latest.json"
    target.write_bytes(b"trusted\n")
    original_fsync = durable_atomic.os.fsync
    directory_fsync_calls = 0

    def fail_cleanup_fsync(fd: int) -> None:
        nonlocal directory_fsync_calls
        info = os.fstat(fd)
        if stat.S_ISDIR(info.st_mode):
            directory_fsync_calls += 1
            if directory_fsync_calls == 3:
                raise OSError("cleanup fsync unavailable")
        original_fsync(fd)

    monkeypatch.setattr(durable_atomic.os, "fsync", fail_cleanup_fsync)
    caplog.set_level("WARNING")
    result = durable_atomic.durable_atomic_replace(target, b"durable\n")
    assert result.sha256 == hashlib.sha256(b"durable\n").hexdigest()
    assert result.cleanup_debt_count == 1
    assert target.read_bytes() == b"durable\n"
    assert "cleanup debt" in caplog.text


def test_durable_writer_bounds_cleanup_debt_under_verified_lease(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    project = tmp_path / "project"
    project.mkdir(mode=0o700)
    target = project / "latest.json"
    target.write_bytes(b"trusted\n")
    gate = _cleanup_debt_gate(project)
    original_unlink = durable_atomic.os.unlink

    def fail_backup_unlink(
        path: str,
        *,
        dir_fd: int | None = None,
    ) -> None:
        if path.startswith(".wb-rollback-v1.") and path.endswith(".debt"):
            raise OSError("cleanup unavailable")
        original_unlink(path, dir_fd=dir_fd)

    monkeypatch.setattr(durable_atomic.os, "unlink", fail_backup_unlink)
    first = durable_atomic.durable_atomic_replace(
        target,
        b"first\n",
        integrity_gate=gate,
    )
    second = durable_atomic.durable_atomic_replace(
        target,
        b"second\n",
        integrity_gate=gate,
    )
    assert first.cleanup_debt_count == 1
    assert second.cleanup_debt_count == 2
    with pytest.raises(
        durable_atomic.DurableCleanupDebtError,
        match="limit reached after commit",
    ):
        durable_atomic.durable_atomic_replace(
            target,
            b"third\n",
            integrity_gate=gate,
        )
    assert target.read_bytes() == b"third\n"
    with pytest.raises(
        durable_atomic.DurableCleanupDebtError,
        match="limit reached",
    ):
        durable_atomic.durable_atomic_replace(
            target,
            b"must-not-commit\n",
            integrity_gate=gate,
        )
    assert target.read_bytes() == b"third\n"
    debt_dir = project / durable_atomic.CLEANUP_DEBT_DIRECTORY
    assert len(tuple(debt_dir.iterdir())) == durable_atomic.CLEANUP_DEBT_LIMIT


def test_durable_writer_sweeps_only_proven_cleanup_debt(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    project = tmp_path / "project"
    project.mkdir(mode=0o700)
    target = project / "latest.json"
    unrelated = project / ".unrelated.rollback"
    target.write_bytes(b"trusted\n")
    unrelated.write_bytes(b"keep\n")
    gate = _cleanup_debt_gate(project)
    original_unlink = durable_atomic.os.unlink

    def fail_once(
        path: str,
        *,
        dir_fd: int | None = None,
    ) -> None:
        if path.startswith(".wb-rollback-v1.") and path.endswith(".debt"):
            raise OSError("cleanup unavailable")
        original_unlink(path, dir_fd=dir_fd)

    monkeypatch.setattr(durable_atomic.os, "unlink", fail_once)
    first = durable_atomic.durable_atomic_replace(
        target,
        b"first\n",
        integrity_gate=gate,
    )
    assert first.cleanup_debt_count == 1
    monkeypatch.setattr(durable_atomic.os, "unlink", original_unlink)
    second = durable_atomic.durable_atomic_replace(
        target,
        b"second\n",
        integrity_gate=gate,
    )
    assert second.cleanup_debt_count == 0
    assert second.cleanup_debt_swept == 1
    assert unrelated.read_bytes() == b"keep\n"
    assert not tuple(
        (project / durable_atomic.CLEANUP_DEBT_DIRECTORY).iterdir()
    )


@pytest.mark.parametrize("unsafe_kind", ("unknown", "symlink", "non_owner"))
def test_cleanup_debt_sweep_refuses_unproved_entries(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    unsafe_kind: str,
) -> None:
    project = tmp_path / "project"
    project.mkdir(mode=0o700)
    target = project / "latest.json"
    target.write_bytes(b"trusted\n")
    gate = _cleanup_debt_gate(project)
    state = project / "state"
    state.mkdir(mode=0o700)
    debt_dir = state / "wb_durable_cleanup_debt"
    debt_dir.mkdir(mode=0o700)
    outside = tmp_path / "outside"
    outside.write_bytes(b"outside\n")
    if unsafe_kind == "unknown":
        unsafe = debt_dir / "unknown"
        unsafe.write_bytes(b"unknown\n")
    elif unsafe_kind == "symlink":
        unsafe = debt_dir / (
            ".wb-durable-debt-v1." + ("a" * 32) + ".json"
        )
        unsafe.symlink_to(outside)
    else:
        unsafe = debt_dir
        original_geteuid = durable_atomic.os.geteuid
        monkeypatch.setattr(
            durable_atomic.os,
            "geteuid",
            lambda: original_geteuid() + 1,
        )
    with pytest.raises(
        (durable_atomic.DurableAtomicWriteError, OSError)
    ):
        durable_atomic.durable_atomic_replace(
            target,
            b"candidate\n",
            integrity_gate=gate,
        )
    assert target.read_bytes() == b"trusted\n"
    assert unsafe.exists() or unsafe.is_symlink()
    assert outside.read_bytes() == b"outside\n"


def test_durable_writer_normal_path_has_no_cleanup_debt(
    tmp_path: Path,
) -> None:
    project = tmp_path / "project"
    project.mkdir(mode=0o700)
    target = project / "latest.json"
    target.write_bytes(b"trusted\n")
    result = durable_atomic.durable_atomic_replace(
        target,
        b"candidate\n",
        integrity_gate=_cleanup_debt_gate(project),
    )
    assert result.cleanup_debt_status == "clear"
    assert result.cleanup_debt_count == 0
    assert not tuple(
        (project / durable_atomic.CLEANUP_DEBT_DIRECTORY).iterdir()
    )


def test_cleanup_debt_contract_revalidates_inherited_host_lease(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    project = tmp_path / "project"
    project.mkdir(mode=0o700)
    target = project / "latest.json"
    target.write_bytes(b"trusted\n")
    validations: list[dict[str, str]] = []
    environment = {"PARSER_WB_LOCK_V3_CONTRACT": "test"}

    def validate(*, environment, policy=None) -> int:
        del policy
        validations.append(dict(environment))
        return 1

    monkeypatch.setattr(
        contract,
        "validate_descendant_marketplace_lease",
        validate,
    )
    gate = attestation.integrity_gate(project, environment)
    result = durable_atomic.durable_atomic_replace(
        target,
        b"candidate\n",
        integrity_gate=gate,
    )
    assert result.cleanup_debt_status == "clear"
    assert len(validations) >= 3
    assert all(
        item == environment
        for item in validations
    )


def test_durable_copy_pins_source_before_and_after_commit(
    tmp_path: Path,
) -> None:
    source = tmp_path / "source.csv"
    target = tmp_path / "latest.csv"
    source.write_bytes(b"source-v1\n")
    target.write_bytes(b"trusted\n")

    def replace_source_before_commit(event: str, _path: Path) -> None:
        if event == "before_commit":
            detached = tmp_path / "source.detached"
            source.rename(detached)
            source.write_bytes(b"source-v2\n")

    with pytest.raises(durable_atomic.DurableAtomicWriteError):
        durable_atomic.durable_atomic_copy(
            source,
            target,
            event_hook=replace_source_before_commit,
        )
    assert target.read_bytes() == b"trusted\n"

    source.write_bytes(b"source-v3\n")

    def mutate_source_after_rename(event: str, _path: Path) -> None:
        if event == "after_rename":
            source.write_bytes(b"source-v4\n")

    with pytest.raises(durable_atomic.DurableAtomicWriteError):
        durable_atomic.durable_atomic_copy(
            source,
            target,
            event_hook=mutate_source_after_rename,
        )
    assert target.read_bytes() == b"trusted\n"
    assert target.stat().st_nlink == 1


def test_deadline_refusal_happens_before_runtime_load_or_child(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    validation = tmp_path / "validation.flock"
    validation.touch()
    validation_fd = os.open(validation, os.O_RDWR)
    invocation = replace(
        _invocation(tmp_path),
        deadline_utc=datetime.now(UTC) - timedelta(seconds=1),
    )
    lease = FakeLease(invocation=invocation, validation_fd=validation_fd)
    captured: dict[str, object] = {}
    monkeypatch.setattr(
        adapter,
        "acquire_marketplace_collection_lease",
        lambda: lease,
    )
    monkeypatch.setattr(
        adapter,
        "load_required_runtime_environment",
        lambda **_kwargs: pytest.fail("runtime must not load after deadline"),
    )
    monkeypatch.setattr(
        adapter.subprocess,
        "Popen",
        lambda *_args, **_kwargs: pytest.fail("child must not start"),
    )

    def fake_write(**kwargs: object) -> int:
        captured.update(kwargs)
        return 75

    monkeypatch.setattr(adapter, "write_terminal_result", fake_write)
    monkeypatch.setattr(
        adapter.os,
        "_exit",
        lambda code: (_ for _ in ()).throw(ExitCalled(code)),
    )
    try:
        with pytest.raises(ExitCalled) as raised:
            adapter._run_four_region(
                COORDINATOR_ARGUMENTS
            )
        assert raised.value.code == 75
        assert captured["outcome"] == "deferred"
        assert captured["reason_code"] == "absolute_deadline_reached_before_runtime"
    finally:
        os.close(validation_fd)


def test_runtime_validation_failure_stops_before_child_spawn(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    validation = tmp_path / "validation.flock"
    validation.touch()
    validation_fd = os.open(validation, os.O_RDWR)
    lease = FakeLease(
        invocation=_invocation(tmp_path),
        validation_fd=validation_fd,
    )
    captured: dict[str, object] = {}
    monkeypatch.setattr(
        adapter,
        "acquire_marketplace_collection_lease",
        lambda: lease,
    )
    monkeypatch.setattr(
        adapter,
        "load_required_runtime_environment",
        lambda **_kwargs: (_ for _ in ()).throw(
            contract.NightlyCoordinatorContractError(
                "runtime_env_syntax_invalid",
                outcome="hard_failure",
            )
        ),
    )
    monkeypatch.setattr(
        adapter.subprocess,
        "Popen",
        lambda *_args, **_kwargs: pytest.fail(
            "child must not start after runtime validation failure"
        ),
    )

    def fake_write(**kwargs: object) -> int:
        captured.update(kwargs)
        return 2

    monkeypatch.setattr(adapter, "write_terminal_result", fake_write)
    monkeypatch.setattr(
        adapter.os,
        "_exit",
        lambda code: (_ for _ in ()).throw(ExitCalled(code)),
    )
    try:
        with pytest.raises(ExitCalled) as raised:
            adapter._run_four_region(
                COORDINATOR_ARGUMENTS
            )
        assert raised.value.code == 2
        assert captured["outcome"] == "hard_failure"
        assert captured["reason_code"] == "runtime_env_syntax_invalid"
    finally:
        os.close(validation_fd)


def test_passthrough_rejects_unreviewed_target_before_spawn(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    validation = tmp_path / "validation.flock"
    validation.touch()
    validation_fd = os.open(validation, os.O_RDWR)
    lease = FakeLease(invocation=None, validation_fd=validation_fd)
    monkeypatch.setattr(
        adapter,
        "acquire_marketplace_collection_lease",
        lambda: lease,
    )
    monkeypatch.setattr(
        adapter.subprocess,
        "Popen",
        lambda *_args, **_kwargs: pytest.fail("unreviewed target must not start"),
    )
    try:
        assert adapter._run_passthrough(["/bin/true"]) == 2
        assert lease.exited is True
    finally:
        os.close(validation_fd)


@pytest.mark.parametrize(
    "target",
    sorted(adapter.OFFICIAL_PASSTHROUGH_TARGETS),
    ids=lambda path: path.name,
)
def test_all_passthrough_targets_receive_exact_attested_environment(
    target: Path,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    validation = tmp_path / "validation.flock"
    validation.touch()
    validation_fd = os.open(validation, os.O_RDWR)
    lease = FakeLease(invocation=None, validation_fd=validation_fd)
    seen: dict[str, object] = {}
    manifest_sha = "1" * 64
    runtime_sha = "2" * 64

    class Child:
        def wait(self) -> int:
            return 0

    monkeypatch.setattr(
        adapter,
        "acquire_marketplace_collection_lease",
        lambda: lease,
    )
    monkeypatch.setattr(
        adapter,
        "load_required_runtime_environment",
        lambda **_kwargs: {"PARSER_WB_RUNTIME_ENV_LOADED": "1"},
    )
    monkeypatch.setattr(
        adapter,
        "capture_attested_environment",
        lambda _root, environment: {
            **environment,
            attestation.MANIFEST_SHA_ENV: manifest_sha,
            attestation.RUNTIME_SHA_ENV: runtime_sha,
        },
    )

    def verify(_root: Path, environment: dict[str, str]) -> None:
        seen["verified_manifest"] = environment[attestation.MANIFEST_SHA_ENV]
        seen["verified_runtime"] = environment[attestation.RUNTIME_SHA_ENV]

    monkeypatch.setattr(adapter, "verify_attested_environment", verify)

    def popen(
        command: tuple[str, ...],
        **kwargs: object,
    ) -> Child:
        environment = kwargs["env"]
        assert isinstance(environment, dict)
        seen["command"] = command
        seen["child_manifest"] = environment[attestation.MANIFEST_SHA_ENV]
        seen["child_runtime"] = environment[attestation.RUNTIME_SHA_ENV]
        return Child()

    monkeypatch.setattr(adapter.subprocess, "Popen", popen)
    try:
        assert adapter._run_passthrough([str(target)]) == 0
        assert str(target) in seen["command"]
        assert seen == {
            "verified_manifest": manifest_sha,
            "verified_runtime": runtime_sha,
            "command": seen["command"],
            "child_manifest": manifest_sha,
            "child_runtime": runtime_sha,
        }
    finally:
        os.close(validation_fd)


def test_subprocess_in_writer_attestation_rejects_post_gate_mutation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    project = tmp_path / "project"
    for relative in ("app", "scripts", "config"):
        shutil.copytree(PROJECT_ROOT / relative, project / relative)
    shutil.copy2(PROJECT_ROOT / "main.py", project / "main.py")
    shutil.copy2(PROJECT_ROOT / "requirements.txt", project / "requirements.txt")
    dependencies = tmp_path / "site-packages"
    dependencies.mkdir()
    (dependencies / "approved.py").write_text("VALUE = 1\n", encoding="utf-8")
    copied_attestation = project / "app/common/nightly_attestation.py"
    copied_source = copied_attestation.read_text(encoding="utf-8")
    copied_source = re.sub(
        r'APPROVED_SITE_PACKAGES = Path\(\n'
        r'\s*"/home/Codex/agent-tools/parser_wb-python/lib/python3\.14/'
        r'site-packages"\n'
        r'\)',
        f'APPROVED_SITE_PACKAGES = Path({str(dependencies)!r})',
        copied_source,
        count=1,
    )
    copied_attestation.write_text(copied_source, encoding="utf-8")
    publisher = project / "scripts/attestation_writer_probe.py"
    publisher.write_text(
        "import sys, time\n"
        "from pathlib import Path\n"
        "root=Path(sys.argv[1])\n"
        "sys.path.insert(0, str(root))\n"
        "from app.common.durable_atomic import durable_atomic_replace\n"
        "from app.common.nightly_attestation import integrity_gate\n"
        "ready=Path(sys.argv[2])\n"
        "go=Path(sys.argv[3])\n"
        "target=Path(sys.argv[4])\n"
        "gate=integrity_gate(root)\n"
        "ready.write_text('ready', encoding='utf-8')\n"
        "while not go.exists(): time.sleep(0.01)\n"
        "try:\n"
        "    durable_atomic_replace(target, b'untrusted\\n', "
        "integrity_gate=gate)\n"
        "except Exception:\n"
        "    raise SystemExit(23)\n"
        "raise SystemExit(0)\n",
        encoding="utf-8",
    )
    config = project / "config"
    runtime = config / "runtime.env"
    headers = config / "wb_request_headers.json"
    cookie = config / "wb_cookie.txt"
    runtime.write_text(
        "PARSER_WB_PROXY_URL=http://proxy.invalid:18080\n"
        f"PARSER_WB_REQUEST_HEADERS_FILE={headers}\n"
        "PARSER_WB_COOKIE_REQUIRED=1\n",
        encoding="utf-8",
    )
    headers.write_text("{}\n", encoding="utf-8")
    cookie.write_text("test-cookie\n", encoding="utf-8")
    _remove_group_world_write(project)
    _remove_group_world_write(dependencies)
    for path in (runtime, headers, cookie):
        path.chmod(0o600)
    monkeypatch.setattr(
        attestation,
        "APPROVED_SITE_PACKAGES",
        dependencies,
    )
    manifest_path = project / attestation.MANIFEST_RELATIVE_PATH
    manifest_path.write_text(
        json.dumps(
            attestation.build_input_manifest(project),
            sort_keys=True,
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )
    manifest_path.chmod(0o644)
    loaded = runtime_env.load_strict_runtime_environment(
        project_root=project,
        base_environment={
            "PATH": "/usr/bin:/bin",
            "HOME": os.environ.get("HOME", "/home/pavel"),
        },
    )
    environment = attestation.capture_attested_environment(
        project,
        loaded.environment,
    )
    publication = project / "publication"
    publication.mkdir(mode=0o700)
    target = publication / "latest.json"
    target.write_bytes(b"trusted\n")
    target.chmod(0o600)
    ready = tmp_path / "ready"
    go = tmp_path / "go"
    child = subprocess.Popen(
        [
            str(attestation.APPROVED_PYTHON_BIN),
            str(publisher),
            str(project),
            str(ready),
            str(go),
            str(target),
        ],
        cwd=project,
        env=environment,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    deadline = time.monotonic() + 30
    while not ready.exists() and time.monotonic() < deadline:
        time.sleep(0.01)
    assert ready.exists()
    influencing = project / "app/common/cleanup.py"
    influencing.write_bytes(influencing.read_bytes() + b"\n")
    go.write_text("go", encoding="utf-8")
    assert child.wait(timeout=30) == 23
    assert target.read_bytes() == b"trusted\n"


def test_absolute_deadline_caps_collection_window() -> None:
    now = datetime(2026, 7, 27, 0, 15, tzinfo=UTC)
    runtime_window = SimpleNamespace(
        scheduled_start_msk="03:15",
        new_run_start_grace_seconds=30 * 60,
        absolute_cutoff_msk="23:00",
        max_invocation_runtime_seconds=6 * 60 * 60,
        finalization_reserve_seconds=120,
        minimum_resume_window_seconds=60,
    )
    absolute = now + timedelta(minutes=5)
    guard = DeadlineGuard.for_runtime_window(
        runtime_window,
        resume=False,
        now=lambda: now,
        absolute_deadline_utc=absolute,
    )
    assert guard.deadline_utc == absolute
    with pytest.raises(
        CollectionPlanRunError,
        match="coordinator absolute deadline must be UTC",
    ):
        DeadlineGuard.for_runtime_window(
            runtime_window,
            resume=False,
            now=lambda: now,
            absolute_deadline_utc=datetime(2026, 7, 27, 0, 20),
        )


def test_completed_resume_skips_collection_and_is_idempotent(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    manifest_path = (
        tmp_path
        / "state/wb_collection_plans"
        / pipeline_launcher.FOUR_REGION_PLAN_ID
        / RUN_REF
        / "manifest.json"
    )
    manifest_path.parent.mkdir(parents=True)
    manifest_path.write_text(
        json.dumps(
            {
                "schema_version": "wb_collection_plan_manifest_v2",
                "collection_plan_id": pipeline_launcher.FOUR_REGION_PLAN_ID,
                "run_id": RUN_REF,
                "status": "success",
                "complete": True,
                "regions": [
                    {
                        "region_id": region_id,
                        "status": "success",
                        "complete": True,
                    }
                    for region_id in (
                        "moscow",
                        "rostov-on-don",
                        "novosibirsk",
                        "kazan",
                    )
                ],
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(
        pipeline_launcher,
        "load_config",
        lambda _path: SimpleNamespace(project_root=tmp_path),
    )
    monkeypatch.setattr(
        pipeline_launcher,
        "load_collection_plan_bundle",
        lambda **_kwargs: SimpleNamespace(
            collection_plan=SimpleNamespace(
                collection_plan_id=pipeline_launcher.FOUR_REGION_PLAN_ID,
            )
        ),
    )
    monkeypatch.setattr(
        pipeline_launcher,
        "run_collection_plan",
        lambda **_kwargs: pytest.fail("completed collection must not rerun"),
    )
    calls: list[str] = []
    monkeypatch.setattr(
        pipeline_launcher,
        "run_four_region_downstream",
        lambda **kwargs: calls.append(kwargs["run_id"])
        or {"status": "success", "complete": True},
    )
    monkeypatch.setattr(
        pipeline_launcher,
        "_emit_adapter_status",
        lambda **_kwargs: None,
    )
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "run_wb_four_region_nightly.py",
            "--plan-file",
            PLAN_ARGUMENT,
            "--no-publish",
            "--resume-run-id",
            RUN_REF,
        ],
    )
    assert pipeline_launcher.main() == 0
    assert calls == [RUN_REF]


@pytest.mark.parametrize(
    ("coordinator_stage", "expected_exit"),
    (("", 2), ("wb_resume", 2)),
)
def test_checkpoint_is_emitted_once_then_resume_failure_is_hard(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    coordinator_stage: str,
    expected_exit: int,
) -> None:
    run_dir = (
        tmp_path
        / "state/wb_collection_plans"
        / pipeline_launcher.FOUR_REGION_PLAN_ID
        / RUN_REF
    )
    run_dir.mkdir(parents=True)
    (run_dir / "manifest.json").write_text(
        json.dumps(
            {
                "schema_version": "wb_collection_plan_manifest_v2",
                "collection_plan_id": pipeline_launcher.FOUR_REGION_PLAN_ID,
                "run_id": RUN_REF,
                "complete": False,
                "resume": {"segments": []},
            }
        ),
        encoding="utf-8",
    )
    (run_dir / "effective_plan.json").write_text(
        json.dumps(
            {
                "schema_version": "wb_effective_collection_plan_v2",
                "collection_plan_id": pipeline_launcher.FOUR_REGION_PLAN_ID,
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(
        pipeline_launcher,
        "validate_resumable_collection_state",
        lambda **_kwargs: False,
    )
    monkeypatch.setattr(
        pipeline_launcher,
        "load_config",
        lambda _path: SimpleNamespace(project_root=tmp_path),
    )
    monkeypatch.setattr(
        pipeline_launcher,
        "run_collection_plan",
        lambda **_kwargs: (_ for _ in ()).throw(
            pipeline_launcher.CriticalPipelineError("collection stopped")
        ),
    )
    monkeypatch.setattr(
        pipeline_launcher,
        "run_four_region_downstream",
        lambda **_kwargs: pytest.fail("downstream must not start"),
    )
    monkeypatch.setattr(
        pipeline_launcher,
        "write_four_region_failure_attempt",
        lambda **_kwargs: None,
    )
    monkeypatch.setenv("PARSER_WB_ADAPTER_RUN_REF", RUN_REF)
    if coordinator_stage:
        monkeypatch.setenv("MARKETPLACE_COORDINATOR_STAGE", coordinator_stage)
    arguments = [
        "run_wb_four_region_nightly.py",
        "--plan-file",
        PLAN_ARGUMENT,
        "--no-publish",
    ]
    if coordinator_stage:
        arguments.extend(["--resume-run-id", RUN_REF])
    monkeypatch.setattr(sys, "argv", arguments)
    assert pipeline_launcher.main() == expected_exit
