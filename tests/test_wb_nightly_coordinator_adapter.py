from __future__ import annotations

import fcntl
import hashlib
import json
import os
import re
import stat
import sys
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace

import pytest

from app.common import cli as wb_cli
from app.common import nightly_coordinator as contract
from app.serp.collection_plan_runner import CollectionPlanRunError, DeadlineGuard
from scripts import check_nightly_coordinator_contract as checker
from scripts import run_wb_four_region_nightly as pipeline_launcher
from scripts import wb_nightly_coordinator_adapter as adapter


PROJECT_ROOT = Path(__file__).resolve().parents[1]
PLAN_ARGUMENT = (
    "config/wb/collection_plans/shevron-four-regions-top1000-v2.json"
)
RUN_REF = "20260727_001500Z"


@pytest.fixture(autouse=True)
def _isolate_pipeline_launcher_host_lock(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        pipeline_launcher,
        "require_official_live_entry_lease",
        lambda: None,
    )


class ExitCalled(RuntimeError):
    def __init__(self, code: int) -> None:
        self.code = code
        super().__init__(str(code))


class FakeLease:
    def __init__(
        self,
        *,
        invocation: contract.CoordinatorInvocation | None,
        validation_fd: int,
    ) -> None:
        self.invocation = invocation
        self.validation_fd = validation_fd
        validation_path = Path(os.readlink(f"/proc/self/fd/{validation_fd}"))
        self.policy = SimpleNamespace(
            guard_path=validation_path,
            validation_path=validation_path,
        )
        self.exited = False
        self.assertions = 0

    @property
    def pass_fds(self) -> tuple[int, ...]:
        return (self.validation_fd,)

    def assert_held(self) -> None:
        os.fstat(self.validation_fd)
        self.assertions += 1

    def __exit__(self, *_args: object) -> None:
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
        "--plan-file",
        PLAN_ARGUMENT,
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
    assert str(PROJECT_ROOT / "scripts/wb_nightly_coordinator_adapter.py") in input_paths
    assert str(PROJECT_ROOT / "app/common/nightly_coordinator.py") in input_paths
    for name in (
        *checker.OFFICIAL_ENTRYPOINTS,
        *checker.COORDINATOR_DISABLED_ENTRYPOINTS,
    ):
        assert str(PROJECT_ROOT / "scripts" / name) in input_paths


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
        runtime_indices = [
            index
            for token in ('source "$RUNTIME_LOADER"', "wb_load_required_runtime_env")
            if (index := source.find(token)) >= 0
        ]
        assert all(adapter_exec < index for index in runtime_indices)
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
        ["--plan-file", PLAN_ARGUMENT, "--no-publish"],
        invocation=initial,
    )
    resume = _invocation(tmp_path, stage="wb_resume", resume_ref=RUN_REF)
    adapter._validate_four_region_command(
        [
            "--plan-file",
            PLAN_ARGUMENT,
            "--no-publish",
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
            ["--plan-file", PLAN_ARGUMENT, "--no-publish"],
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
                ["--plan-file", PLAN_ARGUMENT, "--no-publish"]
            )
        assert raised.value.code == 2
        assert captured["outcome"] == "hard_failure"
        assert captured["reason_code"] == "child_exit_contract_mismatch"
    finally:
        os.close(validation_fd)


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
                ["--plan-file", PLAN_ARGUMENT, "--no-publish"]
            )
        assert raised.value.code == 75
        assert captured["outcome"] == "deferred"
        assert captured["reason_code"] == "absolute_deadline_reached_before_runtime"
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
    (("", 76), ("wb_resume", 2)),
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
