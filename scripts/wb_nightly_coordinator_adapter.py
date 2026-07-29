#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import signal
import stat
import subprocess
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.common.nightly_coordinator import (
    ADAPTER_STATUS_SCHEMA_VERSION,
    CoordinatorInvocation,
    EXIT_BY_OUTCOME,
    NightlyCoordinatorContractError,
    WB_RUN_REF,
    acquire_marketplace_collection_lease,
    descendant_lease_environment,
    load_required_runtime_environment,
    require_official_live_entry_lease,
    write_terminal_result,
)
from app.common.nightly_attestation import (
    capture_attested_environment,
    integrity_gate,
    verify_attested_environment,
    verify_input_manifest,
)


PYTHON_BIN = Path("/home/Codex/agent-tools/parser_wb-python/bin/python")
FOUR_REGION_SCRIPT = PROJECT_ROOT / "scripts/run_wb_four_region_nightly.py"
SUPERVISOR = PROJECT_ROOT / "scripts/marketplace_lock_v3_supervisor.py"
FOUR_REGION_PLAN = (
    "config/wb/collection_plans/shevron-four-regions-top1000-v2.json"
)
FOUR_REGION_MATRIX = (
    "config/wb/execution_matrices/four-region-nightly-v1.json"
)
MAX_STATUS_BYTES = 64 * 1024
OFFICIAL_PASSTHROUGH_TARGETS = frozenset(
    {
        PROJECT_ROOT / "scripts/run_products_sellers_daily.sh",
        PROJECT_ROOT / "scripts/run_wb_collection_plan.sh",
        PROJECT_ROOT / "scripts/run_wb_guarded_regional_pilot.sh",
        PROJECT_ROOT / "scripts/run_wb_live_component.sh",
        PROJECT_ROOT / "scripts/run_wb_cookie_renewal.sh",
        PROJECT_ROOT / "scripts/run_wb_nightly_preflight.sh",
        PROJECT_ROOT / "scripts/run_wb_access_tool.sh",
        PROJECT_ROOT / "scripts/run_wb_warehouse_refresh.sh",
    }
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="WB lock-v3 and coordinator adapter"
    )
    subparsers = parser.add_subparsers(dest="command", required=True)
    four_region = subparsers.add_parser("four-region")
    four_region.add_argument("arguments", nargs=argparse.REMAINDER)
    passthrough = subparsers.add_parser("passthrough")
    passthrough.add_argument("arguments", nargs=argparse.REMAINDER)
    subparsers.add_parser("entry-check")
    return parser


def _strip_separator(arguments: list[str]) -> list[str]:
    return arguments[1:] if arguments[:1] == ["--"] else arguments


def _validate_four_region_command(
    arguments: list[str],
    *,
    invocation: CoordinatorInvocation | None,
) -> None:
    coordinator_base = [
        "--matrix-file",
        FOUR_REGION_MATRIX,
        "--no-publish",
    ]
    if invocation is not None:
        expected = (
            coordinator_base
            if invocation.phase == "initial"
            else [
                *coordinator_base,
                "--resume-run-id",
                invocation.resume_ref,
            ]
        )
        if arguments != expected:
            raise NightlyCoordinatorContractError(
                "coordinator_command_invalid",
                outcome="hard_failure",
            )
        return

    plan_base = ["--plan-file", FOUR_REGION_PLAN, "--no-publish"]
    supported_bases = (
        coordinator_base,
        ["--config", "config/config.yaml", *coordinator_base],
        plan_base,
        ["--config", "config/config.yaml", *plan_base],
    )
    if any(arguments == candidate for candidate in supported_bases):
        return
    for candidate in supported_bases:
        if (
            len(arguments) == len(candidate) + 2
            and arguments[: len(candidate)] == candidate
            and arguments[len(candidate)]
            in {"--resume-run-id", "--downstream-only-run-id"}
            and WB_RUN_REF.fullmatch(arguments[-1])
        ):
            return
    raise NightlyCoordinatorContractError(
        "four_region_command_invalid",
        outcome="hard_failure",
    )


def _generated_run_ref(now: datetime) -> str:
    return now.astimezone(UTC).strftime("%Y%m%d_%H%M%SZ")


def _utc_now() -> datetime:
    return datetime.now(UTC)


def _read_status(
    fd: int,
    *,
    expected_run_ref: str,
    invocation: CoordinatorInvocation,
) -> dict[str, Any] | None:
    chunks: list[bytes] = []
    remaining = MAX_STATUS_BYTES + 1
    while remaining > 0:
        chunk = os.read(fd, min(65536, remaining))
        if not chunk:
            break
        chunks.append(chunk)
        remaining -= len(chunk)
    encoded = b"".join(chunks)
    if not encoded or len(encoded) > MAX_STATUS_BYTES:
        return None
    try:
        value = json.loads(encoded.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        return None
    expected = {
        "schema_version",
        "outcome",
        "run_ref",
        "resume_ref",
        "reason_code",
        "report_refs",
    }
    if (
        not isinstance(value, dict)
        or set(value) != expected
        or value.get("schema_version") != ADAPTER_STATUS_SCHEMA_VERSION
        or value.get("outcome") not in EXIT_BY_OUTCOME
        or not isinstance(value.get("run_ref"), str)
        or not isinstance(value.get("resume_ref"), str)
        or not isinstance(value.get("reason_code"), str)
        or not isinstance(value.get("report_refs"), list)
        or any(not isinstance(item, str) for item in value["report_refs"])
        or value["run_ref"] != expected_run_ref
    ):
        return None
    if value["outcome"] == "checkpoint":
        if (
            invocation.phase != "initial"
            or value["resume_ref"] != expected_run_ref
        ):
            return None
    elif value["resume_ref"] != "":
        return None
    return value


def _terminate_child(child: subprocess.Popen[bytes]) -> None:
    try:
        child.send_signal(signal.SIGTERM)
        child.wait(timeout=10)
    except (ProcessLookupError, subprocess.TimeoutExpired):
        try:
            child.kill()
        except ProcessLookupError:
            return
        child.wait(timeout=10)


def _supervised_command(command: tuple[str, ...]) -> tuple[str, ...]:
    if (
        not SUPERVISOR.is_file()
        or SUPERVISOR.is_symlink()
        or not os.access(SUPERVISOR, os.X_OK)
    ):
        raise NightlyCoordinatorContractError(
            "lock_v3_supervisor_unavailable",
            outcome="hard_failure",
        )
    return (str(PYTHON_BIN), str(SUPERVISOR), "--", *command)


def _prepare_supervisor_environment(
    environment: dict[str, str],
    *,
    pass_fds: tuple[int, ...],
) -> dict[str, str]:
    result = dict(environment)
    result["PYTHONDONTWRITEBYTECODE"] = "1"
    result["PARSER_WB_LOCK_V3_WRAPPED"] = "1"
    result["PARSER_WB_SUPERVISOR_PASS_FDS"] = ",".join(
        str(value) for value in sorted(set(pass_fds))
    )
    return result


def _canonical_passthrough_target(value: str) -> Path:
    candidate = Path(value)
    if not candidate.is_absolute():
        candidate = PROJECT_ROOT / candidate
    try:
        relative = candidate.relative_to(PROJECT_ROOT)
    except ValueError as exc:
        raise NightlyCoordinatorContractError(
            "passthrough_target_invalid",
            outcome="hard_failure",
        ) from exc
    current = PROJECT_ROOT
    for part in relative.parts:
        current /= part
        try:
            info = current.lstat()
        except OSError as exc:
            raise NightlyCoordinatorContractError(
                "passthrough_target_invalid",
                outcome="hard_failure",
            ) from exc
        if stat.S_ISLNK(info.st_mode):
            raise NightlyCoordinatorContractError(
                "passthrough_target_invalid",
                outcome="hard_failure",
            )
    resolved = candidate.resolve(strict=True)
    if (
        resolved not in OFFICIAL_PASSTHROUGH_TARGETS
        or not resolved.is_file()
    ):
        raise NightlyCoordinatorContractError(
            "passthrough_target_invalid",
            outcome="hard_failure",
        )
    return resolved


def _run_four_region(arguments: list[str]) -> int:
    started = _utc_now()
    lease = acquire_marketplace_collection_lease()
    terminal_committed = False
    writing_terminal = False
    parent_env = {
        **os.environ,
        **descendant_lease_environment(lease),
    }
    verify_input_manifest(PROJECT_ROOT)
    publication_gate = integrity_gate(PROJECT_ROOT, parent_env)
    try:
        invocation = lease.invocation
        _validate_four_region_command(arguments, invocation=invocation)
        if invocation is not None and _utc_now() >= invocation.deadline_utc:
            raise NightlyCoordinatorContractError(
                "absolute_deadline_reached_before_runtime",
                outcome="deferred",
            )
        if not PYTHON_BIN.is_file() or not os.access(PYTHON_BIN, os.X_OK):
            raise NightlyCoordinatorContractError(
                "wb_python_runtime_unavailable",
                outcome="hard_failure",
            )
        publication_gate()
        child_env = load_required_runtime_environment(
            project_root=PROJECT_ROOT,
            lease=lease,
        )
        child_env.update(descendant_lease_environment(lease))
        child_env = capture_attested_environment(PROJECT_ROOT, child_env)
        publication_gate = integrity_gate(PROJECT_ROOT, child_env)
        run_ref = (
            invocation.resume_ref
            if invocation is not None and invocation.phase == "resume"
            else _generated_run_ref(started)
        )
        child_env["PARSER_WB_ADAPTER_RUN_REF"] = run_ref
        try:
            read_fd, write_fd = os.pipe2(os.O_CLOEXEC)
        except OSError as exc:
            raise NightlyCoordinatorContractError(
                "adapter_status_pipe_failed",
                outcome="hard_failure",
            ) from exc
        try:
            os.set_inheritable(write_fd, True)
            child_env["PARSER_WB_ADAPTER_STATUS_FD"] = str(write_fd)
            pass_fds = (*lease.pass_fds, write_fd)
            child_env = _prepare_supervisor_environment(
                child_env,
                pass_fds=pass_fds,
            )
            command = _supervised_command(
                (str(PYTHON_BIN), str(FOUR_REGION_SCRIPT), *arguments)
            )
            lease.assert_held()
            verify_attested_environment(PROJECT_ROOT, child_env)
            if (
                invocation is not None
                and _utc_now() >= invocation.deadline_utc
            ):
                raise NightlyCoordinatorContractError(
                    "absolute_deadline_reached_before_spawn",
                    outcome="deferred",
                )
            child = subprocess.Popen(
                command,
                cwd=PROJECT_ROOT,
                env=child_env,
                stdin=subprocess.DEVNULL,
                close_fds=True,
                pass_fds=pass_fds,
            )
        except NightlyCoordinatorContractError:
            os.close(read_fd)
            os.close(write_fd)
            raise
        except OSError as exc:
            os.close(read_fd)
            os.close(write_fd)
            raise NightlyCoordinatorContractError(
                "four_region_child_spawn_failed",
                outcome="hard_failure",
            ) from exc
        os.close(write_fd)
        timed_out = False
        try:
            timeout: float | None = None
            if invocation is not None:
                timeout = max(
                    0.1,
                    (invocation.deadline_utc - _utc_now()).total_seconds(),
                )
            child_exit = child.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            timed_out = True
            _terminate_child(child)
            child_exit = 2
        try:
            status = _read_status(
                read_fd,
                expected_run_ref=run_ref,
                invocation=invocation,
            )
        except OSError as exc:
            raise NightlyCoordinatorContractError(
                "adapter_status_read_failed",
                outcome="hard_failure",
            ) from exc
        finally:
            os.close(read_fd)
        lease.assert_held()
        verify_attested_environment(PROJECT_ROOT, child_env)
        if invocation is None:
            return child_exit
        if timed_out:
            outcome = "hard_failure"
            reason_code = "absolute_deadline_exceeded"
            resume_ref = ""
            report_refs: tuple[str, ...] = ()
        elif status is None:
            outcome = "hard_failure"
            reason_code = "adapter_status_missing"
            resume_ref = ""
            report_refs = ()
        else:
            outcome = status["outcome"]
            reason_code = status["reason_code"]
            resume_ref = status["resume_ref"]
            report_refs = tuple(status["report_refs"])
            if child_exit != EXIT_BY_OUTCOME[outcome]:
                outcome = "hard_failure"
                reason_code = "child_exit_contract_mismatch"
                resume_ref = ""
                report_refs = ()
        finished = _utc_now()
        writing_terminal = True
        exit_code = write_terminal_result(
            invocation=invocation,
            outcome=outcome,
            run_ref=run_ref,
            resume_ref=resume_ref,
            reason_code=reason_code,
            started_at_utc=started,
            finished_at_utc=finished,
            report_refs=report_refs,
            integrity_gate=publication_gate,
        )
        writing_terminal = False
        terminal_committed = True
        os._exit(exit_code)
    except NightlyCoordinatorContractError as exc:
        if writing_terminal:
            raise
        invocation = lease.invocation
        if invocation is not None:
            run_ref = (
                invocation.resume_ref
                if invocation.phase == "resume"
                else _generated_run_ref(started)
            )
            writing_terminal = True
            exit_code = write_terminal_result(
                invocation=invocation,
                outcome=exc.outcome,
                run_ref=run_ref,
                resume_ref=(run_ref if exc.outcome == "checkpoint" else ""),
                reason_code=exc.code,
                started_at_utc=started,
                finished_at_utc=_utc_now(),
                integrity_gate=publication_gate,
            )
            writing_terminal = False
            terminal_committed = True
            os._exit(exit_code)
        print(f"{exc.__class__.__name__}: operation refused", file=sys.stderr)
        return EXIT_BY_OUTCOME.get(exc.outcome, 2)
    finally:
        if not terminal_committed:
            lease.__exit__()


def _run_passthrough(arguments: list[str]) -> int:
    if not arguments:
        print("passthrough target is required", file=sys.stderr)
        return 2
    lease = acquire_marketplace_collection_lease()
    try:
        if lease.invocation is not None:
            raise NightlyCoordinatorContractError(
                "coordinator_passthrough_forbidden",
                outcome="hard_failure",
            )
        target = _canonical_passthrough_target(arguments[0])
        verify_input_manifest(PROJECT_ROOT)
        env = load_required_runtime_environment(
            project_root=PROJECT_ROOT,
            lease=lease,
        )
        env.update(descendant_lease_environment(lease))
        env = capture_attested_environment(PROJECT_ROOT, env)
        pass_fds = lease.pass_fds
        env = _prepare_supervisor_environment(env, pass_fds=pass_fds)
        command = _supervised_command(
            (str(target), *arguments[1:])
        )
        lease.assert_held()
        verify_attested_environment(PROJECT_ROOT, env)
        try:
            child = subprocess.Popen(
                command,
                cwd=PROJECT_ROOT,
                env=env,
                close_fds=True,
                pass_fds=pass_fds,
            )
        except OSError as exc:
            raise NightlyCoordinatorContractError(
                "passthrough_child_spawn_failed",
                outcome="hard_failure",
            ) from exc
        result = child.wait()
        lease.assert_held()
        return result
    except NightlyCoordinatorContractError as exc:
        print(f"{exc.__class__.__name__}: operation refused", file=sys.stderr)
        return EXIT_BY_OUTCOME.get(exc.outcome, 2)
    finally:
        lease.__exit__()


def main() -> int:
    try:
        args = build_parser().parse_args()
        arguments = _strip_separator(args.arguments)
        if args.command == "entry-check":
            require_official_live_entry_lease()
            return 0
        if args.command == "four-region":
            return _run_four_region(arguments)
        return _run_passthrough(arguments)
    except NightlyCoordinatorContractError as exc:
        print(f"{exc.__class__.__name__}: operation refused", file=sys.stderr)
        return EXIT_BY_OUTCOME.get(exc.outcome, 2)


if __name__ == "__main__":
    raise SystemExit(main())
