#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import signal
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
    write_terminal_result,
)


PYTHON_BIN = Path("/home/Codex/agent-tools/parser_wb-python/bin/python")
FOUR_REGION_SCRIPT = PROJECT_ROOT / "scripts/run_wb_four_region_nightly.py"
FOUR_REGION_PLAN = (
    "config/wb/collection_plans/shevron-four-regions-top1000-v2.json"
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
    return parser


def _strip_separator(arguments: list[str]) -> list[str]:
    return arguments[1:] if arguments[:1] == ["--"] else arguments


def _validate_four_region_command(
    arguments: list[str],
    *,
    invocation: CoordinatorInvocation | None,
) -> None:
    base = ["--plan-file", FOUR_REGION_PLAN, "--no-publish"]
    if invocation is not None:
        expected = (
            base
            if invocation.phase == "initial"
            else [*base, "--resume-run-id", invocation.resume_ref]
        )
        if arguments != expected:
            raise NightlyCoordinatorContractError(
                "coordinator_command_invalid",
                outcome="hard_failure",
            )
        return

    supported_bases = (
        base,
        ["--config", "config/config.yaml", *base],
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


def _read_status(fd: int) -> dict[str, Any] | None:
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
    ):
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


def _run_four_region(arguments: list[str]) -> int:
    started = datetime.now(UTC)
    lease = acquire_marketplace_collection_lease()
    terminal_committed = False
    writing_terminal = False
    try:
        invocation = lease.invocation
        _validate_four_region_command(arguments, invocation=invocation)
        if invocation is not None and datetime.now(UTC) >= invocation.deadline_utc:
            raise NightlyCoordinatorContractError(
                "absolute_deadline_reached_before_runtime",
                outcome="deferred",
            )
        if not PYTHON_BIN.is_file() or not os.access(PYTHON_BIN, os.X_OK):
            raise NightlyCoordinatorContractError(
                "wb_python_runtime_unavailable",
                outcome="hard_failure",
            )
        child_env = load_required_runtime_environment(
            project_root=PROJECT_ROOT,
            lease=lease,
        )
        if invocation is not None and datetime.now(UTC) >= invocation.deadline_utc:
            raise NightlyCoordinatorContractError(
                "absolute_deadline_reached_before_spawn",
                outcome="deferred",
            )
        child_env["PARSER_WB_LOCK_V3_WRAPPED"] = "1"
        child_env.update(descendant_lease_environment(lease))
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
            command = (str(PYTHON_BIN), str(FOUR_REGION_SCRIPT), *arguments)
            lease.assert_held()
            child = subprocess.Popen(
                command,
                cwd=PROJECT_ROOT,
                env=child_env,
                stdin=subprocess.DEVNULL,
                close_fds=True,
                pass_fds=(*lease.pass_fds, write_fd),
            )
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
                    (invocation.deadline_utc - datetime.now(UTC)).total_seconds(),
                )
            child_exit = child.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            timed_out = True
            _terminate_child(child)
            child_exit = 2
        try:
            status = _read_status(read_fd)
        except OSError as exc:
            raise NightlyCoordinatorContractError(
                "adapter_status_read_failed",
                outcome="hard_failure",
            ) from exc
        finally:
            os.close(read_fd)
        lease.assert_held()
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
            run_ref = status["run_ref"]
            resume_ref = status["resume_ref"]
            report_refs = tuple(status["report_refs"])
            if child_exit != EXIT_BY_OUTCOME[outcome]:
                outcome = "hard_failure"
                reason_code = "child_exit_contract_mismatch"
                resume_ref = ""
                report_refs = ()
        finished = datetime.now(UTC)
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
                finished_at_utc=datetime.now(UTC),
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
        target = Path(arguments[0])
        if (
            not target.is_absolute()
            or target not in OFFICIAL_PASSTHROUGH_TARGETS
        ):
            raise NightlyCoordinatorContractError(
                "passthrough_target_invalid",
                outcome="hard_failure",
            )
        env = os.environ.copy()
        env["PARSER_WB_LOCK_V3_WRAPPED"] = "1"
        env.update(descendant_lease_environment(lease))
        lease.assert_held()
        try:
            child = subprocess.Popen(
                tuple(arguments),
                cwd=PROJECT_ROOT,
                env=env,
                close_fds=True,
                pass_fds=lease.pass_fds,
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
        if args.command == "four-region":
            return _run_four_region(arguments)
        return _run_passthrough(arguments)
    except NightlyCoordinatorContractError as exc:
        print(f"{exc.__class__.__name__}: operation refused", file=sys.stderr)
        return EXIT_BY_OUTCOME.get(exc.outcome, 2)


if __name__ == "__main__":
    raise SystemExit(main())
