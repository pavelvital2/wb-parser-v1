from __future__ import annotations

import json
import multiprocessing
import os
import time
from pathlib import Path
from typing import Any

import pytest

from app.common.exceptions import RunLockedError
from app.common.run_lock import acquire_run_lock


def _lock_path(state_dir: Path) -> Path:
    path = state_dir / "locks" / "pipeline.lock"
    path.parent.mkdir(parents=True, exist_ok=True)
    return path


def _guard_path(state_dir: Path) -> Path:
    return state_dir / "locks" / "pipeline.lock.guard"


def _acquire_once(state_dir: Path, *, run_id: str = "new_run") -> dict:
    with acquire_run_lock(
        state_dir=state_dir,
        target="serp",
        run_id=run_id,
        enabled=True,
        stale_seconds=21_600,
    ) as path:
        assert path is not None
        return json.loads(path.read_text(encoding="utf-8"))


def test_run_lock_recovers_empty_lock_without_waiting_stale_seconds(tmp_path: Path) -> None:
    state_dir = tmp_path / "state"
    lock_file = _lock_path(state_dir)
    lock_file.write_text("", encoding="utf-8")

    payload = _acquire_once(state_dir, run_id="after_empty")

    assert payload["run_id"] == "after_empty"
    assert payload["target"] == "serp"
    assert payload["pid"] == os.getpid()
    assert payload["lock_version"] == 2
    assert not lock_file.exists()


def test_run_lock_recovers_corrupt_lock_without_waiting_stale_seconds(tmp_path: Path) -> None:
    state_dir = tmp_path / "state"
    lock_file = _lock_path(state_dir)
    lock_file.write_text("{not-json", encoding="utf-8")

    payload = _acquire_once(state_dir, run_id="after_corrupt")

    assert payload["run_id"] == "after_corrupt"
    assert payload["pid"] == os.getpid()
    assert not lock_file.exists()


def test_run_lock_recovers_dead_owner_lock_even_before_stale_age(tmp_path: Path) -> None:
    state_dir = tmp_path / "state"
    lock_file = _lock_path(state_dir)
    lock_file.write_text(
        json.dumps({"pid": 999_999_999, "target": "serp", "run_id": "dead_owner"}),
        encoding="utf-8",
    )

    payload = _acquire_once(state_dir, run_id="after_dead_owner")

    assert payload["run_id"] == "after_dead_owner"
    assert payload["pid"] == os.getpid()
    assert not lock_file.exists()


def test_run_lock_recovers_stale_dead_owner_lock(tmp_path: Path) -> None:
    state_dir = tmp_path / "state"
    lock_file = _lock_path(state_dir)
    lock_file.write_text(
        json.dumps({"pid": 999_999_999, "target": "serp", "run_id": "stale_dead_owner"}),
        encoding="utf-8",
    )
    old_time = time.time() - 21_601
    os.utime(lock_file, (old_time, old_time))

    payload = _acquire_once(state_dir, run_id="after_stale")

    assert payload["run_id"] == "after_stale"
    assert not lock_file.exists()


def test_run_lock_blocks_active_pid_and_preserves_lock_file(tmp_path: Path) -> None:
    state_dir = tmp_path / "state"
    lock_file = _lock_path(state_dir)
    original = json.dumps({"pid": os.getpid(), "target": "serp", "run_id": "active"})
    lock_file.write_text(original, encoding="utf-8")

    with pytest.raises(RunLockedError) as exc_info:
        with acquire_run_lock(
            state_dir=state_dir,
            target="sellers",
            run_id="blocked",
            enabled=True,
            stale_seconds=1,
        ):
            raise AssertionError("active lock should block acquisition")

    assert "active_pid" in str(exc_info.value)
    assert lock_file.exists()
    assert lock_file.read_text(encoding="utf-8") == original


def _concurrent_acquire_worker(
    state_dir: str,
    run_id: str,
    ready_queue: Any,
    start_event: Any,
    result_queue: Any,
) -> None:
    ready_queue.put(run_id)
    start_event.wait(5)
    try:
        with acquire_run_lock(
            state_dir=Path(state_dir),
            target="serp",
            run_id=run_id,
            enabled=True,
            stale_seconds=21_600,
        ):
            result_queue.put(("acquired", run_id))
            time.sleep(0.5)
    except Exception as exc:
        result_queue.put(("blocked", run_id, exc.__class__.__name__, str(exc)))


def test_run_lock_empty_lock_concurrent_recovery_allows_only_one_acquire(tmp_path: Path) -> None:
    state_dir = tmp_path / "state"
    lock_file = _lock_path(state_dir)
    lock_file.write_text("", encoding="utf-8")

    ctx = multiprocessing.get_context("spawn" if os.name == "nt" else "fork")
    ready_queue = ctx.Queue()
    result_queue = ctx.Queue()
    start_event = ctx.Event()
    processes = [
        ctx.Process(
            target=_concurrent_acquire_worker,
            args=(str(state_dir), f"worker_{idx}", ready_queue, start_event, result_queue),
        )
        for idx in range(2)
    ]

    try:
        for process in processes:
            process.start()
        ready = {ready_queue.get(timeout=5), ready_queue.get(timeout=5)}
        assert ready == {"worker_0", "worker_1"}

        start_event.set()
        for process in processes:
            process.join(timeout=5)

        assert all(process.exitcode == 0 for process in processes)
        results = [result_queue.get(timeout=5), result_queue.get(timeout=5)]
    finally:
        for process in processes:
            if process.is_alive():
                process.terminate()
                process.join(timeout=5)

    acquired = [result for result in results if result[0] == "acquired"]
    blocked = [result for result in results if result[0] == "blocked"]

    assert len(acquired) == 1
    assert len(blocked) == 1
    assert blocked[0][2] == "RunLockedError"
    assert "active_advisory_lock" in blocked[0][3]
    assert _guard_path(state_dir).exists()
    assert not lock_file.exists()
