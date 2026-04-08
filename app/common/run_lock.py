from __future__ import annotations

import json
import os
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator

from .exceptions import RunLockedError
from .run_context import utc_now_iso


def _lock_file(state_dir: Path) -> Path:
    lock_dir = state_dir / "locks"
    lock_dir.mkdir(parents=True, exist_ok=True)
    return lock_dir / "pipeline.lock"


@contextmanager
def acquire_run_lock(
    *,
    state_dir: Path,
    target: str,
    run_id: str,
    enabled: bool,
    stale_seconds: int,
) -> Iterator[Path | None]:
    if not enabled:
        yield None
        return

    lock_path = _lock_file(state_dir)

    def _create_lock() -> int:
        return os.open(lock_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)

    try:
        fd = _create_lock()
    except FileExistsError:
        stale = False
        if stale_seconds > 0:
            try:
                age_seconds = max(0.0, time.time() - lock_path.stat().st_mtime)
                stale = age_seconds > stale_seconds
            except Exception:
                stale = False

        if stale:
            try:
                lock_path.unlink(missing_ok=True)
                fd = _create_lock()
            except Exception as exc:
                raise RunLockedError(f"Run lock stale cleanup failed: {lock_path} ({exc})") from exc
        else:
            meta = ""
            try:
                meta = lock_path.read_text(encoding="utf-8")
            except Exception:
                meta = ""
            raise RunLockedError(f"Another run is active. lock={lock_path} meta={meta[:300]}")

    payload = {
        "pid": os.getpid(),
        "target": target,
        "run_id": run_id,
        "started_at_utc": utc_now_iso(),
    }

    with os.fdopen(fd, "w", encoding="utf-8") as f:
        f.write(json.dumps(payload, ensure_ascii=False))

    try:
        yield lock_path
    finally:
        try:
            lock_path.unlink(missing_ok=True)
        except Exception:
            pass
