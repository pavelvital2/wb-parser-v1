from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


@dataclass(slots=True)
class RunContext:
    run_id: str
    pipeline: str
    component: str
    started_at_utc: str
    job_id: str = ""

    def for_component(self, component: str) -> "RunContext":
        return RunContext(
            run_id=self.run_id,
            pipeline=self.pipeline,
            component=component,
            started_at_utc=self.started_at_utc,
            job_id=self.job_id,
        )


def build_run_id() -> str:
    # Compact and filesystem-safe format: 20260307_000000Z
    return datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%SZ")
