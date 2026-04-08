from __future__ import annotations

import logging
import random
import time
from collections.abc import Callable
from typing import TypeVar

T = TypeVar("T")


def with_retry(
    func: Callable[[], T],
    attempts: int = 5,
    base_delay: float = 1.0,
    max_delay: float = 16.0,
    jitter_ratio: float = 0.2,
    retriable_exceptions: tuple[type[Exception], ...] = (Exception,),
    retriable_predicate: Callable[[Exception], bool] | None = None,
    on_retry: Callable[[int, float, Exception], None] | None = None,
) -> T:
    """Exponential backoff with jitter: ~1s,2s,4s,8s,16s (up to attempts=5)."""
    attempts = max(1, int(attempts))
    last_exc: Exception | None = None
    logger = logging.getLogger("retry")

    for attempt in range(1, attempts + 1):
        try:
            return func()
        except retriable_exceptions as exc:
            if retriable_predicate is not None and not retriable_predicate(exc):
                raise

            last_exc = exc
            if attempt >= attempts:
                break

            delay = min(max_delay, base_delay * (2 ** (attempt - 1)))
            if jitter_ratio > 0:
                delay = delay * (1 + random.uniform(-jitter_ratio, jitter_ratio))
            delay = max(0.0, delay)

            if on_retry is not None:
                on_retry(attempt, delay, exc)
            else:
                logger.warning(
                    "retry_scheduled",
                    extra={
                        "attempt": attempt,
                        "max_attempts": attempts,
                        "delay_seconds": round(delay, 3),
                        "error_class": exc.__class__.__name__,
                        "error_message": str(exc),
                    },
                )

            time.sleep(delay)

    assert last_exc is not None
    raise last_exc
