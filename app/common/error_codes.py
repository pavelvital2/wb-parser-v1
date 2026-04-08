from __future__ import annotations

from .constants import (
    ERROR_CODE_COMPONENT_CRITICAL,
    ERROR_CODE_COMPONENT_NON_CRITICAL,
    ERROR_CODE_COMPONENT_NOT_READY,
    ERROR_CODE_COMPONENT_UNHANDLED,
    ERROR_CODE_IO,
    ERROR_CODE_NETWORK,
    ERROR_CODE_RUN_LOCKED,
    ERROR_CODE_SCHEMA,
    ERROR_CODE_UNKNOWN,
    ERROR_CODE_VALIDATION,
)
from .exceptions import ComponentNotReadyError, CriticalPipelineError, NonCriticalPipelineError, RunLockedError


def infer_error_code(exc: Exception, default: str = ERROR_CODE_UNKNOWN) -> str:
    if isinstance(exc, RunLockedError):
        return ERROR_CODE_RUN_LOCKED
    if isinstance(exc, ComponentNotReadyError):
        return ERROR_CODE_COMPONENT_NOT_READY
    if isinstance(exc, NonCriticalPipelineError):
        return ERROR_CODE_COMPONENT_NON_CRITICAL
    if isinstance(exc, CriticalPipelineError):
        message = str(exc).lower()
        if "csv" in message or "column" in message or "schema" in message:
            return ERROR_CODE_SCHEMA
        if "validate" in message or "contract" in message:
            return ERROR_CODE_VALIDATION
        return ERROR_CODE_COMPONENT_CRITICAL

    try:
        import requests  # type: ignore

        if isinstance(exc, requests.RequestException):
            return ERROR_CODE_NETWORK
    except Exception:
        pass

    if isinstance(exc, (FileNotFoundError, PermissionError, OSError)):
        return ERROR_CODE_IO

    return default


def default_unhandled_error_code() -> str:
    return ERROR_CODE_COMPONENT_UNHANDLED
