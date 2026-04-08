from __future__ import annotations


class CriticalPipelineError(RuntimeError):
    """Pipeline cannot continue safely."""


class NonCriticalPipelineError(RuntimeError):
    """Pipeline can continue with partial failures."""


class ComponentNotReadyError(RuntimeError):
    """Component declared in CLI but implementation is scheduled for next stage."""


class ConfigValidationError(ValueError):
    """Runtime config is invalid for safe execution."""


class RunLockedError(CriticalPipelineError):
    """Another pipeline run is already active."""
