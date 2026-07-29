from __future__ import annotations

import hashlib
import os
import re
import shlex
import stat
from dataclasses import dataclass
from pathlib import Path
from typing import Mapping


APPROVED_SHARED_ENV_PATH = Path("/home/pavel/.marketplace-proxy.env")
MAX_ENV_FILE_BYTES = 4 * 1024 * 1024
ASSIGNMENT = re.compile(
    r"^(?:export[ \t]+)?([A-Za-z_][A-Za-z0-9_]*)=(.*)$"
)
VARIABLE = re.compile(
    r"\$(?:\{([A-Za-z_][A-Za-z0-9_]*)\}|([A-Za-z_][A-Za-z0-9_]*))"
)
RESERVED_KEYS = frozenset(
    {
        "PARSER_WB_RUNTIME_ENV_LOADED",
        "PARSER_WB_RUNTIME_ENV_SHA256",
        "PARSER_WB_RUNTIME_SOURCE_SET_SHA256",
    }
)


class RuntimeEnvironmentError(RuntimeError):
    def __init__(self, code: str) -> None:
        self.code = code
        super().__init__(code)


@dataclass(frozen=True, slots=True)
class RuntimeEnvironmentLoad:
    environment: dict[str, str]
    exported_keys: tuple[str, ...]
    runtime_env_sha256: str
    source_set_sha256: str


def _sha256(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def _read_secure_env_file(path: Path) -> bytes:
    if not path.is_absolute():
        raise RuntimeEnvironmentError("runtime_env_path_invalid")
    current = Path(path.anchor)
    for part in path.parts[1:-1]:
        current /= part
        try:
            info = current.lstat()
        except OSError as exc:
            raise RuntimeEnvironmentError(
                "runtime_env_parent_unavailable"
            ) from exc
        if (
            stat.S_ISLNK(info.st_mode)
            or not stat.S_ISDIR(info.st_mode)
            or (
                current == path.parent
                and (
                    info.st_uid != os.geteuid()
                    or info.st_mode & 0o002
                )
            )
        ):
            raise RuntimeEnvironmentError("runtime_env_parent_unsafe")
    flags = os.O_RDONLY | os.O_CLOEXEC
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    try:
        fd = os.open(path, flags)
    except OSError as exc:
        raise RuntimeEnvironmentError("runtime_env_unavailable") from exc
    try:
        before = os.fstat(fd)
        if (
            not stat.S_ISREG(before.st_mode)
            or before.st_uid != os.geteuid()
            or stat.S_IMODE(before.st_mode) != 0o600
            or not 0 < before.st_size <= MAX_ENV_FILE_BYTES
        ):
            raise RuntimeEnvironmentError("runtime_env_metadata_invalid")
        payload = b""
        remaining = before.st_size
        while remaining:
            chunk = os.read(fd, min(65536, remaining))
            if not chunk:
                raise RuntimeEnvironmentError("runtime_env_changed")
            payload += chunk
            remaining -= len(chunk)
        if os.read(fd, 1):
            raise RuntimeEnvironmentError("runtime_env_changed")
        after = os.fstat(fd)
        if (
            after.st_dev,
            after.st_ino,
            after.st_size,
            after.st_mtime_ns,
            after.st_ctime_ns,
        ) != (
            before.st_dev,
            before.st_ino,
            before.st_size,
            before.st_mtime_ns,
            before.st_ctime_ns,
        ):
            raise RuntimeEnvironmentError("runtime_env_changed")
        return payload
    finally:
        os.close(fd)


def _expand(value: str, environment: Mapping[str, str]) -> str:
    output: list[str] = []
    cursor = 0
    for match in VARIABLE.finditer(value):
        if "$" in value[cursor : match.start()]:
            raise RuntimeEnvironmentError("runtime_env_value_invalid")
        output.append(value[cursor : match.start()])
        name = match.group(1) or match.group(2) or ""
        if name not in environment:
            raise RuntimeEnvironmentError("runtime_env_reference_missing")
        output.append(environment[name])
        cursor = match.end()
    if "$" in value[cursor:]:
        raise RuntimeEnvironmentError("runtime_env_value_invalid")
    output.append(value[cursor:])
    expanded = "".join(output)
    if "\x00" in expanded or "\n" in expanded or "\r" in expanded:
        raise RuntimeEnvironmentError("runtime_env_value_invalid")
    return expanded


def _parse_value(raw: str, environment: Mapping[str, str]) -> str:
    if not raw:
        return ""
    if raw.startswith("'"):
        if len(raw) < 2 or not raw.endswith("'") or "'" in raw[1:-1]:
            raise RuntimeEnvironmentError("runtime_env_value_invalid")
        value = raw[1:-1]
    elif raw.startswith('"'):
        if len(raw) < 2 or not raw.endswith('"'):
            raise RuntimeEnvironmentError("runtime_env_value_invalid")
        inner = raw[1:-1]
        if re.search(r"\\(?![\\\"$])", inner):
            raise RuntimeEnvironmentError("runtime_env_value_invalid")
        inner = (
            inner.replace(r"\\", "\x00")
            .replace(r"\"", '"')
            .replace(r"\$", "$")
            .replace("\x00", "\\")
        )
        value = _expand(inner, environment)
    else:
        if (
            any(character.isspace() for character in raw)
            or any(character in raw for character in "`;&|<>()")
            or "\\" in raw
        ):
            raise RuntimeEnvironmentError("runtime_env_value_invalid")
        value = _expand(raw, environment)
    if "\x00" in value or "\n" in value or "\r" in value:
        raise RuntimeEnvironmentError("runtime_env_value_invalid")
    return value


def _parse_assignments(
    payload: bytes,
    *,
    environment: dict[str, str],
    allow_include: bool,
    shared_env_path: Path,
    source_payloads: list[tuple[Path, bytes]],
) -> set[str]:
    try:
        text = payload.decode("utf-8-sig")
    except UnicodeDecodeError as exc:
        raise RuntimeEnvironmentError("runtime_env_encoding_invalid") from exc
    assigned: set[str] = set()
    included = False
    for raw_line in text.splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("source ") or line.startswith(". "):
            if not allow_include or included:
                raise RuntimeEnvironmentError("runtime_env_include_invalid")
            try:
                tokens = shlex.split(line, comments=False, posix=True)
            except ValueError as exc:
                raise RuntimeEnvironmentError(
                    "runtime_env_include_invalid"
                ) from exc
            if (
                len(tokens) != 2
                or tokens[0] not in {"source", "."}
                or Path(tokens[1]) != shared_env_path
            ):
                raise RuntimeEnvironmentError("runtime_env_include_invalid")
            shared_payload = _read_secure_env_file(shared_env_path)
            source_payloads.append((shared_env_path, shared_payload))
            assigned.update(
                _parse_assignments(
                    shared_payload,
                    environment=environment,
                    allow_include=False,
                    shared_env_path=shared_env_path,
                    source_payloads=source_payloads,
                )
            )
            included = True
            continue
        match = ASSIGNMENT.fullmatch(line)
        if match is None:
            raise RuntimeEnvironmentError("runtime_env_syntax_invalid")
        key, raw_value = match.groups()
        if key in RESERVED_KEYS or key in assigned:
            raise RuntimeEnvironmentError("runtime_env_assignment_invalid")
        environment[key] = _parse_value(raw_value, environment)
        assigned.add(key)
    return assigned


def load_strict_runtime_environment(
    *,
    project_root: Path,
    base_environment: Mapping[str, str],
    shared_env_path: Path = APPROVED_SHARED_ENV_PATH,
) -> RuntimeEnvironmentLoad:
    root = project_root.resolve(strict=True)
    runtime_path = root / "config/runtime.env"
    if runtime_path.parent != root / "config":
        raise RuntimeEnvironmentError("runtime_env_path_invalid")
    runtime_payload = _read_secure_env_file(runtime_path)
    environment = dict(base_environment)
    source_payloads = [(runtime_path, runtime_payload)]
    assigned = _parse_assignments(
        runtime_payload,
        environment=environment,
        allow_include=True,
        shared_env_path=shared_env_path,
        source_payloads=source_payloads,
    )
    expected_cookie = root / "config/wb_cookie.txt"
    actual_cookie = environment.get("WB_COOKIE_FILE", str(expected_cookie))
    if Path(actual_cookie) != expected_cookie:
        raise RuntimeEnvironmentError("runtime_cookie_path_invalid")
    environment["WB_COOKIE_FILE"] = str(expected_cookie)
    assigned.add("WB_COOKIE_FILE")
    source_set = hashlib.sha256()
    for path, payload in source_payloads:
        source_set.update(str(path).encode("utf-8"))
        source_set.update(b"\0")
        source_set.update(hashlib.sha256(payload).digest())
        source_set.update(b"\0")
    runtime_sha = _sha256(runtime_payload)
    source_sha = source_set.hexdigest()
    environment["PARSER_WB_RUNTIME_ENV_LOADED"] = "1"
    environment["PARSER_WB_RUNTIME_ENV_SHA256"] = runtime_sha
    environment["PARSER_WB_RUNTIME_SOURCE_SET_SHA256"] = source_sha
    assigned.update(RESERVED_KEYS)
    return RuntimeEnvironmentLoad(
        environment=environment,
        exported_keys=tuple(sorted(assigned)),
        runtime_env_sha256=runtime_sha,
        source_set_sha256=source_sha,
    )
