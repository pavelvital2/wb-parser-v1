from __future__ import annotations

import hashlib
import json
import os
import stat
import sys
from pathlib import Path
from typing import Callable, Mapping

from .nightly_coordinator import NightlyCoordinatorContractError
from .runtime_env import (
    RuntimeEnvironmentError,
    load_strict_runtime_environment,
)


MANIFEST_SCHEMA_VERSION = "wb_nightly_coordinator_input_manifest_v2"
MANIFEST_RELATIVE_PATH = Path(
    "config/wb/nightly_coordinator_adapter_inputs.json"
)
APPROVED_PYTHON_BIN = Path(
    "/home/Codex/agent-tools/parser_wb-python/bin/python"
)
APPROVED_SITE_PACKAGES = Path(
    "/home/Codex/agent-tools/parser_wb-python/lib/python3.14/site-packages"
)
MANIFEST_SHA_ENV = "PARSER_WB_COORDINATOR_INPUT_MANIFEST_SHA256"
RUNTIME_SHA_ENV = "PARSER_WB_COORDINATOR_RUNTIME_INPUT_SHA256"
MAX_INPUT_BYTES = 32 * 1024 * 1024
SHA256 = frozenset("0123456789abcdef")
RUNTIME_ENV_EXCLUDED_PREFIXES = (
    "PARSER_WB_ADAPTER_",
    "PARSER_WB_COORDINATOR_",
    "PARSER_WB_LOCK_V3_",
    "PARSER_WB_SUPERVISOR_",
)
RUNTIME_ENV_INCLUDED_PREFIXES = (
    "PARSER_WB_",
    "MARKETPLACE_PROXY_",
)
RUNTIME_ENV_INCLUDED_NAMES = frozenset(
    {
        "ALL_PROXY",
        "HTTP_PROXY",
        "HTTPS_PROXY",
        "NO_PROXY",
        "WB_COOKIE_FILE",
    }
)


def _fail(code: str) -> None:
    raise NightlyCoordinatorContractError(code, outcome="hard_failure")


def _sha256(encoded: bytes) -> str:
    return hashlib.sha256(encoded).hexdigest()


def _safe_relative(relative_path: str) -> Path:
    path = Path(relative_path)
    if (
        not relative_path
        or path.is_absolute()
        or ".." in path.parts
        or path.as_posix() != relative_path
    ):
        _fail("coordinator_input_manifest_path_invalid")
    return path


def _safe_read(
    project_root: Path,
    relative_path: str,
    *,
    exact_mode: int | None = None,
) -> bytes:
    relative = _safe_relative(relative_path)
    root = project_root.resolve(strict=True)
    current = root
    for part in relative.parts[:-1]:
        current /= part
        try:
            info = current.lstat()
        except OSError:
            _fail("coordinator_input_unavailable")
        if stat.S_ISLNK(info.st_mode) or not stat.S_ISDIR(info.st_mode):
            _fail("coordinator_input_path_unsafe")
    path = root / relative
    flags = os.O_RDONLY | os.O_CLOEXEC
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    try:
        fd = os.open(path, flags)
    except OSError:
        _fail("coordinator_input_unavailable")
    try:
        info = os.fstat(fd)
        if (
            not stat.S_ISREG(info.st_mode)
            or info.st_uid != os.geteuid()
            or info.st_mode & 0o022
            or info.st_nlink != 1
            or (exact_mode is not None and stat.S_IMODE(info.st_mode) != exact_mode)
            or not 0 < info.st_size <= MAX_INPUT_BYTES
        ):
            _fail("coordinator_input_metadata_invalid")
        encoded = b""
        remaining = info.st_size
        while remaining:
            chunk = os.read(fd, min(65536, remaining))
            if not chunk:
                _fail("coordinator_input_changed")
            encoded += chunk
            remaining -= len(chunk)
        if os.read(fd, 1):
            _fail("coordinator_input_changed")
        after = os.fstat(fd)
        if (
            after.st_dev,
            after.st_ino,
            after.st_size,
            after.st_mtime_ns,
        ) != (
            info.st_dev,
            info.st_ino,
            info.st_size,
            info.st_mtime_ns,
        ):
            _fail("coordinator_input_changed")
        return encoded
    finally:
        os.close(fd)


def expected_manifest_files(project_root: Path) -> tuple[str, ...]:
    root = project_root.resolve(strict=True)
    candidates: set[Path] = {
        Path("main.py"),
        Path("requirements.txt"),
        Path("config/config.yaml"),
        Path("config/wb/regions.json"),
        Path(
            "config/wb/collection_plans/"
            "shevron-four-regions-top1000-v2.json"
        ),
        Path(
            "config/wb/query_packs/shevron-core/2026-07-26.1.json"
        ),
    }
    candidates.update(
        path.relative_to(root)
        for path in (root / "app").rglob("*.py")
        if "__pycache__" not in path.parts
    )
    candidates.update(
        path.relative_to(root)
        for path in (root / "scripts").iterdir()
        if path.is_file() and path.suffix in {".py", ".sh"}
    )
    candidates.discard(MANIFEST_RELATIVE_PATH)
    return tuple(sorted(path.as_posix() for path in candidates))


def _read_fd_bytes(fd: int, size: int, *, code: str) -> bytes:
    payload = b""
    remaining = size
    while remaining:
        chunk = os.read(fd, min(1024 * 1024, remaining))
        if not chunk:
            _fail(code)
        payload += chunk
        remaining -= len(chunk)
    if os.read(fd, 1):
        _fail(code)
    return payload


def _dependency_tree_contract(root: Path) -> dict[str, object]:
    try:
        canonical = root.resolve(strict=True)
        root_info = canonical.lstat()
    except OSError:
        _fail("coordinator_python_dependencies_unavailable")
    if (
        stat.S_ISLNK(root_info.st_mode)
        or not stat.S_ISDIR(root_info.st_mode)
        or root_info.st_uid != os.geteuid()
        or root_info.st_mode & 0o022
    ):
        _fail("coordinator_python_dependencies_unsafe")
    digest = hashlib.sha256()
    file_count = 0
    directory_count = 0

    def visit(directory: Path, relative_parent: Path) -> None:
        nonlocal file_count, directory_count
        try:
            entries = sorted(os.scandir(directory), key=lambda item: item.name)
        except OSError:
            _fail("coordinator_python_dependencies_unavailable")
        for entry in entries:
            relative = relative_parent / entry.name
            try:
                info = entry.stat(follow_symlinks=False)
            except OSError:
                _fail("coordinator_python_dependencies_unavailable")
            if stat.S_ISLNK(info.st_mode):
                _fail("coordinator_python_dependencies_unsafe")
            if stat.S_ISDIR(info.st_mode):
                if entry.name == "__pycache__":
                    continue
                if info.st_uid != os.geteuid() or info.st_mode & 0o022:
                    _fail("coordinator_python_dependencies_unsafe")
                directory_count += 1
                digest.update(
                    (
                        f"D\0{relative.as_posix()}\0{info.st_uid}\0"
                        f"{info.st_gid}\0{stat.S_IMODE(info.st_mode):o}\0"
                    ).encode("utf-8")
                )
                visit(Path(entry.path), relative)
                continue
            if (
                not stat.S_ISREG(info.st_mode)
                or info.st_uid != os.geteuid()
                or info.st_mode & 0o022
                or info.st_nlink != 1
            ):
                _fail("coordinator_python_dependencies_unsafe")
            if relative.suffix in {".pyc", ".pyo"}:
                continue
            flags = os.O_RDONLY | os.O_CLOEXEC
            if hasattr(os, "O_NOFOLLOW"):
                flags |= os.O_NOFOLLOW
            try:
                fd = os.open(entry.path, flags)
            except OSError:
                _fail("coordinator_python_dependencies_unavailable")
            try:
                before = os.fstat(fd)
                payload = _read_fd_bytes(
                    fd,
                    before.st_size,
                    code="coordinator_python_dependencies_changed",
                )
                after = os.fstat(fd)
            finally:
                os.close(fd)
            if (
                before.st_dev,
                before.st_ino,
                before.st_size,
                before.st_mtime_ns,
                before.st_ctime_ns,
            ) != (
                after.st_dev,
                after.st_ino,
                after.st_size,
                after.st_mtime_ns,
                after.st_ctime_ns,
            ):
                _fail("coordinator_python_dependencies_changed")
            file_count += 1
            digest.update(
                (
                    f"F\0{relative.as_posix()}\0{before.st_uid}\0"
                    f"{before.st_gid}\0{stat.S_IMODE(before.st_mode):o}\0"
                    f"{before.st_size}\0{_sha256(payload)}\0"
                ).encode("utf-8")
            )

    visit(canonical, Path())
    if file_count < 1:
        _fail("coordinator_python_dependencies_unavailable")
    return {
        "configured_path": str(root),
        "resolved_path": str(canonical),
        "owner_uid": root_info.st_uid,
        "owner_gid": root_info.st_gid,
        "mode": stat.S_IMODE(root_info.st_mode),
        "file_count": file_count,
        "directory_count": directory_count,
        "tree_sha256": digest.hexdigest(),
    }


def _python_runtime_contract() -> dict[str, object]:
    if Path(sys.executable) != APPROVED_PYTHON_BIN:
        _fail("coordinator_python_path_mismatch")
    try:
        configured_info = APPROVED_PYTHON_BIN.lstat()
        resolved = APPROVED_PYTHON_BIN.resolve(strict=True)
        resolved_info = resolved.lstat()
    except OSError:
        _fail("coordinator_python_unavailable")
    if (
        not stat.S_ISLNK(configured_info.st_mode)
        or not stat.S_ISREG(resolved_info.st_mode)
        or resolved_info.st_uid != os.geteuid()
        or resolved_info.st_mode & 0o022
    ):
        _fail("coordinator_python_unsafe")
    flags = os.O_RDONLY | os.O_CLOEXEC
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    try:
        fd = os.open(resolved, flags)
    except OSError:
        _fail("coordinator_python_unavailable")
    try:
        before = os.fstat(fd)
        payload = _read_fd_bytes(
            fd,
            before.st_size,
            code="coordinator_python_changed",
        )
        after = os.fstat(fd)
    finally:
        os.close(fd)
    if (
        before.st_dev,
        before.st_ino,
        before.st_size,
        before.st_mtime_ns,
        before.st_ctime_ns,
    ) != (
        after.st_dev,
        after.st_ino,
        after.st_size,
        after.st_mtime_ns,
        after.st_ctime_ns,
    ):
        _fail("coordinator_python_changed")
    return {
        "configured_path": str(APPROVED_PYTHON_BIN),
        "configured_link_target": os.readlink(APPROVED_PYTHON_BIN),
        "resolved_path": str(resolved),
        "version": ".".join(str(item) for item in sys.version_info[:3]),
        "implementation": sys.implementation.name,
        "owner_uid": resolved_info.st_uid,
        "owner_gid": resolved_info.st_gid,
        "mode": stat.S_IMODE(resolved_info.st_mode),
        "binary_sha256": _sha256(payload),
        "dependencies": _dependency_tree_contract(
            APPROVED_SITE_PACKAGES
        ),
    }


def build_input_manifest(project_root: Path) -> dict[str, object]:
    files = expected_manifest_files(project_root)
    hashes = {
        relative: _sha256(_safe_read(project_root, relative))
        for relative in files
    }
    return {
        "schema_version": MANIFEST_SCHEMA_VERSION,
        "adapter_executable": "scripts/run_wb_four_region_nightly.sh",
        "files": list(files),
        "file_sha256": hashes,
        "python_runtime": _python_runtime_contract(),
    }


def verify_input_manifest(project_root: Path) -> str:
    manifest_bytes = _safe_read(
        project_root,
        MANIFEST_RELATIVE_PATH.as_posix(),
        exact_mode=0o644,
    )
    try:
        value = json.loads(manifest_bytes.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        _fail("coordinator_input_manifest_invalid")
    if (
        not isinstance(value, dict)
        or set(value)
        != {
            "schema_version",
            "adapter_executable",
            "files",
            "file_sha256",
            "python_runtime",
        }
        or value.get("schema_version") != MANIFEST_SCHEMA_VERSION
        or value.get("adapter_executable")
        != "scripts/run_wb_four_region_nightly.sh"
    ):
        _fail("coordinator_input_manifest_invalid")
    files = value.get("files")
    hashes = value.get("file_sha256")
    expected = expected_manifest_files(project_root)
    if (
        not isinstance(files, list)
        or tuple(files) != expected
        or not isinstance(hashes, dict)
        or set(hashes) != set(expected)
    ):
        _fail("coordinator_input_manifest_graph_mismatch")
    for relative in expected:
        expected_hash = hashes.get(relative)
        if (
            not isinstance(expected_hash, str)
            or len(expected_hash) != 64
            or any(character not in SHA256 for character in expected_hash)
            or _sha256(_safe_read(project_root, relative)) != expected_hash
        ):
            _fail("coordinator_input_manifest_hash_mismatch")
    if value.get("python_runtime") != _python_runtime_contract():
        _fail("coordinator_python_runtime_mismatch")
    return _sha256(manifest_bytes)


def _runtime_file_digest(
    project_root: Path,
    path_value: str,
    *,
    required: bool,
    expected_relative: str,
) -> dict[str, object]:
    if not path_value:
        if required:
            _fail("coordinator_runtime_input_missing")
        return {"present": False, "sha256": ""}
    expected = project_root / _safe_relative(expected_relative)
    path = Path(path_value)
    if not path.is_absolute():
        path = project_root / path
    if path != expected:
        _fail("coordinator_runtime_input_path_invalid")
    if not os.path.lexists(path):
        if required:
            _fail("coordinator_runtime_input_missing")
        return {"present": False, "sha256": ""}
    relative = _safe_relative(expected_relative)
    encoded = _safe_read(project_root, relative.as_posix(), exact_mode=0o600)
    return {
        "present": True,
        "relative_path": relative.as_posix(),
        "sha256": _sha256(encoded),
    }


def runtime_input_sha256(
    project_root: Path,
    environment: Mapping[str, str],
) -> str:
    try:
        strict = load_strict_runtime_environment(
            project_root=project_root,
            base_environment=environment,
        )
    except RuntimeEnvironmentError as exc:
        _fail(exc.code)
    if any(
        environment.get(key) != strict.environment.get(key)
        for key in strict.exported_keys
    ):
        _fail("coordinator_runtime_environment_mismatch")
    selected = {
        key: value
        for key, value in environment.items()
        if (
            key in RUNTIME_ENV_INCLUDED_NAMES
            or key.startswith(RUNTIME_ENV_INCLUDED_PREFIXES)
        )
        and not key.startswith(RUNTIME_ENV_EXCLUDED_PREFIXES)
    }
    runtime_env = _runtime_file_digest(
        project_root,
        str(project_root / "config/runtime.env"),
        required=True,
        expected_relative="config/runtime.env",
    )
    headers = _runtime_file_digest(
        project_root,
        environment.get("PARSER_WB_REQUEST_HEADERS_FILE", ""),
        required=True,
        expected_relative="config/wb_request_headers.json",
    )
    cookie = _runtime_file_digest(
        project_root,
        environment.get("WB_COOKIE_FILE", ""),
        required=True,
        expected_relative="config/wb_cookie.txt",
    )
    payload = {
        "schema_version": "wb_runtime_input_fingerprint_v1",
        "environment_sha256": _sha256(
            json.dumps(
                selected,
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
            ).encode("utf-8")
        ),
        "runtime_env": runtime_env,
        "runtime_source_set_sha256": strict.source_set_sha256,
        "request_headers": headers,
        "cookie": cookie,
    }
    return _sha256(
        json.dumps(
            payload,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    )


def capture_attested_environment(
    project_root: Path,
    environment: Mapping[str, str],
) -> dict[str, str]:
    manifest_sha = verify_input_manifest(project_root)
    runtime_sha = runtime_input_sha256(project_root, environment)
    result = dict(environment)
    result[MANIFEST_SHA_ENV] = manifest_sha
    result[RUNTIME_SHA_ENV] = runtime_sha
    return result


def verify_attested_environment(
    project_root: Path,
    environment: Mapping[str, str] | None = None,
) -> None:
    env = environment if environment is not None else os.environ
    expected_manifest = env.get(MANIFEST_SHA_ENV, "")
    expected_runtime = env.get(RUNTIME_SHA_ENV, "")
    if (
        len(expected_manifest) != 64
        or len(expected_runtime) != 64
        or verify_input_manifest(project_root) != expected_manifest
        or runtime_input_sha256(project_root, env) != expected_runtime
    ):
        _fail("coordinator_attested_input_changed")


def integrity_gate(
    project_root: Path,
    environment: Mapping[str, str] | None = None,
) -> Callable[[], None]:
    env = environment if environment is not None else os.environ
    if MANIFEST_SHA_ENV not in env and RUNTIME_SHA_ENV not in env:
        return lambda: None

    def verify() -> None:
        verify_attested_environment(project_root, env)

    verify()
    return verify
