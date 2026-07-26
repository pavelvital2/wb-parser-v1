#!/usr/bin/env python3
"""Run local repository safety checks before pushing parser_wb changes.

The checks are intentionally local-only: they do not call Wildberries, do not
read runtime secret values, and do not print cookie/header contents.
"""

from __future__ import annotations

import argparse
import fnmatch
import os
import subprocess
import sys
from pathlib import Path


PROJECT_DIR = Path(os.environ.get("PARSER_WB_PROJECT_DIR", "/home/pavel/projects/parser_wb"))
PYTHON_BIN = Path(os.environ.get("PARSER_WB_PYTHON_BIN", "/home/Codex/agent-tools/parser_wb-python/bin/python"))
CONFIG_FILE = PROJECT_DIR / "config/config.yaml"


FORBIDDEN_PATH_PATTERNS: tuple[tuple[str, str], ...] = (
    ("state/*", "runtime state must not be committed"),
    ("data/logs/*", "runtime logs must not be committed"),
    ("data/raw/*", "raw runtime data must not be committed"),
    ("data/staging/*", "staging runtime data must not be committed"),
    ("data/marts/*", "mart runtime data must not be committed"),
    ("data/warehouse/*", "warehouse runtime data must not be committed"),
    ("exports/*", "runtime exports must not be committed"),
    ("config/runtime.env*", "runtime env/secrets must not be committed"),
    ("config/*cookie*", "cookies and cookie backups must not be committed"),
    ("config/*request_headers*", "secret request headers must not be committed"),
    ("config/*secret*", "secret local config must not be committed"),
    ("*storage_state*", "browser storage_state must not be committed"),
    ("AGENTS.md.bak*", "local AGENTS backups must not be committed"),
    ("docs/*handoff*", "handoff scratch documents must not be committed"),
    ("docs/*task_handoff*", "task handoff scratch documents must not be committed"),
    (".env", "local env files must not be committed"),
    (".env.*", "local env files must not be committed"),
)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--staged-only", action="store_true", help="Only check staged/tracked path safety and whitespace")
    parser.add_argument("--skip-tests", action="store_true", help="Skip pytest for a faster local check")
    parser.add_argument("--skip-warehouse", action="store_true", help="Skip warehouse dry-run/check")
    return parser.parse_args(argv)


def run_command(command: list[str], *, cwd: Path = PROJECT_DIR, capture: bool = False) -> subprocess.CompletedProcess[str]:
    print("+ " + " ".join(command), flush=True)
    return subprocess.run(
        command,
        cwd=str(cwd),
        text=True,
        stdout=subprocess.PIPE if capture else None,
        stderr=subprocess.PIPE if capture else None,
        check=False,
    )


def git_paths(*args: str) -> list[str]:
    result = run_command(["git", *args, "-z"], capture=True)
    if result.returncode != 0:
        if result.stdout:
            print(result.stdout, end="")
        if result.stderr:
            print(result.stderr, end="", file=sys.stderr)
        raise SystemExit(result.returncode)
    return [item for item in result.stdout.split("\0") if item]


def forbidden_reason(path: str) -> str | None:
    normalized = path.replace("\\", "/")
    for pattern, reason in FORBIDDEN_PATH_PATTERNS:
        if pattern == ".env.*" and normalized == ".env.example":
            continue
        if fnmatch.fnmatchcase(normalized, pattern):
            return reason
    return None


def check_forbidden_paths(label: str, paths: list[str]) -> bool:
    bad: list[tuple[str, str]] = []
    for path in paths:
        reason = forbidden_reason(path)
        if reason:
            bad.append((path, reason))
    if not bad:
        print(f"ok: no forbidden {label} paths")
        return True

    print(f"ERROR: forbidden {label} paths:", file=sys.stderr)
    for path, reason in bad:
        print(f"  {path}: {reason}", file=sys.stderr)
    return False


def check_paths() -> bool:
    ok = True
    ok = check_forbidden_paths("staged", git_paths("diff", "--cached", "--name-only")) and ok
    ok = check_forbidden_paths("tracked", git_paths("ls-files")) and ok
    ok = check_forbidden_paths("untracked", git_paths("ls-files", "--others", "--exclude-standard")) and ok
    return ok


def run_required(command: list[str]) -> bool:
    result = run_command(command)
    if result.returncode == 0:
        return True
    print(f"ERROR: command failed with exit {result.returncode}: {' '.join(command)}", file=sys.stderr)
    return False


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    if not check_paths():
        return 20

    checks: list[list[str]] = [
        ["git", "diff", "--check"],
        ["git", "diff", "--cached", "--check"],
    ]
    if not args.staged_only:
        checks.append(
            [
                "bash",
                "-n",
                "scripts/run_products_sellers_daily.sh",
                "scripts/run_wb_cookie_renewal.sh",
                "scripts/run_wb_warehouse_refresh.sh",
            ]
        )
        checks.append([str(PYTHON_BIN), "main.py", "--config", str(CONFIG_FILE), "validate"])
        if not args.skip_warehouse:
            checks.append([str(PYTHON_BIN), "scripts/wb_warehouse.py", "build", "--dry-run"])
            checks.append([str(PYTHON_BIN), "scripts/wb_warehouse.py", "check"])
        if not args.skip_tests:
            checks.append([str(PYTHON_BIN), "-m", "pytest", "-q"])

    for command in checks:
        if not run_required(command):
            return 21

    print("pre-push check: OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
