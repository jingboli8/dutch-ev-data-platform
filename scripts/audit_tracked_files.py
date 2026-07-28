"""Fail CI if generated, private, or credential-bearing files are tracked."""

from __future__ import annotations

import re
import subprocess
from pathlib import Path

PROHIBITED_PATHS = re.compile(
    r"(^|/)(data|\.state|\.venv|logs)(/|$)"
    r"|(^|/)\.env$"
    r"|(^|/)dbt/(target|logs|dbt_packages)(/|$)"
    r"|(^|/)dbt/(\.user\.yml|package-lock\.yml)$"
    r"|\.(duckdb|parquet|log)$"
    r"|privacy_salt"
    r"|\.pbi$",
    re.IGNORECASE,
)
SECRET_PATTERNS = {
    "GitHub token": re.compile(r"\b(?:ghp_|github_pat_)[A-Za-z0-9_]+"),
    "AWS access key": re.compile(r"\bAKIA[0-9A-Z]{16}\b"),
    "private key": re.compile(r"-----BEGIN (?:RSA |EC |OPENSSH )?PRIVATE KEY-----"),
    "absolute Windows path": re.compile(
        r"(?<![A-Za-z])[A-Za-z]:[\\/]", re.IGNORECASE
    ),
    "absolute user-home path": re.compile(
        r"/(?:home|Users)/[^/\s]+(?:/|\\)"
    ),
}


def main() -> None:
    root = Path(__file__).resolve().parents[1]
    tracked = subprocess.check_output(
        ["git", "ls-files"], cwd=root, text=True
    ).splitlines()
    failures = [
        f"prohibited tracked path: {path}"
        for path in tracked
        if PROHIBITED_PATHS.search(path.replace("\\", "/"))
    ]
    for relative in tracked:
        path = root / relative
        try:
            content = path.read_text(encoding="utf-8")
        except (UnicodeDecodeError, OSError):
            continue
        for label, pattern in SECRET_PATTERNS.items():
            if pattern.search(content):
                failures.append(f"{label} pattern in {relative}")
    if failures:
        raise SystemExit("\n".join(failures))
    print(f"Tracked-file audit passed for {len(tracked)} files.")


if __name__ == "__main__":
    main()
