#!/usr/bin/env python3
"""Refuse a truncated Lambda body (deploy lane v2).

Two rules, both fail the SHA:

1. Absolute floor -- any tracked lambda_function.py under MIN_BYTES is a stub.
   Contents-API stubs have been 9-441 bytes; real small engines are ~900-1400
   bytes (subscribe, dex-scanner, daily-macro-report), so the floor is 500.

2. Shrink guard -- a source that lost more than SHRINK_FRACTION of its previous
   size (and was at least SHRINK_MIN_PREV bytes before) is treated as a
   truncated write, whatever its absolute size. A 40 KB engine that came back
   as 18 KB passes rule 1 and would have deployed. Override by putting
   ``[shrink-ok]`` in the commit message for a deliberate rewrite.

The previous blob is read from ``$GUARD_BASE_SHA`` (the push base) when set,
else ``HEAD~1``. Works on the runner and on a laptop.
"""
from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

MIN_BYTES = 500
SHRINK_FRACTION = 0.5     # lost > 50%  -> suspect
SHRINK_MIN_PREV = 2048    # only when the previous body was a real engine
ROOT = Path(__file__).resolve().parents[1]


def git(*args: str, root: Path = ROOT) -> str:
    return subprocess.run(["git", *args], cwd=root, text=True, capture_output=True).stdout


def tracked_lambda_sources(root: Path = ROOT) -> list[Path]:
    out = git("ls-files", "aws/lambdas/*/source/lambda_function.py", root=root)
    return [root / p for p in out.splitlines() if p.strip()]


def previous_size(rel: str, base: str, root: Path = ROOT) -> int | None:
    result = subprocess.run(["git", "cat-file", "-s", f"{base}:{rel}"], cwd=root,
                            text=True, capture_output=True)
    if result.returncode:
        return None
    return int(result.stdout.strip() or 0)


def shrink_override(root: Path = ROOT, base: str | None = None) -> bool:
    """[shrink-ok] in any commit since base, or GUARD_ALLOW_SHRINK=1 (runner apply lane)."""
    if os.environ.get("GUARD_ALLOW_SHRINK", "") in ("1", "true", "all"):
        return True
    rng = f"{base}..HEAD" if base else "-1"
    msg = git("log", "--format=%B", rng, root=root) if base else git("log", "-1", "--format=%B", root=root)
    return "[shrink-ok]" in msg


def check(root: Path = ROOT, base: str | None = None) -> list[str]:
    base = base or os.environ.get("GUARD_BASE_SHA") or "HEAD~1"
    override = shrink_override(root, None if base == "HEAD~1" else base)
    problems: list[str] = []
    for p in tracked_lambda_sources(root):
        if not p.is_file():
            continue
        rel = str(p.relative_to(root))
        n = p.stat().st_size
        if n < MIN_BYTES:
            problems.append(f"{rel}: {n} bytes (min {MIN_BYTES}) -- stub body")
            continue
        prev = previous_size(rel, base, root)
        if prev and prev >= SHRINK_MIN_PREV and n < prev * (1 - SHRINK_FRACTION) and not override:
            problems.append(f"{rel}: {prev} -> {n} bytes (lost {100 - n * 100 // prev}%) "
                            f"-- looks truncated; add [shrink-ok] to the commit message if intended")
    return problems


def main() -> int:
    problems = check()
    if not problems:
        print("guard_stub_lambdas: ok")
        return 0
    print("STUB / TRUNCATED LAMBDA BODY -- refusing to proceed:")
    for line in problems:
        print("  " + line)
    print("Restore the blob from git history (git show <good-sha>:<path>). Do not deploy.")
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
