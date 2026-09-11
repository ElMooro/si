#!/usr/bin/env python3
"""Refuse to commit or deploy a truncated Lambda body.

The GitHub Contents API used by some agent lanes cannot carry ~40 KB files
and has already written 441-byte keep-alive stubs onto main. This guard
fails if any justhodl-*/source/lambda_function.py in the worktree (or in
the index) is smaller than MIN_BYTES.
"""
from pathlib import Path
import subprocess
import sys

MIN_BYTES = 1500
ROOT = Path(__file__).resolve().parents[1]


def tracked_lambda_sources():
    out = subprocess.check_output(
        ["git", "ls-files", "aws/lambdas/*/source/lambda_function.py"],
        cwd=ROOT, text=True,
    )
    return [ROOT / p for p in out.splitlines() if p.strip()]


def main():
    bad = []
    for p in tracked_lambda_sources():
        if not p.is_file():
            continue
        n = p.stat().st_size
        if n < MIN_BYTES:
            bad.append((str(p.relative_to(ROOT)), n))
    if not bad:
        print("guard_stub_lambdas: ok")
        return 0
    print("STUB LAMBDA BODY — refusing to proceed:")
    for path, n in bad:
        print(f"  {path}: {n} bytes (min {MIN_BYTES})")
    print("Restore the blob from git history. Do not deploy.")
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
