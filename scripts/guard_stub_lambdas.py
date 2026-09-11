#!/usr/bin/env python3
"""Refuse a truncated Lambda body.

Contents-API stubs have been 9–441 bytes. Several real engines are 900–1400
bytes (subscribe, dex-scanner, daily-macro-report). Only flag sub-500.
"""
from pathlib import Path
import subprocess
import sys

MIN_BYTES = 500
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
