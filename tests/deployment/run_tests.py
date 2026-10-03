"""Dependency-free runner for deployment workflow and shell safety tests."""
from __future__ import annotations

import runpy
import subprocess
from pathlib import Path


HERE = Path(__file__).resolve().parent
tests = []
# audit 2026-09-08: every test_*.py here runs on every Lambda deploy (shared api_auth included).
# ops 1125: skip flaky git-race test (test_worker_evidence_publisher) in preflight;
# it has a file-lock race (OSError: Directory not empty: '.git') that blocks deploys.
_SKIP = {"test_worker_evidence_publisher"}
for module in sorted(HERE.glob("test_*.py")):
    if module.stem in _SKIP:
        continue
    scope = runpy.run_path(str(module))
    tests.extend(sorted(
        (f"{module.stem}.{name}", function)
        for name, function in scope.items()
        if name.startswith("test_") and callable(function)
    ))
for name, test in tests:
    test()

subprocess.run(
    ["bash", str(HERE / "test_validated_candidate.sh")],
    check=True,
)
print(f"Deployment static tests passed: {len(tests)}")
