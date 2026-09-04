"""Dependency-free runner for deployment workflow and shell safety tests."""
from __future__ import annotations

import runpy
import subprocess
from pathlib import Path


HERE = Path(__file__).resolve().parent
scope = runpy.run_path(str(HERE / "test_release_workflow.py"))
tests = sorted(
    (name, function)
    for name, function in scope.items()
    if name.startswith("test_") and callable(function)
)
for name, test in tests:
    test()

subprocess.run(
    ["bash", str(HERE / "test_validated_candidate.sh")],
    check=True,
)
print(f"Deployment static tests passed: {len(tests)}")
