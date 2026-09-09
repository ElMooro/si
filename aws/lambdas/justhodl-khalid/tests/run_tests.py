"""Dependency-free runner for Khalid's deterministic scoring tests."""
from __future__ import annotations

import runpy
import sys
import types
from pathlib import Path


# Importing the real handler must never discover AWS credentials or access S3.
class NoCloud:
    def __getattr__(self, name):
        raise AssertionError("Unexpected AWS operation in scoring tests: " + name)
sys.modules["boto3"] = types.SimpleNamespace(client=lambda *args, **kwargs: NoCloud())

scope = runpy.run_path(str(Path(__file__).with_name("test_scoring.py")))
tests = sorted(
    (name, fn)
    for name, fn in scope.items()
    if name.startswith("test_") and callable(fn)
)
for name, test in tests:
    test()
print(f"Khalid scoring tests passed: {len(tests)}")
