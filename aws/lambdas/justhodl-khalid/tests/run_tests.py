"""Dependency-free runner for Khalid's deterministic scoring tests."""
from __future__ import annotations

import runpy
from pathlib import Path


scope = runpy.run_path(str(Path(__file__).with_name("test_scoring.py")))
tests = sorted(
    (name, fn)
    for name, fn in scope.items()
    if name.startswith("test_") and callable(fn)
)
for name, test in tests:
    test()
print(f"Khalid scoring tests passed: {len(tests)}")
