#!/usr/bin/env python3
"""Offline actual-handler private-account regression gate."""
import runpy
from pathlib import Path
ROOT = Path(__file__).resolve().parents[4]
runpy.run_path(str(ROOT / "tests/private_artifact_test_support.py"))["run"]("justhodl-ai-brief")
