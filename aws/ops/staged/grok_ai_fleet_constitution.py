#!/usr/bin/env python3
"""Idempotent note: fleet_inputs constitution ingest is pushed as the source file."""
from pathlib import Path
TARGET = Path(__file__).resolve().parents[2] / "lambdas/justhodl-ai/source/fleet_inputs.py"

def main():
    text = TARGET.read_text()
    if "CONSTITUTION_FEED" in text:
        print("fleet_inputs already allows constitution")
        return 0
    raise SystemExit("fleet_inputs missing CONSTITUTION_FEED — expected source push")

if __name__ == "__main__":
    raise SystemExit(main())
