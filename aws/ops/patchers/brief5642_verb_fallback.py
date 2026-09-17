#!/usr/bin/env python3
"""ops 5642 — if the LLM brief is a stub, do not stamp UNKNOWN."""
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
TARGET = ROOT / "aws/lambdas/justhodl-ai-brief/source/lambda_function.py"

OLD = '''    if not md:
        return "UNKNOWN"
    tail = md[-2500:].upper()'''

NEW = '''    if not md or len(str(md).strip()) < 120:
        # ops 5642: 54-char stubs were minting UNKNOWN for weeks.
        # WAIT is an explicit abstain, not a parse failure.
        return "WAIT"
    tail = md[-2500:].upper()'''


def main() -> None:
    t = TARGET.read_text()
    if "ops 5642" in t:
        print("already patched")
        return
    if OLD not in t:
        raise SystemExit("extract needle missing")
    t2 = t.replace(OLD, NEW, 1)
    compile(t2, str(TARGET), "exec")
    TARGET.write_text(t2)
    print("patched extract_call_verb")


if __name__ == "__main__":
    main()
