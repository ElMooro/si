#!/usr/bin/env python3
"""ops 5631 — risk-regime quality; do not size from 1.0.0 composite."""
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
TARGET = ROOT / "aws/lambdas/justhodl-risk-regime/source/lambda_function.py"


def main() -> None:
    t = TARGET.read_text()
    if "ops 5631 quality" in t:
        print("already patched")
        return
    old = '        "engine": "risk-regime", "version": "1.0.0",'
    new = '        "engine": "risk-regime", "version": "1.0.1",'
    if old not in t:
        raise SystemExit("version line not found")
    t = t.replace(old, new, 1)
    marker = '        "generated_at": datetime.now(timezone.utc).isoformat(),'
    if marker not in t:
        raise SystemExit("generated_at not found")
    t = t.replace(marker, marker + '''
        # ops 5631 quality
        "call": None,
        "quality": {
            "publication_date": datetime.now(timezone.utc).date().isoformat(),
            "frequency": "intraday",
            "freshness_basis": "publication",
            "status": "fresh",
            "note": "Composite of other engines. Not independent evidence. Not Calls-eligible until inputs are quality-gated and scored.",
        },''', 1)
    compile(t, str(TARGET), "exec")
    TARGET.write_text(t)
    print("patched risk-regime")


if __name__ == "__main__":
    main()
