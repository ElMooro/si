#!/usr/bin/env python3
"""ops 5630 — market-extremes quality + no-call unless inputs dated."""
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
TARGET = ROOT / "aws/lambdas/justhodl-market-extremes/source/lambda_function.py"


def main() -> None:
    t = TARGET.read_text()
    if "ops 5630 quality" in t:
        print("already patched")
        return
    needle = None
    for cand in (
        '    s3.put_object(Bucket=BUCKET, Key=OUT_KEY,',
        '        s3.put_object(Bucket=BUCKET, Key=OUT_KEY,',
    ):
        if cand in t:
            needle = cand
            break
    if not needle:
        raise SystemExit("put_object OUT_KEY not found")
    block = '''    # ops 5630 quality
    if isinstance(out, dict) and "quality" not in out:
        out["quality"] = {
            "observation_date": out.get("as_of") or out.get("generated_at"),
            "publication_date": out.get("generated_at"),
            "frequency": "daily",
            "freshness_basis": "publication",
            "status": "fresh" if out.get("ok", True) else "unavailable",
            "note": "Posture is a dashboard composite, not a forecast. Not Calls-eligible until a scorecard exists.",
        }
        out["call"] = None
        out["units_note"] = "percentiles and z-scores are sample-window statistics, not implied vol."
    ''' + needle
    t = t.replace(needle, block, 1)
    compile(t, str(TARGET), "exec")
    TARGET.write_text(t)
    print("patched market-extremes")


if __name__ == "__main__":
    main()
