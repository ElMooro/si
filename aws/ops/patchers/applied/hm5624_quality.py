#!/usr/bin/env python3
"""ops 5624 — hot-money quality contract."""
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
TARGET = ROOT / "aws/lambdas/justhodl-hot-money/source/lambda_function.py"


def main() -> None:
    t = TARGET.read_text()
    if "ops 5624 quality" in t:
        print("already patched")
        return
    old = '''    doc["status"] = "LIVE" if live else "INSUFFICIENT_DATA"
    doc["diag"]["runtime_ms"] = int((time.time() - t0) * 1000)
    _put(OUT_KEY, doc)
'''
    new = '''    doc["status"] = "LIVE" if live else "INSUFFICIENT_DATA"
    doc["diag"]["runtime_ms"] = int((time.time() - t0) * 1000)
    tw = doc["countries"].get("taiwan") or {}
    # ops 5624 quality
    doc["units"] = "TWD_bn"
    doc["quality"] = {
        "observation_date": tw.get("latest_day"),
        "publication_date": now.date().isoformat(),
        "frequency": "daily",
        "freshness_basis": "observation",
        "status": "fresh" if live else "unavailable",
        "missing": [] if live else ["taiwan_twse"],
        "note": "Exchange foreign net only. Not TIC/BOP.",
    }
    _put(OUT_KEY, doc)
'''
    if old not in t:
        raise SystemExit("status/_put block not found")
    t = t.replace(old, new, 1)
    compile(t, str(TARGET), "exec")
    TARGET.write_text(t)
    print("patched hot-money", len(t))


if __name__ == "__main__":
    main()
