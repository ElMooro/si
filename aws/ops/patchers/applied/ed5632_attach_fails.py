#!/usr/bin/env python3
"""ops 5632 — attach FR2004 FTD/FTR onto eurodollar-plumbing."""
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
TARGET = ROOT / "aws/lambdas/justhodl-eurodollar-plumbing/source/lambda_function.py"

BLOCK = '''
    # ops 5632 PD settlement fails (add-only)
    try:
        _sf = json.loads(S3.get_object(Bucket="justhodl-dashboard-live", Key="data/settlement-fails.json")["Body"].read())
    except Exception:
        _sf = {}
    _tr = (_sf.get("treasury") or {}) if isinstance(_sf, dict) else {}
    _hd = (_sf.get("headline") or {}) if isinstance(_sf, dict) else {}
    if isinstance(out, dict):
        out["pd_settlement_fails"] = {
            "as_of": _tr.get("as_of") or _hd.get("as_of") or _sf.get("as_of"),
            "ftd_bn": _tr.get("ftd_bn") or _hd.get("ftd_bn"),
            "ftr_bn": _tr.get("ftr_bn") or _hd.get("ftr_bn"),
            "combined_bn": _tr.get("gross_bn") or _hd.get("combined_bn"),
            "unit": "usd_bn",
            "source": "data/settlement-fails.json",
            "note": "FR2004 FTD + FTR two-sided gross. Plumbing stress, not eurodollar rate itself.",
            "quality": _sf.get("quality") or _tr.get("quality"),
        }
'''


def main() -> None:
    t = TARGET.read_text()
    if "ops 5632 PD settlement fails" in t:
        print("already patched")
        return
    needle = None
    for cand in (
        "    S3.put_object(Bucket=",
        "    s3.put_object(Bucket=",
    ):
        if cand in t:
            needle = cand
            break
    if not needle:
        raise SystemExit("eurodollar put_object not found")
    t = t.replace(needle, BLOCK + "\n" + needle, 1)
    compile(t, str(TARGET), "exec")
    TARGET.write_text(t)
    print("patched eurodollar-plumbing", len(t))


if __name__ == "__main__":
    main()
