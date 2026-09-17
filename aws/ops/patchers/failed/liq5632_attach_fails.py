#!/usr/bin/env python3
"""ops 5632 — add FR2004 FTD/FTR onto liquidity-flow without touching WALCL math."""
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
TARGET = ROOT / "aws/lambdas/justhodl-liquidity-flow/source/lambda_function.py"

BLOCK = '''
    # ops 5632 PD settlement fails (add-only; does not change net liquidity)
    try:
        _sf = json.loads(s3.get_object(Bucket=S3_BUCKET if "S3_BUCKET" in dir() else BUCKET, Key="data/settlement-fails.json")["Body"].read())
    except Exception:
        try:
            _sf = json.loads(s3.get_object(Bucket=BUCKET, Key="data/settlement-fails.json")["Body"].read())
        except Exception:
            _sf = {}
    _tr = (_sf.get("treasury") or {}) if isinstance(_sf, dict) else {}
    _hd = (_sf.get("headline") or {}) if isinstance(_sf, dict) else {}
    if isinstance(out, dict):
        out["pd_settlement_fails"] = {
            "as_of": _tr.get("as_of") or _hd.get("as_of") or (_sf.get("as_of") if isinstance(_sf, dict) else None),
            "ftd_bn": _tr.get("ftd_bn") or _hd.get("ftd_bn"),
            "ftr_bn": _tr.get("ftr_bn") or _hd.get("ftr_bn"),
            "combined_bn": _tr.get("gross_bn") or _hd.get("combined_bn"),
            "unit": "usd_bn",
            "source": "data/settlement-fails.json",
            "note": "NY Fed FR2004 two-sided gross FTD+FTR. Not unique par. Not part of WALCL-TGA-RRP.",
            "quality": (_sf.get("quality") if isinstance(_sf, dict) else None) or _tr.get("quality"),
        }
'''


def main() -> None:
    t = TARGET.read_text()
    if "ops 5632 PD settlement fails" in t:
        print("already patched")
        return
    needle = None
    for cand in (
        "    s3.put_object(Bucket=BUCKET, Key=OUT_KEY",
        "    S3.put_object(Bucket=BUCKET, Key=OUT_KEY",
        "        s3.put_object(Bucket=BUCKET, Key=OUT_KEY",
    ):
        if cand in t:
            needle = cand
            break
    if not needle:
        raise SystemExit("liquidity put_object not found")
    t = t.replace(needle, BLOCK + "\n" + needle, 1)
    compile(t, str(TARGET), "exec")
    TARGET.write_text(t)
    print("patched liquidity-flow fails attach", len(t))


if __name__ == "__main__":
    main()
