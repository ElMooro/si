#!/usr/bin/env python3
"""ops 5641 — add pd_settlement_fails onto liquidity-flow. Add-only."""
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
TARGET = ROOT / "aws/lambdas/justhodl-liquidity-flow/source/lambda_function.py"

NEEDLE = '''    print("[liq] writing %s" % S3_KEY)
    s3.put_object(Bucket=S3_BUCKET, Key=S3_KEY,'''

INSERT = '''    # ops 5641 PD settlement fails (add-only; WALCL math unchanged)
    try:
        _sf = json.loads(s3.get_object(Bucket=S3_BUCKET, Key="data/settlement-fails.json")["Body"].read())
    except Exception:
        _sf = {}
    _tr = (_sf.get("treasury") or {}) if isinstance(_sf, dict) else {}
    _hd = (_sf.get("headline") or {}) if isinstance(_sf, dict) else {}
    output["pd_settlement_fails"] = {
        "as_of": _tr.get("as_of") or _hd.get("as_of") or ( _sf.get("as_of") if isinstance(_sf, dict) else None),
        "ftd_bn": _tr.get("ftd_bn") if _tr.get("ftd_bn") is not None else _hd.get("ftd_bn"),
        "ftr_bn": _tr.get("ftr_bn") if _tr.get("ftr_bn") is not None else _hd.get("ftr_bn"),
        "combined_bn": _tr.get("gross_bn") if _tr.get("gross_bn") is not None else _hd.get("combined_bn"),
        "unit": "usd_bn",
        "source": "data/settlement-fails.json",
        "note": "NY Fed FR2004 two-sided gross FTD+FTR. Not unique par. Not a WALCL term.",
        "quality": (_sf.get("quality") if isinstance(_sf, dict) else None) or _tr.get("quality"),
    }

''' + NEEDLE


def main() -> None:
    t = TARGET.read_text()
    if "ops 5641 PD settlement fails" in t:
        print("already patched")
        return
    if NEEDLE not in t:
        raise SystemExit("needle not found")
    t2 = t.replace(NEEDLE, INSERT, 1)
    compile(t2, str(TARGET), "exec")
    TARGET.write_text(t2)
    print("patched liquidity-flow 5641", len(t2))


if __name__ == "__main__":
    main()
