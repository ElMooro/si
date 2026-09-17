#!/usr/bin/env python3
from pathlib import Path
ROOT = Path(__file__).resolve().parents[3]
TARGET = ROOT / "aws/lambdas/justhodl-market-extremes/source/lambda_function.py"
NEEDLE = "    s3.put_object(Bucket=BUCKET, Key=OUT_KEY,"
ALT = "        s3.put_object(Bucket=BUCKET, Key=OUT_KEY,"
INSERT = '''    # ops 5692 quality
    if isinstance(out, dict) and "quality" not in out:
        out["quality"] = {
            "publication_date": out.get("generated_at"),
            "frequency": "daily",
            "freshness_basis": "publication",
            "status": "fresh",
            "note": "Composite dashboard. call is null until a scorecard exists.",
        }
        out["call"] = None
    if isinstance(out, dict) and "pd_settlement_fails" not in out:
        try:
            _sf = json.loads(s3.get_object(Bucket=BUCKET, Key="data/settlement-fails.json")["Body"].read())
        except Exception:
            try:
                _sf = json.loads(__import__("boto3").client("s3").get_object(Bucket="justhodl-dashboard-live", Key="data/settlement-fails.json")["Body"].read())
            except Exception:
                _sf = {}
        _tr = (_sf.get("treasury") or {}) if isinstance(_sf, dict) else {}
        _hd = (_sf.get("headline") or {}) if isinstance(_sf, dict) else {}
        out["pd_settlement_fails"] = {
            "as_of": _tr.get("as_of") or _hd.get("as_of"),
            "ftd_bn": _tr.get("ftd_bn") if _tr.get("ftd_bn") is not None else _hd.get("ftd_bn"),
            "ftr_bn": _tr.get("ftr_bn") if _tr.get("ftr_bn") is not None else _hd.get("ftr_bn"),
            "combined_bn": _tr.get("gross_bn") if _tr.get("gross_bn") is not None else _hd.get("combined_bn"),
            "unit": "usd_bn",
            "source": "data/settlement-fails.json",
        }
'''

def main():
    t = TARGET.read_text()
    if "ops 5692 quality" in t:
        print("already"); return
    needle = NEEDLE if NEEDLE in t else ALT if ALT in t else None
    if not needle:
        raise SystemExit("put_object needle missing")
    t2 = t.replace(needle, INSERT + needle, 1)
    compile(t2, str(TARGET), "exec")
    TARGET.write_text(t2)
    print("patched market-extremes")
if __name__ == "__main__":
    main()
