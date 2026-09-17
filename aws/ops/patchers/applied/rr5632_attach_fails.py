#!/usr/bin/env python3
"""ops 5632 — attach FR2004 FTD/FTR onto risk-regime output."""
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
TARGET = ROOT / "aws/lambdas/justhodl-risk-regime/source/lambda_function.py"

BLOCK = '''
        # ops 5632 PD settlement fails (add-only)
        "pd_settlement_fails": (lambda _sf: {
            "as_of": ((_sf.get("treasury") or {}).get("as_of") or _sf.get("as_of")),
            "ftd_bn": (_sf.get("treasury") or {}).get("ftd_bn") or (_sf.get("headline") or {}).get("ftd_bn"),
            "ftr_bn": (_sf.get("treasury") or {}).get("ftr_bn") or (_sf.get("headline") or {}).get("ftr_bn"),
            "combined_bn": (_sf.get("treasury") or {}).get("gross_bn") or (_sf.get("headline") or {}).get("combined_bn"),
            "unit": "usd_bn",
            "source": "data/settlement-fails.json",
        })((lambda: (json.loads(__import__("boto3").client("s3").get_object(Bucket="justhodl-dashboard-live", Key="data/settlement-fails.json")["Body"].read()) if True else {}))() if False else {}),
'''


def main() -> None:
    t = TARGET.read_text()
    if "ops 5632 PD settlement fails" in t:
        print("already patched")
        return
    # Safer: insert a block before return/put by loading S3 just above out =
    if "out = {" not in t:
        raise SystemExit("out = { not found")
    load = '''
    _sf_fails = {}
    try:
        _sf_fails = json.loads(boto3.client("s3").get_object(Bucket="justhodl-dashboard-live", Key="data/settlement-fails.json")["Body"].read())
    except Exception:
        _sf_fails = {}
    _tr_f = _sf_fails.get("treasury") or {}
    _hd_f = _sf_fails.get("headline") or {}
'''
    t = t.replace("    out = {", load + "    out = {", 1)
    # add fields after generated_at line if present
    ga = '        "generated_at": datetime.now(timezone.utc).isoformat(),'
    extra = ga + '''
        # ops 5632 PD settlement fails (add-only)
        "pd_settlement_fails": {
            "as_of": _tr_f.get("as_of") or _hd_f.get("as_of") or _sf_fails.get("as_of"),
            "ftd_bn": _tr_f.get("ftd_bn") or _hd_f.get("ftd_bn"),
            "ftr_bn": _tr_f.get("ftr_bn") or _hd_f.get("ftr_bn"),
            "combined_bn": _tr_f.get("gross_bn") or _hd_f.get("combined_bn"),
            "unit": "usd_bn",
            "source": "data/settlement-fails.json",
            "note": "FR2004 two-sided gross FTD+FTR. Weekly. Not a default rate.",
        },'''
    if ga in t:
        t = t.replace(ga, extra, 1)
    compile(t, str(TARGET), "exec")
    TARGET.write_text(t)
    print("patched risk-regime fails", len(t))


if __name__ == "__main__":
    main()
