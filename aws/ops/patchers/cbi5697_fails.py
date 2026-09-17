#!/usr/bin/env python3
from pathlib import Path
ROOT = Path(__file__).resolve().parents[3]
TARGET = ROOT / "aws/lambdas/justhodl-cb-injection/source/lambda_function.py"
NEEDLE = "    out=build_measurements(data,ecb);out.update(errors=errors,elapsed_s=round(time.monotonic()-started,2))\n"
INSERT = NEEDLE + '''    # ops 5697 PD settlement fails (add-only; no WALCL/impulse change)\n    try:\n        _sf = read_existing("data/settlement-fails.json") or {}\n        _tr = _sf.get("treasury") or {}\n        _hd = _sf.get("headline") or {}\n        out["pd_settlement_fails"] = {\n            "as_of": _tr.get("as_of") or _hd.get("as_of"),\n            "ftd_bn": _tr.get("ftd_bn") if _tr.get("ftd_bn") is not None else _hd.get("ftd_bn"),\n            "ftr_bn": _tr.get("ftr_bn") if _tr.get("ftr_bn") is not None else _hd.get("ftr_bn"),\n            "combined_bn": _tr.get("gross_bn") if _tr.get("gross_bn") is not None else _hd.get("combined_bn"),\n            "unit": "usd_bn",\n            "source": "data/settlement-fails.json",\n            "note": "FR2004 two-sided gross. Not a CB injection term.",\n        }\n    except Exception:\n        pass\n'''

def main():
    t = TARGET.read_text()
    if "ops 5697 PD settlement fails" in t:
        print("already"); return
    if NEEDLE not in t:
        raise SystemExit("needle missing")
    t2 = t.replace(NEEDLE, INSERT, 1)
    compile(t2, str(TARGET), "exec")
    TARGET.write_text(t2)
    print("patched cb-injection")

if __name__ == "__main__":
    main()
