#!/usr/bin/env python3
"""Step 340 — Verify v2 fix worked (force-invoke vol-regime + implied-prob).

After replacing Polygon options (paid-tier) with FRED VIX-family,
re-invoke both Lambdas and check that IV is now populated.
"""
import json
import os
import time
from datetime import datetime, timezone

import boto3

REPORT = "aws/ops/reports/340_v2_fix_verify.json"
lam = boto3.client("lambda", region_name="us-east-1")
s3 = boto3.client("s3", region_name="us-east-1")
BUCKET = "justhodl-dashboard-live"


def force_invoke(name, payload=None):
    started = time.time()
    resp = lam.invoke(
        FunctionName=name, InvocationType="RequestResponse",
        Payload=json.dumps(payload or {}).encode("utf-8"),
    )
    body = resp["Payload"].read().decode("utf-8")
    out = {
        "status": resp.get("StatusCode"),
        "fn_err": resp.get("FunctionError"),
        "duration_s": round(time.time() - started, 1),
    }
    try:
        out["body"] = json.loads(body)
    except Exception:
        out["body_raw"] = body[:300]
    return out


def s3_get(key):
    obj = s3.get_object(Bucket=BUCKET, Key=key)
    return json.loads(obj["Body"].read())


def main():
    out = {"started": datetime.now(timezone.utc).isoformat(), "tests": {}}

    # ─── Vol Regime v2 ───────────────────────────────────────────────────────
    print("\n══ Force-invoking vol-regime v2 ══")
    inv = force_invoke("justhodl-vol-regime")
    print(f"  Invoke: {json.dumps(inv, default=str)[:200]}")

    time.sleep(2)
    d = s3_get("data/vol-regime.json")
    spy = next((t for t in d.get("tickers", []) if t.get("ticker") == "SPY"), None)
    qqq = next((t for t in d.get("tickers", []) if t.get("ticker") == "QQQ"), None)
    iwm = next((t for t in d.get("tickers", []) if t.get("ticker") == "IWM"), None)
    gld = next((t for t in d.get("tickers", []) if t.get("ticker") == "GLD"), None)

    out["tests"]["vol_regime"] = {
        "schema": d.get("schema_version"),
        "method": d.get("method"),
        "n_with_iv": d.get("n_with_iv"),
        "composite_score": d.get("composite_score"),
        "composite_regime": d.get("composite_regime"),
        "duration_s": d.get("duration_s"),
        "checks": [
            {"name": "Schema bumped to 2.0", "pass": d.get("schema_version") == "2.0"},
            {"name": "Method = vol_regime_v2_fred", "pass": d.get("method") == "vol_regime_v2_fred"},
            {"name": "SPY IV populated (was null in v1)",
             "pass": spy is not None and spy.get("iv_atm_30d") is not None,
             "got": {"iv_atm_30d": (spy or {}).get("iv_atm_30d"),
                     "iv_source": (spy or {}).get("iv_source"),
                     "iv_date": (spy or {}).get("iv_date")}},
            {"name": "SPY term structure populated",
             "pass": spy is not None and spy.get("term_slope") is not None,
             "got": {"iv_30d": (spy or {}).get("iv_atm_30d"),
                     "iv_90d": (spy or {}).get("iv_atm_90d"),
                     "term_slope": (spy or {}).get("term_slope"),
                     "term": (spy or {}).get("term_structure")}},
            {"name": "SPY IV/RV ratio populated",
             "pass": spy is not None and spy.get("iv_rv_ratio") is not None,
             "got": {"iv_rv": (spy or {}).get("iv_rv_ratio"),
                     "rv_20d": (spy or {}).get("rv_20d"),
                     "regime": (spy or {}).get("regime")}},
            {"name": "QQQ IV populated",
             "pass": qqq is not None and qqq.get("iv_atm_30d") is not None,
             "got": {"iv": (qqq or {}).get("iv_atm_30d"),
                     "source": (qqq or {}).get("iv_source")}},
            {"name": "IWM IV populated",
             "pass": iwm is not None and iwm.get("iv_atm_30d") is not None,
             "got": {"iv": (iwm or {}).get("iv_atm_30d"),
                     "source": (iwm or {}).get("iv_source")}},
            {"name": "GLD IV populated",
             "pass": gld is not None and gld.get("iv_atm_30d") is not None,
             "got": {"iv": (gld or {}).get("iv_atm_30d"),
                     "source": (gld or {}).get("iv_source")}},
        ],
    }

    # ─── Implied Prob ────────────────────────────────────────────────────────
    print("\n══ Force-invoking implied-prob ══")
    inv2 = force_invoke("justhodl-implied-prob")
    print(f"  Invoke: {json.dumps(inv2, default=str)[:200]}")

    time.sleep(2)
    d2 = s3_get("data/implied-prob.json")
    spy_imp = d2.get("spy") or {}
    qqq_imp = d2.get("qqq") or {}

    out["tests"]["implied_prob"] = {
        "spy_spot": spy_imp.get("spot"),
        "spy_iv_30d": spy_imp.get("iv_30d"),
        "spy_iv_90d": spy_imp.get("iv_90d"),
        "qqq_iv_30d": qqq_imp.get("iv_30d"),
        "checks": [
            {"name": "SPY IV 30d populated (was null in v1)",
             "pass": spy_imp.get("iv_30d") is not None,
             "got": {"iv_30d": spy_imp.get("iv_30d"), "spot": spy_imp.get("spot")}},
            {"name": "SPY IV 90d populated",
             "pass": spy_imp.get("iv_90d") is not None,
             "got": {"iv_90d": spy_imp.get("iv_90d")}},
            {"name": "SPY 30d move probabilities computed",
             "pass": bool((spy_imp.get("moves_30d") or {}).get("p_up_5")),
             "got": spy_imp.get("moves_30d", {})},
            {"name": "SPY 90d move probabilities computed",
             "pass": bool((spy_imp.get("moves_90d") or {}).get("p_up_5")),
             "got": {k: v for k, v in (spy_imp.get("moves_90d") or {}).items() if k.startswith("p_") or k.startswith("expected")}},
            {"name": "QQQ IV 30d populated",
             "pass": qqq_imp.get("iv_30d") is not None,
             "got": {"iv_30d": qqq_imp.get("iv_30d")}},
            {"name": "Recession composite valid",
             "pass": (d2.get("recession") or {}).get("composite_score_0_100") is not None,
             "got": {"score": (d2.get("recession") or {}).get("composite_score_0_100"),
                     "label": (d2.get("recession") or {}).get("composite_label")}},
            {"name": "Fed stance valid",
             "pass": (d2.get("fed") or {}).get("near_term_stance") is not None,
             "got": {"current_rate": (d2.get("fed") or {}).get("current_rate"),
                     "stance": (d2.get("fed") or {}).get("near_term_stance")}},
        ],
    }

    # Summary
    summary = {}
    for fname, t in out["tests"].items():
        checks = t.get("checks") or []
        passed = sum(1 for c in checks if c.get("pass"))
        summary[fname] = {"passed": passed, "total": len(checks),
                          "pct": round(passed / max(1, len(checks)) * 100, 0)}
    out["summary"] = summary

    os.makedirs(os.path.dirname(REPORT), exist_ok=True)
    with open(REPORT, "w") as f:
        json.dump(out, f, indent=2, default=str)
    print(json.dumps(out, indent=2, default=str)[:6000])


if __name__ == "__main__":
    main()
