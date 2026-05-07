#!/usr/bin/env python3
"""Step 342 — FINAL verification: all 5 features end-to-end after all fixes."""
import json
import os
import time
from datetime import datetime, timezone

import boto3

REPORT = "aws/ops/reports/342_final.json"
lam = boto3.client("lambda", region_name="us-east-1")
s3 = boto3.client("s3", region_name="us-east-1")
ssm = boto3.client("ssm", region_name="us-east-1")
BUCKET = "justhodl-dashboard-live"


def s3_get(key):
    return json.loads(s3.get_object(Bucket=BUCKET, Key=key)["Body"].read())


def force(name, payload=None):
    started = time.time()
    resp = lam.invoke(FunctionName=name, InvocationType="RequestResponse",
                       Payload=json.dumps(payload or {}).encode("utf-8"))
    body = resp["Payload"].read().decode("utf-8")
    out = {"status": resp.get("StatusCode"), "fn_err": resp.get("FunctionError"),
            "duration_s": round(time.time() - started, 1)}
    try:
        out["body"] = json.loads(body)
    except Exception:
        out["body_raw"] = body[:200]
    return out


def main():
    out = {"started": datetime.now(timezone.utc).isoformat(), "features": {}}

    # ─── Feature 1: Watchlist ────────────────────────────────────────────────
    print("\n══ FEATURE 1: Watchlist ══")
    inv = force("justhodl-watchlist", {
        "requestContext": {"http": {"method": "GET"}},
        "rawPath": "/", "headers": {"origin": "https://justhodl.ai"}, "body": "",
    })
    body = json.loads((inv.get("body") or {}).get("body") or "{}")
    wl = body.get("watchlist", {})
    out["features"]["watchlist"] = {
        "lambda_url": "https://kzwn6o5kbm7kqdqoioiw32hd4m0rzzsr.lambda-url.us-east-1.on.aws/",
        "page": "/watchlist.html",
        "status": inv.get("status"),
        "duration_s": inv.get("duration_s"),
        "version": wl.get("version"),
        "n_total_tickers": sum(len(v) for v in (wl.get("categories") or {}).values()),
        "categories": {k: len(v) for k, v in (wl.get("categories") or {}).items()},
        "checks": [
            {"name": "Lambda live + responding", "pass": inv.get("status") == 200 and body.get("ok") is True},
            {"name": "Schema correct (categories present)",
             "pass": all(c in (wl.get("categories") or {}) for c in ("holdings", "watching", "research", "exit_zone"))},
        ],
    }

    # ─── Feature 2: Catalyst Calendar ────────────────────────────────────────
    print("\n══ FEATURE 2: Catalyst Calendar ══")
    d = s3_get("data/catalyst-calendar.json")
    out["features"]["catalyst_calendar"] = {
        "page": "/catalyst-calendar.html",
        "as_of": d.get("as_of"),
        "n_events": d.get("n_events"),
        "by_type": d.get("by_type"),
        "high_impact_next_7d": d.get("high_impact_next_7d"),
        "high_impact_next_30d": d.get("high_impact_next_30d"),
        "checks": [
            {"name": "Events present", "pass": (d.get("n_events") or 0) > 0},
            {"name": "Multiple sources", "pass": len(d.get("by_type") or {}) >= 3},
            {"name": "FOMC in window", "pass": "FOMC" in (d.get("by_type") or {})},
        ],
    }

    # ─── Feature 3: Vol Regime (re-trigger first) ─────────────────────────────
    print("\n══ FEATURE 3: Vol Regime ══")
    force("justhodl-vol-regime")
    time.sleep(2)
    d3 = s3_get("data/vol-regime.json")
    spy = next((t for t in d3.get("tickers", []) if t.get("ticker") == "SPY"), {})
    out["features"]["vol_regime"] = {
        "page": "/vol-regime.html",
        "schema": d3.get("schema_version"),
        "method": d3.get("method"),
        "n_tickers": d3.get("n_tickers"),
        "n_with_iv": d3.get("n_with_iv"),
        "composite_score": d3.get("composite_score"),
        "composite_regime": d3.get("composite_regime"),
        "spy": {"iv_30d": spy.get("iv_atm_30d"), "iv_90d": spy.get("iv_atm_90d"),
                "iv_rv": spy.get("iv_rv_ratio"), "regime": spy.get("regime")},
        "checks": [
            {"name": "Schema v2.0", "pass": d3.get("schema_version") == "2.0"},
            {"name": "SPY IV populated (FRED VIX)", "pass": spy.get("iv_atm_30d") is not None},
            {"name": "Composite computed", "pass": d3.get("composite_score") is not None},
            {"name": "≥6 of 8 core tickers have IV", "pass": (d3.get("n_with_iv") or 0) >= 6},
        ],
    }

    # ─── Feature 4: Implied Probability (re-trigger) ──────────────────────────
    print("\n══ FEATURE 4: Implied Probability ══")
    force("justhodl-implied-prob")
    time.sleep(2)
    d4 = s3_get("data/implied-prob.json")
    out["features"]["implied_prob"] = {
        "page": "/implied-prob.html",
        "schema": d4.get("schema_version"),
        "method": d4.get("method"),
        "fed": {"current_rate": (d4.get("fed") or {}).get("current_rate"),
                "stance": (d4.get("fed") or {}).get("near_term_stance"),
                "implied_3m_bp": (d4.get("fed") or {}).get("implied_3m_change_bp")},
        "recession": {"score": (d4.get("recession") or {}).get("composite_score_0_100"),
                       "label": (d4.get("recession") or {}).get("composite_label")},
        "spy_iv_30d": (d4.get("spy") or {}).get("iv_30d"),
        "spy_expected_30d": ((d4.get("spy") or {}).get("moves_30d") or {}).get("expected_move_pct"),
        "qqq_iv_30d": (d4.get("qqq") or {}).get("iv_30d"),
        "btc_iv_30d": (d4.get("btc") or {}).get("iv_30d"),
        "btc_iv_proxy": (d4.get("btc") or {}).get("iv_proxy_note"),
        "checks": [
            {"name": "Schema v2.0", "pass": d4.get("schema_version") == "2.0"},
            {"name": "Fed current_rate", "pass": (d4.get("fed") or {}).get("current_rate") is not None},
            {"name": "Recession composite", "pass": (d4.get("recession") or {}).get("composite_score_0_100") is not None},
            {"name": "SPY IV via FRED", "pass": (d4.get("spy") or {}).get("iv_30d") is not None},
            {"name": "QQQ IV via FRED", "pass": (d4.get("qqq") or {}).get("iv_30d") is not None},
            {"name": "BTC IV (RV proxy)", "pass": (d4.get("btc") or {}).get("iv_30d") is not None},
        ],
    }

    # ─── Feature 5: Trade Journal ────────────────────────────────────────────
    print("\n══ FEATURE 5: Trade Journal ══")
    inv5 = force("justhodl-trade-journal", {
        "requestContext": {"http": {"method": "GET"}},
        "rawPath": "/", "headers": {"origin": "https://justhodl.ai"}, "body": "",
    })
    body5 = json.loads((inv5.get("body") or {}).get("body") or "{}")
    out["features"]["trade_journal"] = {
        "lambda_url": "https://c6bhlnugikpdjulunpf6qeu66q0ugtbg.lambda-url.us-east-1.on.aws/",
        "page": "/trade-journal.html",
        "status": inv5.get("status"),
        "duration_s": inv5.get("duration_s"),
        "n_trades": (body5.get("trades") or {}).get("trades", []).__len__() if body5.get("trades") else 0,
        "stats": body5.get("stats", {}),
        "checks": [
            {"name": "Lambda live + responding", "pass": inv5.get("status") == 200 and body5.get("ok") is True},
            {"name": "Stats schema present",
             "pass": "n_total" in (body5.get("stats") or {})},
        ],
    }

    # ─── Aggregate ───────────────────────────────────────────────────────────
    summary = {}
    for fname, feat in out["features"].items():
        checks = feat.get("checks") or []
        passed = sum(1 for c in checks if c.get("pass"))
        summary[fname] = {"passed": passed, "total": len(checks),
                          "pct": round(passed / max(1, len(checks)) * 100, 0)}
    out["summary"] = summary
    out["overall"] = {
        "total_passed": sum(s["passed"] for s in summary.values()),
        "total": sum(s["total"] for s in summary.values()),
    }

    os.makedirs(os.path.dirname(REPORT), exist_ok=True)
    with open(REPORT, "w") as f:
        json.dump(out, f, indent=2, default=str)
    print(json.dumps(out, indent=2, default=str)[:7000])


if __name__ == "__main__":
    main()
