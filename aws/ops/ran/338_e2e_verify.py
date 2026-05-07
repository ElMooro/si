#!/usr/bin/env python3
"""Step 338 — End-to-end verification of all 5 features (bug-check pass).

Tests every Lambda + S3 output + checks for known bug classes:
  - Watchlist: GET works, POST without auth fails 401, POST with auth succeeds, validation rejects bad tickers
  - Catalyst Calendar: events sorted, days_to correct, sources non-empty
  - Vol Regime: composite valid, regimes valid, RV/IV both populated for SPY
  - Implied Prob: Fed section has rates, recession section has score, SPY moves computed
  - Trade Journal: GET works, POST add validates, full lifecycle (add → close)
"""
import json
import os
import time
import urllib.parse
import urllib.request
from datetime import datetime, timezone

import boto3

REGION = "us-east-1"
REPORT = "aws/ops/reports/338_e2e_verify.json"

lam = boto3.client("lambda", region_name=REGION)
s3 = boto3.client("s3", region_name=REGION)
ssm = boto3.client("ssm", region_name=REGION)

BUCKET = "justhodl-dashboard-live"
WATCHLIST_URL = "https://kzwn6o5kbm7kqdqoioiw32hd4m0rzzsr.lambda-url.us-east-1.on.aws/"
JOURNAL_URL   = "https://c6bhlnugikpdjulunpf6qeu66q0ugtbg.lambda-url.us-east-1.on.aws/"


def http_invoke(name, payload):
    started = time.time()
    resp = lam.invoke(FunctionName=name, InvocationType="RequestResponse",
                      Payload=json.dumps(payload).encode("utf-8"))
    body = resp["Payload"].read().decode("utf-8")
    try:
        return json.loads(body), round(time.time() - started, 2)
    except Exception:
        return {"raw": body[:300]}, round(time.time() - started, 2)


def get_admin_token():
    p = ssm.get_parameter(Name="/justhodl/api-admin/token", WithDecryption=True)
    return p["Parameter"]["Value"]


def s3_get(key):
    obj = s3.get_object(Bucket=BUCKET, Key=key)
    return json.loads(obj["Body"].read())


def main():
    out = {"started": datetime.now(timezone.utc).isoformat(), "tests": {}}

    # ─────────────────────────────────────────────────────────────────────────
    # #1 WATCHLIST — full CRUD + auth + validation
    # ─────────────────────────────────────────────────────────────────────────
    print("\n══ TEST #1 WATCHLIST ══")
    test = {"checks": []}
    try:
        admin_token = get_admin_token()

        # Test 1.1: GET (public, no auth) returns empty watchlist
        r, dur = http_invoke("justhodl-watchlist", {
            "requestContext": {"http": {"method": "GET"}},
            "rawPath": "/", "headers": {"origin": "https://justhodl.ai"}, "body": "",
        })
        body = json.loads(r.get("body", "{}"))
        test["checks"].append({
            "name": "GET (no auth, public read)",
            "pass": r.get("statusCode") == 200 and body.get("ok") is True,
            "got": {"status": r.get("statusCode"), "ok": body.get("ok"), "version": body.get("watchlist", {}).get("version")},
        })

        # Test 1.2: POST without auth → 401
        r, _ = http_invoke("justhodl-watchlist", {
            "requestContext": {"http": {"method": "POST"}},
            "rawPath": "/add", "headers": {"origin": "https://justhodl.ai"},
            "body": json.dumps({"ticker": "AAPL", "category": "holdings"}),
        })
        body_no_auth = json.loads(r.get("body", "{}"))
        test["checks"].append({
            "name": "POST without auth → 401",
            "pass": r.get("statusCode") == 401 and body_no_auth.get("ok") is False,
            "got": {"status": r.get("statusCode"), "err": body_no_auth.get("err")},
        })

        # Test 1.3: POST with auth + invalid ticker → 400 + clean error
        r, _ = http_invoke("justhodl-watchlist", {
            "requestContext": {"http": {"method": "POST"}},
            "rawPath": "/add",
            "headers": {"origin": "https://justhodl.ai", "x-justhodl-token": admin_token},
            "body": json.dumps({"ticker": "invalid ticker with spaces", "category": "holdings"}),
        })
        body_invalid = json.loads(r.get("body", "{}"))
        test["checks"].append({
            "name": "POST with bad ticker → 400 (validation)",
            "pass": r.get("statusCode") == 400 and "ticker" in (body_invalid.get("err") or "").lower(),
            "got": {"status": r.get("statusCode"), "err": body_invalid.get("err")},
        })

        # Test 1.4: POST add valid ticker → 200 + persists
        r, _ = http_invoke("justhodl-watchlist", {
            "requestContext": {"http": {"method": "POST"}},
            "rawPath": "/add",
            "headers": {"origin": "https://justhodl.ai", "x-justhodl-token": admin_token},
            "body": json.dumps({"ticker": "NVDA", "category": "holdings"}),
        })
        body_add = json.loads(r.get("body", "{}"))
        wl_after = body_add.get("watchlist", {})
        test["checks"].append({
            "name": "POST add NVDA → 200 + persisted",
            "pass": r.get("statusCode") == 200 and "NVDA" in wl_after.get("categories", {}).get("holdings", []),
            "got": {"status": r.get("statusCode"), "holdings": wl_after.get("categories", {}).get("holdings"), "version": wl_after.get("version")},
        })

        # Test 1.5: Idempotency — add same ticker twice
        r, _ = http_invoke("justhodl-watchlist", {
            "requestContext": {"http": {"method": "POST"}},
            "rawPath": "/add",
            "headers": {"origin": "https://justhodl.ai", "x-justhodl-token": admin_token},
            "body": json.dumps({"ticker": "NVDA", "category": "holdings"}),
        })
        body_dup = json.loads(r.get("body", "{}"))
        wl_dup = body_dup.get("watchlist", {})
        holdings = wl_dup.get("categories", {}).get("holdings", [])
        test["checks"].append({
            "name": "Idempotent add (NVDA twice) → no duplicate",
            "pass": holdings.count("NVDA") <= 1,
            "got": {"holdings": holdings},
        })

        # Test 1.6: Remove
        r, _ = http_invoke("justhodl-watchlist", {
            "requestContext": {"http": {"method": "POST"}},
            "rawPath": "/remove",
            "headers": {"origin": "https://justhodl.ai", "x-justhodl-token": admin_token},
            "body": json.dumps({"ticker": "NVDA"}),
        })
        body_rm = json.loads(r.get("body", "{}"))
        wl_rm = body_rm.get("watchlist", {})
        test["checks"].append({
            "name": "POST remove NVDA → 200 + removed",
            "pass": r.get("statusCode") == 200 and "NVDA" not in wl_rm.get("categories", {}).get("holdings", []),
            "got": {"status": r.get("statusCode"), "holdings": wl_rm.get("categories", {}).get("holdings")},
        })
    except Exception as e:
        import traceback
        test["fatal"] = str(e)
        test["trace"] = traceback.format_exc()[-500:]
    out["tests"]["watchlist"] = test

    # ─────────────────────────────────────────────────────────────────────────
    # #2 CATALYST CALENDAR — output integrity
    # ─────────────────────────────────────────────────────────────────────────
    print("\n══ TEST #2 CATALYST CALENDAR ══")
    test = {"checks": []}
    try:
        d = s3_get("data/catalyst-calendar.json")
        # 2.1: events sorted ascending by date
        events = d.get("events") or []
        sorted_check = all(events[i]["date"] <= events[i+1]["date"] for i in range(len(events)-1))
        test["checks"].append({
            "name": "Events sorted by date ascending",
            "pass": sorted_check,
            "got": {"n_events": len(events), "first_date": events[0]["date"] if events else None, "last_date": events[-1]["date"] if events else None},
        })
        # 2.2: days_to ≥ 0 for all
        all_future = all(e.get("days_to", -1) >= 0 for e in events)
        test["checks"].append({
            "name": "All events have days_to >= 0 (no past events)",
            "pass": all_future,
            "got": {"min_days_to": min((e.get("days_to") for e in events), default=None)},
        })
        # 2.3: each event has required fields
        required_fields = {"date", "type", "title", "impact", "days_to"}
        all_have_fields = all(required_fields.issubset(e.keys()) for e in events)
        test["checks"].append({
            "name": "All events have required fields",
            "pass": all_have_fields,
            "got": {"sample": events[0] if events else None},
        })
        # 2.4: by_type has non-zero counts
        by_type = d.get("by_type") or {}
        test["checks"].append({
            "name": "Multiple sources contribute",
            "pass": len(by_type) >= 2,
            "got": {"by_type": by_type},
        })
        # 2.5: FOMC event present
        has_fomc = any(e["type"] == "FOMC" for e in events)
        test["checks"].append({
            "name": "FOMC event in 60d window",
            "pass": has_fomc,
            "got": {"has_fomc": has_fomc, "fomc_dates": [e["date"] for e in events if e["type"] == "FOMC"]},
        })
    except Exception as e:
        import traceback
        test["fatal"] = str(e)
        test["trace"] = traceback.format_exc()[-500:]
    out["tests"]["catalyst_calendar"] = test

    # ─────────────────────────────────────────────────────────────────────────
    # #3 VOL REGIME — statistical validity
    # ─────────────────────────────────────────────────────────────────────────
    print("\n══ TEST #3 VOL REGIME ══")
    test = {"checks": []}
    try:
        d = s3_get("data/vol-regime.json")
        # 3.1: composite score 0-100
        comp = d.get("composite_score")
        test["checks"].append({
            "name": "Composite score in [0, 100]",
            "pass": comp is not None and 0 <= comp <= 100,
            "got": {"score": comp, "regime": d.get("composite_regime")},
        })
        # 3.2: every ticker has valid regime
        valid_regimes = {"COMPLACENT", "NORMAL", "CONCERNED", "PANIC", "UNKNOWN"}
        tickers = d.get("tickers") or []
        all_valid_regime = all(t.get("regime") in valid_regimes for t in tickers)
        test["checks"].append({
            "name": "All tickers have valid regime classification",
            "pass": all_valid_regime,
            "got": {"n_tickers": len(tickers), "regimes": list({t.get("regime") for t in tickers})},
        })
        # 3.3: SPY has both RV and IV populated (it's the most liquid)
        spy_rec = next((t for t in tickers if t.get("ticker") == "SPY"), None)
        test["checks"].append({
            "name": "SPY has RV + IV populated",
            "pass": spy_rec is not None and spy_rec.get("rv_20d") is not None and spy_rec.get("iv_atm_30d") is not None,
            "got": {"spy_rv_20d": spy_rec.get("rv_20d") if spy_rec else None, "spy_iv_30d": spy_rec.get("iv_atm_30d") if spy_rec else None},
        })
        # 3.4: RV values plausible (annualized > 1%, < 200%)
        rvs = [t.get("rv_20d") for t in tickers if t.get("rv_20d") is not None]
        plausible = all(1 < rv < 200 for rv in rvs) if rvs else False
        test["checks"].append({
            "name": "RV values plausible (1% < rv_20d < 200%)",
            "pass": plausible,
            "got": {"n_with_rv": len(rvs), "min": min(rvs) if rvs else None, "max": max(rvs) if rvs else None},
        })
        # 3.5: IV/RV ratio reasonable (0.3 < ratio < 5)
        ratios = [t.get("iv_rv_ratio") for t in tickers if t.get("iv_rv_ratio") is not None]
        ratio_ok = all(0.3 < r < 5 for r in ratios) if ratios else False
        test["checks"].append({
            "name": "IV/RV ratios reasonable",
            "pass": ratio_ok,
            "got": {"n_with_ratio": len(ratios), "ratios": [round(r, 2) for r in ratios[:5]]},
        })
    except Exception as e:
        import traceback
        test["fatal"] = str(e)
        test["trace"] = traceback.format_exc()[-500:]
    out["tests"]["vol_regime"] = test

    # ─────────────────────────────────────────────────────────────────────────
    # #4 IMPLIED PROB — section validity
    # ─────────────────────────────────────────────────────────────────────────
    print("\n══ TEST #4 IMPLIED PROB ══")
    test = {"checks": []}
    try:
        d = s3_get("data/implied-prob.json")
        # 4.1: Fed section populated
        fed = d.get("fed") or {}
        test["checks"].append({
            "name": "Fed section has current rate",
            "pass": fed.get("current_rate") is not None and fed.get("current_rate") > 0,
            "got": {"current_rate": fed.get("current_rate"), "stance": fed.get("near_term_stance")},
        })
        # 4.2: recession composite valid
        rec = d.get("recession") or {}
        score = rec.get("composite_score_0_100")
        test["checks"].append({
            "name": "Recession composite score in [0, 100]",
            "pass": score is not None and 0 <= score <= 100,
            "got": {"score": score, "label": rec.get("composite_label")},
        })
        # 4.3: NY Fed prob in [0, 100]
        ny = rec.get("ny_fed_12m_prob_pct")
        test["checks"].append({
            "name": "NY Fed 12m prob valid",
            "pass": ny is not None and 0 <= ny <= 100,
            "got": {"ny_fed": ny},
        })
        # 4.4: SPY moves probabilities in [0, 100]
        spy_moves = (d.get("spy") or {}).get("moves_30d") or {}
        all_probs_valid = all(
            v is None or 0 <= v <= 100
            for k, v in spy_moves.items() if k.startswith("p_")
        )
        test["checks"].append({
            "name": "SPY 30d move probabilities in [0, 100]",
            "pass": all_probs_valid,
            "got": {"spy_30d_moves": spy_moves},
        })
        # 4.5: SPY iv_30d > 0
        spy_iv = (d.get("spy") or {}).get("iv_30d")
        test["checks"].append({
            "name": "SPY IV 30d positive",
            "pass": spy_iv is not None and spy_iv > 0,
            "got": {"spy_iv_30d": spy_iv},
        })
    except Exception as e:
        import traceback
        test["fatal"] = str(e)
        test["trace"] = traceback.format_exc()[-500:]
    out["tests"]["implied_prob"] = test

    # ─────────────────────────────────────────────────────────────────────────
    # #5 TRADE JOURNAL — full lifecycle
    # ─────────────────────────────────────────────────────────────────────────
    print("\n══ TEST #5 TRADE JOURNAL ══")
    test = {"checks": []}
    try:
        admin_token = get_admin_token()

        # 5.1: GET (public)
        r, _ = http_invoke("justhodl-trade-journal", {
            "requestContext": {"http": {"method": "GET"}},
            "rawPath": "/", "headers": {"origin": "https://justhodl.ai"}, "body": "",
        })
        body = json.loads(r.get("body", "{}"))
        test["checks"].append({
            "name": "GET works (public)",
            "pass": r.get("statusCode") == 200 and body.get("ok") is True,
            "got": {"status": r.get("statusCode"), "n_total": body.get("stats", {}).get("n_total")},
        })

        # 5.2: POST add without auth → 401
        r, _ = http_invoke("justhodl-trade-journal", {
            "requestContext": {"http": {"method": "POST"}},
            "rawPath": "/add", "headers": {"origin": "https://justhodl.ai"},
            "body": json.dumps({"ticker": "AAPL"}),
        })
        body = json.loads(r.get("body", "{}"))
        test["checks"].append({
            "name": "POST /add without auth → 401",
            "pass": r.get("statusCode") == 401,
            "got": {"status": r.get("statusCode"), "err": body.get("err")},
        })

        # 5.3: POST add with bad price → 400
        r, _ = http_invoke("justhodl-trade-journal", {
            "requestContext": {"http": {"method": "POST"}},
            "rawPath": "/add",
            "headers": {"origin": "https://justhodl.ai", "x-justhodl-token": admin_token},
            "body": json.dumps({"ticker": "AAPL", "entry_price": -100, "size_usd": 1000, "entry_date": "2026-05-01"}),
        })
        body = json.loads(r.get("body", "{}"))
        test["checks"].append({
            "name": "POST add with negative price → 400",
            "pass": r.get("statusCode") == 400 and "positive" in (body.get("err") or "").lower(),
            "got": {"status": r.get("statusCode"), "err": body.get("err")},
        })

        # 5.4: POST add with valid trade → 200
        r, _ = http_invoke("justhodl-trade-journal", {
            "requestContext": {"http": {"method": "POST"}},
            "rawPath": "/add",
            "headers": {"origin": "https://justhodl.ai", "x-justhodl-token": admin_token},
            "body": json.dumps({
                "ticker": "TEST_VERIFY", "direction": "LONG",
                "entry_date": "2026-05-01", "entry_price": 100, "size_usd": 1000,
                "stop": 90, "target": 120, "signals_used": ["test_sig"],
                "thesis": "automated test trade — will be deleted",
            }),
        })
        body = json.loads(r.get("body", "{}"))
        added_trade = None
        if r.get("statusCode") == 200 and body.get("ok"):
            added_trade = next(iter(body.get("trades", {}).get("trades") or []), None)
        # ticker validator may reject TEST_VERIFY (>5 chars + underscore disallowed)
        test["checks"].append({
            "name": "POST add valid trade → 200 OR validates ticker",
            "pass": r.get("statusCode") in (200, 400),
            "got": {"status": r.get("statusCode"), "trade_id": (added_trade or {}).get("id"), "err": body.get("err")},
        })

        # 5.5: Try with proper ticker
        r, _ = http_invoke("justhodl-trade-journal", {
            "requestContext": {"http": {"method": "POST"}},
            "rawPath": "/add",
            "headers": {"origin": "https://justhodl.ai", "x-justhodl-token": admin_token},
            "body": json.dumps({
                "ticker": "ZZZTEST", "direction": "LONG",
                "entry_date": "2026-05-01", "entry_price": 100, "size_usd": 1000,
                "signals_used": ["test_sig"],
                "thesis": "automated test — will be deleted",
            }),
        })
        body = json.loads(r.get("body", "{}"))
        test_trade = None
        if r.get("statusCode") == 200 and body.get("ok"):
            test_trade = next(iter(body.get("trades", {}).get("trades") or []), None)
        test["checks"].append({
            "name": "POST add ZZZTEST trade → 200",
            "pass": r.get("statusCode") == 200 and test_trade is not None,
            "got": {"status": r.get("statusCode"), "trade_id": (test_trade or {}).get("id")},
        })

        # 5.6: Close the test trade
        if test_trade:
            r, _ = http_invoke("justhodl-trade-journal", {
                "requestContext": {"http": {"method": "POST"}},
                "rawPath": "/close",
                "headers": {"origin": "https://justhodl.ai", "x-justhodl-token": admin_token},
                "body": json.dumps({
                    "id": test_trade["id"],
                    "exit_price": 110,
                    "exit_reason": "MANUAL",
                    "exit_date": "2026-05-05",
                }),
            })
            body = json.loads(r.get("body", "{}"))
            closed_trade = next((t for t in body.get("trades", {}).get("trades", []) if t.get("id") == test_trade["id"]), None)
            test["checks"].append({
                "name": "POST close → 200 + outcome computed",
                "pass": (r.get("statusCode") == 200 and closed_trade
                         and closed_trade.get("status") == "CLOSED"
                         and closed_trade.get("outcome_pct") == 10.0),
                "got": {
                    "status": r.get("statusCode"),
                    "outcome_pct": (closed_trade or {}).get("outcome_pct"),
                    "outcome_dollars": (closed_trade or {}).get("outcome_dollars"),
                    "days_held": (closed_trade or {}).get("days_held"),
                },
            })

            # 5.7: Cleanup — delete the test trade
            r, _ = http_invoke("justhodl-trade-journal", {
                "requestContext": {"http": {"method": "POST"}},
                "rawPath": "/delete",
                "headers": {"origin": "https://justhodl.ai", "x-justhodl-token": admin_token},
                "body": json.dumps({"id": test_trade["id"]}),
            })
            body = json.loads(r.get("body", "{}"))
            test["checks"].append({
                "name": "POST delete (cleanup test trade)",
                "pass": r.get("statusCode") == 200,
                "got": {"status": r.get("statusCode")},
            })

    except Exception as e:
        import traceback
        test["fatal"] = str(e)
        test["trace"] = traceback.format_exc()[-500:]
    out["tests"]["trade_journal"] = test

    # Aggregate
    summary = {}
    for fname, t in out["tests"].items():
        checks = t.get("checks") or []
        passed = sum(1 for c in checks if c.get("pass"))
        summary[fname] = {
            "passed": passed, "total": len(checks),
            "pct": round(passed / max(1, len(checks)) * 100, 0),
            "fatal": "fatal" in t,
        }
    out["summary"] = summary

    os.makedirs(os.path.dirname(REPORT), exist_ok=True)
    with open(REPORT, "w") as f:
        json.dump(out, f, indent=2, default=str)
    print(json.dumps(out, indent=2, default=str)[:8000])


if __name__ == "__main__":
    main()
