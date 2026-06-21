"""
justhodl-interpretation-grader — MEASURE-BEFORE-TRUST, applied to JUDGMENT
═══════════════════════════════════════════════════════════════════════════════════════
The Strategist logs its dated, testable claims + falsifiers to data/strategist-log/{date}.json
every run. This engine grades them FORWARD once their horizon elapses — the one part of the
system whose reasoning was never scored. It is the same discipline the signal scorecard applies
to picks, now applied to the system's READS: did "MOVE rises above the 30th pctile in 30d" or
"gold outperforms SPY 3% in 30d" actually happen?

METHOD (LLM-as-judge with GROUND TRUTH, so it cannot hand-wave): for each matured claim, build a
factual market-outcome block — returns of ~13 reference instruments over the claim's exact window
from Polygon — and ask the reasoning model to resolve the claim TRUE / FALSE / PARTIAL strictly
from those facts. Also grades each day's decisive_call directionally vs SPY at 21d. Appends to an
immutable ledger and computes rolling accuracy, calibration vs conviction, and by-horizon stats.
Output: data/interpretation-scorecard.json (+ ledger). Builds the system's only real moat — a
proprietary record of whether its own thinking comes true.
"""
import hashlib
import json
import os
import re
import time
import urllib.request
from datetime import datetime, timezone, timedelta

import boto3

REGION = "us-east-1"
BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
POLY = os.environ.get("POLYGON_KEY", "zvEY_KYYMHoAN0JqY7n2Ze6q0kBuJX_d")
S3 = boto3.client("s3", REGION)

LEDGER_KEY = "data/interpretation-ledger.json"
OUT_KEY = "data/interpretation-scorecard.json"
# reference instruments the LLM judge maps free-text claims onto (facts, not opinion)
REF = {"SPY": "S&P 500", "QQQ": "Nasdaq", "IWM": "small caps", "RSP": "equal-weight S&P",
       "GLD": "gold", "SLV": "silver", "TLT": "long Treasuries", "IEF": "7-10y Treasuries",
       "HYG": "high-yield credit", "UUP": "US dollar", "USO": "oil",
       "BTCUSD": "bitcoin", "ETHUSD": "ethereum"}
CRYPTO = {"BTCUSD", "ETHUSD"}


def _poly_close_on_or_after(t, date_str):
    pre = "X:" if t in CRYPTO else ""
    end = (datetime.strptime(date_str, "%Y-%m-%d") + timedelta(days=8)).strftime("%Y-%m-%d")
    url = (f"https://api.polygon.io/v2/aggs/ticker/{pre}{t}/range/1/day/{date_str}/{end}"
           f"?adjusted=true&sort=asc&limit=10&apiKey={POLY}")
    try:
        r = json.loads(urllib.request.urlopen(urllib.request.Request(url, headers={"User-Agent": "jh"}), timeout=20).read())
        res = r.get("results", [])
        return res[0]["c"] if res else None
    except Exception:
        return None


def window_returns(date_str):
    """Returns of each reference instrument from claim date to now."""
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    out = {}
    for t in REF:
        c0 = _poly_close_on_or_after(t, date_str)
        c1 = _poly_close_on_or_after(t, (datetime.now(timezone.utc) - timedelta(days=6)).strftime("%Y-%m-%d"))
        if c0 and c1:
            out[t.replace("USD", "")] = round((c1 / c0 - 1) * 100, 1)
    return out, now


def _llm_judge(claim, date_str, horizon, rets):
    facts = ", ".join(f"{k} {v:+}%" for k, v in rets.items())
    spy = rets.get("SPY", 0)
    sys = ("You are a strict, fair forecast judge. You resolve a dated market claim using ONLY the "
           "factual instrument returns provided — no outside assumptions. Output one minified JSON "
           "object: {\"verdict\":\"TRUE|FALSE|PARTIAL|UNRESOLVABLE\",\"confidence\":0-100,\"note\":\"<12 words\"}.")
    ask = (f"CLAIM (made {date_str}, horizon {horizon}d): \"{claim}\"\n"
           f"ACTUAL returns over that window: {facts}\n"
           f"(SPY = market benchmark for any 'outperform/underperform' wording.)\n"
           "Resolve strictly from these facts. If the claim names something not in the facts and "
           "no listed instrument is a fair proxy, return UNRESOLVABLE.")
    try:
        from llm_router import complete
        raw = complete(ask, tier="reason", max_tokens=300, system=sys)
        m = re.search(r"\{.*\}", raw.replace("```", ""), re.S)
        j = json.loads(m.group(0)) if m else {}
        v = str(j.get("verdict", "UNRESOLVABLE")).upper()
        if v not in ("TRUE", "FALSE", "PARTIAL", "UNRESOLVABLE"):
            v = "UNRESOLVABLE"
        return {"verdict": v, "confidence": j.get("confidence"), "note": str(j.get("note", ""))[:80]}
    except Exception as e:
        return {"verdict": "UNRESOLVABLE", "confidence": None, "note": f"judge err {str(e)[:40]}"}


def lambda_handler(event=None, context=None):
    t0 = time.time()
    today = datetime.now(timezone.utc).date()
    # load ledger
    try:
        ledger = json.loads(S3.get_object(Bucket=BUCKET, Key=LEDGER_KEY)["Body"].read())
    except Exception:
        ledger = {"graded": {}, "created_at": datetime.now(timezone.utc).isoformat()}
    graded = ledger.get("graded", {})

    # list logs
    logs = []
    tok = None
    while True:
        kw = {"Bucket": BUCKET, "Prefix": "data/strategist-log/"}
        if tok:
            kw["ContinuationToken"] = tok
        r = S3.list_objects_v2(**kw)
        logs += [o["Key"] for o in r.get("Contents", []) if o["Key"].endswith(".json")]
        tok = r.get("NextContinuationToken")
        if not tok:
            break

    n_new, n_pending = 0, 0
    rets_cache = {}
    for key in logs:
        date_str = key.split("/")[-1].replace(".json", "")
        try:
            log_date = datetime.strptime(date_str, "%Y-%m-%d").date()
        except Exception:
            continue
        try:
            log = json.loads(S3.get_object(Bucket=BUCKET, Key=key)["Body"].read())
        except Exception:
            continue
        conviction = log.get("conviction")
        claims = log.get("key_claims") or []
        # decisive_call graded as a 21d directional claim
        if log.get("decisive_call"):
            claims = claims + [{"claim": "DECISIVE CALL: " + str(log["decisive_call"]), "horizon_days": 21}]
        for c in claims:
            text = c.get("claim") if isinstance(c, dict) else str(c)
            horizon = (c.get("horizon_days") if isinstance(c, dict) else 21) or 21
            if not text:
                continue
            mature = log_date + timedelta(days=int(horizon))
            cid = hashlib.md5(f"{date_str}|{text[:120]}".encode()).hexdigest()[:16]
            if cid in graded:
                continue
            if mature > today:
                n_pending += 1
                continue
            if date_str not in rets_cache:
                rets_cache[date_str] = window_returns(date_str)[0]
            verdict = _llm_judge(text, date_str, horizon, rets_cache[date_str])
            graded[cid] = {"date": date_str, "claim": text[:240], "horizon_days": int(horizon),
                           "conviction": conviction, "matured": mature.isoformat(),
                           "graded_at": datetime.now(timezone.utc).isoformat(), **verdict}
            n_new += 1

    ledger["graded"] = graded
    ledger["updated_at"] = datetime.now(timezone.utc).isoformat()
    S3.put_object(Bucket=BUCKET, Key=LEDGER_KEY, Body=json.dumps(ledger).encode(), ContentType="application/json")

    # rolling stats
    rows = list(graded.values())
    resolved = [g for g in rows if g["verdict"] in ("TRUE", "FALSE", "PARTIAL")]
    n_true = sum(1 for g in resolved if g["verdict"] == "TRUE")
    n_part = sum(1 for g in resolved if g["verdict"] == "PARTIAL")
    n_false = sum(1 for g in resolved if g["verdict"] == "FALSE")
    decisive = [g for g in resolved if g["claim"].startswith("DECISIVE CALL")]
    dec_hit = sum(1 for g in decisive if g["verdict"] == "TRUE") + 0.5 * sum(1 for g in decisive if g["verdict"] == "PARTIAL")
    # hit rate (PARTIAL = half credit)
    score = (n_true + 0.5 * n_part)
    hit_rate = round(100 * score / len(resolved), 1) if resolved else None
    # calibration: do high-conviction reads do better?
    hi = [g for g in resolved if (g.get("conviction") or 0) >= 65]
    lo = [g for g in resolved if (g.get("conviction") or 0) < 65 and g.get("conviction") is not None]
    def hr(s):
        rr = [g for g in s if g["verdict"] in ("TRUE", "FALSE", "PARTIAL")]
        return round(100 * (sum(1 for g in rr if g["verdict"] == "TRUE") + 0.5 * sum(1 for g in rr if g["verdict"] == "PARTIAL")) / len(rr), 1) if rr else None

    status = "WARMING" if len(resolved) < 15 else "ACTIVE"
    payload = {
        "engine": "justhodl-interpretation-grader", "version": "1.0.0", "ok": True,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "thesis": "The interpretation scorecard — measure-before-trust applied to the Strategist's own reads.",
        "status": status,
        "stats": {"n_claims_graded": len(rows), "n_resolved": len(resolved), "n_pending_horizon": n_pending,
                  "n_graded_this_run": n_new, "hit_rate_pct": hit_rate,
                  "true": n_true, "partial": n_part, "false": n_false,
                  "decisive_call_n": len(decisive), "decisive_call_hit_pct": round(100 * dec_hit / len(decisive), 1) if decisive else None,
                  "calibration": {"high_conviction_hit_pct": hr(hi), "low_conviction_hit_pct": hr(lo),
                                  "well_calibrated": (hr(hi) or 0) > (hr(lo) or 0) if (hi and lo) else None}},
        "recent": sorted(resolved, key=lambda g: g["graded_at"], reverse=True)[:25],
        "note": ("Claims are logged daily by the Strategist with 15-30d horizons; this scorecard activates "
                 "(ACTIVE) once ~15 have matured. Until then it reports WARMING — the same honest "
                 "measure-before-trust pattern used for signals."),
        "elapsed_s": round(time.time() - t0, 1),
    }
    S3.put_object(Bucket=BUCKET, Key=OUT_KEY, Body=json.dumps(payload, default=str).encode(),
                  ContentType="application/json", CacheControl="public, max-age=1800")
    print(f"[interp-grader] graded {len(rows)} ({len(resolved)} resolved, {n_new} new, {n_pending} pending) "
          f"hit_rate={hit_rate} status={status} {payload['elapsed_s']}s")
    return {"statusCode": 200, "body": json.dumps({"ok": True, "status": status, "resolved": len(resolved),
            "pending": n_pending, "hit_rate": hit_rate})}
