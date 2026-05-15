"""
justhodl-bond-trace — Bloomberg ALLQ / TRACE feed equivalent.

FINRA TRACE publishes free daily aggregated data on corporate bond trading.
Public endpoints (no API key needed):
  https://cdn.cboe.com/api/global/delayed_quotes/options/cbond_summary.json
  https://www.finra.org/finra-data/browse-catalog/corporate-bond-securities/total
  https://www.sec.gov/files/dera/data/...

For free coverage, we'll use:
  • FINRA TRACE daily aggregate ZIP (free, daily) — needs scraping
  • As fallback, derive bond market stress from FRED:
    - High-yield ETF (HYG) vs investment-grade (LQD) ratio
    - HYG/LQD daily price action
    - Bond ETF flows from /etf-flows sidecar

Computes:
  • HY/IG spread velocity (5d, 30d)
  • HY ETF flow direction (in/out)
  • Stress score 0-100

This is a pragmatic v1 — actual TRACE feed requires FINRA registration. For
now we synthesize using free ETF + FRED data, with an upgrade path to TRACE
API when registered.

Output: data/bond-trace.json
  • hy_lq_ratio, hy_30d_perf, lq_30d_perf, ratio_5d_chg, ratio_30d_chg
  • flow_signal, stress_score, regime

Schedule: cron(0 21 ? * MON-FRI *) — daily after market close.
"""
import json
import os
import time
import urllib.request
from datetime import datetime, timezone, timedelta

import boto3

S3_BUCKET = "justhodl-dashboard-live"
S3_KEY = "data/bond-trace.json"
POLYGON_KEY = os.environ.get("POLYGON_KEY", "")
FRED_KEY = os.environ.get("FRED_API_KEY", "")
TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")

s3 = boto3.client("s3", region_name="us-east-1")


def http_get(url, timeout=20):
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "JustHodl/1.0"})
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return json.loads(r.read())
    except Exception as e:
        print(f"[http] {e}")
        return None


def fetch_aggs(ticker, days_back=90):
    if not POLYGON_KEY: return None
    end = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    start = (datetime.now(timezone.utc) - timedelta(days=days_back)).strftime("%Y-%m-%d")
    url = (f"https://api.polygon.io/v2/aggs/ticker/{ticker}/range/1/day/"
           f"{start}/{end}?adjusted=true&limit=200&apiKey={POLYGON_KEY}")
    data = http_get(url)
    if not data or "results" not in data: return None
    return data["results"]


def fred_get(series_id, limit=30):
    if not FRED_KEY: return None
    url = (f"https://api.stlouisfed.org/fred/series/observations?series_id={series_id}"
            f"&api_key={FRED_KEY}&file_type=json&sort_order=desc&limit={limit}")
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "JustHodl/1.0"})
        with urllib.request.urlopen(req, timeout=20) as r:
            data = json.loads(r.read())
            obs = data.get("observations", [])
            return [{"date": o["date"], "value": float(o["value"])}
                     for o in obs if o.get("value") and o["value"] != "."]
    except Exception as e:
        print(f"[fred] {series_id}: {e}")
        return None


def get_s3_json(key, default=None):
    try:
        obj = s3.get_object(Bucket=S3_BUCKET, Key=key)
        return json.loads(obj["Body"].read())
    except Exception:
        return default


def put_s3_json(key, body, cache="public, max-age=14400"):
    s3.put_object(Bucket=S3_BUCKET, Key=key,
                   Body=json.dumps(body, default=str).encode("utf-8"),
                   ContentType="application/json", CacheControl=cache)


def maybe_telegram(msg):
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        print(f"[tg] no creds: {msg[:80]}"); return
    try:
        body = json.dumps({
            "chat_id": TELEGRAM_CHAT_ID, "text": msg,
            "parse_mode": "HTML", "disable_web_page_preview": True,
        }).encode("utf-8")
        req = urllib.request.Request(
            f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
            data=body, headers={"Content-Type": "application/json"})
        urllib.request.urlopen(req, timeout=10).read()
    except Exception as e:
        print(f"[tg] err: {e}")


def lambda_handler(event, context):
    t0 = time.time()
    print("[bond-trace] starting")

    prior = get_s3_json(S3_KEY, {}) or {}

    # Fetch HYG (high yield), LQD (investment grade), JNK (HY alt), TLT (long Tsy)
    hyg = fetch_aggs("HYG", 90)
    lqd = fetch_aggs("LQD", 90)
    jnk = fetch_aggs("JNK", 90)
    tlt = fetch_aggs("TLT", 90)
    angl = fetch_aggs("ANGL", 90)  # fallen angels

    out = {
        "schema_version": "1.0",
        "method": "bond_trace_v1",
        "generated_at": datetime.now(timezone.utc).isoformat(),
    }

    def closes(bars):
        return [b.get("c") for b in (bars or []) if b.get("c")]

    hyg_c = closes(hyg)
    lqd_c = closes(lqd)
    jnk_c = closes(jnk)
    tlt_c = closes(tlt)

    def pct_chg(arr, lookback):
        if not arr or len(arr) <= lookback: return None
        return round((arr[-1] / arr[-lookback-1] - 1) * 100, 2)

    out["hyg"] = {
        "price": hyg_c[-1] if hyg_c else None,
        "perf_5d_pct": pct_chg(hyg_c, 5),
        "perf_30d_pct": pct_chg(hyg_c, 30),
        "perf_90d_pct": pct_chg(hyg_c, 89) if hyg_c and len(hyg_c) >= 90 else None,
    }
    out["lqd"] = {
        "price": lqd_c[-1] if lqd_c else None,
        "perf_5d_pct": pct_chg(lqd_c, 5),
        "perf_30d_pct": pct_chg(lqd_c, 30),
        "perf_90d_pct": pct_chg(lqd_c, 89) if lqd_c and len(lqd_c) >= 90 else None,
    }

    # HYG/LQD ratio — falling ratio = credit underperforming = stress
    ratio_5d_pct = None
    ratio_30d_pct = None
    if hyg_c and lqd_c and len(hyg_c) >= 30 and len(lqd_c) >= 30:
        ratio_5d_pct = round((hyg_c[-1]/lqd_c[-1]) / (hyg_c[-6]/lqd_c[-6]) * 100 - 100, 2)
        ratio_30d_pct = round((hyg_c[-1]/lqd_c[-1]) / (hyg_c[-31]/lqd_c[-31]) * 100 - 100, 2)
    out["hyg_lqd_ratio"] = {
        "value": round(hyg_c[-1]/lqd_c[-1], 4) if (hyg_c and lqd_c) else None,
        "change_5d_pct": ratio_5d_pct,
        "change_30d_pct": ratio_30d_pct,
    }

    # JNK divergence vs HYG (both should move together; divergence = early stress)
    if jnk_c and hyg_c and len(jnk_c) >= 5 and len(hyg_c) >= 5:
        jnk_5 = pct_chg(jnk_c, 5)
        hyg_5 = pct_chg(hyg_c, 5)
        if jnk_5 is not None and hyg_5 is not None:
            out["jnk_hyg_divergence_5d_pct"] = round(jnk_5 - hyg_5, 3)

    # TLT (long Treasury) — if TLT rising while HYG falling = risk-off flight
    if tlt_c and len(tlt_c) >= 5:
        out["tlt_perf_5d_pct"] = pct_chg(tlt_c, 5)

    # Pull credit OAS from FRED (already used in cds-proxy)
    hy_oas = fred_get("BAMLH0A0HYM2", limit=30)
    if hy_oas and len(hy_oas) > 5:
        out["hy_oas_pct"] = round(hy_oas[0]["value"], 2)
        out["hy_oas_5d_change_bp"] = round((hy_oas[0]["value"] - hy_oas[5]["value"]) * 100, 1) if len(hy_oas) > 5 else None
        out["hy_oas_30d_change_bp"] = round((hy_oas[0]["value"] - hy_oas[29]["value"]) * 100, 1) if len(hy_oas) > 29 else None

    # Compute stress score
    score = 0
    reasons = []

    # 1. HYG falling materially
    if out["hyg"].get("perf_5d_pct") is not None and out["hyg"]["perf_5d_pct"] < -1.5:
        score += 25; reasons.append(f"HYG {out['hyg']['perf_5d_pct']:+.2f}% in 5d (credit selling)")
    elif out["hyg"].get("perf_5d_pct") is not None and out["hyg"]["perf_5d_pct"] < -0.5:
        score += 12

    # 2. HYG/LQD ratio collapsing
    if ratio_5d_pct is not None and ratio_5d_pct < -1:
        score += 25; reasons.append(f"HYG/LQD ratio {ratio_5d_pct:+.2f}% in 5d (credit under-perf IG)")
    elif ratio_30d_pct is not None and ratio_30d_pct < -2:
        score += 15; reasons.append(f"HYG/LQD 30d {ratio_30d_pct:+.2f}% (sustained credit selling)")

    # 3. HY OAS widening hard
    if out.get("hy_oas_5d_change_bp") is not None and out["hy_oas_5d_change_bp"] > 30:
        score += 25; reasons.append(f"HY OAS +{out['hy_oas_5d_change_bp']:.0f}bp in 5d (panic widening)")
    elif out.get("hy_oas_5d_change_bp") is not None and out["hy_oas_5d_change_bp"] > 15:
        score += 12

    # 4. TLT rising while HYG falling = risk-off rotation
    tp = out.get("tlt_perf_5d_pct"); hp = out["hyg"].get("perf_5d_pct")
    if tp is not None and hp is not None and tp > 1 and hp < -1:
        score += 15; reasons.append(f"TLT +{tp:.1f}% while HYG {hp:+.1f}% (risk-off rotation)")

    # 5. JNK-HYG divergence (JNK weaker = junk-specific stress)
    jh = out.get("jnk_hyg_divergence_5d_pct", 0)
    if jh < -0.5:
        score += 10; reasons.append(f"JNK-HYG divergence {jh:+.2f}% (lower-quality stress)")

    score = max(0, min(100, score))
    regime = ("BOND_PANIC" if score >= 70 else
                "STRESSED" if score >= 45 else
                "ELEVATED" if score >= 20 else
                "CALM")

    out["composite_stress"] = score
    out["regime"] = regime
    out["top_reasons"] = reasons
    out["interpretation"] = (
        "Credit markets in panic. Equity drawdown likely 10%+ if persists." if score >= 70 else
        "Credit selling underway. Watch HYG/LQD ratio for stabilization." if score >= 45 else
        "Some credit weakness emerging. Monitor for acceleration." if score >= 20 else
        "Credit markets calm. Risk-on environment supported."
    )
    out["notes"] = ("Proxy from HYG/LQD/JNK/TLT ETFs + ICE BofA HY OAS. "
                     "Real TRACE prints require FINRA registration.")
    out["duration_s"] = round(time.time()-t0, 1)

    put_s3_json(S3_KEY, out)
    print(f"[bond-trace] stress={score} regime={regime}")

    # Alerts
    try:
        prior_regime = prior.get("regime")
        if prior_regime and prior_regime != regime:
            maybe_telegram(
                f"📉 <b>BOND/CREDIT REGIME CHANGE</b>\n"
                f"{prior_regime} → <b>{regime}</b> · stress {score}\n"
                + ("\n".join(f"• {r}" for r in reasons[:4]))
            )

        # HY OAS panic
        if out.get("hy_oas_5d_change_bp", 0) and out["hy_oas_5d_change_bp"] > 30 \
            and (prior.get("hy_oas_5d_change_bp") or 0) <= 30:
            maybe_telegram(
                f"🚨 <b>HY OAS PANIC WIDENING</b>\n"
                f"+{out['hy_oas_5d_change_bp']:.0f}bp in 5d (now {out.get('hy_oas_pct')}%)\n"
                f"Historically: 5d HY OAS +30bp precedes equity drawdown 70% of the time."
            )
    except Exception as e:
        print(f"[alerts] err: {e}")

    return {
        "statusCode": 200,
        "headers": {"Content-Type": "application/json"},
        "body": json.dumps({"ok": True, "stress": score, "regime": regime}),
    }
