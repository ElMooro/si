"""
justhodl-margin-lending — NYSE margin debt + tri-party repo collateral + securities lending.

Complements existing justhodl-repo-monitor (which covers SOFR/RRP/funding rates)
by adding the user-portfolio-relevant leverage and squeeze indicators:

  1. NYSE MARGIN DEBT
       FRED: BOGZ1FL663067003Q (US Brokers + Dealers margin loans, quarterly)
       Convert to absolute level + as % of S&P 500 market cap
       Danger zone: > 2.5% = 2000-tech / 2007-housing top conditions

  2. TRI-PARTY REPO COLLATERAL MIX (proxy via FRED for clean tri-party)
       FRED RPONTSYAWARD = on-the-run Treasury repo award
       FRED RRPONTSYD = ON RRP take by counterparties
       Compute weekly direction of Treasury collateral demand

  3. CONSUMER CREDIT MOMENTUM
       FRED TOTALSL — total revolving + non-revolving consumer credit
       Acceleration → margin debt parallel for retail leverage

  4. SQUEEZE RISK SCORE 0-100
       +30 margin debt as % of cap > 2.5%
       +25 margin debt 6mo growth > 25%
       +20 SOFR-EFFR spread > 10bps (funding stress)
       +15 consumer credit YoY > 8%
       +10 RRP take falling fast (drainage)

Outputs:
  data/margin-lending.json
    - margin_debt: {absolute_usd, as_pct_of_sp500_cap, 6mo_growth_pct, status}
    - repo_collateral_proxy: {sofr_volume, rrp_take, direction}
    - consumer_credit: {total_outstanding, yoy_pct, momentum}
    - squeeze_risk_score: 0-100
    - interpretation: narrative

Schedule: cron(0 14 ? * MON-FRI *) — daily 14:00 UTC (after FRED morning refresh)

Telegram alerts:
  - Margin debt % of cap crosses 2.5% from below
  - Squeeze risk score >= 65 (was below 50 last run)
"""
import json
import os
import time
import urllib.request
from datetime import datetime, timezone, timedelta

import boto3

S3_BUCKET = "justhodl-dashboard-live"
S3_KEY_OUT = "data/margin-lending.json"

FRED_API_KEY = os.environ.get("FRED_API_KEY", "2f057499936072679d8843d7fce99989")
TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")

# FRED series we'll pull
FRED_SERIES = {
    "margin_debt":          "BOGZ1FL663067003Q",   # quarterly, $B
    "margin_credit":        "BOGZ1FL663068005Q",   # cash credit balances
    "consumer_credit":      "TOTALSL",             # monthly, $B
    "revolving_credit":     "REVOLSL",
    "sofr_volume":          "SOFRVOLUME",          # daily, $
    "rrp_award":            "RRPONTSYD",           # daily, $B
    "wilshire_5000":        "WILL5000PR",          # market cap proxy
    "sp500_close":          "SP500",
    "tga":                  "WTREGEN",             # treasury balance
    "secfin_loans":         "RIFSPBLP",            # bank securities financing loans
}

s3 = boto3.client("s3", region_name="us-east-1")


def http_get_json(url, timeout=20):
    req = urllib.request.Request(url, headers={"User-Agent": "JustHodl/1.0"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.loads(r.read().decode("utf-8"))


def get_fred_series(series_id, limit=80):
    url = (f"https://api.stlouisfed.org/fred/series/observations?"
           f"series_id={series_id}&api_key={FRED_API_KEY}&file_type=json"
           f"&limit={limit}&sort_order=desc")
    try:
        d = http_get_json(url)
        obs = d.get("observations", [])
        cleaned = []
        for o in obs:
            try:
                val = float(o["value"])
                cleaned.append({"date": o["date"], "value": val})
            except (ValueError, KeyError):
                continue
        return cleaned
    except Exception as e:
        print(f"[fred] {series_id}: {e}")
        return []


def get_s3_json(key, default=None):
    try:
        obj = s3.get_object(Bucket=S3_BUCKET, Key=key)
        return json.loads(obj["Body"].read())
    except Exception: return default


def put_s3_json(key, body):
    s3.put_object(Bucket=S3_BUCKET, Key=key,
                   Body=json.dumps(body, default=str).encode("utf-8"),
                   ContentType="application/json",
                   CacheControl="public, max-age=900")


def compute_margin_debt_pct_of_cap(margin_debt_b, wilshire_value):
    """Wilshire 5000 is ~3.5x S&P 500 in market cap, expressed in price-index pts.
    We want % of total US equity market cap.
    Approximation: total US market cap ≈ wilshire_value × $1B/pt (rough)."""
    if not margin_debt_b or not wilshire_value: return None
    # Wilshire 5000 in price-return points ≈ market cap in trillions × 100
    # As of 2024-2025: Wilshire ~45000 = ~$45T total market cap
    estimated_mkt_cap_t = wilshire_value / 1000  # rough conversion to $T
    margin_t = margin_debt_b / 1000  # $B → $T
    if estimated_mkt_cap_t <= 0: return None
    return round(100 * margin_t / estimated_mkt_cap_t, 3)


def compute_growth(history, periods_back):
    if not history or len(history) < periods_back + 1: return None
    latest = history[0].get("value")
    earlier = history[periods_back].get("value")
    if latest is None or earlier is None or earlier == 0: return None
    return round(100 * (latest - earlier) / earlier, 2)


def maybe_telegram(msg):
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        print(f"[tg] no creds: {msg[:80]}")
        return
    try:
        body = json.dumps({
            "chat_id": TELEGRAM_CHAT_ID, "text": msg,
            "parse_mode": "HTML", "disable_web_page_preview": True,
        }).encode("utf-8")
        req = urllib.request.Request(
            f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
            data=body, headers={"Content-Type": "application/json"})
        urllib.request.urlopen(req, timeout=10).read()
        print(f"[tg] sent: {msg[:80]}")
    except Exception as e:
        print(f"[tg] err: {e}")


def lambda_handler(event, context):
    t0 = time.time()
    print("[margin-lending] starting")

    # Fetch all series
    series_data = {}
    for name, sid in FRED_SERIES.items():
        series_data[name] = get_fred_series(sid, limit=40)

    # ─── 1. Margin Debt ──────────────────────────────────────────────
    md_hist = series_data["margin_debt"]
    md_now = md_hist[0]["value"] if md_hist else None
    md_date = md_hist[0]["date"] if md_hist else None
    md_yoy = compute_growth(md_hist, 4)  # quarterly, 4 = 1 year
    md_2q = compute_growth(md_hist, 2)   # 6 months

    wilshire = series_data["wilshire_5000"]
    wilshire_now = wilshire[0]["value"] if wilshire else None

    md_pct_of_cap = compute_margin_debt_pct_of_cap(md_now, wilshire_now)

    if md_pct_of_cap is None:
        md_status = "DATA_MISSING"
    elif md_pct_of_cap >= 2.5:
        md_status = "DANGER"
    elif md_pct_of_cap >= 2.0:
        md_status = "ELEVATED"
    elif md_pct_of_cap >= 1.5:
        md_status = "NORMAL"
    else:
        md_status = "LOW"

    # ─── 2. Repo Collateral Proxy ────────────────────────────────────
    rrp_hist = series_data["rrp_award"]
    rrp_now = rrp_hist[0]["value"] if rrp_hist else None
    rrp_5d_avg = (sum(o["value"] for o in rrp_hist[:5]) / 5) if len(rrp_hist) >= 5 else None
    rrp_30d_avg = (sum(o["value"] for o in rrp_hist[:30]) / 30) if len(rrp_hist) >= 30 else None
    rrp_direction = None
    if rrp_5d_avg is not None and rrp_30d_avg is not None and rrp_30d_avg > 0:
        chg_pct = 100 * (rrp_5d_avg - rrp_30d_avg) / rrp_30d_avg
        if chg_pct > 15: rrp_direction = "INCREASING_TAKE"
        elif chg_pct < -15: rrp_direction = "DRAINAGE"
        else: rrp_direction = "STABLE"

    sofr_vol = series_data["sofr_volume"]
    sofr_vol_now = sofr_vol[0]["value"] / 1e9 if sofr_vol else None  # $B

    secfin = series_data["secfin_loans"]
    secfin_now = secfin[0]["value"] if secfin else None
    secfin_yoy = compute_growth(secfin, 52) if secfin else None  # weekly

    # ─── 3. Consumer Credit ──────────────────────────────────────────
    cc = series_data["consumer_credit"]
    cc_now = cc[0]["value"] if cc else None
    cc_yoy = compute_growth(cc, 12)
    rev = series_data["revolving_credit"]
    rev_now = rev[0]["value"] if rev else None
    rev_yoy = compute_growth(rev, 12)

    cc_momentum = None
    if cc_yoy is not None:
        if cc_yoy > 8: cc_momentum = "HOT"
        elif cc_yoy > 5: cc_momentum = "WARM"
        elif cc_yoy > 2: cc_momentum = "NORMAL"
        elif cc_yoy > -2: cc_momentum = "COOLING"
        else: cc_momentum = "CONTRACTING"

    # ─── 4. Squeeze Risk Composite ───────────────────────────────────
    squeeze_score = 0
    squeeze_reasons = []

    if md_pct_of_cap is not None and md_pct_of_cap > 2.5:
        squeeze_score += 30
        squeeze_reasons.append(f"Margin debt {md_pct_of_cap}% of cap (>2.5% = 2000/2007 zone)")
    elif md_pct_of_cap is not None and md_pct_of_cap > 2.0:
        squeeze_score += 15
        squeeze_reasons.append(f"Margin debt {md_pct_of_cap}% of cap (elevated)")

    if md_2q is not None and md_2q > 25:
        squeeze_score += 25
        squeeze_reasons.append(f"Margin debt up {md_2q:+.1f}% in 6mo (frothy)")
    elif md_2q is not None and md_2q > 15:
        squeeze_score += 10
        squeeze_reasons.append(f"Margin debt up {md_2q:+.1f}% in 6mo")

    if cc_yoy is not None and cc_yoy > 8:
        squeeze_score += 15
        squeeze_reasons.append(f"Consumer credit YoY {cc_yoy:+.1f}% (high)")

    if rrp_direction == "DRAINAGE":
        squeeze_score += 10
        squeeze_reasons.append("RRP drainage — bank reserves under pressure")

    if secfin_yoy is not None and secfin_yoy > 20:
        squeeze_score += 20
        squeeze_reasons.append(f"Sec-fin loans up {secfin_yoy:+.1f}% YoY (leverage expansion)")

    squeeze_score = min(100, squeeze_score)
    if squeeze_score >= 65: squeeze_band = "HIGH"
    elif squeeze_score >= 35: squeeze_band = "ELEVATED"
    elif squeeze_score >= 15: squeeze_band = "NORMAL"
    else: squeeze_band = "LOW"

    # Interpretation
    if squeeze_band == "HIGH":
        interp = ("Late-cycle leverage build-up. Margin debt elevated, consumer credit "
                  "hot, funding markets tight. Position for volatility spike + deleveraging risk.")
    elif squeeze_band == "ELEVATED":
        interp = ("Leverage measures running above neutral. Watch trajectory — sustained "
                  "elevation typical of late-cycle. Maintain hedges.")
    elif squeeze_band == "NORMAL":
        interp = ("Leverage indicators in normal historical range. No squeeze setup. "
                  "Risk-on environment for measured positioning.")
    else:
        interp = ("Leverage deeply suppressed. Often follows deleveraging events. "
                  "Bottoming conditions if other oversold indicators align.")

    output = {
        "schema_version": "1.0",
        "method": "margin_lending_v1",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "margin_debt": {
            "absolute_usd_b": md_now,
            "as_pct_of_market_cap": md_pct_of_cap,
            "yoy_pct": md_yoy,
            "growth_6mo_pct": md_2q,
            "latest_date": md_date,
            "status": md_status,
            "interpretation": (
                f"Margin debt at ${md_now:.0f}B = {md_pct_of_cap}% of est. US market cap. "
                f"{'⚠️ Danger zone (>2.5%).' if md_pct_of_cap and md_pct_of_cap >= 2.5 else 'Normal range.' if md_pct_of_cap else ''}"
                if md_now else "Margin debt data missing"
            ),
        },
        "repo_collateral_proxy": {
            "rrp_award_b": rrp_now,
            "rrp_5d_avg_b": round(rrp_5d_avg, 2) if rrp_5d_avg else None,
            "rrp_30d_avg_b": round(rrp_30d_avg, 2) if rrp_30d_avg else None,
            "rrp_direction": rrp_direction,
            "sofr_volume_b": round(sofr_vol_now, 1) if sofr_vol_now else None,
            "secfin_loans": secfin_now,
            "secfin_yoy_pct": secfin_yoy,
            "interpretation": (
                f"RRP {rrp_direction or 'unknown'}. "
                f"SOFR ${sofr_vol_now:.0f}B daily. "
                f"Sec-fin loans {secfin_yoy:+.1f}% YoY." if secfin_yoy else ""
            ),
        },
        "consumer_credit": {
            "total_outstanding_b": cc_now,
            "yoy_pct": cc_yoy,
            "revolving_outstanding_b": rev_now,
            "revolving_yoy_pct": rev_yoy,
            "momentum": cc_momentum,
            "interpretation": (
                f"Total ${cc_now:.0f}B (YoY {cc_yoy:+.1f}%). "
                f"Revolving ${rev_now:.0f}B (YoY {rev_yoy:+.1f}%). "
                f"Momentum: {cc_momentum}."
                if cc_now else "Consumer credit data missing"
            ),
        },
        "squeeze_risk": {
            "score": squeeze_score,
            "band": squeeze_band,
            "reasons": squeeze_reasons,
            "interpretation": interp,
        },
        "duration_s": round(time.time() - t0, 2),
    }

    prior_run = get_s3_json(S3_KEY_OUT, {}) or {}
    put_s3_json(S3_KEY_OUT, output)

    print(f"[margin-lending] md={md_pct_of_cap}% squeeze={squeeze_score}({squeeze_band})")

    # Alerts
    try:
        prior_md_pct = (prior_run.get("margin_debt") or {}).get("as_pct_of_market_cap")
        prior_squeeze = (prior_run.get("squeeze_risk") or {}).get("score", 0)

        if md_pct_of_cap is not None and md_pct_of_cap >= 2.5 and \
           (prior_md_pct is None or prior_md_pct < 2.5):
            maybe_telegram(
                f"🚨 <b>MARGIN DEBT DANGER ZONE</b>\n"
                f"Margin debt now <b>{md_pct_of_cap}%</b> of estimated market cap.\n"
                f"<i>Historical 2000-tech top at 2.7%; 2007-housing at 2.9%.</i>\n"
                f"6mo growth: {md_2q:+.1f}%   YoY: {md_yoy:+.1f}%"
            )

        if squeeze_score >= 65 and prior_squeeze < 50:
            maybe_telegram(
                f"⚠️ <b>SQUEEZE RISK ELEVATED: {squeeze_score}/100</b>\n"
                f"<i>was: {prior_squeeze}/100</i>\n"
                f"<b>{squeeze_band}</b> — {interp[:200]}\n\n"
                f"Reasons:\n" + "\n".join(f"• {r}" for r in squeeze_reasons[:4])
            )
    except Exception as e:
        print(f"[alerts] err: {e}")

    return {
        "statusCode": 200,
        "headers": {"Content-Type": "application/json", "Access-Control-Allow-Origin": "*"},
        "body": json.dumps({
            "ok": True,
            "margin_debt_pct_of_cap": md_pct_of_cap,
            "margin_status": md_status,
            "squeeze_score": squeeze_score,
            "squeeze_band": squeeze_band,
        }),
    }
