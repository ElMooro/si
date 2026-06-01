"""
justhodl-repo-lending — Leverage stress + securities lending intelligence.

Three signal pillars:

  1. NYSE/FINRA Margin Debt as % of market cap
     - Source: FRED BOGZ1FL663067003Q (Margin Accounts at Brokers/Dealers)
       quarterly, in millions USD.
     - Market cap proxy: SPY market cap × 5.0 (approx S&P 500 → US market)
       OR pull from FRED total US equity market cap (DDDM01USA156NWDB
       isn't ideal — use Wilshire 5000 if available, fallback SPY × ratio).
     - >2.5% = danger zone (2000, 2007 peaks). 1.5-2.5% = elevated.
       <1.5% = normal.

  2. Tri-Party Repo Collateral
     - Source: OFR's secured-funding-data (if reachable). Otherwise infer
       stress from current repo rate vs Fed Funds target spread.
     - Reads existing data/eurodollar-stress.json + FRED SOFR (SOFR) and
       RRP (RRPONTSYD).
     - Flight to quality signal: rising Treasury collateral share = risk-off.

  3. Securities Lending Utilization (proxy via short interest)
     - Uses existing data/short-interest.json (if present) or scrapes
       a sample of top-utilized names. Computes count of high-utilization
       (>80%) tickers — squeeze risk indicator.

Composite "Leverage Stress" score 0-100:
  margin_score (40%) + repo_score (30%) + utilization_score (30%)

Output: data/repo-lending.json
Schedule: cron(0 16 ? * MON-FRI *) — daily 16:00 UTC (4pm London close)
Memory: 256MB / 90s / arm64

Telegram alerts:
  - Margin debt / market cap crosses 2.5% (danger zone)
  - Repo stress regime flip
  - Securities lending utilization extreme (>30 high-util names = squeeze
    risk elevated)
"""
import io
import json
import os
import time
import urllib.request
import urllib.parse
from datetime import datetime, timezone

import boto3
import _fred_shim  # noqa: F401  — cache-first FRED + 429 backoff (ops/1073)

S3_BUCKET = "justhodl-dashboard-live"
S3_KEY_OUT = "data/repo-lending.json"

FRED_KEY = os.environ.get("FRED_API_KEY", "")
TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")

s3 = boto3.client("s3", region_name="us-east-1")


def get_s3_json(key, default=None):
    try:
        obj = s3.get_object(Bucket=S3_BUCKET, Key=key)
        return json.loads(obj["Body"].read())
    except Exception as e:
        print(f"[s3] {key}: {e}")
        return default


def put_s3_json(key, body, cache="public, max-age=900"):
    s3.put_object(
        Bucket=S3_BUCKET, Key=key,
        Body=json.dumps(body, default=str).encode("utf-8"),
        ContentType="application/json", CacheControl=cache,
    )


def fred_latest(series_id, lookback_obs=12):
    """Fetch latest observations from FRED series. Returns list of {date, value} or None."""
    if not FRED_KEY: return None
    url = (
        f"https://api.stlouisfed.org/fred/series/observations?series_id={series_id}"
        f"&api_key={FRED_KEY}&file_type=json&sort_order=desc&limit={lookback_obs}"
    )
    try:
        with urllib.request.urlopen(url, timeout=15) as r:
            data = json.loads(r.read())
        obs = data.get("observations", [])
        out = []
        for o in obs:
            v = o.get("value")
            try: v = float(v)
            except (TypeError, ValueError): continue
            out.append({"date": o.get("date"), "value": v})
        return out
    except Exception as e:
        print(f"[fred-err] {series_id}: {e}")
        return None


def maybe_telegram(msg):
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        print(f"[tg] no creds, would send: {msg[:80]}")
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


def estimate_market_cap_trillions():
    """Estimate US equity market cap. Try Wilshire 5000 (WILL5000PR) from FRED.
    Fallback: SPY × ratio."""
    # Try Wilshire 5000 Full Cap Price Index (close to total market cap)
    wil = fred_latest("WILL5000IND", lookback_obs=5)
    if wil:
        # WILL5000IND is an index level; the level is approximately equal to
        # total US equity market cap in billions during 2026 (~50000 ≈ $50T).
        latest_level = wil[0]["value"]
        return round(latest_level / 1000, 2)  # convert to trillions
    # Fallback: use SP500 total market cap proxy via FRED SP500
    sp = fred_latest("SP500", lookback_obs=3)
    if sp:
        # Heuristic: SPY level × 200 = approximate S&P market cap in $B
        # S&P 500 represents ~80% of US market cap, so US ≈ S&P / 0.80
        # This is rough; better data sources improve later.
        return round(sp[0]["value"] * 0.0095, 2)  # ~ $50T at SPY 5800
    return None


def compute_margin_score(margin_debt_b, market_cap_t):
    """Margin debt / market cap → stress score 0-100 (higher = more stress)."""
    if not margin_debt_b or not market_cap_t or market_cap_t <= 0:
        return None, None
    pct = (margin_debt_b / 1000) / market_cap_t * 100  # margin_b in $B, cap in $T
    # Stress mapping: <1.5% = 20 (low), 1.5-2.0% = 40, 2.0-2.5% = 70,
    # 2.5-3.0% = 90, >3.0% = 100
    if pct < 1.0: score = 10
    elif pct < 1.5: score = 25
    elif pct < 2.0: score = 45
    elif pct < 2.5: score = 70
    elif pct < 3.0: score = 90
    else: score = 100
    return score, round(pct, 3)


def compute_repo_score(eurodollar_stress, rrp_billions, sofr_iorb_spread):
    """Repo market stress from existing signals."""
    score = 0
    components = {}
    if eurodollar_stress is not None:
        # eurodollar_stress already 0-100
        score += eurodollar_stress * 0.6
        components["eurodollar_stress"] = eurodollar_stress
    if rrp_billions is not None:
        # RRP < 100B = ALL liquidity buffer exhausted (high stress)
        # RRP 100-300B = depleting
        # RRP > 300B = abundant
        if rrp_billions < 100: rrp_s = 95
        elif rrp_billions < 200: rrp_s = 75
        elif rrp_billions < 400: rrp_s = 45
        elif rrp_billions < 800: rrp_s = 20
        else: rrp_s = 5
        score += rrp_s * 0.25
        components["rrp_score"] = rrp_s
        components["rrp_billions"] = rrp_billions
    if sofr_iorb_spread is not None:
        # SOFR - IORB > 5bps = stress
        if sofr_iorb_spread > 0.10: sp = 95
        elif sofr_iorb_spread > 0.05: sp = 70
        elif sofr_iorb_spread > 0.02: sp = 40
        elif sofr_iorb_spread > -0.02: sp = 20
        else: sp = 5
        score += sp * 0.15
        components["sofr_iorb_score"] = sp
        components["sofr_iorb_spread_bps"] = round(sofr_iorb_spread * 100, 1)
    if not components: return None, components
    # Normalize by weights actually used
    total_w = 0
    if "eurodollar_stress" in components: total_w += 0.6
    if "rrp_score" in components: total_w += 0.25
    if "sofr_iorb_score" in components: total_w += 0.15
    return round(score / total_w, 1) if total_w > 0 else None, components


def compute_utilization_score(short_interest_data):
    """Securities lending utilization stress from short interest data."""
    if not short_interest_data:
        return None, None
    items = short_interest_data.get("items") or short_interest_data.get("data") or []
    if not isinstance(items, list): items = []
    high_util = []
    for it in items:
        if not isinstance(it, dict): continue
        ut = it.get("utilization") or it.get("util_pct") or it.get("short_pct_of_float")
        try: ut = float(ut) if ut is not None else None
        except (TypeError, ValueError): continue
        if ut is not None and ut >= 80:
            high_util.append({
                "ticker": it.get("ticker") or it.get("symbol"),
                "utilization": ut,
                "cost_to_borrow": it.get("cost_to_borrow") or it.get("ctb"),
            })

    n_high = len(high_util)
    if n_high == 0: score = 10
    elif n_high < 5: score = 25
    elif n_high < 10: score = 45
    elif n_high < 20: score = 65
    elif n_high < 40: score = 85
    else: score = 100
    return score, {"n_high_utilization": n_high, "top_high_util": high_util[:10]}


def lambda_handler(event, context):
    t0 = time.time()
    print("[repo-lending] starting")
    prior = get_s3_json(S3_KEY_OUT, {})

    # Margin debt (in $M, monthly, latest)
    margin_obs = fred_latest("BOGZ1FL663067003Q", lookback_obs=12)
    margin_latest_b = None
    if margin_obs:
        # FRED reports this in millions of dollars; convert to billions
        margin_latest_b = margin_obs[0]["value"] / 1000

    # Market cap (trillions)
    market_cap_t = estimate_market_cap_trillions()

    margin_score, margin_pct = compute_margin_score(margin_latest_b, market_cap_t)

    # YoY growth
    margin_yoy_pct = None
    if margin_obs and len(margin_obs) >= 5:  # quarterly → 4 quarters ago
        latest = margin_obs[0]["value"]
        year_ago = margin_obs[min(4, len(margin_obs)-1)]["value"]
        if year_ago > 0:
            margin_yoy_pct = round((latest - year_ago) / year_ago * 100, 2)

    # Repo stress inputs
    eurodollar = get_s3_json("data/eurodollar-stress.json", {})
    es_score = eurodollar.get("score") if eurodollar else None
    try: es_score = float(es_score) if es_score is not None else None
    except (TypeError, ValueError): es_score = None

    rrp_obs = fred_latest("RRPONTSYD", lookback_obs=3)
    rrp_b = rrp_obs[0]["value"] / 1000 if rrp_obs else None  # millions → billions

    sofr_obs = fred_latest("SOFR", lookback_obs=2)
    iorb_obs = fred_latest("IORB", lookback_obs=2)
    sofr_iorb_spread = None
    if sofr_obs and iorb_obs:
        sofr_iorb_spread = sofr_obs[0]["value"] - iorb_obs[0]["value"]

    repo_score, repo_components = compute_repo_score(es_score, rrp_b, sofr_iorb_spread)

    # Securities lending utilization
    short_data = get_s3_json("data/short-interest.json", {}) or get_s3_json("data/finra-short.json", {})
    util_score, util_components = compute_utilization_score(short_data)

    # Composite leverage stress
    weights = {"margin": 0.40, "repo": 0.30, "utilization": 0.30}
    composite = 0
    total_w = 0
    if margin_score is not None:
        composite += margin_score * weights["margin"]; total_w += weights["margin"]
    if repo_score is not None:
        composite += repo_score * weights["repo"]; total_w += weights["repo"]
    if util_score is not None:
        composite += util_score * weights["utilization"]; total_w += weights["utilization"]
    composite_final = round(composite / total_w, 1) if total_w > 0 else None

    # Regime classification
    if composite_final is None: regime = "INSUFFICIENT_DATA"
    elif composite_final >= 80: regime = "CRISIS_LEVERAGE"
    elif composite_final >= 65: regime = "ELEVATED_STRESS"
    elif composite_final >= 50: regime = "NORMAL_LATE_CYCLE"
    elif composite_final >= 30: regime = "NORMAL"
    else: regime = "ABUNDANT_LIQUIDITY"

    output = {
        "schema_version": "1.0",
        "method": "repo_lending_intelligence_v1",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "composite_leverage_stress": composite_final,
        "regime": regime,
        "margin_debt": {
            "score": margin_score,
            "level_billions": margin_latest_b,
            "level_date": margin_obs[0]["date"] if margin_obs else None,
            "market_cap_trillions": market_cap_t,
            "pct_of_market_cap": margin_pct,
            "yoy_growth_pct": margin_yoy_pct,
            "danger_zone": margin_pct is not None and margin_pct >= 2.5,
            "interpretation": (
                "Speculation extreme — 2000/2007-level leverage" if margin_pct and margin_pct >= 2.5
                else "Elevated leverage" if margin_pct and margin_pct >= 2.0
                else "Normal" if margin_pct and margin_pct >= 1.5
                else "Below average leverage" if margin_pct is not None
                else "No data"
            ),
        },
        "repo": {
            "score": repo_score,
            "components": repo_components,
        },
        "securities_lending": {
            "score": util_score,
            "components": util_components,
        },
        "duration_s": round(time.time() - t0, 2),
    }

    put_s3_json(S3_KEY_OUT, output)
    print(f"[repo-lending] composite={composite_final} regime={regime} "
          f"margin_pct={margin_pct}% repo={repo_score} util={util_score}")

    # Alerts
    try:
        prior_regime = prior.get("regime")
        prior_margin_pct = (prior.get("margin_debt") or {}).get("pct_of_market_cap")

        if prior_regime and prior_regime != regime and regime in ("CRISIS_LEVERAGE", "ELEVATED_STRESS"):
            maybe_telegram(
                f"⚠️ <b>LEVERAGE STRESS REGIME FLIP</b>\n"
                f"<b>{prior_regime} → {regime}</b>\n"
                f"Composite: {composite_final}/100\n"
                f"Margin: {margin_pct}% of cap (${margin_latest_b:.0f}B / ${market_cap_t}T)\n"
                f"Repo: {repo_score}/100  Sec-Lending: {util_score}/100"
            )

        if margin_pct is not None and margin_pct >= 2.5 and (prior_margin_pct or 0) < 2.5:
            maybe_telegram(
                f"🚨 <b>MARGIN DEBT CROSSED 2.5% OF MARKET CAP</b>\n"
                f"<b>{margin_pct}%</b> — 2000/2007-class leverage\n"
                f"Total margin: ${margin_latest_b:.0f}B\n"
                f"YoY: {margin_yoy_pct:+.1f}%\n"
                f"Historically precedes major drawdowns within 6-18 months."
            )

        # Sec lending squeeze risk
        n_high = (util_components or {}).get("n_high_utilization", 0)
        prior_n_high = ((prior.get("securities_lending") or {}).get("components") or {}).get("n_high_utilization", 0)
        if n_high >= 30 and prior_n_high < 30:
            maybe_telegram(
                f"📈 <b>SECURITIES LENDING UTILIZATION SPIKE</b>\n"
                f"<b>{n_high} names</b> with utilization ≥ 80%\n"
                f"Squeeze risk elevated. Watch for short-cover rallies."
            )
    except Exception as e:
        print(f"[alerts] err: {e}")

    return {
        "statusCode": 200,
        "headers": {"Content-Type": "application/json", "Access-Control-Allow-Origin": "*"},
        "body": json.dumps({
            "ok": True,
            "composite": composite_final,
            "regime": regime,
            "margin_pct_of_cap": margin_pct,
        }),
    }
