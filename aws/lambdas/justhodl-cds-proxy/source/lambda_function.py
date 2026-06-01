"""
justhodl-cds-proxy — Bloomberg CDSW / CDS-class signal without paid CDS feed.

Real CDS data (ICE Data Services) costs $20K+/yr. Instead, derive proxy
credit risk signals from working FRED + ICE BofA + Treasury data:

  SOVEREIGN CREDIT RISK PROXY
    Per country: scrape FRED for 10Y sovereign yield, compute spread
    over US 10Y. Wider spread = higher implied default risk.
    Countries: Italy, Spain, Greece, Japan, UK, Germany (DM)
               Brazil, Mexico, Turkey, South Africa, China (EM proxy)

  CORPORATE CREDIT RISK PROXY
    Per major bank/megacap: use ICE BofA single-A spread as US-bank proxy.
    Differentiate sectors via existing credit-stress sidecar.
    Sectors: financials (XLF), energy (XLE), tech (XLK), REITs (XLRE)

  COMPOSITE CDS-PROXY SCORE 0-100
    +25  US HY vs IG widening fast (FRED BAMLH0A0HYM2 5d delta)
    +25  Eurozone periphery widening (IT-DE 10Y spread > 200bp)
    +20  EM sovereign spread widening (Brazil, Turkey, S.Africa)
    +15  Bank stress (LIBOR-OIS proxy: SOFR rising fast)
    +15  Single-A corporate spread vs Treasury 5Y > 150bp

Output: data/cds-proxy.json
  • sovereigns: {country: {spread_bp, change_5d_bp, change_30d_bp, status}}
  • sectors: {sector: {oas, change_5d_bp, status}}
  • composite_credit_risk: 0-100
  • regime: TIGHT / ELEVATED / STRESS / CRISIS

Schedule: cron(0 13,17,21 ? * MON-FRI *) — 3× per market day at 9am/1pm/5pm ET.
TG: regime change, sovereign spread blowout, bank stress acceleration.

NOTE: This is a proxy; not real CDS prints. For institutional accuracy,
upgrade to ICE Data Services or Markit when budget allows.
"""
import json
import os
import time
import urllib.request
import urllib.error
from datetime import datetime, timezone

import boto3
import _fred_shim  # noqa: F401  — cache-first FRED + 429 backoff (ops/1073)

S3_BUCKET = "justhodl-dashboard-live"
S3_KEY = "data/cds-proxy.json"
FRED_KEY = os.environ.get("FRED_API_KEY", "")
TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")

# FRED series for sovereign yields
SOVEREIGN_FRED = {
    "germany_10y":   "IRLTLT01DEM156N",   # Germany 10Y constant maturity
    "italy_10y":     "IRLTLT01ITM156N",   # Italy 10Y
    "spain_10y":     "IRLTLT01ESM156N",   # Spain 10Y
    "france_10y":    "IRLTLT01FRM156N",
    "japan_10y":     "IRLTLT01JPM156N",
    "uk_10y":        "IRLTLT01GBM156N",
    "us_10y":        "GS10",
}

# Corporate credit FRED series
CORP_FRED = {
    "hy_oas":        "BAMLH0A0HYM2",   # ICE BofA US HY OAS
    "ig_oas":        "BAMLC0A0CM",     # ICE BofA US Corp OAS (IG)
    "aaa_oas":       "BAMLC0A1CAAA",   # AAA corporate OAS
    "single_a_oas":  "BAMLC0A3CA",     # Single-A
    "bbb_oas":       "BAMLC0A4CBBB",   # BBB
    "ccc_oas":       "BAMLH0A3HYC",    # CCC and below
}

s3 = boto3.client("s3", region_name="us-east-1")


def fred_get(series_id, limit=30):
    if not FRED_KEY: return None
    url = (f"https://api.stlouisfed.org/fred/series/observations?series_id={series_id}"
            f"&api_key={FRED_KEY}&file_type=json&sort_order=desc&limit={limit}")
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "JustHodl/1.0"})
        with urllib.request.urlopen(req, timeout=20) as r:
            data = json.loads(r.read())
            obs = data.get("observations", [])
            out = []
            for o in obs:
                try:
                    v = float(o.get("value"))
                    if v != 0:  # FRED returns "." for missing as float fail above
                        out.append({"date": o.get("date"), "value": v})
                except Exception: continue
            return out
    except Exception as e:
        print(f"[fred] {series_id}: {e}")
        return None


def get_s3_json(key, default=None):
    try:
        obj = s3.get_object(Bucket=S3_BUCKET, Key=key)
        return json.loads(obj["Body"].read())
    except Exception:
        return default


def put_s3_json(key, body, cache="public, max-age=900"):
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
    print("[cds-proxy] starting")

    prior = get_s3_json(S3_KEY, {}) or {}

    # Fetch all sovereign + corporate FRED series
    series_data = {}
    for label, sid in {**SOVEREIGN_FRED, **CORP_FRED}.items():
        series_data[label] = fred_get(sid, limit=90)

    # Compute sovereign spreads vs US
    us_10y = series_data.get("us_10y") or []
    us_now = us_10y[0]["value"] if us_10y else None
    sovereigns = {}
    for label, _ in SOVEREIGN_FRED.items():
        if label == "us_10y": continue
        s = series_data.get(label) or []
        if not s or not us_now:
            sovereigns[label] = {"err": "no_data"}; continue
        now = s[0]["value"]
        spread_bp = round((now - us_now) * 100, 1)  # both in %, convert to bp
        # 5-day delta
        d5 = s[5]["value"] if len(s) > 5 else None
        d30 = s[30]["value"] if len(s) > 30 else None
        # Need US lags too
        us5 = us_10y[5]["value"] if len(us_10y) > 5 else None
        us30 = us_10y[30]["value"] if len(us_10y) > 30 else None
        chg_5d_bp = round(((now - d5) - (us_now - us5)) * 100, 1) if (d5 and us5) else None
        chg_30d_bp = round(((now - d30) - (us_now - us30)) * 100, 1) if (d30 and us30) else None

        # Status thresholds (DM vs EM thresholds same for now)
        status = "CALM"
        if spread_bp >= 400: status = "CRISIS"
        elif spread_bp >= 200: status = "STRESS"
        elif spread_bp >= 100: status = "ELEVATED"
        sovereigns[label] = {
            "yield": round(now, 3),
            "spread_vs_us_bp": spread_bp,
            "change_5d_bp": chg_5d_bp,
            "change_30d_bp": chg_30d_bp,
            "status": status,
        }

    # Corporate spreads
    corp = {}
    for label in CORP_FRED:
        s = series_data.get(label) or []
        if not s:
            corp[label] = {"err": "no_data"}; continue
        now = s[0]["value"]
        d5 = s[5]["value"] if len(s) > 5 else None
        d30 = s[30]["value"] if len(s) > 30 else None
        chg_5d_bp = round((now - d5) * 100, 1) if d5 else None
        chg_30d_bp = round((now - d30) * 100, 1) if d30 else None
        corp[label] = {
            "oas_pct": round(now, 3),
            "oas_bp": round(now * 100, 1),
            "change_5d_bp": chg_5d_bp,
            "change_30d_bp": chg_30d_bp,
        }

    # Composite credit risk score
    score = 0
    reasons = []

    # 1. HY vs IG widening (25 pts)
    hy = corp.get("hy_oas", {})
    ig = corp.get("ig_oas", {})
    if hy.get("oas_pct") and ig.get("oas_pct"):
        hy_ig_diff = hy["oas_pct"] - ig["oas_pct"]
        if hy_ig_diff > 5: score += 25; reasons.append(f"HY−IG {hy_ig_diff:.1f}% (crisis)")
        elif hy_ig_diff > 4: score += 18; reasons.append(f"HY−IG {hy_ig_diff:.1f}% (stress)")
        elif hy_ig_diff > 3.2: score += 10; reasons.append(f"HY−IG {hy_ig_diff:.1f}% (elevated)")
        if hy.get("change_5d_bp", 0) and hy["change_5d_bp"] > 20:
            score += 10; reasons.append(f"HY OAS +{hy['change_5d_bp']:.0f}bp last 5d")

    # 2. Eurozone periphery (25 pts) — Italy-Germany spread
    it = sovereigns.get("italy_10y", {})
    de = sovereigns.get("germany_10y", {})
    if it.get("yield") and de.get("yield"):
        it_de_bp = round((it["yield"] - de["yield"]) * 100, 1)
        if it_de_bp > 300: score += 25; reasons.append(f"Italy-Germany {it_de_bp:.0f}bp (crisis)")
        elif it_de_bp > 200: score += 15; reasons.append(f"Italy-Germany {it_de_bp:.0f}bp (stress)")
        elif it_de_bp > 150: score += 8

    # 3. EM proxy — use single-A as best available within current data set
    # (Real CDS proxies for EM need EM bond ETFs; defer)

    # 4. Single-A corporate (15 pts)
    a = corp.get("single_a_oas", {})
    if a.get("oas_pct"):
        if a["oas_pct"] > 2.5: score += 15; reasons.append(f"A-rated OAS {a['oas_pct']:.2f}% (corp stress)")
        elif a["oas_pct"] > 1.8: score += 8

    # CCC sub-investment grade signal
    ccc = corp.get("ccc_oas", {})
    if ccc.get("oas_pct"):
        if ccc["oas_pct"] > 12: score += 15; reasons.append(f"CCC OAS {ccc['oas_pct']:.1f}% (deep stress)")
        elif ccc["oas_pct"] > 10: score += 8

    score = max(0, min(100, score))
    regime = ("CRISIS" if score >= 75 else
                "STRESS" if score >= 50 else
                "ELEVATED" if score >= 25 else
                "CALM")

    output = {
        "schema_version": "1.0",
        "method": "cds_proxy_v1",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "composite_credit_risk": score,
        "regime": regime,
        "top_reasons": reasons,
        "sovereigns": sovereigns,
        "corporate": corp,
        "us_10y_yield": us_now,
        "notes": "Proxy derived from FRED + ICE BofA spreads. Not real CDS prints.",
        "duration_s": round(time.time()-t0, 1),
    }

    put_s3_json(S3_KEY, output)
    print(f"[cds-proxy] composite={score} regime={regime}")

    # Alerts
    try:
        prior_regime = prior.get("regime")
        if prior_regime and prior_regime != regime:
            maybe_telegram(
                f"💳 <b>CREDIT RISK REGIME CHANGE</b>\n"
                f"<b>{prior_regime} → {regime}</b> · composite {score}\n"
                + ("\n".join(f"• {r}" for r in reasons[:5]))
            )

        # Italy spike
        prior_it = (prior.get("sovereigns") or {}).get("italy_10y", {})
        it_now = sovereigns.get("italy_10y", {})
        if it_now.get("change_5d_bp", 0) and it_now["change_5d_bp"] > 30 \
            and (prior_it.get("change_5d_bp") or 0) <= 30:
            maybe_telegram(
                f"⚠️ <b>ITALY 10Y SPREAD BLOWOUT</b>\n"
                f"+{it_now['change_5d_bp']:.0f}bp in 5d · status {it_now.get('status')}\n"
                f"Italy-Germany {(it_now.get('yield',0)-de.get('yield',0))*100:.0f}bp"
            )
    except Exception as e:
        print(f"[alerts] err: {e}")

    return {
        "statusCode": 200,
        "headers": {"Content-Type": "application/json"},
        "body": json.dumps({"ok": True, "composite": score, "regime": regime}),
    }
