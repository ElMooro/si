"""
justhodl-labor-leading — JOLTS + Challenger labor leading indicators

Three labor data sources monitored:

  1. JOLTS (Job Openings and Labor Turnover Survey) — BLS, monthly
     - Job openings, hires, total separations, layoffs/discharges, quits
     - Quits rate is the cleanest "worker confidence" signal: workers
       only quit when they have something else lined up, so a falling
       quits rate signals weakening labor market
     - Series: JTSJOL (openings), JTSQUR (quits rate), JTSLDR (layoffs/discharges rate)

  2. Challenger Job Cut Report — Challenger, Gray & Christmas, monthly
     - Announced layoffs by sector
     - First public read each month, ~3 weeks before NFP
     - Free via FRED proxy (CHGTOT)

  3. Initial Claims — DOL, weekly
     - Real-time labor signal; rising 4-week MA = recession risk
     - FRED series: ICSA

All free via FRED API (already in your stack).

Output (data/labor-leading.json):
  {
    "generated_at": ...,
    "jolts": {
      "as_of": "2026-02",
      "openings": 7.5e6,
      "openings_per_unemployed": 1.2,
      "quits_rate": 2.1,
      "quits_rate_z6m": -0.8,
      "layoffs_rate": 1.0,
    },
    "challenger": {
      "as_of": "2026-03",
      "monthly_announcements": 95000,
      "yoy_change": 0.18,
    },
    "claims": {
      "latest_week": "2026-04-19",
      "initial": 215000,
      "ma4w": 217500,
      "ma4w_3mo_change": +5.2,
    },
    "regime": "tight" | "loosening" | "weakening" | "deteriorating",
    "interpretation": "<plain English>"
  }
"""
from __future__ import annotations
import json
import os
import time
import urllib.request
from datetime import datetime, timezone

import boto3

S3_BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
S3_KEY = os.environ.get("S3_KEY", "data/labor-leading.json")
FRED_KEY = os.environ.get("FRED_KEY", "2f057499936072679d8843d7fce99989")
USER_AGENT = os.environ.get("USER_AGENT", "JustHodl Research raafouis@gmail.com")

FRED_BASE = "https://api.stlouisfed.org/fred/series/observations"

# FRED series IDs
SERIES = {
    "openings":      "JTSJOL",     # Job openings (thousands)
    "quits_rate":    "JTSQUR",     # Quits rate (%)
    "layoffs_rate":  "JTSLDR",     # Layoffs/discharges rate (%)
    "unemployed":    "UNEMPLOY",   # Unemployed (thousands)
    "initial_claims": "ICSA",      # Initial claims (NSA, weekly)
    "claims_ma4w":   "IC4WSA",     # 4-week moving average
    "challenger":    "CHGTOT",     # Challenger announced job cuts (FRED indirect)
}


def _fetch(url: str, timeout: int = 20):
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.loads(r.read())


def fred_obs(series_id: str, limit: int = 24):
    url = (f"{FRED_BASE}?series_id={series_id}&api_key={FRED_KEY}&file_type=json"
           f"&sort_order=desc&limit={limit}")
    try:
        data = _fetch(url)
        obs = [o for o in data.get("observations", []) if o.get("value") not in (".", "", None)]
        return obs
    except Exception as e:
        return []


def latest(series_id: str) -> tuple:
    obs = fred_obs(series_id, limit=1)
    if not obs:
        return None, None
    try:
        return float(obs[0]["value"]), obs[0]["date"]
    except (ValueError, KeyError):
        return None, None


def z_recent(series_id: str, n: int = 6):
    """z-score of latest vs trailing-n window."""
    obs = fred_obs(series_id, limit=n)
    if len(obs) < 4:
        return None
    try:
        values = [float(o["value"]) for o in obs]
        mean = sum(values) / len(values)
        var = sum((x - mean) ** 2 for x in values) / len(values)
        sd = var ** 0.5
        if sd == 0:
            return 0.0
        return round((values[0] - mean) / sd, 2)
    except (ValueError, KeyError):
        return None


def lambda_handler(event, context):
    s3 = boto3.client("s3")
    started = time.time()

    # JOLTS
    openings_val, openings_date = latest(SERIES["openings"])
    unemp_val, _ = latest(SERIES["unemployed"])
    quits_val, _ = latest(SERIES["quits_rate"])
    layoffs_val, _ = latest(SERIES["layoffs_rate"])
    quits_z6m = z_recent(SERIES["quits_rate"], n=6)

    jolts = {
        "as_of": openings_date,
        "openings": int(openings_val * 1000) if openings_val else None,  # FRED reports thousands
        "openings_per_unemployed": round(openings_val * 1000 / (unemp_val * 1000), 2) if (openings_val and unemp_val) else None,
        "quits_rate": quits_val,
        "quits_rate_z6m": quits_z6m,
        "layoffs_rate": layoffs_val,
    }

    # Challenger
    challenger_val, challenger_date = latest(SERIES["challenger"])
    challenger_obs = fred_obs(SERIES["challenger"], limit=13)
    yoy = None
    if len(challenger_obs) >= 13:
        try:
            cur = float(challenger_obs[0]["value"])
            yr_ago = float(challenger_obs[12]["value"])
            yoy = round((cur / yr_ago) - 1, 3) if yr_ago > 0 else None
        except (ValueError, KeyError):
            pass

    challenger = {
        "as_of": challenger_date,
        "monthly_announcements": int(challenger_val) if challenger_val else None,
        "yoy_change": yoy,
    }

    # Initial claims
    claims_val, claims_date = latest(SERIES["initial_claims"])
    ma4w_val, _ = latest(SERIES["claims_ma4w"])
    ma_obs = fred_obs(SERIES["claims_ma4w"], limit=14)
    ma4w_3mo_chg = None
    if len(ma_obs) >= 13:
        try:
            cur = float(ma_obs[0]["value"])
            qtr_ago = float(ma_obs[12]["value"])
            ma4w_3mo_chg = round((cur / qtr_ago - 1) * 100, 1)
        except (ValueError, KeyError):
            pass

    claims = {
        "latest_week": claims_date,
        "initial": int(claims_val) if claims_val else None,
        "ma4w": int(ma4w_val) if ma4w_val else None,
        "ma4w_3mo_change_pct": ma4w_3mo_chg,
    }

    # Regime classification
    regime = "neutral"
    drivers = []
    if jolts["openings_per_unemployed"]:
        opu = jolts["openings_per_unemployed"]
        if opu > 1.5:
            drivers.append("openings per unemployed > 1.5 (very tight)")
            regime = "tight"
        elif opu < 1.0:
            drivers.append(f"openings per unemployed = {opu} (loosening)")
            regime = "loosening"
    if jolts.get("quits_rate_z6m") and jolts["quits_rate_z6m"] < -1.0:
        drivers.append("quits rate falling fast (worker confidence dropping)")
        if regime in ("neutral", "loosening"):
            regime = "weakening"
    if claims.get("ma4w_3mo_change_pct") and claims["ma4w_3mo_change_pct"] > 10:
        drivers.append(f"claims 4-week MA up {claims['ma4w_3mo_change_pct']}% in 3 months")
        regime = "deteriorating"
    if challenger.get("yoy_change") and challenger["yoy_change"] > 0.40:
        drivers.append(f"Challenger layoffs up {challenger['yoy_change']*100:.0f}% YoY")
        regime = "deteriorating"

    if drivers:
        interp = ". ".join(drivers).capitalize() + "."
    else:
        interp = "Labor market reading: neutral. No leading indicators flagging deterioration."

    output = {
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "jolts": jolts,
        "challenger": challenger,
        "claims": claims,
        "regime": regime,
        "interpretation": interp,
        "fetch_duration_s": round(time.time() - started, 1),
    }
    s3.put_object(Bucket=S3_BUCKET, Key=S3_KEY,
                  Body=json.dumps(output).encode(),
                  ContentType="application/json", CacheControl="no-cache")

    print(f"labor-leading: regime={regime} | claims={claims['initial']} | quits={jolts['quits_rate']}")
    return {
        "statusCode": 200,
        "headers": {"Content-Type": "application/json", "Access-Control-Allow-Origin": "*"},
        "body": json.dumps({"ok": True, "regime": regime}),
    }
