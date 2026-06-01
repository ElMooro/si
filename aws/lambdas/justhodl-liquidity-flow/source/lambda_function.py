"""
justhodl-liquidity-flow — TGA + RRP + WALCL daily delta tracker

Three balance-sheet-of-the-Fed numbers that drive risk-asset performance
month-to-month more than people realize:

  WALCL  Fed Balance Sheet (Total Assets)            FRED: WALCL
         When this rises = QE-equivalent; when falls = QT-equivalent.

  RRPONTSYD  Overnight Reverse Repo Balance          FRED: RRPONTSYD
         Money market funds parking cash here. Falling RRP = money
         flowing back into markets (bullish risk). Rising = de-risking.

  WTREGEN  Treasury General Account (TGA)            FRED: WTREGEN
         Treasury's checking account. When Treasury pays bills →
         TGA falls → liquidity flows TO markets. When they refill
         (auctions, tax season) → TGA rises → liquidity drained.

Net liquidity formula (the critical aggregate):
  net_liquidity = WALCL - WTREGEN - RRPONTSYD

When net_liquidity rises → risk assets benefit.
When net_liquidity falls → risk assets struggle.

Output (data/liquidity-flow.json):
  {
    "generated_at": ...,
    "as_of": "2026-04-25",
    "current": {
      "fed_balance_sheet_b": 7320,
      "tga_b": 824,
      "rrp_b": 480,
      "net_liquidity_b": 6016,
    },
    "deltas": {
      "1d":  {"net":  -12, "tga": +5, "rrp": -8, "walcl": -15},
      "1w":  {"net":  -85, "tga": +40, "rrp": -25, "walcl": -50},
      "1m":  {"net": -180, "tga": +120, "rrp": -85, "walcl": -150},
      "3m":  {"net": -420, "tga": +250, "rrp": -180, "walcl": -350},
    },
    "regime": "draining" | "stable" | "expanding",
    "interpretation": "<plain English>",
    "history_180d": [
      {"date": "2026-04-25", "walcl": ..., "tga": ..., "rrp": ..., "net": ...},
      ...
    ]
  }

Schedule: rate(1 day) — FRED data updates daily for these series.
"""
from __future__ import annotations
import json
import os
import time
import urllib.request
from datetime import datetime, timezone

import boto3
import _fred_shim  # noqa: F401  — cache-first FRED + 429 backoff (ops/1073)

S3_BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
S3_KEY = os.environ.get("S3_KEY", "data/liquidity-flow.json")
FRED_KEY = os.environ.get("FRED_KEY", "2f057499936072679d8843d7fce99989")
USER_AGENT = os.environ.get("USER_AGENT", "JustHodl Research raafouis@gmail.com")

FRED_BASE = "https://api.stlouisfed.org/fred/series/observations"
SERIES = {
    "walcl": "WALCL",       # Fed Balance Sheet, weekly, units in millions
    "tga":   "WTREGEN",     # TGA, weekly, units in billions
    "rrp":   "RRPONTSYD",   # Overnight RRP, daily, units in billions
}


def _fetch_json(url, timeout=15):
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.loads(r.read())


def fred_obs(series_id, limit=400):
    """Returns list of {date, value} sorted ascending by date."""
    url = (f"{FRED_BASE}?series_id={series_id}&api_key={FRED_KEY}"
           f"&file_type=json&sort_order=desc&limit={limit}")
    data = _fetch_json(url)
    out = []
    for o in data.get("observations", []):
        v = o.get("value")
        if v in (".", "", None):
            continue
        try:
            out.append({"date": o["date"], "value": float(v)})
        except (ValueError, KeyError):
            continue
    out.sort(key=lambda x: x["date"])
    return out


def _value_at_or_before(obs_list, target_date_str):
    """Find the latest observation on or before target_date."""
    valid = [o for o in obs_list if o["date"] <= target_date_str]
    return valid[-1] if valid else None


def _delta(obs_list, days_ago):
    """Return delta from N days ago to latest. None if data insufficient."""
    if not obs_list or len(obs_list) < 2:
        return None
    latest = obs_list[-1]
    try:
        target_dt = datetime.fromisoformat(latest["date"]).date()
        from datetime import timedelta as td
        target_str = (target_dt - td(days=days_ago)).isoformat()
    except Exception:
        return None
    prior = _value_at_or_before(obs_list, target_str)
    if not prior:
        return None
    return latest["value"] - prior["value"]


def lambda_handler(event, context):
    s3 = boto3.client("s3")
    started = time.time()

    # Fetch each series
    data = {}
    fetch_errors = []
    for key, series_id in SERIES.items():
        try:
            data[key] = fred_obs(series_id)
        except Exception as e:
            fetch_errors.append(f"{key}: {type(e).__name__}")
            data[key] = []
        time.sleep(0.1)

    if not data["walcl"] or not data["tga"] or not data["rrp"]:
        return {"statusCode": 502,
                "body": json.dumps({"error": "Missing FRED data",
                                    "errors": fetch_errors})}

    # ─── Unit normalization ──────────────────────────────────────────
    # FRED publishes:
    #   WALCL         "Millions of Dollars"  (raw value ~6,500,000)
    #   WTREGEN       "Millions of Dollars"  (raw value ~800,000)
    #   RRPONTSYD     "Billions of Dollars"  (raw value ~500)
    # We normalize everything to BILLIONS for consistency.
    #
    # If the raw values look like they're in millions (any > 50,000),
    # we divide by 1000. This is safer than hardcoding the unit per
    # series since FRED occasionally changes units.

    def _normalize_to_billions(observations, label):
        if not observations:
            return observations
        # If the latest value is > 50,000 it must be in millions
        # (no realistic Fed/Treasury number is > $50T-as-billions)
        latest_val = observations[-1]["value"]
        if latest_val > 50_000:
            for o in observations:
                o["value"] = o["value"] / 1000
            print(f"  {label}: detected millions-units, normalized to billions")
        return observations

    data["walcl"] = _normalize_to_billions(data["walcl"], "WALCL")
    data["tga"]   = _normalize_to_billions(data["tga"], "WTREGEN")
    data["rrp"]   = _normalize_to_billions(data["rrp"], "RRPONTSYD")

    # Latest values
    walcl_latest = data["walcl"][-1]
    tga_latest = data["tga"][-1]
    rrp_latest = data["rrp"][-1]

    net_liq = walcl_latest["value"] - tga_latest["value"] - rrp_latest["value"]

    # Deltas at 1d, 1w, 1m, 3m
    deltas = {}
    for label, days in [("1d", 1), ("1w", 7), ("1m", 30), ("3m", 90)]:
        d_walcl = _delta(data["walcl"], days)
        d_tga = _delta(data["tga"], days)
        d_rrp = _delta(data["rrp"], days)
        if d_walcl is None or d_tga is None or d_rrp is None:
            deltas[label] = None
            continue
        deltas[label] = {
            "walcl": round(d_walcl, 1),
            "tga": round(d_tga, 1),
            "rrp": round(d_rrp, 1),
            "net": round(d_walcl - d_tga - d_rrp, 1),
        }

    # Regime classification — focus on 1m delta
    one_m = deltas.get("1m")
    if one_m is None:
        regime = "unknown"
    elif one_m["net"] > 75:
        regime = "expanding"
    elif one_m["net"] < -75:
        regime = "draining"
    else:
        regime = "stable"

    # Plain-English interpretation
    parts = []
    if regime == "draining":
        parts.append(f"Net liquidity has DRAINED ${abs(one_m['net']):.0f}B over the last month")
        if one_m["tga"] > 50:
            parts.append(f"primarily driven by TGA refill (+${one_m['tga']:.0f}B)")
        elif one_m["walcl"] < -50:
            parts.append(f"primarily driven by Fed balance-sheet runoff ({one_m['walcl']:+.0f}B)")
    elif regime == "expanding":
        parts.append(f"Net liquidity has EXPANDED ${one_m['net']:.0f}B over the last month")
        if one_m["tga"] < -50:
            parts.append(f"primarily driven by TGA spending (-${abs(one_m['tga']):.0f}B)")
        elif one_m["rrp"] < -50:
            parts.append(f"with money flowing OUT of RRP and INTO markets ({one_m['rrp']:+.0f}B)")
    else:
        parts.append(f"Net liquidity is roughly stable ({one_m['net']:+.0f}B over 30 days)" if one_m else "Net liquidity status unknown")

    if regime == "draining":
        parts.append("Risk-asset performance typically lags during drain periods. Defensive positioning and quality bias have historically outperformed.")
    elif regime == "expanding":
        parts.append("Risk-asset performance has historically benefited during expansion periods. Beta tends to outperform.")

    # Build 180-day history (joined across the 3 series)
    history = []
    walcl_by_date = {o["date"]: o["value"] for o in data["walcl"]}
    tga_by_date = {o["date"]: o["value"] for o in data["tga"]}
    rrp_by_date = {o["date"]: o["value"] for o in data["rrp"]}

    # Use the union of dates from all three, take last 180
    all_dates = sorted(set(walcl_by_date) | set(tga_by_date) | set(rrp_by_date))[-180:]
    for d in all_dates:
        # Carry-forward: use latest available <= d for each series
        wv = walcl_by_date.get(d) or _value_at_or_before(data["walcl"], d)
        tv = tga_by_date.get(d) or _value_at_or_before(data["tga"], d)
        rv = rrp_by_date.get(d) or _value_at_or_before(data["rrp"], d)
        if isinstance(wv, dict): wv = wv["value"]
        if isinstance(tv, dict): tv = tv["value"]
        if isinstance(rv, dict): rv = rv["value"]
        if wv is None or tv is None or rv is None:
            continue
        history.append({
            "date": d,
            "walcl": round(wv, 1),
            "tga": round(tv, 1),
            "rrp": round(rv, 1),
            "net": round(wv - tv - rv, 1),
        })

    output = {
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "as_of": walcl_latest["date"],
        "current": {
            "fed_balance_sheet_b": round(walcl_latest["value"], 1),
            "tga_b": round(tga_latest["value"], 1),
            "rrp_b": round(rrp_latest["value"], 1),
            "net_liquidity_b": round(net_liq, 1),
        },
        "deltas": deltas,
        "regime": regime,
        "interpretation": ". ".join(parts) + ".",
        "history_180d": history,
        "fetch_errors": fetch_errors,
        "fetch_duration_s": round(time.time() - started, 1),
    }

    s3.put_object(Bucket=S3_BUCKET, Key=S3_KEY,
                  Body=json.dumps(output).encode(),
                  ContentType="application/json", CacheControl="no-cache")

    print(f"liquidity-flow: regime={regime} net=${output['current']['net_liquidity_b']}B "
          f"30d_delta={one_m['net'] if one_m else '?':.0f}B")
    return {
        "statusCode": 200,
        "headers": {"Content-Type": "application/json", "Access-Control-Allow-Origin": "*"},
        "body": json.dumps({"ok": True,
                            "regime": regime,
                            "net_liquidity_b": output["current"]["net_liquidity_b"]}),
    }
