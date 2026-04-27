"""
justhodl-vix-curve — VIX term structure (VX1 - VX9 futures curve)

The shape of the VIX futures curve carries information distinct from
spot VIX:

  Steep contango (front month much lower than back months):
    - Markets calm now, but pricing risk further out
    - The "vol selling" trade structurally pays roll
    - Common in stable bull markets

  Mild contango (small upward slope):
    - Normal regime; vol pricing is balanced
    - This is the typical state ~60% of trading days

  Flat curve:
    - Transition zone; uncertainty about which regime is next
    - Often precedes a regime shift

  Backwardation (front month above back months):
    - Markets pricing IMMEDIATE stress
    - Vol sellers are getting destroyed
    - Historically marks ~80% of equity drawdowns
    - When backwardation reverts to contango = often the bottom

Methodology
===========
CBOE publishes daily VX futures settlement data at:
  https://www.cboe.com/us/futures/market_statistics/historical_data/

The historical data CSV requires constructing per-contract URLs by
expiry month. To avoid the daily-CSV scraping fragility, we use:

  - Yahoo Finance ^VIX9D, ^VIX, ^VIX3M, ^VIX6M
    These are CBOE's VIX-style indices at different tenors. They
    approximate VX futures of similar maturity and are available
    via Yahoo's chart API for free, no key.

  - VVIX as a sanity check (vol-of-vol)

Computed metrics:
  - 9-day vs 30-day spread (term-structure shape at very front)
  - 30-day vs 3-month spread (front to belly)
  - 3-month vs 6-month spread (belly to back)
  - Composite contango score (-100 to +100)
  - Regime classification

Output (data/vix-curve.json):
  {
    "generated_at": ...,
    "as_of": "2026-04-25",
    "vix_9d": 14.2,
    "vix_30d": 16.1,    # spot VIX
    "vix_3m": 17.8,
    "vix_6m": 19.2,
    "vvix": 92,
    "contango_9_30": 0.134,    # (30d/9d) - 1
    "contango_30_3m": 0.106,
    "contango_3m_6m": 0.078,
    "term_structure_slope": 0.106,   # avg
    "regime": "steep_contango" | "mild_contango" | "flat" | "backwardation",
    "interpretation": "<plain English>",
  }

Schedule: rate(1 hour) market hours weekdays. Outside market hours
the data is stale anyway, so we throttle to once per 4h.

For simplicity we use rate(4 hours) which captures all market open/close
movements + maintains weekend visibility.
"""
from __future__ import annotations
import json
import os
import time
import urllib.request
from datetime import datetime, timezone

import boto3

S3_BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
S3_KEY = os.environ.get("S3_KEY", "data/vix-curve.json")
USER_AGENT = os.environ.get("USER_AGENT", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36")

YAHOO_BASE = "https://query1.finance.yahoo.com/v8/finance/chart"

# Symbols on Yahoo for term-structure approximation
SYMBOLS = {
    "vix_9d":  "^VIX9D",   # 9-day VIX
    "vix_30d": "^VIX",     # standard 30-day VIX
    "vix_3m":  "^VIX3M",   # 3-month VIX
    "vix_6m":  "^VIX6M",   # 6-month VIX
    "vvix":    "^VVIX",    # vol-of-vol
}


def fetch_quote(symbol: str):
    """Get latest close from Yahoo chart API."""
    url = f"{YAHOO_BASE}/{symbol}?interval=1d&range=2d"
    try:
        req = urllib.request.Request(url, headers={
            "User-Agent": USER_AGENT,
            "Accept": "application/json",
        })
        with urllib.request.urlopen(req, timeout=12) as r:
            data = json.loads(r.read())
        result = data["chart"]["result"][0]
        closes = result["indicators"]["quote"][0]["close"]
        # latest non-null close
        for c in reversed(closes):
            if c is not None:
                return float(c)
        return None
    except Exception as e:
        print(f"  yahoo fetch fail {symbol}: {e}")
        return None


def classify_regime(vix_9d, vix_30d, vix_3m, vix_6m):
    """Classify the overall term structure shape."""
    if not (vix_9d and vix_30d and vix_3m and vix_6m):
        return "unknown", None

    # All slopes (positive = contango, negative = backwardation)
    slope_9_30 = (vix_30d / vix_9d) - 1
    slope_30_3m = (vix_3m / vix_30d) - 1
    slope_3m_6m = (vix_6m / vix_3m) - 1
    avg_slope = (slope_9_30 + slope_30_3m + slope_3m_6m) / 3

    # Backwardation if any front slope is significantly negative
    if slope_9_30 < -0.03 or slope_30_3m < -0.02:
        regime = "backwardation"
    elif avg_slope > 0.07:
        regime = "steep_contango"
    elif avg_slope > 0.02:
        regime = "mild_contango"
    elif abs(avg_slope) <= 0.02:
        regime = "flat"
    else:
        regime = "mild_contango"

    return regime, {
        "slope_9_30": round(slope_9_30, 4),
        "slope_30_3m": round(slope_30_3m, 4),
        "slope_3m_6m": round(slope_3m_6m, 4),
        "avg_slope": round(avg_slope, 4),
    }


def interpret(regime: str, slopes: dict, vix_30d: float, vvix: float) -> str:
    if regime == "backwardation":
        text = (f"VIX in backwardation (front month {slopes['slope_9_30']*100:+.1f}% vs 30d). "
                f"Markets pricing immediate stress. "
                f"Historically ~80% of equity drawdowns occur in this regime — "
                f"vol sellers get crushed. Watch for reversal to contango as a potential bottom marker.")
    elif regime == "steep_contango":
        text = (f"Steep contango (avg slope {slopes['avg_slope']*100:+.1f}%). "
                f"Markets calm at front month, pricing risk further out. "
                f"The 'vol selling' trade structurally pays roll. "
                f"Often associated with stable bull markets.")
    elif regime == "mild_contango":
        text = (f"Mild contango (avg slope {slopes['avg_slope']*100:+.1f}%). "
                f"Normal regime — typical ~60% of trading days. "
                f"Vol pricing is balanced; no clear directional signal.")
    elif regime == "flat":
        text = (f"Flat curve (avg slope {slopes['avg_slope']*100:+.1f}%). "
                f"Transition zone — uncertainty about which regime is next. "
                f"Often precedes a regime shift; watch for confirmation.")
    else:
        text = "Term structure data incomplete."

    # VVIX context
    if vvix:
        if vvix > 110:
            text += f" VVIX at {vvix:.0f} = elevated (vol-of-vol > 110 = stressed risk pricing)."
        elif vvix < 80:
            text += f" VVIX at {vvix:.0f} = compressed (vol-of-vol < 80 = complacency)."
    if vix_30d:
        if vix_30d > 30:
            text += f" Spot VIX {vix_30d:.1f} elevated."
        elif vix_30d < 13:
            text += f" Spot VIX {vix_30d:.1f} suppressed."

    return text


def lambda_handler(event, context):
    s3 = boto3.client("s3")
    started = time.time()

    quotes = {}
    fetch_errors = []
    for key, symbol in SYMBOLS.items():
        v = fetch_quote(symbol)
        if v is not None:
            quotes[key] = v
        else:
            fetch_errors.append(key)
        time.sleep(0.2)  # gentle on Yahoo

    vix_9d = quotes.get("vix_9d")
    vix_30d = quotes.get("vix_30d")
    vix_3m = quotes.get("vix_3m")
    vix_6m = quotes.get("vix_6m")
    vvix = quotes.get("vvix")

    if not (vix_30d and vix_3m):
        return {"statusCode": 502,
                "body": json.dumps({"error": "Missing VIX data",
                                    "errors": fetch_errors})}

    regime, slopes = classify_regime(vix_9d, vix_30d, vix_3m, vix_6m)
    interp = interpret(regime, slopes or {}, vix_30d, vvix)

    output = {
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "vix_9d":  round(vix_9d, 2)  if vix_9d else None,
        "vix_30d": round(vix_30d, 2) if vix_30d else None,
        "vix_3m":  round(vix_3m, 2)  if vix_3m else None,
        "vix_6m":  round(vix_6m, 2)  if vix_6m else None,
        "vvix":    round(vvix, 2)    if vvix else None,
        "slopes":  slopes,
        "regime":  regime,
        "interpretation": interp,
        "fetch_errors": fetch_errors,
        "fetch_duration_s": round(time.time() - started, 1),
    }

    s3.put_object(Bucket=S3_BUCKET, Key=S3_KEY,
                  Body=json.dumps(output).encode(),
                  ContentType="application/json", CacheControl="no-cache")

    print(f"vix-curve: regime={regime} VIX30d={vix_30d:.1f} VIX3M={vix_3m:.1f} avg_slope={slopes.get('avg_slope') if slopes else '?'}")
    return {
        "statusCode": 200,
        "headers": {"Content-Type": "application/json", "Access-Control-Allow-Origin": "*"},
        "body": json.dumps({
            "ok": True,
            "regime": regime,
            "vix_30d": output["vix_30d"],
            "vix_3m": output["vix_3m"],
        }),
    }
