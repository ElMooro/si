"""
justhodl-construction-housing — Housing & Construction Cycle Engine

═══════════════════════════════════════════════════════════════════════
WHY THIS EXISTS
───────────────
The platform had no read on the US housing & construction cycle — a
gap given both its macro mandate and the owner's home-builder licence.
This engine pulls the canonical housing-cycle series from FRED (free,
already a core provider) and fuses them into one decisive read:

  • Building permits  — the LEADING indicator (authorised, not yet built)
  • Housing starts / completions — the activity pipeline
  • New + existing home sales — demand
  • Months' supply — inventory tightness / slack
  • 30Y mortgage rate — the affordability lever
  • Case-Shiller — price momentum
  • Residential construction spending — dollar activity
  • PPI residential-construction inputs — builder input-cost inflation

It classifies the cycle (EXPANSION / RECOVERY / SLOWING / CONTRACTION)
from permits, starts, sales, supply and the rate trend.

OUTPUT: data/construction-housing.json   SCHEDULE: daily 11:00 UTC
═══════════════════════════════════════════════════════════════════════
"""
import json
import os
import time
import urllib.request
import urllib.error
from datetime import datetime, timezone

import boto3
import _fred_shim  # noqa: F401  — cache-first FRED + 429 backoff (ops/1074)

S3_BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
S3_KEY = "data/construction-housing.json"
FRED_KEY = os.environ.get("FRED_KEY", "2f057499936072679d8843d7fce99989")

s3 = boto3.client("s3", region_name="us-east-1")

# (series_id, label, unit, role) — role drives the cycle scoring
SERIES = [
    ("PERMIT",        "Building Permits",                "K units SAAR", "leading"),
    ("HOUST",         "Housing Starts",                  "K units SAAR", "activity"),
    ("COMPUTSA",      "Housing Completions",             "K units SAAR", "lagging"),
    ("HSN1F",         "New Home Sales",                  "K units SAAR", "demand"),
    ("EXHOSLUSM495S", "Existing Home Sales",             "units SAAR",   "demand"),
    ("MSACSR",        "Months' Supply, New Homes",       "months",       "supply"),
    ("TLRESCONS",     "Residential Construction Spend",  "$M SAAR",      "activity"),
    ("MORTGAGE30US",  "30Y Fixed Mortgage Rate",         "%",            "rate"),
    ("CSUSHPISA",     "Case-Shiller Home Price Index",   "index",        "price"),
    ("WPUSI012011",   "PPI: Residential Constr. Inputs", "index",        "cost"),
]


def fred(series_id, limit=42):
    """Latest `limit` observations of a FRED series, newest first."""
    url = ("https://api.stlouisfed.org/fred/series/observations"
           f"?series_id={series_id}&api_key={FRED_KEY}&file_type=json"
           f"&sort_order=desc&limit={limit}")
    for attempt in range(3):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "JustHodl/1.0"})
            with urllib.request.urlopen(req, timeout=20) as r:
                obs = json.loads(r.read()).get("observations", [])
            out = []
            for o in obs:
                v = o.get("value")
                if v not in (".", "", None):
                    try:
                        out.append((o["date"], float(v)))
                    except ValueError:
                        pass
            return out  # newest first
        except urllib.error.HTTPError as e:
            if e.code in (429, 503) and attempt < 2:
                time.sleep(2 ** attempt)
                continue
            return []
        except Exception:
            if attempt < 2:
                time.sleep(1)
                continue
            return []
    return []


def pct(a, b):
    return round((a - b) / b * 100, 1) if (b not in (0, None) and a is not None) else None


def analyse(series_id, label, unit, role):
    obs = fred(series_id)
    if not obs:
        return {"series_id": series_id, "label": label, "unit": unit,
                "role": role, "ok": False}
    latest_date, latest = obs[0]
    prev = obs[1][1] if len(obs) > 1 else None
    yoy_ref = obs[12][1] if len(obs) > 12 else None
    trend3 = obs[3][1] if len(obs) > 3 else None
    return {
        "series_id": series_id, "label": label, "unit": unit, "role": role,
        "ok": True,
        "latest": round(latest, 2), "as_of": latest_date,
        "mom_pct": pct(latest, prev),
        "yoy_pct": pct(latest, yoy_ref),
        "chg_3m_pct": pct(latest, trend3),
        "ok_": True,
    }


def classify(rows):
    """Score the housing cycle from the fused series."""
    by = {r["series_id"]: r for r in rows if r.get("ok")}
    score, signals = 0, []

    def yoy(sid):
        return (by.get(sid) or {}).get("yoy_pct")

    permit_yoy = yoy("PERMIT")
    if permit_yoy is not None:
        if permit_yoy > 2:
            score += 1; signals.append(f"Permits +{permit_yoy}% YoY — pipeline building")
        elif permit_yoy < -2:
            score -= 1; signals.append(f"Permits {permit_yoy}% YoY — pipeline shrinking")
    starts_yoy = yoy("HOUST")
    if starts_yoy is not None:
        if starts_yoy > 2:
            score += 1
        elif starts_yoy < -2:
            score -= 1
        signals.append(f"Starts {starts_yoy:+}% YoY")
    sales_yoy = yoy("HSN1F")
    if sales_yoy is not None:
        if sales_yoy > 2:
            score += 1; signals.append(f"New-home sales +{sales_yoy}% YoY — demand firm")
        elif sales_yoy < -2:
            score -= 1; signals.append(f"New-home sales {sales_yoy}% YoY — demand soft")
    supply = by.get("MSACSR", {}).get("latest")
    if supply is not None:
        if supply <= 6:
            score += 1; signals.append(f"Months' supply {supply} — inventory tight")
        elif supply >= 8:
            score -= 1; signals.append(f"Months' supply {supply} — inventory heavy")
    rate = by.get("MORTGAGE30US", {})
    rchg = rate.get("chg_3m_pct")
    if rchg is not None:
        if rchg < -2:
            score += 1; signals.append(f"Mortgage rate falling ({rchg}% 3m) — affordability easing")
        elif rchg > 2:
            score -= 1; signals.append(f"Mortgage rate rising (+{rchg}% 3m) — affordability worsening")
    cost = by.get("WPUSI012011", {}).get("yoy_pct")
    if cost is not None:
        signals.append(f"Builder input costs {cost:+}% YoY")

    if score >= 3:
        regime, color = "EXPANSION", "green"
    elif score >= 1:
        regime = "RECOVERY" if (permit_yoy or 0) > 0 else "SLOWING"
        color = "cyan" if regime == "RECOVERY" else "yellow"
    elif score <= -2:
        regime, color = "CONTRACTION", "red"
    else:
        regime, color = "SLOWING", "yellow"
    return regime, color, score, signals


def lambda_handler(event, context):
    t0 = time.time()
    rows = [analyse(sid, lbl, unit, role) for sid, lbl, unit, role in SERIES]
    regime, color, score, signals = classify(rows)

    by = {r["series_id"]: r for r in rows if r.get("ok")}
    permit = by.get("PERMIT", {})
    read = (f"Housing cycle reads {regime} (score {score:+}). "
            f"Permits — the leading indicator — are "
            f"{permit.get('yoy_pct', 'n/a')}% YoY. "
            + ("Affordability is the swing factor: watch the 30Y mortgage rate."
               if regime in ("SLOWING", "CONTRACTION")
               else "Pipeline and demand are aligned to the upside."))

    out = {
        "schema_version": "1.0",
        "method": "fred_housing_cycle",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "elapsed_s": round(time.time() - t0, 1),
        "regime": regime,
        "regime_color": color,
        "cycle_score": score,
        "read": read,
        "signals": signals,
        "series": rows,
        "n_resolved": sum(1 for r in rows if r.get("ok")),
        "n_series": len(rows),
        "note": ("US housing & construction cycle from FRED monthly series. "
                 "Permits lead starts by ~1-2 months; months' supply and the "
                 "mortgage rate gate the cycle. A macro read, not advice."),
    }
    s3.put_object(Bucket=S3_BUCKET, Key=S3_KEY,
                  Body=json.dumps(out, default=str).encode("utf-8"),
                  ContentType="application/json",
                  CacheControl="public, max-age=3600")
    print(f"[construction-housing] regime={regime} score={score} "
          f"{out['n_resolved']}/{out['n_series']} series {out['elapsed_s']}s")
    return {"statusCode": 200, "body": json.dumps({
        "ok": True, "regime": regime, "cycle_score": score,
        "n_resolved": out["n_resolved"]})}
