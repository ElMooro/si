"""justhodl-fiat-peg-monitor — currency-peg break / devaluation-pressure board.

Tracks the world's managed/pegged currencies against their official pegs or
bands and scores how close each is to its weak-side intervention edge — the
classic dollar-shortage / capital-flight stress signal. HKD (7.75-7.85 HKMA
band), CNY (PBOC managed float), the Gulf hard pegs (SAR/AED/QAR), and DKK
(ERM II to EUR). FRED for HKD/CNY/DKK, Polygon FX for the Gulf pegs.
"""
import json
import os
import urllib.request
from datetime import datetime, timezone

import boto3

s3 = boto3.client("s3", "us-east-1")
BUCKET = "justhodl-dashboard-live"
OUT_KEY = "data/fiat-peg-monitor.json"
FRED_KEY = os.environ.get("FRED_KEY", "2f057499936072679d8843d7fce99989")
POLY_KEY = os.environ.get("POLYGON_KEY", "zvEY_KYYMHoAN0JqY7n2Ze6q0kBuJX_d")


def _get(url):
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
        return json.loads(urllib.request.urlopen(req, timeout=20).read())
    except Exception as e:
        print(f"  fetch err {url[:60]}: {str(e)[:60]}")
        return {}


def fred_latest(sid):
    d = _get(f"https://api.stlouisfed.org/fred/series/observations?series_id={sid}"
             f"&api_key={FRED_KEY}&file_type=json&sort_order=desc&limit=1")
    o = (d.get("observations") or [{}])[0]
    try:
        return float(o.get("value"))
    except (TypeError, ValueError):
        return None


def poly_prev(pair):
    d = _get(f"https://api.polygon.io/v2/aggs/ticker/C:{pair}/prev?apiKey={POLY_KEY}")
    r = (d.get("results") or [{}])[0]
    return r.get("c")


def lambda_handler(event=None, context=None):
    # spot sources
    hkd = fred_latest("DEXHKUS")
    cny = fred_latest("DEXCHUS")
    dkk_usd = fred_latest("DEXDNUS")
    eur_usd = fred_latest("DEXUSEU")
    dkk_eur = round(dkk_usd * eur_usd, 4) if (dkk_usd and eur_usd) else None
    sar = poly_prev("USDSAR")
    aed = poly_prev("USDAED")
    qar = poly_prev("USDQAR")

    pegs = []

    def band_peg(ccy, spot, strong, weak, note, invert=False):
        # position toward WEAK edge (capital-outflow side). High spot (more local
        # ccy per USD) = weaker currency = outflow pressure, unless invert.
        if spot is None:
            return
        lo, hi = (strong, weak)
        pos = (spot - lo) / (hi - lo) if hi != lo else 0.0
        toward_weak = pos if not invert else (1 - pos)
        pressure = round(max(0.0, min(100.0, toward_weak * 100)), 1)
        dist = round((weak - spot) / weak * 100, 3) if not invert else round((spot - strong) / strong * 100, 3)
        reg = ("BREAK_RISK" if pressure >= 90 else "PRESSURE" if pressure >= 70
               else "DRIFTING" if pressure >= 50 else "ON_PEG")
        pegs.append({"ccy": ccy, "spot": spot, "peg": round((strong + weak) / 2, 4),
                     "band": [strong, weak], "type": "band", "pressure": pressure,
                     "distance_to_weak_edge_pct": dist, "regime": reg, "note": note})

    def hard_peg(ccy, spot, peg, note, tol=0.5):
        if spot is None:
            return
        dev = (spot - peg) / peg * 100
        pressure = round(max(0.0, min(100.0, abs(dev) / tol * 100)), 1)
        reg = ("BREAK_RISK" if pressure >= 90 else "PRESSURE" if pressure >= 60
               else "DRIFTING" if pressure >= 30 else "ON_PEG")
        pegs.append({"ccy": ccy, "spot": spot, "peg": peg, "band": None, "type": "hard",
                     "pressure": pressure, "deviation_pct": round(dev, 3), "regime": reg, "note": note})

    def managed(ccy, spot, soft_weak, note):
        if spot is None:
            return
        # distance to the soft psychological weak level
        pressure = round(max(0.0, min(100.0, (spot / soft_weak) * 100 - (100 - 12) if spot else 0)), 1)
        # simpler: how far spot is toward soft_weak from a benign 6.4 anchor
        anchor = 6.40
        pos = (spot - anchor) / (soft_weak - anchor) if soft_weak != anchor else 0
        pressure = round(max(0.0, min(100.0, pos * 100)), 1)
        reg = ("BREAK_RISK" if pressure >= 90 else "PRESSURE" if pressure >= 70
               else "DRIFTING" if pressure >= 50 else "ON_PEG")
        pegs.append({"ccy": ccy, "spot": spot, "peg": None, "soft_weak_level": soft_weak,
                     "type": "managed", "pressure": pressure, "regime": reg, "note": note})

    band_peg("HKD", hkd, 7.75, 7.85, "HKMA Convertibility Undertaking 7.75-7.85; weak edge = USD demand / outflows")
    managed("CNY", cny, 7.35, "PBOC managed float; 7.30-7.35 = devaluation-pressure zone")
    hard_peg("SAR", sar, 3.75, "SAMA hard peg 3.75; forward points price oil/fiscal stress")
    hard_peg("AED", aed, 3.6725, "CBUAE hard peg 3.6725")
    hard_peg("QAR", qar, 3.64, "QCB hard peg 3.64")
    if dkk_eur:
        band_peg("DKK", dkk_eur, 7.29265, 7.62811, "ERM II central 7.46038 ±2.25% vs EUR")

    pegs.sort(key=lambda p: -(p.get("pressure") or 0))
    worst = pegs[0] if pegs else {}
    composite = round(sum(p["pressure"] for p in pegs) / len(pegs), 1) if pegs else 0
    any_break = any(p["pressure"] >= 90 for p in pegs)
    regime = ("BREAK_RISK" if any_break else "ELEVATED" if composite >= 50
              else "PRESSURE_BUILDING" if composite >= 30 else "STABLE")
    headline = (f"Peg stress {regime} ({composite}/100). "
                f"Most pressured: {worst.get('ccy','?')} {worst.get('pressure','?')}/100"
                + (f" — {worst.get('regime')}" if worst else "") + ".")

    out = {
        "engine": "fiat-peg-monitor", "version": "1.0.0",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "composite_peg_stress": composite, "regime": regime,
        "worst_peg": worst.get("ccy"), "headline": headline,
        "pegs": pegs,
        "methodology": "Spot vs official peg/band. Band pegs scored by position toward the weak-side "
                       "intervention edge (capital-outflow side); hard pegs by deviation from peg; "
                       "managed floats by proximity to the devaluation-pressure zone.",
        "sources": "FRED (DEXHKUS, DEXCHUS, DEXDNUS, DEXUSEU) + Polygon FX (Gulf pegs)",
    }
    s3.put_object(Bucket=BUCKET, Key=OUT_KEY, Body=json.dumps(out, default=str).encode(),
                  ContentType="application/json", CacheControl="public, max-age=900")
    print(f"[fiat-peg] {headline}")
    return {"statusCode": 200, "body": headline}
