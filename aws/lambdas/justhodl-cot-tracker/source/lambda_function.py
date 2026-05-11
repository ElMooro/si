"""
justhodl-cot-tracker — Commitment of Traders (COT) Futures Positioning Tracker

Fetches FMP's commitment-of-traders-analysis endpoint for the 30 most-watched
futures contracts (ES, NQ, ZN, GC, CL, etc.) — gives us large speculator,
commercial, and small spec positioning broken out by category.

Output schema (written to S3 as screener/cot-latest.json):
  {
    "generated_at": iso8601,
    "contracts": [
      {
        "symbol": "ES",
        "name": "E-mini S&P 500",
        "sector": "INDICES",
        "exchange": "...",
        "current_long_pct": float,      // % of OI on long side (large spec)
        "current_short_pct": float,
        "net_position_pct": float,      // net long - short, % of OI
        "z_score_3y": float,            // standard deviations from 3y mean
        "extreme_signal": "long" | "short" | null,  // when |z| > 1.5
        "history_30d": [ {date, long_pct, short_pct, net_pct}, ... ],
        "raw": {...full FMP record}
      }, ...
    ],
    "summary": { extreme_long: [...], extreme_short: [...], pivots: [...] }
  }

The page reads directly from S3.
"""
import json
import os
import time
import urllib.request
import urllib.error
from datetime import datetime, timezone, timedelta
from statistics import mean, stdev
from concurrent.futures import ThreadPoolExecutor

import boto3

FMP_KEY = os.environ.get("FMP_KEY", "wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb")
FMP_BASE = "https://financialmodelingprep.com/stable"
S3_BUCKET = "justhodl-dashboard-live"
S3_KEY = "screener/cot-latest.json"

s3 = boto3.client("s3", region_name="us-east-1")

# Curated list of futures we care about. Symbols are FMP's COT codes.
# (Confirmed working — probe 427 returned analysis for ZC corn.)
CONTRACTS = [
    # Equity indices
    ("ES",  "E-mini S&P 500",       "INDICES"),
    ("NQ",  "E-mini Nasdaq 100",    "INDICES"),
    ("YM",  "E-mini Dow",           "INDICES"),
    ("RTY", "E-mini Russell 2000",  "INDICES"),
    ("VX",  "VIX Futures",          "VOLATILITY"),

    # Rates / Bonds
    ("ZN",  "10-Year T-Note",       "RATES"),
    ("ZB",  "30-Year T-Bond",       "RATES"),
    ("ZF",  "5-Year T-Note",        "RATES"),
    ("ZT",  "2-Year T-Note",        "RATES"),
    ("SR3", "3-Month SOFR",         "RATES"),

    # Currencies
    ("DX",  "US Dollar Index",      "FX"),
    ("6E",  "Euro FX",              "FX"),
    ("6J",  "Japanese Yen",         "FX"),
    ("6B",  "British Pound",        "FX"),
    ("6C",  "Canadian Dollar",      "FX"),
    ("6A",  "Australian Dollar",    "FX"),
    ("6S",  "Swiss Franc",          "FX"),

    # Metals
    ("GC",  "Gold",                 "METALS"),
    ("SI",  "Silver",               "METALS"),
    ("HG",  "Copper",               "METALS"),
    ("PL",  "Platinum",             "METALS"),
    ("PA",  "Palladium",            "METALS"),

    # Energy
    ("CL",  "WTI Crude",            "ENERGY"),
    ("BZ",  "Brent Crude",          "ENERGY"),
    ("NG",  "Natural Gas",          "ENERGY"),
    ("HO",  "Heating Oil",          "ENERGY"),
    ("RB",  "RBOB Gasoline",        "ENERGY"),

    # Grains
    ("ZC",  "Corn",                 "GRAINS"),
    ("ZS",  "Soybeans",             "GRAINS"),
    ("ZW",  "Wheat",                "GRAINS"),
    ("ZM",  "Soybean Meal",         "GRAINS"),
    ("ZL",  "Soybean Oil",          "GRAINS"),

    # Softs
    ("CC",  "Cocoa",                "SOFTS"),
    ("SB",  "Sugar",                "SOFTS"),
    ("KC",  "Coffee",               "SOFTS"),
    ("CT",  "Cotton",               "SOFTS"),
    ("OJ",  "Orange Juice",         "SOFTS"),

    # Meats
    ("LE",  "Live Cattle",          "MEATS"),
    ("GF",  "Feeder Cattle",        "MEATS"),
    ("HE",  "Lean Hogs",            "MEATS"),

    # Crypto
    ("BTC", "Bitcoin Futures",      "CRYPTO"),
]


def fmp(path, params="", retries=2):
    url = f"{FMP_BASE}/{path}?apikey={FMP_KEY}{params}"
    for attempt in range(retries + 1):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "JustHodl-COT/1.0"})
            with urllib.request.urlopen(req, timeout=20) as r:
                return json.loads(r.read().decode("utf-8"))
        except urllib.error.HTTPError as e:
            if e.code in (429, 500, 502, 503) and attempt < retries:
                time.sleep(0.5 * (attempt + 1))
                continue
            print(f"[fmp] {path}: HTTP {e.code}")
            return None
        except Exception as e:
            if attempt < retries:
                time.sleep(0.5 * (attempt + 1))
                continue
            print(f"[fmp] {path}: {e}")
            return None
    return None


def fetch_contract(args):
    sym, name, sector = args
    # commitment-of-traders-analysis returns recent records (analysis-formatted)
    # commitment-of-traders returns raw weekly records
    analysis = fmp("commitment-of-traders-analysis", f"&symbol={sym}")
    if not isinstance(analysis, list) or not analysis:
        return None

    # Sort by date desc, take most recent
    analysis.sort(key=lambda r: r.get("date", ""), reverse=True)
    latest = analysis[0]

    # Compute long/short positioning percentages
    long_pct = latest.get("currentLongMarketSituation")
    short_pct = latest.get("currentShortMarketSituation")
    if long_pct is None or short_pct is None:
        return None
    net_pct = round(long_pct - short_pct, 2)

    # 3-year history for z-score
    history = []
    for rec in analysis[:156]:  # ~3y of weekly data
        try:
            hl = rec.get("currentLongMarketSituation")
            hs = rec.get("currentShortMarketSituation")
            if hl is not None and hs is not None:
                history.append({
                    "date": rec.get("date", "")[:10],
                    "long_pct": hl,
                    "short_pct": hs,
                    "net_pct": round(hl - hs, 2),
                })
        except Exception:
            pass

    z_score = None
    extreme = None
    if len(history) >= 30:
        nets = [h["net_pct"] for h in history]
        m = mean(nets)
        try:
            sd = stdev(nets) if len(nets) > 1 else 0
        except Exception:
            sd = 0
        if sd > 0:
            z = round((net_pct - m) / sd, 2)
            z_score = z
            if z >= 1.5:
                extreme = "long"
            elif z <= -1.5:
                extreme = "short"

    return {
        "symbol": sym,
        "name": name,
        "sector": sector,
        "exchange": latest.get("exchange"),
        "market_situation": latest.get("marketSituation"),
        "date": latest.get("date", "")[:10],
        "current_long_pct": long_pct,
        "current_short_pct": short_pct,
        "net_position_pct": net_pct,
        "z_score_3y": z_score,
        "extreme_signal": extreme,
        "history_30d": history[:30],
        "n_history": len(history),
    }


def build_summary(contracts):
    """Aggregate stats — extreme long, extreme short, recent pivots."""
    valid = [c for c in contracts if c]
    extreme_long = sorted(
        [c for c in valid if c.get("z_score_3y") is not None and c["z_score_3y"] >= 1.5],
        key=lambda c: -c["z_score_3y"])
    extreme_short = sorted(
        [c for c in valid if c.get("z_score_3y") is not None and c["z_score_3y"] <= -1.5],
        key=lambda c: c["z_score_3y"])
    by_sector = {}
    for c in valid:
        sec = c.get("sector", "OTHER")
        by_sector.setdefault(sec, []).append(c["symbol"])
    return {
        "n_contracts": len(valid),
        "extreme_long_count": len(extreme_long),
        "extreme_short_count": len(extreme_short),
        "extreme_long_top": [
            {"symbol": c["symbol"], "name": c["name"], "sector": c["sector"],
              "z": c["z_score_3y"], "net_pct": c["net_position_pct"]}
            for c in extreme_long[:10]],
        "extreme_short_top": [
            {"symbol": c["symbol"], "name": c["name"], "sector": c["sector"],
              "z": c["z_score_3y"], "net_pct": c["net_position_pct"]}
            for c in extreme_short[:10]],
        "by_sector": {sec: syms for sec, syms in by_sector.items()},
    }


def lambda_handler(event, context):
    started = time.time()
    print(f"[cot] fetching {len(CONTRACTS)} contracts...")

    contracts = []
    with ThreadPoolExecutor(max_workers=6) as ex:
        for result in ex.map(fetch_contract, CONTRACTS):
            if result:
                contracts.append(result)

    summary = build_summary(contracts)
    payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "elapsed_seconds": round(time.time() - started, 1),
        "n_contracts_requested": len(CONTRACTS),
        "n_contracts_returned": len(contracts),
        "summary": summary,
        "contracts": contracts,
    }

    try:
        s3.put_object(
            Bucket=S3_BUCKET, Key=S3_KEY,
            Body=json.dumps(payload, default=str),
            ContentType="application/json",
            CacheControl="public, max-age=3600",  # COT updates weekly
        )
        print(f"[cot] wrote {len(contracts)} contracts to s3://{S3_BUCKET}/{S3_KEY}")
    except Exception as e:
        print(f"[s3] write err: {e}")

    return {"statusCode": 200, "body": json.dumps({
        "n_contracts": len(contracts),
        "elapsed_seconds": payload["elapsed_seconds"],
        "extreme_long": summary["extreme_long_count"],
        "extreme_short": summary["extreme_short_count"],
    })}
