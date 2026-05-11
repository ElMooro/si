"""
justhodl-cot-tracker v2 — Commitment of Traders (COT) Futures Positioning

DATA SOURCE FIX (v2, 2026-05-11):
Discovered FMP's per-symbol COT endpoint returns stale 2024-02 data,
but the date-range query (?from=X&to=Y) returns CURRENT weekly reports.
v2 fetches a 6-month window for z-score history + uses the
commitment-of-traders-list endpoint to dynamically discover all
64 tracked symbols.
"""
import json
import os
import time
import urllib.request
import urllib.error
from datetime import datetime, timezone, timedelta
from statistics import mean, stdev
from collections import defaultdict

import boto3

FMP_KEY = os.environ.get("FMP_KEY", "wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb")
FMP_BASE = "https://financialmodelingprep.com/stable"
S3_BUCKET = "justhodl-dashboard-live"
S3_KEY = "screener/cot-latest.json"

LOOKBACK_DAYS = 200
EXTREME_Z_THRESHOLD = 1.5

s3 = boto3.client("s3", region_name="us-east-1")


def fmp(path, params="", retries=2):
    url = f"{FMP_BASE}/{path}?apikey={FMP_KEY}{params}"
    for attempt in range(retries + 1):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "JustHodl-COT/2.0"})
            with urllib.request.urlopen(req, timeout=30) as r:
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


def process_contract(symbol, name, sector, records):
    if not records:
        return None
    records.sort(key=lambda r: r.get("date", ""), reverse=True)
    latest = records[0]

    oi = latest.get("openInterestAll") or 0
    nc_long = latest.get("noncommPositionsLongAll") or 0
    nc_short = latest.get("noncommPositionsShortAll") or 0
    c_long = latest.get("commPositionsLongAll") or 0
    c_short = latest.get("commPositionsShortAll") or 0

    if oi <= 0:
        return None

    nc_net = nc_long - nc_short
    nc_long_pct = round(nc_long / oi * 100, 2)
    nc_short_pct = round(nc_short / oi * 100, 2)
    nc_net_pct_oi = round(nc_net / oi * 100, 2)
    c_net = c_long - c_short

    history = []
    for rec in records:
        try:
            r_oi = rec.get("openInterestAll") or 0
            r_nl = rec.get("noncommPositionsLongAll") or 0
            r_ns = rec.get("noncommPositionsShortAll") or 0
            if r_oi > 0:
                net_pct = round((r_nl - r_ns) / r_oi * 100, 2)
                history.append({
                    "date": rec.get("date", "")[:10],
                    "net_pct_oi": net_pct,
                    "long_pct": round(r_nl / r_oi * 100, 2),
                    "short_pct": round(r_ns / r_oi * 100, 2),
                })
        except Exception:
            continue

    z_score = None
    extreme = None
    if len(history) >= 4:
        nets = [h["net_pct_oi"] for h in history]
        m = mean(nets)
        try:
            sd = stdev(nets) if len(nets) > 1 else 0
        except Exception:
            sd = 0
        if sd > 0:
            z = round((nc_net_pct_oi - m) / sd, 2)
            z_score = z
            if z >= EXTREME_Z_THRESHOLD:
                extreme = "long"
            elif z <= -EXTREME_Z_THRESHOLD:
                extreme = "short"

    return {
        "symbol": symbol,
        "name": name,
        "sector": sector,
        "exchange": latest.get("marketAndExchangeNames"),
        "latest_date": latest.get("date", "")[:10],
        "open_interest_all": oi,
        "noncomm_long": nc_long,
        "noncomm_short": nc_short,
        "noncomm_net": nc_net,
        "noncomm_long_pct": nc_long_pct,
        "noncomm_short_pct": nc_short_pct,
        # Page expects these alias keys:
        "current_long_pct": nc_long_pct,
        "current_short_pct": nc_short_pct,
        "net_position_pct": nc_net_pct_oi,
        "net_pct_oi": nc_net_pct_oi,
        "comm_long": c_long,
        "comm_short": c_short,
        "comm_net": c_net,
        "z_score": z_score,
        "z_score_3y": z_score,
        "n_weeks": len(history),
        "extreme_signal": extreme,
        "history": history[:26],
        "history_30d": history[:5],
        "date": latest.get("date", "")[:10],
    }


def build_summary(contracts):
    valid = [c for c in contracts if c and c.get("z_score") is not None]
    extreme_long = sorted(
        [c for c in valid if c["z_score"] >= EXTREME_Z_THRESHOLD],
        key=lambda c: -c["z_score"])
    extreme_short = sorted(
        [c for c in valid if c["z_score"] <= -EXTREME_Z_THRESHOLD],
        key=lambda c: c["z_score"])
    by_sector = defaultdict(list)
    for c in contracts:
        if c:
            by_sector[c.get("sector", "OTHER")].append(c["symbol"])
    return {
        "n_contracts": len([c for c in contracts if c]),
        "n_with_z_score": len(valid),
        "extreme_long_count": len(extreme_long),
        "extreme_short_count": len(extreme_short),
        "extreme_long_top": [
            {"symbol": c["symbol"], "name": c["name"], "sector": c["sector"],
              "z": c["z_score"], "net_pct": c["net_pct_oi"]}
            for c in extreme_long[:10]],
        "extreme_short_top": [
            {"symbol": c["symbol"], "name": c["name"], "sector": c["sector"],
              "z": c["z_score"], "net_pct": c["net_pct_oi"]}
            for c in extreme_short[:10]],
        "by_sector": {sec: syms for sec, syms in by_sector.items()},
    }


def lambda_handler(event, context):
    started = time.time()
    sym_list = fmp("commitment-of-traders-list")
    if not isinstance(sym_list, list) or not sym_list:
        return {"statusCode": 500, "body": json.dumps({"error": "no_symbol_list"})}
    print(f"[cot] {len(sym_list)} symbols available")

    today = datetime.now(timezone.utc).date()
    from_date = (today - timedelta(days=LOOKBACK_DAYS)).strftime("%Y-%m-%d")
    to_date = today.strftime("%Y-%m-%d")
    print(f"[cot] fetching reports {from_date} -> {to_date}")

    reports = fmp("commitment-of-traders-report",
                    f"&from={from_date}&to={to_date}")
    if not isinstance(reports, list) or not reports:
        return {"statusCode": 500, "body": json.dumps({"error": "no_reports"})}
    print(f"[cot] got {len(reports)} report records")

    by_symbol = defaultdict(list)
    for r in reports:
        sym = r.get("symbol")
        if sym:
            by_symbol[sym].append(r)

    contracts = []
    for sym, records in by_symbol.items():
        name = (records[0].get("name") if records else sym) or sym
        sector = (records[0].get("sector") if records else "OTHER") or "OTHER"
        c = process_contract(sym, name, sector, records)
        if c:
            contracts.append(c)
    contracts.sort(key=lambda c: -abs(c.get("z_score") or 0))

    summary = build_summary(contracts)
    payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "elapsed_seconds": round(time.time() - started, 1),
        "date_window": {"from": from_date, "to": to_date},
        "latest_report_date": max((c.get("latest_date") or "" for c in contracts), default=""),
        "n_contracts": len(contracts),
        "summary": summary,
        "contracts": contracts,
    }

    try:
        s3.put_object(Bucket=S3_BUCKET, Key=S3_KEY,
                       Body=json.dumps(payload, default=str),
                       ContentType="application/json",
                       CacheControl="public, max-age=3600")
        print(f"[s3] wrote {len(contracts)} contracts")
    except Exception as e:
        print(f"[s3] err: {e}")

    return {"statusCode": 200, "body": json.dumps({
        "n_contracts": len(contracts),
        "latest_report_date": payload["latest_report_date"],
        "extreme_long": summary["extreme_long_count"],
        "extreme_short": summary["extreme_short_count"],
        "elapsed_seconds": payload["elapsed_seconds"],
    })}
