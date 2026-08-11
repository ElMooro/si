"""justhodl-pjm-grid v1.0.1 (ops 4609)

PJM Data Miner 2 → the electricity-demand leg of the AI-infrastructure
thesis (GEV / EME / NVT) and the newest Physical Economy engine, sitting
next to grid-queue / port-cargo / freight-pulse.

Real feeds (non-member subscription, key via env PJM_API_KEY only):
  inst_load          5-min RTO instantaneous load (8 days → momentum)
  load_frcstd_7_day  RTO 7-day forecast (peak + headroom)
  gen_by_fuel        hourly generation fuel mix
  rt_hrl_lmps        RTO-aggregate real-time hourly LMP (pnode_id=1)

Signals: current load, 7-day demand momentum (the data-center growth
read), forecast-peak headroom, RT LMP daily average day-over-day
%-change with the house shock doctrine (huge one-day % moves = alarm),
and fuel-mix shares. Output: data/pjm-grid.json (+ history 400 pts).
No key → honest ok:false; nothing faked.
v1.0.1: last-good fallback — if a feed rate-limits (non-member
~6 req/min), reuse the prior run's block flagged "stale": true
instead of publishing an empty section.
"""
import json
import os
import urllib.parse
import urllib.request
import time
from datetime import datetime, timedelta, timezone

import boto3

BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
OUT_KEY = os.environ.get("S3_KEY_OUT", "data/pjm-grid.json")
HIST_KEY = "data/pjm-grid-history.json"
PJM_KEY = os.environ.get("PJM_API_KEY", "")
BASE = "https://api.pjm.com/api/v1/"
s3 = boto3.client("s3")


def dm2(feed, params):
    """Data Miner 2 GET with subscription-key header."""
    qs = urllib.parse.urlencode(params, quote_via=urllib.parse.quote)
    req = urllib.request.Request(
        BASE + feed + "?" + qs,
        headers={"Ocp-Apim-Subscription-Key": PJM_KEY,
                 "User-Agent": "justhodl-pjm-grid"})
    try:
        with urllib.request.urlopen(req, timeout=40) as h:
            d = json.loads(h.read())
            return d.get("items", d if isinstance(d, list) else [])
    except Exception as e:
        print(f"[pjm] {feed}: {e}")
        return []


def ept_range(days_back, field="datetime_beginning_ept"):
    now = datetime.now(timezone.utc) - timedelta(hours=4)  # ~EPT
    start = now - timedelta(days=days_back)
    return {field: start.strftime("%m/%d/%Y 00:00") + "to"
            + now.strftime("%m/%d/%Y 23:59")}


def parse_ts(s):
    for fmt in ("%m/%d/%Y %I:%M:%S %p", "%Y-%m-%dT%H:%M:%S",
                "%m/%d/%Y %H:%M:%S"):
        try:
            return datetime.strptime(s, fmt)
        except Exception:
            continue
    return None


def lambda_handler(event, context):
    now = datetime.now(timezone.utc)
    if not PJM_KEY:
        return {"statusCode": 200,
                "body": json.dumps({"ok": False,
                                    "error": "PJM_API_KEY not set"})}

    # ── 1. Instantaneous load, RTO, 8 days ───────────────────────────
    p = {"rowCount": 50000, "startRow": 1}
    p.update(ept_range(8))
    rows = dm2("inst_load", p)
    time.sleep(1.5)
    rto = []
    for x in rows:
        area = str(x.get("area", ""))
        if "RTO" not in area.upper():
            continue
        ts = parse_ts(str(x.get("datetime_beginning_ept", "")))
        v = x.get("instantaneous_load")
        if ts and isinstance(v, (int, float)):
            rto.append((ts, float(v)))
    rto.sort()
    load = {}
    if rto:
        cur_ts, cur = rto[-1]
        gw = lambda mw: round(mw / 1000.0, 2)
        last7 = [v for t, v in rto if t >= cur_ts - timedelta(days=7)]
        first24 = [v for t, v in rto
                   if t <= rto[0][0] + timedelta(days=1)]
        mom = None
        if last7 and first24:
            a = sum(last7[-288:]) / len(last7[-288:])
            b = sum(first24) / len(first24)
            mom = round((a / b - 1) * 100, 2) if b else None
        load = {"current_gw": gw(cur),
                "current_ts_ept": cur_ts.isoformat(timespec="minutes"),
                "avg_24h_gw": gw(sum(last7[-288:]) / len(last7[-288:]))
                if last7 else None,
                "momentum_8d_pct": mom,
                "n_obs": len(rto)}

    # ── 2. 7-day forecast peak ───────────────────────────────────────
    p = {"rowCount": 5000, "startRow": 1}
    rows = dm2("load_frcstd_7_day", p)
    time.sleep(1.5)
    fpeak, fpeak_ts = None, None
    for x in rows:
        area = str(x.get("forecast_area", "")) + str(x.get("area", ""))
        if "RTO" not in area.upper():
            continue
        v = x.get("forecast_load_mw")
        if isinstance(v, (int, float)) and (fpeak is None or v > fpeak):
            fpeak = float(v)
            fpeak_ts = str(x.get("forecast_datetime_beginning_ept", ""))
    forecast = {}
    if fpeak:
        forecast = {"peak_gw": round(fpeak / 1000.0, 2),
                    "peak_at_ept": fpeak_ts}
        if load.get("current_gw"):
            forecast["current_vs_peak_pct"] = round(
                load["current_gw"] / forecast["peak_gw"] * 100, 1)

    # ── 3. Generation fuel mix (latest hour) ─────────────────────────
    p = {"rowCount": 3000, "startRow": 1}
    p.update(ept_range(2))
    rows = dm2("gen_by_fuel", p)
    time.sleep(1.5)
    byhour = {}
    for x in rows:
        ts = parse_ts(str(x.get("datetime_beginning_ept", "")))
        ft = str(x.get("fuel_type", "?"))
        mw = x.get("mw")
        if ts and isinstance(mw, (int, float)):
            byhour.setdefault(ts, {})[ft] = float(mw)
    fuel = {}
    if byhour:
        latest = max(byhour)
        mix = byhour[latest]
        tot = sum(mix.values()) or 1.0
        shares = {k: round(v / tot * 100, 1)
                  for k, v in sorted(mix.items(),
                                     key=lambda kv: -kv[1])}
        fuel = {"as_of_ept": latest.isoformat(timespec="minutes"),
                "total_gw": round(tot / 1000.0, 2),
                "shares_pct": shares,
                "gas_pct": shares.get("Gas"),
                "nuclear_pct": shares.get("Nuclear"),
                "coal_pct": shares.get("Coal")}

    # ── 4. RT hourly LMP, RTO aggregate, 8 days ──────────────────────
    p = {"rowCount": 300, "startRow": 1, "pnode_id": 1}
    p.update(ept_range(8))
    rows = dm2("rt_hrl_lmps", p)
    daily = {}
    latest_lmp, latest_lmp_ts = None, None
    for x in rows:
        ts = parse_ts(str(x.get("datetime_beginning_ept", "")))
        v = x.get("total_lmp_rt")
        if not ts or not isinstance(v, (int, float)):
            continue
        daily.setdefault(ts.date(), []).append(float(v))
        if latest_lmp_ts is None or ts > latest_lmp_ts:
            latest_lmp_ts, latest_lmp = ts, float(v)
    lmp = {}
    if daily:
        days = sorted(daily)
        avgs = {d: sum(daily[d]) / len(daily[d]) for d in days}
        dod = None
        if len(days) >= 2:
            a, b = avgs[days[-1]], avgs[days[-2]]
            dod = round((a / b - 1) * 100, 1) if b else None
        state = "CALM"
        if dod is not None:
            ad = abs(dod)
            state = ("RED" if ad >= 150 else
                     "AMBER" if ad >= 50 else "CALM")
        lmp = {"latest_rt": round(latest_lmp, 2)
               if latest_lmp is not None else None,
               "latest_ts_ept": latest_lmp_ts.isoformat(
                   timespec="minutes") if latest_lmp_ts else None,
               "daily_avg": round(avgs[days[-1]], 2),
               "daily_avg_dod_pct": dod,
               "shock_state": state,
               "shock_doctrine": "huge day-over-day %-change in the "
                                 "price of power = grid stress alarm "
                                 "(|Δ|>=50% amber, >=150% red)"}

    # last-good fallback (rate-limit resilience; stale flagged)
    try:
        prev = json.loads(s3.get_object(Bucket=BUCKET,
                                        Key=OUT_KEY)["Body"].read())
    except Exception:
        prev = {}
    def keep(cur, key):
        if cur:
            return cur
        old = prev.get(key) or {}
        if old:
            old = dict(old)
            old["stale"] = True
        return old
    load = keep(load, "load")
    forecast = keep(forecast, "forecast")
    fuel = keep(fuel, "fuel_mix")
    lmp = keep(lmp, "lmp")

    ai_read = None
    if load.get("momentum_8d_pct") is not None:
        m = load["momentum_8d_pct"]
        ai_read = (f"RTO demand {'+' if m >= 0 else ''}{m}% over 8 days"
                   f" — {'expanding' if m > 1 else 'flat' if m > -1 else 'contracting'}"
                   " electricity pull (data-center / AI-infra thesis"
                   " monitor)")

    canaries = {}
    if lmp:
        canaries["lmp_spike"] = {"state": lmp["shock_state"],
                                 "dod_pct": lmp["daily_avg_dod_pct"]}
    if forecast.get("current_vs_peak_pct") is not None:
        v = forecast["current_vs_peak_pct"]
        canaries["load_vs_forecast_peak"] = {
            "state": "RED" if v >= 100 else
                     "AMBER" if v >= 96 else "CALM",
            "pct": v}

    payload = {
        "schema_version": "1.0",
        "engine": "justhodl-pjm-grid",
        "as_of": now.isoformat(timespec="seconds"),
        "source": "PJM Data Miner 2 (non-member subscription)",
        "load": load, "forecast": forecast, "fuel_mix": fuel,
        "lmp": lmp, "ai_demand_read": ai_read, "canaries": canaries,
        "thesis": "Electricity is the rate-limiter of the AI buildout; "
                  "PJM is the largest US grid and the first place "
                  "incremental data-center demand prints.",
    }
    ok = bool(load and lmp)
    s3.put_object(Bucket=BUCKET, Key=OUT_KEY,
                  Body=json.dumps(payload).encode(),
                  ContentType="application/json",
                  CacheControl="max-age=300")
    try:
        hist = {}
        try:
            hist = json.loads(s3.get_object(
                Bucket=BUCKET, Key=HIST_KEY)["Body"].read())
        except Exception:
            hist = {"points": []}
        hist["points"].append({
            "t": now.isoformat(timespec="seconds"),
            "gw": load.get("current_gw"),
            "lmp": lmp.get("daily_avg"),
            "mom": load.get("momentum_8d_pct")})
        hist["points"] = hist["points"][-400:]
        s3.put_object(Bucket=BUCKET, Key=HIST_KEY,
                      Body=json.dumps(hist).encode(),
                      ContentType="application/json",
                      CacheControl="max-age=300")
    except Exception as e:
        print(f"[pjm] history: {e}")

    return {"statusCode": 200,
            "body": json.dumps({
                "ok": ok,
                "load_gw": load.get("current_gw"),
                "momentum_pct": load.get("momentum_8d_pct"),
                "lmp": lmp.get("daily_avg"),
                "lmp_shock": lmp.get("shock_state"),
                "n_load_obs": load.get("n_obs")})}
