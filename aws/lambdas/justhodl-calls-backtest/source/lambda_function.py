"""justhodl-calls-backtest — replay the decisive-call ledger.

For every brief generation in data/decisive-call-history.json, simulate the
implied SPY exposure and compute what NAV would have done if Khalid had
followed the system's calls verbatim.

Mapping verbs to allocations (SPY-only for clean accounting):
  EXIT_ALL_RISK  → 0%   SPY (all cash, earns 0% — could swap for SHV/BIL @ 4-5%)
  EXIT           → 15%  SPY
  TRIM           → 50%  SPY
  HEDGE          → 50%  SPY (could also long TLT but we keep simple)
  WAIT           → 70%  SPY
  HOLD           → 100% SPY (default)
  LONG           → 100% SPY
  LOAD           → 110% SPY  (slight margin)
  LEVER          → 200% SPY  (2x via SSO/UPRO)
  UNKNOWN        → 100% SPY (treat as HOLD)

Each call is held until the next call. Daily SPY price comes from Polygon.

Output backtest/calls-results.json with:
  - summary: total_return_pct, n_calls, n_changes, sharpe, max_dd
  - calls[]: [verb, start, end, n_days, spy_change_pct, exposure, contribution]
  - nav_curve: [{date, calls_nav, spy_nav}]
  - vs_spy: alpha_pct

Schedule: cron(15 14 * * ? *) — daily 14:15 UTC, after position-sizer.
"""
import json
import os
import time
import urllib.request
from datetime import datetime, timezone, timedelta
from decimal import Decimal

import boto3

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
S3 = boto3.client("s3", region_name=REGION)
SSM = boto3.client("ssm", region_name=REGION)
INITIAL_NAV = 100_000.0
POLYGON_KEY = os.environ.get("POLYGON_KEY") or "zvEY_KYYMHoAN0JqY7n2Ze6q0kBuJX_d"

# Verb-to-SPY-exposure mapping. Conservative version of the trader handbook.
EXPOSURE = {
    "EXIT_ALL_RISK": 0.00,
    "EXIT":          0.15,
    "TRIM":          0.50,
    "HEDGE":         0.50,
    "WAIT":          0.70,
    "HOLD":          1.00,
    "LONG":          1.00,
    "LOAD":          1.10,
    "LEVER":         2.00,
    "UNKNOWN":       1.00,
}


def load_json(key, default=None):
    try:
        obj = S3.get_object(Bucket=BUCKET, Key=key)
        return json.loads(obj["Body"].read())
    except Exception as e:
        print(f"[load] {key}: {e}")
        return default if default is not None else {}


def write_json(key, data, max_age=300):
    body = json.dumps(data, default=str).encode("utf-8")
    S3.put_object(
        Bucket=BUCKET, Key=key, Body=body,
        ContentType="application/json",
        CacheControl=f"public, max-age={max_age}",
    )


def fetch_spy_daily(start_iso, end_iso):
    """Pull daily SPY closes between two dates (YYYY-MM-DD)."""
    url = f"https://api.polygon.io/v2/aggs/ticker/SPY/range/1/day/{start_iso}/{end_iso}?adjusted=true&sort=asc&limit=5000&apiKey={POLYGON_KEY}"
    try:
        req = urllib.request.Request(url, headers={"Accept": "application/json"})
        with urllib.request.urlopen(req, timeout=15) as r:
            d = json.loads(r.read())
            results = d.get("results") or []
            # Normalize to {YYYY-MM-DD: close}
            out = {}
            for row in results:
                ts = row.get("t")
                if ts:
                    dt = datetime.fromtimestamp(ts / 1000, tz=timezone.utc)
                    key = dt.strftime("%Y-%m-%d")
                    out[key] = float(row.get("c") or 0)
            return out
    except Exception as e:
        print(f"[polygon-spy] {e}")
        return {}


def lambda_handler(event=None, context=None):
    started = time.time()
    now = datetime.now(timezone.utc)
    print(f"[calls-backtest] starting at {now.isoformat()}")

    # 1. Load ledger
    history = (load_json("data/decisive-call-history.json") or {}).get("snapshots") or []
    history.sort(key=lambda x: x.get("timestamp") or "")
    if not history:
        print("[calls-backtest] no calls in ledger yet — bailing")
        return {"statusCode": 200, "body": json.dumps({"ok": False, "reason": "empty_ledger"})}
    print(f"[calls-backtest] {len(history)} calls in ledger")

    first_dt = datetime.fromisoformat(history[0]["timestamp"].replace("Z", "+00:00"))
    first_date = first_dt.strftime("%Y-%m-%d")
    end_date = now.strftime("%Y-%m-%d")

    # Pull SPY closes for the full window + a 5-day buffer for the start
    pad_start = (first_dt - timedelta(days=5)).strftime("%Y-%m-%d")
    spy_closes = fetch_spy_daily(pad_start, end_date)
    if not spy_closes:
        print("[calls-backtest] failed to fetch SPY data — bailing")
        return {"statusCode": 500, "body": json.dumps({"ok": False, "reason": "no_spy_data"})}
    spy_dates = sorted(spy_closes.keys())
    print(f"[calls-backtest] {len(spy_dates)} SPY trading days fetched [{spy_dates[0]} → {spy_dates[-1]}]")

    # 2. Build call timeline — each call has a start_date and end_date
    call_segments = []
    for i, snap in enumerate(history):
        call_dt = datetime.fromisoformat(snap["timestamp"].replace("Z", "+00:00"))
        next_dt = (datetime.fromisoformat(history[i+1]["timestamp"].replace("Z", "+00:00"))
                   if i+1 < len(history) else now)
        start_date = call_dt.strftime("%Y-%m-%d")
        end_date_seg = next_dt.strftime("%Y-%m-%d")
        verb = (snap.get("call_verb") or "UNKNOWN").upper()
        exposure = EXPOSURE.get(verb, EXPOSURE["UNKNOWN"])
        call_segments.append({
            "verb": verb,
            "exposure": exposure,
            "start_date": start_date,
            "end_date": end_date_seg,
            "start_ts": snap.get("timestamp"),
            "khalid_score": snap.get("khalid_score"),
            "phase": snap.get("phase"),
        })

    # 3. Walk daily — for each trading day, attribute it to the call active
    # at start-of-day and compute daily return = exposure × spy_daily_return
    nav = INITIAL_NAV
    spy_nav = INITIAL_NAV
    nav_curve = []
    daily_records = []
    seg_idx = 0

    # Find the SPY start price (first close on or after first call's date)
    spy_start_price = None
    for d in spy_dates:
        if d >= first_date:
            spy_start_price = spy_closes[d]
            break
    if not spy_start_price:
        spy_start_price = spy_closes[spy_dates[0]]

    prev_close = None
    for d in spy_dates:
        if d < first_date:
            continue  # before any call
        spy_close = spy_closes[d]
        if prev_close is None:
            prev_close = spy_close
            # First day — initialize
            nav_curve.append({
                "date": d, "nav": round(nav, 2), "spy_nav": round(spy_nav, 2),
                "spy_close": spy_close, "active_verb": call_segments[0]["verb"],
                "active_exposure": call_segments[0]["exposure"],
            })
            continue

        # Find the active segment for this date
        while seg_idx < len(call_segments) - 1 and call_segments[seg_idx+1]["start_date"] <= d:
            seg_idx += 1
        active = call_segments[seg_idx]
        exposure = active["exposure"]

        # Daily SPY return
        spy_ret = (spy_close - prev_close) / prev_close
        # Strategy daily return = exposure × spy_ret (with cash-portion = 0% return for simplicity)
        strat_ret = exposure * spy_ret
        nav = nav * (1 + strat_ret)
        spy_nav = spy_nav * (1 + spy_ret)

        nav_curve.append({
            "date": d,
            "nav": round(nav, 2),
            "spy_nav": round(spy_nav, 2),
            "spy_close": spy_close,
            "active_verb": active["verb"],
            "active_exposure": active["exposure"],
            "spy_ret_pct": round(spy_ret * 100, 4),
            "strat_ret_pct": round(strat_ret * 100, 4),
        })
        daily_records.append({"date": d, "verb": active["verb"], "exposure": exposure,
                              "spy_ret": spy_ret, "strat_ret": strat_ret})
        prev_close = spy_close

    # 4. Per-call P&L attribution
    by_call = []
    if call_segments:
        for seg in call_segments:
            seg_records = [dr for dr in daily_records
                           if seg["start_date"] <= dr["date"] < seg["end_date"]]
            if not seg_records:
                # No trading days yet (call too recent)
                by_call.append({**seg, "n_days": 0, "spy_change_pct": 0,
                                "strat_change_pct": 0, "contribution_pct": 0})
                continue
            spy_cum = 1.0
            strat_cum = 1.0
            for dr in seg_records:
                spy_cum *= (1 + dr["spy_ret"])
                strat_cum *= (1 + dr["strat_ret"])
            by_call.append({
                **seg,
                "n_days": len(seg_records),
                "spy_change_pct": round((spy_cum - 1) * 100, 4),
                "strat_change_pct": round((strat_cum - 1) * 100, 4),
                "contribution_pct": round((strat_cum - 1) * 100, 4),
            })

    # 5. Summary stats
    final_nav = nav_curve[-1]["nav"] if nav_curve else INITIAL_NAV
    final_spy_nav = nav_curve[-1]["spy_nav"] if nav_curve else INITIAL_NAV
    total_return_pct = (final_nav - INITIAL_NAV) / INITIAL_NAV * 100
    spy_return_pct = (final_spy_nav - INITIAL_NAV) / INITIAL_NAV * 100
    alpha_pct = total_return_pct - spy_return_pct

    # Drawdown
    peak = INITIAL_NAV
    max_dd = 0
    for n in nav_curve:
        peak = max(peak, n["nav"])
        dd = (peak - n["nav"]) / peak * 100
        max_dd = max(max_dd, dd)

    # Sharpe
    daily_strat = [dr["strat_ret"] for dr in daily_records]
    if len(daily_strat) >= 5:
        mean_d = sum(daily_strat) / len(daily_strat)
        var = sum((x - mean_d) ** 2 for x in daily_strat) / len(daily_strat)
        std = var ** 0.5
        sharpe = (mean_d / std * (252 ** 0.5)) if std > 0 else None
    else:
        sharpe = None

    n_changes = sum(1 for i in range(1, len(call_segments))
                    if call_segments[i]["verb"] != call_segments[i-1]["verb"])

    out = {
        "v": "1.0",
        "generated_at": now.isoformat(),
        "method": "decisive_call_replay_v1",
        "method_description": (
            "Maps each decisive call (EXIT_ALL_RISK..LEVER) to a SPY-only "
            "exposure level (0% to 200%), holds until next call, computes "
            "daily NAV vs 100% SPY buy-and-hold benchmark. Cash earns 0% "
            "(approximation; real treasury yield ~4-5% would lift returns). "
            "First call's start_date defines the strategy's epoch."
        ),
        "exposure_map": EXPOSURE,
        "summary": {
            "n_calls": len(call_segments),
            "n_changes": n_changes,
            "first_call_date": first_date,
            "last_date": end_date,
            "n_days": len(daily_records),
            "n_trading_days": len(daily_records),
            "initial_nav": INITIAL_NAV,
            "final_nav": round(final_nav, 2),
            "spy_final_nav": round(final_spy_nav, 2),
            "total_return_pct": round(total_return_pct, 4),
            "spy_return_pct": round(spy_return_pct, 4),
            "alpha_vs_spy_pct": round(alpha_pct, 4),
            "max_drawdown_pct": round(max_dd, 4),
            "sharpe_proxy": round(sharpe, 4) if sharpe is not None else None,
        },
        "calls": by_call,
        "nav_curve": nav_curve,
    }
    write_json("backtest/calls-results.json", out)
    duration = round(time.time() - started, 2)
    print(f"[calls-backtest] {len(call_segments)} calls, {len(daily_records)} trading days")
    print(f"[calls-backtest] strategy {total_return_pct:+.2f}%  SPY {spy_return_pct:+.2f}%  alpha {alpha_pct:+.2f}%")
    return {
        "statusCode": 200,
        "body": json.dumps({
            "ok": True,
            "n_calls": len(call_segments),
            "n_trading_days": len(daily_records),
            "total_return_pct": round(total_return_pct, 4),
            "spy_return_pct": round(spy_return_pct, 4),
            "alpha_vs_spy_pct": round(alpha_pct, 4),
            "max_dd_pct": round(max_dd, 4),
            "duration_s": duration,
        }),
    }
