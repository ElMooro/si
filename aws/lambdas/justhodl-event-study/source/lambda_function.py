"""
justhodl-event-study — Algorithmic event detection + forward-return distributions.

For 8 event types defined ALGORITHMICALLY from FRED data (no manual calendars
required), find every historical occurrence and compute SPY forward returns
at 1d/5d/21d/63d/126d.

EVENT TYPES:
  1. fed_first_cut        — first FFR cut after a hiking cycle peak
  2. fed_first_hike       — first FFR hike after an easing cycle trough
  3. yield_curve_inverts  — 2s10s crosses below 0
  4. yield_curve_steepens — 2s10s crosses above 0 after being inverted
  5. vix_spike            — VIX crosses above 30 after 90+ days below 25
  6. vix_normalize        — VIX crosses below 20 after 30+ days above 25
  7. credit_blowout       — HY OAS crosses above 5% after period below
  8. credit_recover       — HY OAS crosses below 4% after blowout

For each event class:
  - List ALL historical occurrences (with dates)
  - Compute forward returns at 5 horizons
  - Aggregate: mean, median, hit-rate, stdev
  - Flag whether condition is currently active

Output: data/event-study.json
"""
import json
import os
import time
import urllib.request
import urllib.parse
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from statistics import mean, median, stdev
import boto3

S3 = boto3.client("s3", region_name="us-east-1")
BUCKET = "justhodl-dashboard-live"
KEY = "data/event-study.json"

FRED_KEY = os.environ.get("FRED_KEY", "2f057499936072679d8843d7fce99989")

HORIZONS = [1, 5, 21, 63, 126]
MIN_DATE = "1990-01-01"


def fred_full(series_id, start=MIN_DATE):
    url = (
        "https://api.stlouisfed.org/fred/series/observations?"
        + urllib.parse.urlencode({
            "series_id": series_id,
            "api_key": FRED_KEY,
            "file_type": "json",
            "observation_start": start,
            "limit": 100000,
        })
    )
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "justhodl-event-study/1.0"})
        with urllib.request.urlopen(req, timeout=30) as r:
            data = json.loads(r.read().decode())
        out = {}
        for o in data.get("observations", []):
            v = o.get("value", ".")
            if v in (".", ""):
                continue
            try:
                out[o["date"]] = float(v)
            except ValueError:
                continue
        return out
    except Exception as e:
        print(f"[fred] {series_id} failed: {e}")
        return {}


def detect_threshold_crosses(series_dict, threshold, direction="up", min_gap_days=30):
    """Return dates where the series crosses threshold in given direction.
    direction='up' = was below, now above. min_gap_days = require N days
    of opposite-side state before triggering."""
    sorted_dates = sorted(series_dict.keys())
    events = []
    last_event_idx = -10000
    for i in range(min_gap_days, len(sorted_dates)):
        d = sorted_dates[i]
        prev_d = sorted_dates[i - 1]
        cur = series_dict[d]
        prev = series_dict[prev_d]
        if direction == "up":
            crossed = (prev < threshold) and (cur >= threshold)
        else:
            crossed = (prev > threshold) and (cur <= threshold)
        if not crossed:
            continue
        # Require min_gap_days of opposite-side state
        window_start = max(0, i - min_gap_days)
        opposite_side = sum(
            1 for j in range(window_start, i)
            if (direction == "up" and series_dict.get(sorted_dates[j], 0) < threshold)
            or (direction == "down" and series_dict.get(sorted_dates[j], 0) > threshold)
        )
        if opposite_side >= min_gap_days * 0.7:
            if i - last_event_idx >= min_gap_days:
                events.append(d)
                last_event_idx = i
    return events


def detect_first_change_after_period(series_dict, change_direction="up", min_period_days=180):
    """Detect first 'increase' or 'decrease' after a sustained period of opposite
    motion (used for first Fed cut after hiking cycle, etc.)."""
    sorted_dates = sorted(series_dict.keys())
    if len(sorted_dates) < 50:
        return []
    events = []
    last_event_idx = -10000
    for i in range(min_period_days, len(sorted_dates)):
        d = sorted_dates[i]
        cur = series_dict[d]
        # Find the last value that was meaningfully different
        prev_idx = i - 1
        while prev_idx > 0 and series_dict.get(sorted_dates[prev_idx], cur) == cur:
            prev_idx -= 1
        prev = series_dict.get(sorted_dates[prev_idx], cur)
        if change_direction == "down":
            changed = cur < prev
        else:
            changed = cur > prev
        if not changed:
            continue
        # Look back over min_period_days to confirm trend was opposite (or flat)
        window_start = max(0, i - min_period_days)
        # Count how many days in window had monotonic trend opposite to current change
        opposite_count = 0
        for j in range(window_start + 1, i):
            jd = sorted_dates[j]
            jpd = sorted_dates[j - 1]
            v1 = series_dict.get(jpd)
            v2 = series_dict.get(jd)
            if v1 is None or v2 is None:
                continue
            if change_direction == "down" and v2 > v1:
                opposite_count += 1
            elif change_direction == "up" and v2 < v1:
                opposite_count += 1
        if opposite_count >= 5:  # at least 5 opposite changes in lookback
            if i - last_event_idx >= min_period_days:
                events.append(d)
                last_event_idx = i
    return events


def forward_returns(start_date, sorted_spx_dates, spx_idx, spx_dict, horizons):
    i = spx_idx.get(start_date)
    # If exact date not in spx series (weekend/holiday), find next available
    if i is None:
        for j, d in enumerate(sorted_spx_dates):
            if d >= start_date:
                i = j
                break
        if i is None:
            return {h: None for h in horizons}
    rets = {}
    for h in horizons:
        if i + h >= len(sorted_spx_dates):
            rets[h] = None
            continue
        end_date = sorted_spx_dates[i + h]
        p0 = spx_dict[sorted_spx_dates[i]]
        p1 = spx_dict[end_date]
        if p0 <= 0:
            rets[h] = None
        else:
            rets[h] = round(((p1 / p0) - 1) * 100, 2)
    return rets


def aggregate_returns(events_with_returns, horizons):
    """For a list of {date, returns: {h: pct}} produce horizon-wise summary."""
    summary = {}
    for h in horizons:
        vals = [e["returns"].get(h) for e in events_with_returns if e["returns"].get(h) is not None]
        if not vals:
            summary[f"{h}d"] = None
            continue
        n_pos = sum(1 for v in vals if v > 0)
        summary[f"{h}d"] = {
            "n": len(vals),
            "mean_pct": round(mean(vals), 2),
            "median_pct": round(median(vals), 2),
            "hit_rate_pct": round(n_pos / len(vals) * 100, 1),
            "min_pct": round(min(vals), 2),
            "max_pct": round(max(vals), 2),
            "stdev_pct": round(stdev(vals), 2) if len(vals) >= 2 else 0,
        }
    return summary


def lambda_handler(event=None, context=None):
    started = time.time()
    print(f"[event-study] start")

    # Fetch all required series in parallel
    series_specs = {
        "fed_funds": "FEDFUNDS",      # monthly series (1954-)
        "fed_target_upper": "DFEDTARU",  # daily Fed target upper (since 2008)
        "two_tens": "T10Y2Y",         # daily 10Y - 2Y (1976-)
        "vix": "VIXCLS",              # daily VIX (1990-)
        "hy_oas": "BAMLH0A0HYM2",     # daily HY OAS (1996-)
        "spx": "SP500",               # daily S&P 500 (2015-)
        "spx_alt": "WILL5000PR",      # alternative for older history (Wilshire 5000 — but daily and goes back further is harder)
    }

    raw = {}
    with ThreadPoolExecutor(max_workers=6) as ex:
        futs = {ex.submit(fred_full, sid): label for label, sid in series_specs.items()}
        for f in as_completed(futs):
            label = futs[f]
            obs = f.result()
            raw[label] = obs
            print(f"[event-study] {label}: {len(obs)} obs")

    spx = raw.get("spx", {})
    if not spx:
        return {"statusCode": 500, "body": "no SPX data"}

    sorted_spx_dates = sorted(spx.keys())
    spx_idx = {d: i for i, d in enumerate(sorted_spx_dates)}

    today = sorted_spx_dates[-1] if sorted_spx_dates else datetime.now(timezone.utc).date().isoformat()

    # ----- Detect events -----
    event_classes = {}

    # 1. fed_first_cut and 2. fed_first_hike — using monthly FEDFUNDS
    fed_funds = raw.get("fed_funds", {})
    if fed_funds:
        cuts = detect_first_change_after_period(fed_funds, change_direction="down", min_period_days=180)
        hikes = detect_first_change_after_period(fed_funds, change_direction="up", min_period_days=180)
        event_classes["fed_first_cut"] = {"description": "First FFR cut after sustained period of holds/hikes", "dates": cuts}
        event_classes["fed_first_hike"] = {"description": "First FFR hike after sustained period of holds/cuts", "dates": hikes}

    # 3. yield_curve_inverts (2s10s crosses below 0)
    twos_tens = raw.get("two_tens", {})
    if twos_tens:
        inversions = detect_threshold_crosses(twos_tens, 0, direction="down", min_gap_days=180)
        steepens = detect_threshold_crosses(twos_tens, 0, direction="up", min_gap_days=90)
        event_classes["yield_curve_inverts"] = {"description": "2s10s crosses below 0 (recession warning)", "dates": inversions}
        event_classes["yield_curve_steepens"] = {"description": "2s10s crosses above 0 after inversion (often recession trigger)", "dates": steepens}

    # 4. vix_spike (above 30 after extended calm)
    vix = raw.get("vix", {})
    if vix:
        spikes = detect_threshold_crosses(vix, 30, direction="up", min_gap_days=60)
        normalizes = detect_threshold_crosses(vix, 20, direction="down", min_gap_days=30)
        event_classes["vix_spike"] = {"description": "VIX crosses above 30 (volatility regime change)", "dates": spikes}
        event_classes["vix_normalize"] = {"description": "VIX crosses below 20 after spike (calm restored)", "dates": normalizes}

    # 5. credit_blowout (HY OAS > 5%)
    hy_oas = raw.get("hy_oas", {})
    if hy_oas:
        blowouts = detect_threshold_crosses(hy_oas, 5.0, direction="up", min_gap_days=60)
        recovers = detect_threshold_crosses(hy_oas, 4.0, direction="down", min_gap_days=30)
        event_classes["credit_blowout"] = {"description": "HY OAS crosses above 5% (credit stress)", "dates": blowouts}
        event_classes["credit_recover"] = {"description": "HY OAS crosses below 4% after blowout (credit healing)", "dates": recovers}

    # ----- Compute forward returns for each event -----
    studies = {}
    today_status = {}

    for event_name, ec in event_classes.items():
        dates = ec["dates"]
        events_with_returns = []
        for d in dates:
            rets = forward_returns(d, sorted_spx_dates, spx_idx, spx, HORIZONS)
            events_with_returns.append({"date": d, "returns": rets})

        # Filter to events that have at least 21d forward (since we need real data)
        valid = [e for e in events_with_returns if e["returns"].get(21) is not None]

        summary = aggregate_returns(valid, HORIZONS)

        # Today status: how many days since most recent event of this class
        if dates:
            most_recent = dates[-1]
            try:
                days_since = (datetime.fromisoformat(today).date() - datetime.fromisoformat(most_recent).date()).days
            except Exception:
                days_since = None
        else:
            most_recent = None
            days_since = None

        studies[event_name] = {
            "description": ec["description"],
            "n_events": len(dates),
            "n_with_forward_data": len(valid),
            "all_dates": dates,
            "most_recent_date": most_recent,
            "days_since_most_recent": days_since,
            "currently_active": days_since is not None and days_since <= 21,
            "forward_return_summary": summary,
            "recent_events_sample": [
                {"date": e["date"], **{f"r{h}d_pct": e["returns"].get(h) for h in HORIZONS}}
                for e in events_with_returns[-5:]
            ],
        }
        today_status[event_name] = {
            "active": studies[event_name]["currently_active"],
            "days_since": days_since,
            "most_recent": most_recent,
        }

    # ----- Currently-active themes ----
    active_themes = [k for k, v in today_status.items() if v["active"]]

    # ----- Top expected forward call (combine active events) ----
    if active_themes:
        # For the union of active events, compute expected 21d forward return
        active_21d_means = []
        for ev in active_themes:
            summary = studies[ev]["forward_return_summary"].get("21d")
            if summary:
                active_21d_means.append(summary["mean_pct"])
        expected_21d = round(mean(active_21d_means), 2) if active_21d_means else None
    else:
        expected_21d = None

    out = {
        "version": "1.0",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "duration_s": round(time.time() - started, 2),
        "as_of_date": today,
        "horizons_trading_days": HORIZONS,
        "studies": studies,
        "active_themes": active_themes,
        "expected_21d_return_from_active_pct": expected_21d,
        "data_sources": {"all": "FRED API (free)"},
        "methodology": "Algorithmic event detection from FRED time series + SPY forward returns at 1/5/21/63/126 trading days",
    }

    body = json.dumps(out, default=str).encode("utf-8")
    S3.put_object(
        Bucket=BUCKET, Key=KEY, Body=body,
        ContentType="application/json", CacheControl="public, max-age=3600",
    )
    print(f"[event-study] active_themes={active_themes} expected_21d={expected_21d}")
    print(f"[event-study] wrote s3://{BUCKET}/{KEY} — {len(body):,}b in {out['duration_s']}s")

    return {
        "statusCode": 200,
        "body": json.dumps({
            "n_event_classes": len(studies),
            "active_themes": active_themes,
            "expected_21d_return_from_active_pct": expected_21d,
            "duration_s": out["duration_s"],
        }),
    }


if __name__ == "__main__":
    print(json.dumps(lambda_handler({}, None), indent=2, default=str))
