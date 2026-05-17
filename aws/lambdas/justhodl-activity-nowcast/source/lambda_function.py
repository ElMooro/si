"""
justhodl-activity-nowcast — Real-Time Activity Nowcast.

The monthly official data — payrolls, industrial production, retail sales —
is what the tape reacts to, but it lands two-to-six weeks stale. By the time
PAYEMS confirms a slowdown, the slowdown is a quarter old. Macro desks bridge
that gap with a high-frequency nowcast: a basket of weekly and daily series
that turn FIRST.

This engine builds that nowcast from genuinely high-frequency FRED series:

  WEI      Weekly Economic Index (Lewis-Mertens-Stock) — the real-activity
           anchor; itself blends ~10 daily/weekly series (rail, retail,
           electricity output, fuel, steel, claims) scaled to GDP growth.
  ICSA     Initial jobless claims — weekly, the cleanest labour lead.
  CCSA     Continuing claims — weekly, the persistence of labour slack.
  NFCI     Chicago Fed financial conditions — weekly.
  STLFSI4  St Louis Fed financial stress — weekly.
  BAA10Y   Baa corporate credit spread — daily, the credit lead.

Each series is reduced to a standardised contribution (level z-score +
momentum), direction-corrected so "supports activity" is always positive,
and blended into a 0-100 Activity Index with a regime read.

The institutional layer on top: it cross-references the lagging monthly
composite (justhodl-macro-nowcast) and flags DIVERGENCE — when the
high-frequency data has rolled over below, or run hotter than, the official
monthly picture. That gap is the early-warning signal.

OUTPUT: data/activity-nowcast.json (+ daily snapshot)   SCHEDULE: daily
Built on real FRED data — not investment advice.
"""
import json
import os
import time
import urllib.request
from datetime import datetime, timezone

import boto3

s3 = boto3.client("s3")
S3_BUCKET = "justhodl-dashboard-live"
OUT_KEY = "data/activity-nowcast.json"
FRED_KEY = os.environ.get("FRED_API_KEY", "2f057499936072679d8843d7fce99989")

# (series_id, display name, frequency w/d, invert?, weight)
# invert=True : a RISING value is a DRAG on activity (claims, stress, spreads)
SERIES = [
    ("WEI",     "Weekly Economic Index",             "w", False, 1.4),
    ("ICSA",    "Initial Jobless Claims",            "w", True,  1.1),
    ("CCSA",    "Continuing Jobless Claims",         "w", True,  0.9),
    ("NFCI",    "Financial Conditions (Chicago Fed)", "w", True,  1.0),
    ("STLFSI4", "Financial Stress (St Louis Fed)",   "w", True,  1.0),
    ("BAA10Y",  "Baa Corporate Credit Spread",       "d", True,  0.9),
]


def fred(series_id, limit):
    url = (f"https://api.fred.stlouisfed.org/fred/series/observations"
           f"?series_id={series_id}&api_key={FRED_KEY}&file_type=json"
           f"&sort_order=desc&limit={limit}")
    req = urllib.request.Request(
        url, headers={"User-Agent": "justhodl-activity-nowcast"})
    with urllib.request.urlopen(req, timeout=20) as r:
        d = json.loads(r.read())
    out = []
    for o in d.get("observations", []):
        v = o.get("value")
        if v in (None, ".", ""):
            continue
        try:
            out.append((o.get("date"), float(v)))
        except (TypeError, ValueError):
            continue
    return out  # newest-first [(date, value)]


def mean(xs):
    return sum(xs) / len(xs) if xs else 0.0


def std(xs):
    if len(xs) < 2:
        return 0.0
    m = mean(xs)
    return (sum((x - m) ** 2 for x in xs) / (len(xs) - 1)) ** 0.5


def clamp(v, lo, hi):
    return max(lo, min(hi, v))


def five_state(c):
    return (2 if c >= 1.0 else 1 if c >= 0.3 else 0 if c > -0.3
            else -1 if c > -1.0 else -2)


SIG_LABEL = {2: "STRONG TAILWIND", 1: "TAILWIND", 0: "NEUTRAL",
             -1: "DRAG", -2: "STRONG DRAG"}


def process(series_id, name, freq, invert, weight):
    limit = 260 if freq == "w" else 700
    obs = fred(series_id, limit)
    if len(obs) < 12:
        return None
    vals = [v for _, v in obs]
    latest, latest_date = vals[0], obs[0][0]
    z_window = vals[:min(len(vals), 156 if freq == "w" else 504)]
    sd = std(z_window)
    z = (latest - mean(z_window)) / sd if sd > 0 else 0.0
    mom_lag = 4 if freq == "w" else 20
    prior = vals[mom_lag] if len(vals) > mom_lag else vals[-1]
    mom_z = ((latest - prior) / sd) if sd > 0 else 0.0

    sign = -1.0 if invert else 1.0
    zc = clamp(z * sign, -3.0, 3.0)
    mc = clamp(mom_z * sign, -3.0, 3.0)
    contribution = 0.55 * zc + 0.45 * mc          # standardised, -3..3
    sig = five_state(contribution)

    arrow = ("rising" if (latest - prior) > 0 else
             "falling" if (latest - prior) < 0 else "flat")
    read = (f"{name} {latest:g} ({arrow}); "
            f"{'drag' if contribution < -0.1 else 'tailwind' if contribution > 0.1 else 'neutral'} "
            f"on activity")
    return {
        "series": series_id, "name": name, "frequency": freq,
        "latest": round(latest, 4), "latest_date": latest_date,
        "level_z": round(zc, 2), "momentum_z": round(mc, 2),
        "contribution": round(contribution, 3), "weight": weight,
        "signal": sig, "signal_label": SIG_LABEL[sig], "read": read,
    }


def divergence(activity_index):
    """Cross-reference the lagging monthly composite (macro-nowcast)."""
    try:
        d = json.loads(s3.get_object(
            Bucket=S3_BUCKET, Key="data/macro-nowcast.json")["Body"].read())
    except Exception:
        return {"available": False,
                "read": "monthly composite (macro-nowcast) unavailable"}
    monthly = d.get("normalized_score")
    if monthly is None:
        monthly = d.get("score")
    monthly_regime = d.get("regime") or d.get("label")
    if not isinstance(monthly, (int, float)):
        return {"available": False, "monthly_regime": monthly_regime,
                "read": "monthly composite score not numeric"}
    gap = round(activity_index - monthly, 1)
    if gap <= -12:
        flag = "HIGH-FREQ LEADING LOWER"
        read = ("High-frequency activity has rolled over well below the "
                "monthly composite — early warning of a slowdown the "
                "official data has not yet printed.")
    elif gap >= 12:
        flag = "HIGH-FREQ LEADING HIGHER"
        read = ("High-frequency data is running hotter than the monthly "
                "composite — a recovery may be arriving ahead of the "
                "official prints.")
    else:
        flag = "ALIGNED"
        read = ("High-frequency nowcast and the monthly composite agree — "
                "no leading divergence.")
    return {"available": True, "monthly_score": round(monthly, 1),
            "monthly_regime": monthly_regime,
            "highfreq_index": activity_index, "gap": gap,
            "flag": flag, "read": read}


def lambda_handler(event, context):
    t0 = time.time()
    now = datetime.now(timezone.utc)

    comps, errors = [], []
    for sid, name, freq, invert, weight in SERIES:
        try:
            r = process(sid, name, freq, invert, weight)
            if r:
                comps.append(r)
            else:
                errors.append(f"{sid}: insufficient history")
        except Exception as e:
            errors.append(f"{sid}: {str(e)[:90]}")

    if len(comps) < 3:
        out = {"schema_version": "1.0", "generated_at": now.isoformat(),
               "ok": False, "regime": "UNKNOWN",
               "error": "insufficient series", "errors": errors,
               "n_ok": len(comps)}
        s3.put_object(Bucket=S3_BUCKET, Key=OUT_KEY,
                      Body=json.dumps(out).encode("utf-8"),
                      ContentType="application/json")
        return {"statusCode": 200, "body": json.dumps({"ok": False})}

    wsum = sum(c["weight"] for c in comps)
    activity_z = sum(c["contribution"] * c["weight"] for c in comps) / wsum
    activity_index = int(clamp(round(50 + 16.7 * activity_z), 0, 100))
    mom = sum(c["momentum_z"] * c["weight"] for c in comps) / wsum

    if activity_index >= 65:
        regime, rlabel = "ACCELERATING", "Activity running well above trend"
    elif activity_index >= 55:
        regime, rlabel = "EXPANDING", "Activity above trend"
    elif activity_index >= 45:
        regime, rlabel = "STEADY", "Activity around trend"
    elif activity_index >= 35:
        regime, rlabel = "SLOWING", "Activity below trend"
    else:
        regime, rlabel = "CONTRACTING", "Activity well below trend"

    momentum = ("RISING" if mom > 0.25 else "FALLING" if mom < -0.25
                else "FLAT")
    div = divergence(activity_index)

    drivers = sorted(comps, key=lambda c: abs(c["contribution"]),
                     reverse=True)
    headline = (f"Activity Nowcast {activity_index}/100 — {regime}, "
                f"momentum {momentum}. "
                f"Lead driver: {drivers[0]['name']} "
                f"({drivers[0]['signal_label'].lower()}).")

    out = {
        "schema_version": "1.0",
        "method": "high_frequency_activity_nowcast",
        "generated_at": now.isoformat(),
        "elapsed_s": round(time.time() - t0, 2),
        "ok": True,
        "activity_index": activity_index,
        "activity_z": round(activity_z, 3),
        "regime": regime,
        "regime_label": rlabel,
        "momentum": momentum,
        "momentum_z": round(mom, 3),
        "headline": headline,
        "divergence": div,
        "components": comps,
        "n_series": len(SERIES),
        "n_ok": len(comps),
        "errors": errors,
        "note": ("Real-time activity nowcast from high-frequency FRED series "
                 "(weekly economic index, jobless claims, financial "
                 "conditions, credit spreads). Each series is direction-"
                 "corrected and standardised so positive always means a "
                 "tailwind to activity. The divergence block flags when this "
                 "fast read leads the lagging monthly composite. Built on "
                 "real FRED data — not investment advice."),
    }
    s3.put_object(Bucket=S3_BUCKET, Key=OUT_KEY,
                  Body=json.dumps(out, default=str).encode("utf-8"),
                  ContentType="application/json",
                  CacheControl="public, max-age=3600")

    try:
        snap = {"date": now.date().isoformat(),
                "activity_index": activity_index, "regime": regime,
                "momentum": momentum, "activity_z": round(activity_z, 3)}
        s3.put_object(
            Bucket=S3_BUCKET,
            Key=f"data/activity-nowcast/snapshots/{now.date().isoformat()}.json",
            Body=json.dumps(snap).encode("utf-8"),
            ContentType="application/json")
    except Exception as e:
        print(f"[activity-nowcast] snapshot skipped: {e}")

    print(f"[activity-nowcast] index={activity_index} regime={regime} "
          f"momentum={momentum} div={div.get('flag')} "
          f"{len(comps)}/{len(SERIES)} series {out['elapsed_s']}s")
    return {"statusCode": 200, "body": json.dumps({
        "ok": True, "activity_index": activity_index, "regime": regime,
        "momentum": momentum, "divergence_flag": div.get("flag")})}
