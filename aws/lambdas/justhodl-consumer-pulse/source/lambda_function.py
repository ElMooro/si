"""
justhodl-consumer-pulse — Consumer Health & Job-Postings Pulse.

The platform already runs justhodl-labor-leading for the OFFICIAL labour
read — JOLTS openings, quits, Challenger layoffs, claims. What it has never
had is two things a macro desk watches just as closely:

  1. A dedicated CONSUMER HEALTH composite. ~70% of US GDP is the consumer,
     yet the platform only saw consumer data as scattered inputs to other
     engines. This builds the standalone read: sentiment, real spending,
     real income, retail sales, and credit-card delinquency stress.

  2. ALT-DATA labour demand. Official JOLTS lands six weeks stale. The
     Indeed Hiring Lab job-postings index (IHLIDXUS) is daily and turns
     first — it is the alt-data complement to the official labor-leading
     engine, not a duplicate of it. Paired with temporary-help employment
     (the classic early-cut signal) and Census business-application data
     (formation / dynamism), it is the forward labour read.

Two sub-indices — Consumer Health and Labour Demand (alt-data) — each a
direction-corrected, standardised composite, blended into one Consumer &
Labour Pulse with a regime read. A divergence block flags when labour
demand leads consumer health (historically the order a cycle turns in),
and the engine cross-references labor-leading.json so the alt-data and
official reads sit side by side.

OUTPUT: data/consumer-pulse.json (+ daily snapshot)   SCHEDULE: daily
Built on real FRED data — not investment advice.
"""
import json
import os
import time
import urllib.request
from datetime import datetime, timezone

import boto3
import _fred_shim  # noqa: F401  — cache-first FRED + 429 backoff (ops/1074)

s3 = boto3.client("s3")
S3_BUCKET = "justhodl-dashboard-live"
OUT_KEY = "data/consumer-pulse.json"
FRED_KEY = os.environ.get("FRED_API_KEY", "2f057499936072679d8843d7fce99989")

# (series_id, display name, group, frequency, invert?, weight)
# invert=True : a RISING value is a NEGATIVE for the pulse (delinquencies)
SERIES = [
    ("UMCSENT",       "Consumer Sentiment (U-Mich)",       "Consumer Health", "m", False, 1.1),
    ("RSAFS",         "Retail Sales (Advance)",            "Consumer Health", "m", False, 1.1),
    ("PCEC96",        "Real Consumer Spending (PCE)",      "Consumer Health", "m", False, 1.2),
    ("DSPIC96",       "Real Disposable Personal Income",   "Consumer Health", "m", False, 1.0),
    ("DRCCLACBS",     "Credit-Card Delinquency Rate",      "Consumer Health", "q", True,  0.9),
    ("IHLIDXUS",      "Indeed Job Postings Index",         "Labour Demand",   "d", False, 1.5),
    ("TEMPHELPS",     "Temporary-Help Employment",         "Labour Demand",   "m", False, 1.0),
    ("BABATOTALSAUS", "Business Applications (Census BFS)", "Labour Demand",  "m", False, 0.9),
]

FREQ = {
    "d": {"limit": 700, "zwin": 260, "mom": 20},
    "w": {"limit": 260, "zwin": 156, "mom": 4},
    "m": {"limit": 150, "zwin": 84,  "mom": 3},
    "q": {"limit": 70,  "zwin": 36,  "mom": 2},
}

SIG_LABEL = {2: "STRONG", 1: "FIRM", 0: "NEUTRAL", -1: "SOFT", -2: "WEAK"}


def fred(series_id, limit):
    # FRED API host: api.stlouisfed.org
    url = ("https://api.stlouisfed.org/fred/series/observations"
           f"?series_id={series_id}&api_key={FRED_KEY}&file_type=json"
           f"&sort_order=desc&limit={limit}")
    last_err, d = None, None
    for attempt in range(3):
        try:
            req = urllib.request.Request(
                url, headers={"User-Agent": "justhodl-consumer-pulse/1.0"})
            with urllib.request.urlopen(req, timeout=25) as r:
                d = json.loads(r.read())
            last_err = None
            break
        except Exception as e:
            last_err = e
            if attempt < 2:
                time.sleep(1.0 * (attempt + 1))
    if d is None:
        raise last_err or RuntimeError(f"FRED fetch failed: {series_id}")
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


def to_index(z):
    return int(clamp(round(50 + 16.7 * z), 0, 100))


def process(series_id, name, group, freq, invert, weight):
    p = FREQ.get(freq, FREQ["m"])
    obs = fred(series_id, p["limit"])
    if len(obs) < 8:
        return None
    vals = [v for _, v in obs]
    latest, latest_date = vals[0], obs[0][0]
    zwin = vals[:min(len(vals), p["zwin"])]
    sd = std(zwin)
    z = (latest - mean(zwin)) / sd if sd > 0 else 0.0
    lag = p["mom"]
    prior = vals[lag] if len(vals) > lag else vals[-1]
    mom_z = ((latest - prior) / sd) if sd > 0 else 0.0

    sign = -1.0 if invert else 1.0
    zc = clamp(z * sign, -3.0, 3.0)
    mc = clamp(mom_z * sign, -3.0, 3.0)
    contribution = 0.55 * zc + 0.45 * mc
    sig = five_state(contribution)
    arrow = ("rising" if (latest - prior) > 0 else
             "falling" if (latest - prior) < 0 else "flat")
    return {
        "series": series_id, "name": name, "group": group, "frequency": freq,
        "latest": round(latest, 4), "latest_date": latest_date, "arrow": arrow,
        "level_z": round(zc, 2), "momentum_z": round(mc, 2),
        "contribution": round(contribution, 3), "weight": weight,
        "signal": sig, "signal_label": SIG_LABEL[sig],
        "read": f"{name} {latest:g} ({arrow})",
    }


def labor_leading_context():
    """Cross-reference the official labour engine (labor-leading)."""
    try:
        d = json.loads(s3.get_object(
            Bucket=S3_BUCKET, Key="data/labor-leading.json")["Body"].read())
    except Exception:
        return {"available": False,
                "read": "official labour engine (labor-leading) unavailable"}
    for k in ("headline", "interpretation", "summary", "signal_read",
              "label", "read", "verdict"):
        if d.get(k):
            return {"available": True, "official_read": str(d[k])[:220]}
    return {"available": True,
            "official_read": "official JOLTS/claims read — see labor-leading"}


def lambda_handler(event, context):
    t0 = time.time()
    now = datetime.now(timezone.utc)

    comps, errors = [], []
    for sid, name, group, freq, invert, weight in SERIES:
        try:
            r = process(sid, name, group, freq, invert, weight)
            if r:
                comps.append(r)
            else:
                errors.append(f"{sid}: insufficient history")
        except Exception as e:
            errors.append(f"{sid}: {str(e)[:90]}")

    if len(comps) < 4:
        out = {"schema_version": "1.0", "generated_at": now.isoformat(),
               "ok": False, "regime": "UNKNOWN",
               "error": "insufficient series", "errors": errors,
               "n_ok": len(comps)}
        s3.put_object(Bucket=S3_BUCKET, Key=OUT_KEY,
                      Body=json.dumps(out).encode("utf-8"),
                      ContentType="application/json")
        return {"statusCode": 200, "body": json.dumps({"ok": False})}

    # per-group sub-indices
    sub = {}
    for g in ("Consumer Health", "Labour Demand"):
        gc = [c for c in comps if c["group"] == g]
        if not gc:
            continue
        w = sum(c["weight"] for c in gc)
        gz = sum(c["contribution"] * c["weight"] for c in gc) / w
        gm = sum(c["momentum_z"] * c["weight"] for c in gc) / w
        sub[g] = {"score": to_index(gz), "z": round(gz, 3),
                  "momentum_z": round(gm, 3), "n": len(gc)}

    # headline pulse — blend of all components
    wsum = sum(c["weight"] for c in comps)
    pulse_z = sum(c["contribution"] * c["weight"] for c in comps) / wsum
    pulse_index = to_index(pulse_z)
    mom = sum(c["momentum_z"] * c["weight"] for c in comps) / wsum

    if pulse_index >= 62:
        regime, rlabel = "STRONG", "Consumer & labour demand running hot"
    elif pulse_index >= 54:
        regime, rlabel = "FIRM", "Consumer & labour demand above trend"
    elif pulse_index >= 46:
        regime, rlabel = "STEADY", "Consumer & labour demand around trend"
    elif pulse_index >= 38:
        regime, rlabel = "SOFTENING", "Consumer & labour demand below trend"
    else:
        regime, rlabel = "WEAK", "Consumer & labour demand contracting"
    momentum = ("RISING" if mom > 0.25 else "FALLING" if mom < -0.25
                else "FLAT")

    # divergence — does labour demand lead the consumer?
    ch = sub.get("Consumer Health", {}).get("score")
    ld = sub.get("Labour Demand", {}).get("score")
    if ch is not None and ld is not None:
        gap = ld - ch
        if gap <= -12:
            dflag = "LABOUR LEADING LOWER"
            dread = ("Labour demand has rolled over below consumer health — "
                     "job postings and hiring appetite typically lead "
                     "consumer spending, so the consumer read is at risk of "
                     "following lower.")
        elif gap >= 12:
            dflag = "LABOUR LEADING HIGHER"
            dread = ("Labour demand is running ahead of consumer health — "
                     "hiring appetite is supportive and the consumer data "
                     "may be lagging a genuine improvement.")
        else:
            dflag = "ALIGNED"
            dread = ("Labour demand and consumer health are broadly aligned "
                     "— no leading divergence.")
        divergence = {"consumer_health": ch, "labour_demand": ld,
                      "gap": round(gap, 1), "flag": dflag, "read": dread}
    else:
        divergence = {"flag": "N/A", "read": "one sub-index unavailable"}

    # the alt-data lead — Indeed job postings
    postings = next((c for c in comps if c["series"] == "IHLIDXUS"), None)
    lead = None
    if postings:
        lead = {"series": "IHLIDXUS", "name": postings["name"],
                "latest": postings["latest"],
                "latest_date": postings["latest_date"],
                "momentum_z": postings["momentum_z"],
                "signal_label": postings["signal_label"],
                "read": (f"Indeed job postings {postings['arrow']}, "
                         f"{postings['signal_label'].lower()} — the "
                         f"high-frequency alt-data labour lead.")}

    drivers = sorted(comps, key=lambda c: abs(c["contribution"]), reverse=True)
    headline = (f"Consumer & Labour Pulse {pulse_index}/100 — {regime}, "
                f"momentum {momentum}. Lead driver: {drivers[0]['name']} "
                f"({drivers[0]['signal_label'].lower()}).")

    out = {
        "schema_version": "1.0",
        "method": "consumer_health_and_jobpostings_pulse",
        "generated_at": now.isoformat(),
        "elapsed_s": round(time.time() - t0, 2),
        "ok": True,
        "pulse_index": pulse_index,
        "pulse_z": round(pulse_z, 3),
        "regime": regime,
        "regime_label": rlabel,
        "momentum": momentum,
        "momentum_z": round(mom, 3),
        "headline": headline,
        "sub_indices": sub,
        "divergence": divergence,
        "lead_signal": lead,
        "official_labour_cross_ref": labor_leading_context(),
        "components": comps,
        "n_series": len(SERIES),
        "n_ok": len(comps),
        "errors": errors,
        "note": ("Consumer health composite (sentiment, real spending, real "
                 "income, retail sales, credit-card delinquency) plus the "
                 "alt-data labour lead (Indeed job postings, temp-help "
                 "employment, business applications). Each series is "
                 "direction-corrected and standardised. Complements — does "
                 "not duplicate — the official labor-leading engine. Built "
                 "on real FRED data — not investment advice."),
    }
    s3.put_object(Bucket=S3_BUCKET, Key=OUT_KEY,
                  Body=json.dumps(out, default=str).encode("utf-8"),
                  ContentType="application/json",
                  CacheControl="public, max-age=3600")

    try:
        snap = {"date": now.date().isoformat(), "pulse_index": pulse_index,
                "regime": regime, "momentum": momentum,
                "consumer_health": ch, "labour_demand": ld}
        s3.put_object(
            Bucket=S3_BUCKET,
            Key=f"data/consumer-pulse/snapshots/{now.date().isoformat()}.json",
            Body=json.dumps(snap).encode("utf-8"),
            ContentType="application/json")
    except Exception as e:
        print(f"[consumer-pulse] snapshot skipped: {e}")

    print(f"[consumer-pulse] pulse={pulse_index} regime={regime} "
          f"momentum={momentum} div={divergence.get('flag')} "
          f"{len(comps)}/{len(SERIES)} series {out['elapsed_s']}s")
    return {"statusCode": 200, "body": json.dumps({
        "ok": True, "pulse_index": pulse_index, "regime": regime,
        "momentum": momentum, "divergence_flag": divergence.get("flag")})}
