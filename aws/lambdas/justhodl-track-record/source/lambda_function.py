"""
justhodl-track-record — Opportunity Engine Track Record

WHAT IT DOES
Turns the daily opportunity snapshots into an honest, auditable scorecard:
"How have the engine's verdicts actually performed?"

For each horizon (30 / 60 / 90 days, and since-inception) it takes the
snapshot from that long ago, marks every pick to its current price, and
reports — per verdict tier — average return, hit rate, and the number
that matters most: ALPHA versus the benchmark (the equal-weight average
return of every S&P 500 stock over the same window). A "+8%" pick means
nothing if the market did +10%; alpha is the real claim.

It tracks EVERY tier, including HIGH RISK and EXPENSIVE — so the record
proves the AVOID calls too, and cannot be cherry-picked.

HONESTY NOTES
  • These are HYPOTHETICAL results — the engine's model verdicts marked to
    market, not real trades. No transaction costs, no slippage.
  • Equal-weight, fixed horizons.
  • A split / data-error guard drops any single-name move beyond +120% /
    -65% (a stock split would otherwise create a fake return).
  • Past performance does not predict future results.

INPUT   data/track-record/snapshots/YYYY-MM-DD.json  (written by the
        opportunity engine) + data/opportunities.json (current prices)
OUTPUT  data/track-record.json        SCHEDULE  daily 15:00 UTC
"""
import json
import os
import time
from datetime import datetime, timezone, date

import boto3

S3_BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
SNAP_PREFIX = "data/track-record/snapshots/"
OUT_KEY = "data/track-record.json"
s3 = boto3.client("s3", region_name="us-east-1")

HORIZONS = [("30d", 30), ("60d", 60), ("90d", 90)]
TIERS = ["STRONG OPPORTUNITY", "OPPORTUNITY", "FAIR VALUE",
         "EXPENSIVE", "HIGH RISK", "HOLD / NEUTRAL"]
RET_HI, RET_LO = 1.20, -0.65  # split / data-error guard


def num(v):
    try:
        f = float(v)
        return f if f == f else None
    except (TypeError, ValueError):
        return None


def median(xs):
    xs = sorted(x for x in xs if x is not None)
    n = len(xs)
    if n == 0:
        return None
    return xs[n // 2] if n % 2 else (xs[n // 2 - 1] + xs[n // 2]) / 2.0


def mean(xs):
    xs = [x for x in xs if x is not None]
    return sum(xs) / len(xs) if xs else None


def list_snapshots():
    """Return [(date, key), ...] sorted oldest-first."""
    out = []
    tok = None
    while True:
        kw = {"Bucket": S3_BUCKET, "Prefix": SNAP_PREFIX}
        if tok:
            kw["ContinuationToken"] = tok
        resp = s3.list_objects_v2(**kw)
        for o in resp.get("Contents", []):
            name = o["Key"].split("/")[-1]
            if name.endswith(".json"):
                try:
                    d = date.fromisoformat(name[:-5])
                    out.append((d, o["Key"]))
                except ValueError:
                    pass
        if resp.get("IsTruncated"):
            tok = resp.get("NextContinuationToken")
        else:
            break
    out.sort()
    return out


def load(key):
    try:
        return json.loads(s3.get_object(Bucket=S3_BUCKET, Key=key)["Body"].read())
    except Exception as e:
        print(f"[track] WARN read {key}: {e}")
        return None


def pick_snapshot(snaps, days_old):
    """Newest snapshot at least `days_old` days old, else None."""
    target = date.today() - __import__("datetime").timedelta(days=days_old)
    eligible = [(d, k) for d, k in snaps if d <= target]
    return eligible[-1] if eligible else None


def evaluate(snap, current):
    """Mark a snapshot to current prices. Returns per-tier stats + benchmark."""
    picks = (snap or {}).get("picks", {})
    rets = []          # (ticker, tier, ret)
    dropped = 0
    for tk, p in picks.items():
        entry = num(p.get("p"))
        cur = num((current.get(tk) or {}).get("price"))
        if not entry or not cur or entry <= 0 or cur <= 0:
            continue
        r = cur / entry - 1.0
        if r > RET_HI or r < RET_LO:
            dropped += 1
            continue
        rets.append((tk, p.get("v"), r))
    if not rets:
        return None
    uni = mean([r for _, _, r in rets])
    tiers = {}
    for tier in TIERS:
        tr = [(tk, r) for tk, v, r in rets if v == tier]
        if not tr:
            continue
        vals = [r for _, r in tr]
        tr_sorted = sorted(tr, key=lambda x: x[1])
        tiers[tier] = {
            "n": len(tr),
            "avg_return_pct": round(mean(vals) * 100, 2),
            "median_return_pct": round(median(vals) * 100, 2),
            "hit_rate_pct": round(100 * sum(1 for v in vals if v > 0) / len(vals), 1),
            "avg_alpha_pct": round((mean(vals) - uni) * 100, 2),
            "beat_benchmark_pct": round(100 * sum(1 for v in vals if v > uni) / len(vals), 1),
            "best": {"ticker": tr_sorted[-1][0],
                     "return_pct": round(tr_sorted[-1][1] * 100, 2)},
            "worst": {"ticker": tr_sorted[0][0],
                      "return_pct": round(tr_sorted[0][1] * 100, 2)},
        }
    return {"n_picks": len(rets), "n_dropped_outliers": dropped,
            "universe_return_pct": round(uni * 100, 2), "tiers": tiers}


def lambda_handler(event, context):
    t0 = time.time()
    snaps = list_snapshots()
    opp = load("data/opportunities.json") or {}
    current = {r["ticker"]: {"price": r.get("price"), "verdict": r.get("verdict")}
               for r in opp.get("all", [])}

    horizons = {}
    today = date.today()
    for label, days in HORIZONS:
        chosen = pick_snapshot(snaps, days)
        if not chosen:
            horizons[label] = {"ready": False,
                               "note": f"needs a snapshot {days}+ days old"}
            continue
        d, key = chosen
        res = evaluate(load(key), current)
        if not res:
            horizons[label] = {"ready": False, "note": "no priceable picks"}
            continue
        res.update({"ready": True, "snapshot_date": d.isoformat(),
                    "days_elapsed": (today - d).days})
        horizons[label] = res

    # since-inception
    if snaps:
        d0, k0 = snaps[0]
        res = evaluate(load(k0), current)
        if res:
            res.update({"ready": (today - d0).days >= 1,
                        "snapshot_date": d0.isoformat(),
                        "days_elapsed": (today - d0).days})
            horizons["inception"] = res
        else:
            horizons["inception"] = {"ready": False, "note": "no priceable picks"}

    # headline — longest ready horizon, STRONG OPPORTUNITY tier
    headline, ready = None, [l for l, _ in HORIZONS
                             if horizons.get(l, {}).get("ready")]
    if ready:
        lbl = ready[-1]
        h = horizons[lbl]
        so = h["tiers"].get("STRONG OPPORTUNITY") or h["tiers"].get("OPPORTUNITY")
        if so:
            a = so["avg_alpha_pct"]
            tname = "STRONG OPPORTUNITY" if "STRONG OPPORTUNITY" in h["tiers"] else "OPPORTUNITY"
            headline = (f"{tname} picks are {'beating' if a >= 0 else 'trailing'} "
                        f"the market by {abs(a):.1f}% over {h['days_elapsed']} days "
                        f"({so['avg_return_pct']:+.1f}% vs universe "
                        f"{h['universe_return_pct']:+.1f}%).")
    if not headline:
        n = len(snaps)
        headline = (f"Track record accruing — {n} day{'s' if n != 1 else ''} "
                    f"logged. First 30-day results unlock once a snapshot is "
                    f"30 days old.")

    out = {
        "schema_version": "1.0",
        "method": "snapshot_mark_to_market_alpha",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "elapsed_s": round(time.time() - t0, 2),
        "n_snapshots": len(snaps),
        "inception_date": snaps[0][0].isoformat() if snaps else None,
        "days_logged": (today - snaps[0][0]).days if snaps else 0,
        "headline": headline,
        "horizons": horizons,
        "methodology": ("Each day the Opportunity Engine logs every S&P 500 "
                        "verdict and price. We mark each past pick to its "
                        "current price and report return by verdict tier. "
                        "ALPHA = a tier's average return minus the benchmark "
                        "(the equal-weight average return of all stocks in "
                        "that snapshot). Hypothetical, equal-weight, no costs; "
                        "a +120% / -65% guard removes stock-split distortions."),
        "disclaimer": ("Hypothetical results — model verdicts marked to "
                       "market, not actual trades. Past performance does not "
                       "predict future results. Research and education only."),
    }
    s3.put_object(Bucket=S3_BUCKET, Key=OUT_KEY,
                  Body=json.dumps(out, default=str).encode("utf-8"),
                  ContentType="application/json",
                  CacheControl="public, max-age=3600")
    print(f"[track] {len(snaps)} snapshots · ready={ready} · {out['elapsed_s']}s")
    return {"statusCode": 200, "body": json.dumps({
        "ok": True, "n_snapshots": len(snaps), "ready_horizons": ready,
        "headline": headline})}
