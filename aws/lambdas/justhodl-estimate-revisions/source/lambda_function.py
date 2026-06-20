"""justhodl-estimate-revisions — pre-earnings consensus estimate-revision momentum.

The Benzinga earnings feed exposes the *current* consensus estimate per upcoming
(ticker, fiscal period) but no history. This engine builds that history itself:
each day it snapshots forward consensus EPS + revenue estimates and diffs them
against its stored snapshots, producing revision momentum — estimates drifting
UP into a print (a documented pre-earnings anomaly) vs drifting DOWN.

State lives in S3 (estimate-revisions/state.json): per (ticker,period) a short
history of [date, eps_est, rev_est]. First run = baseline only (WARMING); signals
appear once prior snapshots exist.

Bullish upward-revision names (importance-weighted, revenue-confirmed) are logged
to the signal-harvester as eng:estimate-revisions (MEASURE-BEFORE-TRUST).
"""
import json
import time
from datetime import datetime, timezone, date as dtdate

import boto3

from benzinga import fetch_calendar

S3 = boto3.client("s3", region_name="us-east-1")
BUCKET = "justhodl-dashboard-live"
OUT_KEY = "data/estimate-revisions.json"
STATE_KEY = "estimate-revisions/state.json"

HORIZON_DAYS = 75       # track names reporting within this window
MIN_IMPORTANCE = 2
MAX_OBS = 10            # history depth per name
MAX_KEYS = 1200         # bound state size
REV_THRESHOLD_PCT = 1.0  # |revision| below this = noise/affirmed


def getj(key):
    try:
        return json.loads(S3.get_object(Bucket=BUCKET, Key=key)["Body"].read())
    except Exception:
        return None


def _num(v):
    return v if isinstance(v, (int, float)) else None


def _days_between(d1, d2):
    try:
        return (dtdate.fromisoformat(d1) - dtdate.fromisoformat(d2)).days
    except Exception:
        return None


def lambda_handler(event=None, context=None):
    t0 = time.time()
    today = datetime.now(timezone.utc).date().isoformat()

    rows = fetch_calendar(days_ahead=HORIZON_DAYS, min_importance=MIN_IMPORTANCE, limit=1000) or []
    print(f"[revisions] forward calendar rows={len(rows)}")

    state = getj(STATE_KEY) or {"keys": {}}
    keys = state.get("keys", {})

    signals = []
    n_with_history = 0
    for r in rows:
        tk = r.get("ticker")
        fp, fy = r.get("fiscal_period"), r.get("fiscal_year")
        if not tk or not fp:
            continue
        cur_eps = _num(r.get("estimated_eps"))
        cur_rev = _num(r.get("estimated_revenue"))
        if cur_eps is None and cur_rev is None:
            continue
        key = f"{tk}|{fp}|{fy}"
        rec = keys.get(key) or {"t": tk, "fp": fp, "fy": fy, "obs": []}
        obs = rec["obs"]

        eps_rev_pct = eps_rev_recent = None
        rev_rev_pct = None
        baseline_date = None
        if obs:
            n_with_history += 1
            base = obs[0]            # oldest stored
            last = obs[-1]           # most recent stored
            baseline_date = base[0]
            b_eps, l_eps = base[1], last[1]
            b_rev = base[2]
            if cur_eps is not None and isinstance(b_eps, (int, float)) and b_eps:
                eps_rev_pct = round((cur_eps - b_eps) / abs(b_eps) * 100, 2)
            if cur_eps is not None and isinstance(l_eps, (int, float)) and l_eps:
                eps_rev_recent = round((cur_eps - l_eps) / abs(l_eps) * 100, 2)
            if cur_rev is not None and isinstance(b_rev, (int, float)) and b_rev:
                rev_rev_pct = round((cur_rev - b_rev) / abs(b_rev) * 100, 2)

        # append today's observation (one per day)
        if not obs or obs[-1][0] != today:
            obs.append([today, cur_eps, cur_rev])
            rec["obs"] = obs[-MAX_OBS:]
        keys[key] = rec

        if eps_rev_pct is not None and abs(eps_rev_pct) >= REV_THRESHOLD_PCT:
            d2e = _days_between(r.get("date"), today)
            same_dir = (rev_rev_pct is not None
                        and (eps_rev_pct > 0) == (rev_rev_pct > 0))
            signals.append({
                "ticker": tk, "company": r.get("company"),
                "earnings_date": r.get("date"), "session": r.get("session"),
                "days_to_earnings": d2e, "fiscal_period": fp, "fiscal_year": fy,
                "importance": r.get("importance"),
                "current_eps_est": cur_eps, "baseline_eps_est": obs[0][1] if obs else None,
                "eps_rev_pct": eps_rev_pct, "eps_rev_recent_pct": eps_rev_recent,
                "rev_rev_pct": rev_rev_pct, "revenue_confirms": same_dir,
                "baseline_date": baseline_date,
                "n_obs": len(obs),
                "direction": "UP" if eps_rev_pct > 0 else "DOWN",
            })

    # prune reported / overflow
    live = {k: v for k, v in keys.items()
            if not v["obs"] or (_days_between(today, v["obs"][-1][0]) or 0) <= 5}
    # keep names whose latest earnings still ahead by capping to MAX_KEYS most-recently-seen
    if len(live) > MAX_KEYS:
        live = dict(sorted(live.items(), key=lambda kv: kv[1]["obs"][-1][0], reverse=True)[:MAX_KEYS])
    state = {"updated": datetime.now(timezone.utc).isoformat(), "keys": live}
    S3.put_object(Bucket=BUCKET, Key=STATE_KEY, Body=json.dumps(state).encode(),
                  ContentType="application/json")

    up = sorted([s for s in signals if s["direction"] == "UP"],
                key=lambda s: s["eps_rev_pct"], reverse=True)
    down = sorted([s for s in signals if s["direction"] == "DOWN"],
                  key=lambda s: s["eps_rev_pct"])
    # harvestable: upward revision, importance>=2, reporting within 40d, revenue-confirmed
    picks = [s for s in up if (s.get("importance") or 0) >= 2
             and (s.get("days_to_earnings") or 999) <= 40
             and s.get("revenue_confirms")][:15]
    if len(picks) < 8:
        picks += [s for s in up if s not in picks
                  and (s.get("importance") or 0) >= 2][:8 - len(picks)]
    top_picks = [{"ticker": s["ticker"], "score": s["eps_rev_pct"],
                  "earnings_date": s["earnings_date"], "days_to_earnings": s["days_to_earnings"],
                  "revenue_confirms": s["revenue_confirms"]} for s in picks]

    status = ("WARMING" if n_with_history == 0 else
              "LIVE" if n_with_history >= 30 else "PARTIAL")
    out = {
        "engine": "justhodl-estimate-revisions",
        "version": "1.0.0",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "status": status,
        "thesis": "Consensus EPS/revenue estimates revised UP into a print tend to "
                  "drift positively (and vice versa). Built by snapshotting forward "
                  "consensus daily and diffing — revision momentum the raw feed can't show.",
        "horizon_days": HORIZON_DAYS,
        "n_tracked": len(rows),
        "n_with_history": n_with_history,
        "n_state_keys": len(live),
        "upward_revisions": up[:40],
        "downward_revisions": down[:30],
        "top_picks": top_picks,
        "data_source": "Benzinga earnings consensus (via Massive) — self-built history",
        "caveats": [
            "First runs are WARMING — revisions appear once prior daily snapshots exist.",
            "eps_rev_pct is cumulative since the oldest stored snapshot for that name.",
            "Tiny-estimate names can show large % swings; importance + revenue confirm filter.",
            "Picks logged to the harvester and graded vs SPY before any engine trusts them.",
        ],
        "elapsed_s": round(time.time() - t0, 1),
    }
    S3.put_object(Bucket=BUCKET, Key=OUT_KEY, Body=json.dumps(out, default=str).encode(),
                  ContentType="application/json", CacheControl="public, max-age=900")
    return {"statusCode": 200, "body": json.dumps({
        "status": status, "n_tracked": len(rows), "n_with_history": n_with_history,
        "n_up": len(up), "n_down": len(down), "n_picks": len(top_picks),
        "elapsed_s": out["elapsed_s"]})}
