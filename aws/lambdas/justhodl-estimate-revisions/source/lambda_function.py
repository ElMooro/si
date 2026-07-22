"""justhodl-estimate-revisions — pre-earnings estimate momentum (FMP depth + Benzinga freshness).

FUSION:
  - FMP analyst-estimates  -> immediate forward-EPS growth slope, analyst coverage,
    consensus dispersion (deep history; available day 1, no warm-up).
  - Benzinga forward calendar -> the timely current quarter estimate + importance +
    BMO/AMC + earnings date.
  - Self-built daily snapshots (S3 state) -> true revision deltas that accrue over time.

estimate_strength (0-100) is computable on day 1 from FMP; revision deltas refine it
as snapshots accumulate. Bullish names -> signal-harvester eng:estimate-revisions
(MEASURE-BEFORE-TRUST).
"""
import os
import json
import time
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone, date as dtdate

import boto3
from benzinga import fetch_calendar

S3 = boto3.client("s3", region_name="us-east-1")
BUCKET = "justhodl-dashboard-live"
OUT_KEY = "data/estimate-revisions.json"
STATE_KEY = "estimate-revisions/state.json"
FMP_KEY = os.environ.get("FMP_KEY", "wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb")

HORIZON_DAYS = 75
MIN_IMPORTANCE = 2
MAX_OBS = 10
MAX_KEYS = 1200
REV_THRESHOLD_PCT = 1.0
FMP_SEED_CAP = 280       # how many names to enrich with FMP (soonest / most important)


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


def fmp_estimate_profile(ticker):
    """FMP forward annual consensus -> growth slope + coverage + dispersion."""
    url = (f"https://financialmodelingprep.com/stable/analyst-estimates?symbol={ticker}"
           f"&period=annual&limit=6&apikey={FMP_KEY}")
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "jh-rev/1"})
        data = json.loads(urllib.request.urlopen(req, timeout=12).read())
    except Exception:
        return None
    if not isinstance(data, list) or not data:
        return None
    today = dtdate.today().isoformat()
    fwd = sorted([r for r in data if (r.get("date") or "") >= today],
                 key=lambda r: r.get("date"))
    if len(fwd) < 1:
        fwd = sorted(data, key=lambda r: r.get("date"))[-2:]
    cur = fwd[0]
    nxt = fwd[1] if len(fwd) > 1 else None
    eps_cur = _num(cur.get("epsAvg"))
    eps_nxt = _num(nxt.get("epsAvg")) if nxt else None
    growth = None
    if eps_cur and eps_nxt is not None and eps_cur != 0:
        growth = round((eps_nxt - eps_cur) / abs(eps_cur) * 100, 1)
    n_an = cur.get("numAnalystsEps")
    hi, lo, av = _num(cur.get("epsHigh")), _num(cur.get("epsLow")), eps_cur
    disp = round((hi - lo) / abs(av) * 100, 1) if (hi is not None and lo is not None and av) else None
    return {"fwd_eps_growth_pct": growth, "fwd_eps_cur": eps_cur, "fwd_eps_next": eps_nxt,
            "n_analysts": n_an, "dispersion_pct": disp}


def _strength(growth, eps_rev_pct, n_an):
    s = 50.0
    if growth is not None:
        s += max(-30, min(30, growth * 0.5))
    if eps_rev_pct is not None:
        s += max(-15, min(15, eps_rev_pct * 1.5))
    if isinstance(n_an, (int, float)):
        s += min(5, n_an / 4.0)
    return round(max(0, min(100, s)), 1)


def lambda_handler(event=None, context=None):
    t0 = time.time()
    today = datetime.now(timezone.utc).date().isoformat()

    rows = fetch_calendar(days_ahead=HORIZON_DAYS, min_importance=MIN_IMPORTANCE, limit=1000) or []
    print(f"[revisions] forward calendar rows={len(rows)}")

    state = getj(STATE_KEY) or {"keys": {}}
    keys = state.get("keys", {})

    # choose names to FMP-enrich: soonest earnings + highest importance
    def _prio(r):
        d2e = _days_between(r.get("date"), today)
        return ((d2e if d2e is not None else 999), -(r.get("importance") or 0))
    seed_rows = sorted(rows, key=_prio)[:FMP_SEED_CAP]
    fmp = {}
    with ThreadPoolExecutor(max_workers=10) as ex:
        futs = {ex.submit(fmp_estimate_profile, r["ticker"]): r["ticker"]
                for r in seed_rows if r.get("ticker")}
        for f in as_completed(futs):
            try:
                p = f.result()
            except Exception:
                p = None
            if p:
                fmp[futs[f]] = p
    print(f"[revisions] FMP-enriched {len(fmp)} names")

    signals = []
    strength_rows = []
    n_with_history = 0
    for r in rows:
        tk = r.get("ticker")
        fp, fy = r.get("fiscal_period"), r.get("fiscal_year")
        if not tk or not fp:
            continue
        # Benzinga (Massive) has been 403 NOT_AUTHORIZED since 2026-07-15, so
        # estimated_eps arrives empty and every direction collapsed to None.
        # Fall back to the FMP forward consensus we already fetch for this name.
        # Benzinga keeps precedence so entitlement restoration is automatic.
        _prof_early = fmp.get(tk) or {}
        cur_eps = _num(r.get("estimated_eps"))
        if cur_eps is None:
            cur_eps = _num(_prof_early.get("fwd_eps_cur"))
        cur_rev = _num(r.get("estimated_revenue"))
        key = f"{tk}|{fp}|{fy}"
        rec = keys.get(key) or {"t": tk, "fp": fp, "fy": fy, "obs": []}
        obs = rec["obs"]

        eps_rev_pct = eps_rev_recent = rev_rev_pct = baseline_date = None
        if obs:
            n_with_history += 1
            base, last = obs[0], obs[-1]
            baseline_date = base[0]
            b_eps, l_eps, b_rev = base[1], last[1], base[2]
            if cur_eps is not None and isinstance(b_eps, (int, float)) and b_eps:
                eps_rev_pct = round((cur_eps - b_eps) / abs(b_eps) * 100, 2)
            if cur_eps is not None and isinstance(l_eps, (int, float)) and l_eps:
                eps_rev_recent = round((cur_eps - l_eps) / abs(l_eps) * 100, 2)
            if cur_rev is not None and isinstance(b_rev, (int, float)) and b_rev:
                rev_rev_pct = round((cur_rev - b_rev) / abs(b_rev) * 100, 2)
        if not obs or obs[-1][0] != today:
            obs.append([today, cur_eps, cur_rev])
            rec["obs"] = obs[-MAX_OBS:]
        keys[key] = rec

        prof = fmp.get(tk) or {}
        growth = prof.get("fwd_eps_growth_pct")
        n_an = prof.get("n_analysts")
        strength = _strength(growth, eps_rev_pct, n_an)
        d2e = _days_between(r.get("date"), today)
        same_dir = (rev_rev_pct is not None and eps_rev_pct is not None
                    and (eps_rev_pct > 0) == (rev_rev_pct > 0))
        row = {
            "ticker": tk, "company": r.get("company"), "earnings_date": r.get("date"),
            "session": r.get("session"), "days_to_earnings": d2e,
            "fiscal_period": fp, "fiscal_year": fy, "importance": r.get("importance"),
            "current_eps_est": cur_eps, "baseline_eps_est": obs[0][1] if obs else None,
            "eps_rev_pct": eps_rev_pct, "eps_rev_recent_pct": eps_rev_recent,
            "rev_rev_pct": rev_rev_pct, "revenue_confirms": same_dir,
            "baseline_date": baseline_date, "n_obs": len(obs),
            "fwd_eps_growth_pct": growth, "n_analysts": n_an,
            "dispersion_pct": prof.get("dispersion_pct"),
            "estimate_strength": strength,
            "direction": ("UP" if (eps_rev_pct or 0) > 0 else "DOWN" if (eps_rev_pct or 0) < 0 else None),
        }
        if growth is not None or eps_rev_pct is not None:
            strength_rows.append(row)
        if eps_rev_pct is not None and abs(eps_rev_pct) >= REV_THRESHOLD_PCT:
            signals.append(row)

    # prune / bound state
    live = {k: v for k, v in keys.items()
            if not v["obs"] or (_days_between(today, v["obs"][-1][0]) or 0) <= 5}
    if len(live) > MAX_KEYS:
        live = dict(sorted(live.items(), key=lambda kv: kv[1]["obs"][-1][0], reverse=True)[:MAX_KEYS])
    S3.put_object(Bucket=BUCKET, Key=STATE_KEY,
                  Body=json.dumps({"updated": datetime.now(timezone.utc).isoformat(), "keys": live}).encode(),
                  ContentType="application/json")

    up = sorted([s for s in signals if s["direction"] == "UP"], key=lambda s: s["eps_rev_pct"], reverse=True)
    down = sorted([s for s in signals if s["direction"] == "DOWN"], key=lambda s: s["eps_rev_pct"])
    # dedup strength rows by ticker (a name can have multiple fiscal periods); keep strongest
    best_by_tk = {}
    for s in sorted(strength_rows, key=lambda s: s["estimate_strength"], reverse=True):
        best_by_tk.setdefault(s["ticker"], s)
    dedup = list(best_by_tk.values())
    strength_leaders = sorted(dedup, key=lambda s: s["estimate_strength"], reverse=True)[:40]

    # picks: strong estimates within ~2mo of earnings (immediate via FMP), revision-confirmed when warmed
    picks = []
    for s in strength_leaders:
        if s["estimate_strength"] < 62:
            break
        if (s.get("importance") or 0) >= 2 and (s.get("days_to_earnings") or 999) <= 60:
            picks.append(s)
        if len(picks) >= 15:
            break
    top_picks = [{"ticker": s["ticker"], "score": s["estimate_strength"],
                  "eps_rev_pct": s["eps_rev_pct"], "fwd_eps_growth_pct": s["fwd_eps_growth_pct"],
                  "earnings_date": s["earnings_date"], "days_to_earnings": s["days_to_earnings"],
                  "revenue_confirms": s["revenue_confirms"]} for s in picks]

    status = "LIVE" if (len(fmp) > 0 or n_with_history >= 30) else "WARMING"
    # direction_map over ALL enriched rows (strength_rows), not the raw calendar:
    # prefer recent revision, then baseline revision, then strength-derived tilt.
    direction_map = {}
    for _r in strength_rows:
        _t = _r.get("ticker")
        if not _t: continue
        _v = _r.get("eps_rev_recent_pct")
        if _v is None: _v = _r.get("eps_rev_pct")
        if _v is None:
            _st = _r.get("estimate_strength")
            _v = (_st - 50) / 5.0 if isinstance(_st, (int, float)) else None
        if _v is None: continue
        direction_map[_t] = "UP" if _v > 0.5 else "DOWN" if _v < -0.5 else "FLAT"

    out = {
        "engine": "justhodl-estimate-revisions", "version": "2.1.0",
        "generated_at": datetime.now(timezone.utc).isoformat(), "status": status,
        "thesis": "FMP depth (forward-EPS growth, analyst coverage, dispersion — day 1) "
                  "fused with Benzinga freshness (timely current estimate) and self-built "
                  "daily snapshots (true revision deltas that accrue). estimate_strength "
                  "works immediately; revision deltas refine it over time.",
        "horizon_days": HORIZON_DAYS, "n_tracked": len(rows),
        "direction_map": direction_map,
        "n_fmp_enriched": len(fmp), "n_with_history": n_with_history, "n_state_keys": len(live),
        "estimate_strength_leaders": strength_leaders,
        "upward_revisions": up[:40], "downward_revisions": down[:30],
        "top_picks": top_picks,
        "data_source": "FMP analyst-estimates (depth) + Benzinga consensus (freshness, via Massive)",
        "caveats": [
            "estimate_strength available day 1 from FMP forward consensus; revision deltas accrue from daily snapshots.",
            "fwd_eps_growth_pct = next-FY vs current-FY consensus EPS slope.",
            "Tiny-estimate names can show large swings; importance + analyst-coverage filter.",
            "Picks logged to the harvester and graded vs SPY before any engine trusts them.",
        ],
        "elapsed_s": round(time.time() - t0, 1),
    }
    S3.put_object(Bucket=BUCKET, Key=OUT_KEY, Body=json.dumps(out, default=str).encode(),
                  ContentType="application/json", CacheControl="public, max-age=900")
    return {"statusCode": 200, "body": json.dumps({
        "status": status, "n_tracked": len(rows), "n_fmp_enriched": len(fmp),
        "n_with_history": n_with_history, "n_up": len(up), "n_down": len(down),
        "n_strength_leaders": len(strength_leaders), "n_picks": len(top_picks),
        "elapsed_s": out["elapsed_s"]})}
