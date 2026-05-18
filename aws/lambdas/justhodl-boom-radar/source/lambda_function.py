"""
justhodl-boom-radar — Hypergrowth Breakout Radar.

The platform already has deep single-purpose scanners (bagger-engine,
pead-detector, eps-revision-velocity, revenue-acceleration). What it lacked
is the INTERSECTION a retail investor actually wants in one ranked place:
small/mid-cap companies that

  1. KEEP BEATING earnings estimates  (a beat streak — the business is
     outrunning the Street and management is sandbagging guidance),
  2. are GROWING FAST and ACCELERATING (the rate of growth is itself
     rising — the second derivative is what re-rates a stock), and
  3. are still PROFITABLE and CHEAP for that growth (a low PEG — the
     market is still paying a value multiple for a growth engine).

Beat + accelerate + cheap is the classic set-up for a violent re-rating.

Data discipline (v2 — after the FMP field probe, ops 808):
  • Growth is measured on TRAILING-12-MONTH revenue/EPS, not single
    quarters — lumpy, deal-driven businesses (advisory, miners) post wild
    one-quarter YoY swings off weak bases; TTM smooths that noise.
  • Earnings beats come from the FMP `earnings` endpoint (epsActual vs
    epsEstimated), skipping not-yet-reported quarters.
  • The price target is built only from RELIABLE trailing data: a growth-
    justified P/E on a conservative one-year-forward EPS (TTM EPS grown at
    a capped rate). The target is hard-clamped to <=2.5x the current price
    so no data artefact can ever print an absurd upside.

OUTPUT: data/boom-radar.json   SCHEDULE: daily 14:00 UTC
Real data only — FMP /stable/. Research, not investment advice.
"""
import json
import time
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone

import boto3

s3 = boto3.client("s3", region_name="us-east-1")
S3_BUCKET = "justhodl-dashboard-live"
OUT_KEY = "data/boom-radar.json"
FMP = "wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb"
BASE = "https://financialmodelingprep.com/stable"

MCAP_MIN = 300_000_000
MCAP_MAX = 15_000_000_000
PRICE_MIN = 3.0
VOL_MIN = 250_000
UNIVERSE_CAP = 230
WORKERS = 6
TARGET_CAP_MULT = 2.5          # target can never exceed 2.5x current price


def fmp(path, params="", retries=3):
    url = f"{BASE}/{path}?apikey={FMP}{params}"
    for attempt in range(retries):
        try:
            req = urllib.request.Request(
                url, headers={"User-Agent": "JustHodl-BoomRadar/2.0"})
            with urllib.request.urlopen(req, timeout=25) as r:
                return json.loads(r.read().decode("utf-8"))
        except urllib.error.HTTPError as e:
            if e.code == 429:
                time.sleep(1 + attempt * 2 + attempt ** 2)
                continue
            return None
        except Exception:
            if attempt < retries - 1:
                time.sleep(1.0)
                continue
            return None
    return None


def f(v):
    try:
        x = float(v)
        return x if x == x else None
    except (TypeError, ValueError):
        return None


def clamp(v, lo, hi):
    return max(lo, min(hi, v))


def ttm_growth(series, end):
    """TTM-vs-prior-TTM % growth. series newest-first; `end` is the start
    index of the recent 4-quarter window (0 = latest)."""
    if len(series) < end + 8:
        return None
    recent = series[end:end + 4]
    prior = series[end + 4:end + 8]
    if any(v is None for v in recent + prior):
        return None
    r, p = sum(recent), sum(prior)
    if p <= 0:
        return None
    return (r - p) / p * 100.0


def get_universe():
    p = (f"&marketCapMoreThan={MCAP_MIN}&marketCapLowerThan={MCAP_MAX}"
         f"&priceMoreThan={PRICE_MIN}&volumeMoreThan={VOL_MIN}"
         "&isEtf=false&isFund=false&isActivelyTrading=true"
         "&country=US&limit=600")
    rows = fmp("company-screener", p) or []
    names = []
    for r in rows:
        sym = r.get("symbol")
        if not sym or "." in sym or "-" in sym:
            continue
        names.append({"symbol": sym,
                      "name": r.get("companyName") or sym,
                      "market_cap": f(r.get("marketCap")),
                      "sector": r.get("sector")})
    names.sort(key=lambda x: x.get("market_cap") or 0, reverse=True)
    return names[:UNIVERSE_CAP]


def scan_name(meta):
    sym = meta["symbol"]
    try:
        q = fmp("quote", f"&symbol={sym}")
        q = q[0] if isinstance(q, list) and q else None
        if not q:
            return None
        price = f(q.get("price"))
        mcap = f(q.get("marketCap")) or meta.get("market_cap")
        if not price or price < PRICE_MIN or not mcap:
            return None

        # ── quarterly income statement (need >=8q for TTM YoY) ──
        inc = fmp("income-statement",
                  f"&symbol={sym}&period=quarter&limit=9") or []
        if len(inc) < 8:
            return None
        rev = [f(r.get("revenue")) for r in inc]
        eps = [f(r.get("epsDiluted")) for r in inc]
        if eps[0] is None:
            eps = [f(r.get("eps")) for r in inc]

        rev_growth = ttm_growth(rev, 0)
        rev_growth_prev = ttm_growth(rev, 1)
        rev_accel = (rev_growth - rev_growth_prev
                     if rev_growth is not None and rev_growth_prev is not None
                     else None)
        eps_growth = ttm_growth(eps, 0)
        ttm_eps = (sum(eps[0:4])
                   if all(e is not None for e in eps[0:4]) else None)

        # ── earnings beat streak (FMP `earnings` endpoint) ──
        beats, beat_mags = 0, []
        earn = fmp("earnings", f"&symbol={sym}&limit=10")
        if isinstance(earn, list):
            reported = [e for e in earn
                        if f(e.get("epsActual")) is not None]
            reported.sort(key=lambda e: e.get("date") or "", reverse=True)
            for e in reported[:8]:
                act = f(e.get("epsActual"))
                est = f(e.get("epsEstimated"))
                if act is None or est is None:
                    break
                if act > est:
                    beats += 1
                    if est != 0:
                        beat_mags.append((act - est) / abs(est) * 100.0)
                else:
                    break
        avg_beat = sum(beat_mags) / len(beat_mags) if beat_mags else None

        return {
            "symbol": sym, "name": meta["name"],
            "sector": meta.get("sector"),
            "price": round(price, 2),
            "market_cap_usd_mn": round(mcap / 1e6, 0),
            "rev_growth_ttm_pct": (round(rev_growth, 1)
                                   if rev_growth is not None else None),
            "rev_accel_pp": (round(rev_accel, 1)
                             if rev_accel is not None else None),
            "eps_growth_ttm_pct": (round(eps_growth, 1)
                                   if eps_growth is not None else None),
            "beat_streak": beats,
            "avg_beat_pct": round(avg_beat, 1) if avg_beat is not None
            else None,
            "ttm_eps": round(ttm_eps, 2) if ttm_eps is not None else None,
        }
    except Exception:
        return None


def score_and_target(x):
    rev_g = x.get("rev_growth_ttm_pct")
    accel = x.get("rev_accel_pp")
    eps_g = x.get("eps_growth_ttm_pct")
    streak = x.get("beat_streak") or 0
    avg_beat = x.get("avg_beat_pct")
    ttm_eps = x.get("ttm_eps")
    price = x.get("price")

    comps, score = {}, 0.0
    # 1. beat streak & magnitude — 25
    s_beat = clamp(streak / 4.0, 0, 1) * 16
    if avg_beat is not None:
        s_beat += clamp(avg_beat / 15.0, 0, 1) * 9
    comps["beat_streak"] = round(s_beat, 1); score += s_beat
    # 2. revenue growth (TTM) — 20
    s_rev = clamp((rev_g or 0) / 35.0, 0, 1) * 20
    comps["revenue_growth"] = round(s_rev, 1); score += s_rev
    # 3. revenue acceleration — 22 (the re-rating trigger)
    s_acc = clamp((accel or 0) / 10.0, 0, 1) * 22
    comps["revenue_acceleration"] = round(s_acc, 1); score += s_acc
    # 4. earnings growth (TTM) — 13
    s_eps = clamp((eps_g or 0) / 40.0, 0, 1) * 13
    comps["earnings_growth"] = round(s_eps, 1); score += s_eps
    # 5. valuation gap — 20 (cheap for growth = low PEG, trailing P/E)
    pe_ttm = (price / ttm_eps) if (ttm_eps and ttm_eps > 0) else None
    peg = None
    growth_for_peg = eps_g if (eps_g and eps_g > 0) else rev_g
    if pe_ttm and growth_for_peg and growth_for_peg > 0:
        peg = pe_ttm / growth_for_peg
        s_val = clamp((2.0 - peg) / 1.5, 0, 1) * 20
    else:
        s_val = 0.0
    comps["valuation_gap"] = round(s_val, 1); score += s_val
    score = round(clamp(score, 0, 100), 1)

    # ── price target: growth-justified P/E on conservative forward EPS ──
    target = upside = fair_pe = fwd_eps = None
    capped = False
    if ttm_eps and ttm_eps > 0 and price:
        # one year forward EPS — TTM EPS grown at a capped rate
        fwd_growth = clamp((eps_g or rev_g or 10) / 100.0, 0.0, 0.50)
        fwd_eps = round(ttm_eps * (1 + fwd_growth), 2)
        # Lynch-style fair P/E tracks growth, capped for sanity
        g = max(eps_g or 0, rev_g or 0, 10)
        fair_pe = clamp(g, 14, 40)
        if streak >= 3:
            fair_pe = min(fair_pe * 1.10, 44)
        raw_target = fair_pe * fwd_eps
        cap = price * TARGET_CAP_MULT
        target = round(min(raw_target, cap), 2)
        capped = raw_target > cap
        upside = round((target / price - 1) * 100, 1)
        fair_pe = round(fair_pe, 1)

    # ── plain-English reasoning ──
    bits = []
    if streak >= 2:
        bits.append(
            f"has beaten EPS estimates {streak} quarters running"
            + (f" by an average of {avg_beat:.0f}%" if avg_beat else ""))
    if rev_g is not None:
        if accel is not None and accel > 1:
            bits.append(
                f"trailing-12-month revenue growth is accelerating — "
                f"{rev_g:.0f}%, up {accel:.0f}pp from the prior quarter's "
                "trend")
        else:
            bits.append(f"trailing-12-month revenue is growing {rev_g:.0f}%")
    if peg is not None:
        bits.append(
            f"yet it trades at {pe_ttm:.0f}x trailing earnings on roughly "
            f"{growth_for_peg:.0f}% growth — a PEG of {peg:.2f}"
            + (", below the 1.0 'fair-for-growth' line" if peg < 1.0
               else ""))
    reason = (x["name"] + " " + "; ".join(bits) + "."
              if bits else x["name"] + " — limited data.")
    if target and upside is not None:
        reason += (f" A growth-justified {fair_pe:.0f}x on an estimated "
                   f"forward EPS of ${fwd_eps:.2f} implies a target of "
                   f"${target:.2f} — about {upside:+.0f}% from ${price:.2f}")
        reason += (" (target capped at 2.5x price)." if capped else ".")
    return {"score": score, "comps": comps, "peg": peg, "pe_ttm": pe_ttm,
            "fair_pe": fair_pe, "fwd_eps": fwd_eps, "target": target,
            "upside": upside, "capped": capped, "reason": reason}


def grade(s):
    return ("PRIME" if s >= 75 else "STRONG" if s >= 60
            else "BUILDING" if s >= 45 else "WATCH")


def lambda_handler(event, context):
    t0 = time.time()
    now = datetime.now(timezone.utc)

    universe = get_universe()
    if not universe:
        out = {"schema_version": "2.0", "ok": False,
               "generated_at": now.isoformat(),
               "error": "universe fetch failed", "picks": []}
        s3.put_object(Bucket=S3_BUCKET, Key=OUT_KEY,
                      Body=json.dumps(out).encode("utf-8"),
                      ContentType="application/json")
        return {"statusCode": 200, "body": json.dumps({"ok": False})}

    scanned = []
    with ThreadPoolExecutor(max_workers=WORKERS) as ex:
        futs = {ex.submit(scan_name, m): m for m in universe}
        for fut in as_completed(futs):
            row = fut.result()
            if row:
                scanned.append(row)

    picks = []
    for x in scanned:
        # qualifier: profitable, genuinely growing
        if x.get("ttm_eps") is None or x["ttm_eps"] <= 0:
            continue
        if (x.get("rev_growth_ttm_pct") or 0) < 8:
            continue
        r = score_and_target(x)
        if r["score"] < 40:
            continue
        x["boom_score"] = r["score"]
        x["score_components"] = r["comps"]
        x["grade"] = grade(r["score"])
        x["pe_ttm"] = round(r["pe_ttm"], 1) if r["pe_ttm"] else None
        x["peg"] = round(r["peg"], 2) if r["peg"] is not None else None
        x["fair_pe"] = r["fair_pe"]
        x["fwd_eps"] = r["fwd_eps"]
        x["price_target"] = r["target"]
        x["upside_pct"] = r["upside"]
        x["target_capped"] = r["capped"]
        x["reasoning"] = r["reason"]
        picks.append(x)

    picks.sort(key=lambda r: r["boom_score"], reverse=True)
    picks = picks[:60]

    n_prime = sum(1 for p in picks if p["grade"] == "PRIME")
    n_strong = sum(1 for p in picks if p["grade"] == "STRONG")
    top = picks[0] if picks else None
    headline = (
        f"Boom Radar: {n_prime} PRIME + {n_strong} STRONG hypergrowth "
        f"set-ups from {len(scanned)} small/mid-caps scanned."
        + (f" Top: {top['name']} ({top['symbol']}) — boom score "
           f"{top['boom_score']:.0f}, {top['upside_pct']:+.0f}% to target."
           if top and top.get("upside_pct") is not None else "")
        if picks else "Boom Radar: no qualifying set-ups in this scan.")

    out = {
        "schema_version": "2.0",
        "method": "hypergrowth_breakout_radar_ttm",
        "generated_at": now.isoformat(),
        "elapsed_s": round(time.time() - t0, 1),
        "ok": len(picks) > 0,
        "headline": headline,
        "universe_size": len(universe),
        "n_scanned": len(scanned),
        "n_qualified": len(picks),
        "n_prime": n_prime, "n_strong": n_strong,
        "scoring": ("0-100 boom score — beat streak & magnitude (25), TTM "
                    "revenue growth (20), revenue acceleration (22), TTM "
                    "earnings growth (13), valuation gap / low PEG (20). "
                    "Growth uses trailing-12-month figures to remove "
                    "single-quarter noise. Price target = a growth-"
                    "justified P/E (Lynch-style, 14-44x) on a conservative "
                    "one-year-forward EPS, hard-capped at 2.5x price."),
        "picks": picks,
        "disclaimer": ("Research and education only — not investment "
                       "advice. Small/mid-caps are volatile; targets are "
                       "model estimates, not guarantees. Do your own work."),
    }
    s3.put_object(Bucket=S3_BUCKET, Key=OUT_KEY,
                  Body=json.dumps(out, default=str).encode("utf-8"),
                  ContentType="application/json",
                  CacheControl="public, max-age=3600")
    print(f"[boom-radar] {len(picks)} picks ({n_prime} PRIME) from "
          f"{len(scanned)} scanned · {out['elapsed_s']}s")
    return {"statusCode": 200,
            "body": json.dumps({"ok": out["ok"], "n_qualified": len(picks),
                                "n_prime": n_prime,
                                "n_scanned": len(scanned)})}
