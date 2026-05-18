"""
justhodl-boom-radar — Hypergrowth Breakout Radar.

The platform already has deep single-purpose scanners (bagger-engine,
pead-detector, eps-revision-velocity, revenue-acceleration, deep-value).
What it lacks is the INTERSECTION a retail investor actually wants in one
place: small- and mid-cap companies that

  1. KEEP BEATING earnings estimates  (a beat streak — management is
     sandbagging and the business is outrunning the Street),
  2. are GROWING FAST and ACCELERATING (revenue growth inflecting up, not
     just high — the second derivative is what re-rates a stock), and
  3. whose PRICE HAS NOT CAUGHT UP (a low PEG — the market is still paying
     a value multiple for a growth engine).

That combination — beat + accelerate + cheap — is the classic set-up for
a violent re-rating. This engine scans a live small/mid-cap universe, scores
each name 0-100 on that DNA, and for every qualifier produces a plain-
English reason to own it and a growth-justified PRICE TARGET (a PEG-fair
forward multiple applied to forward EPS), with the upside spelled out.

OUTPUT: data/boom-radar.json   SCHEDULE: daily 14:00 UTC
Real data only — FMP /stable/. Research, not investment advice.
"""
import json
import time
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone

import boto3

s3 = boto3.client("s3", region_name="us-east-1")
S3_BUCKET = "justhodl-dashboard-live"
OUT_KEY = "data/boom-radar.json"
FMP = "wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb"
BASE = "https://financialmodelingprep.com/stable"

# universe limits — small / mid cap, liquid, real businesses
MCAP_MIN = 300_000_000
MCAP_MAX = 15_000_000_000
PRICE_MIN = 3.0
VOL_MIN = 250_000
UNIVERSE_CAP = 230          # names taken into the deep scan
WORKERS = 6


def fmp(path, params="", retries=3):
    url = f"{BASE}/{path}?apikey={FMP}{params}"
    for attempt in range(retries):
        try:
            req = urllib.request.Request(
                url, headers={"User-Agent": "JustHodl-BoomRadar/1.0"})
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


def pct(a, b):
    """Growth % of a over b, guarding tiny / negative bases."""
    a, b = f(a), f(b)
    if a is None or b is None or b == 0:
        return None
    if b < 0:                       # turn from loss -> not a clean %
        return None
    return (a - b) / b * 100.0


# ───────────────────────── universe ─────────────────────────
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
                      "sector": r.get("sector"),
                      "exchange": r.get("exchangeShortName")
                      or r.get("exchange")})
    # prefer the larger, more liquid end first; cap the deep scan
    names.sort(key=lambda x: x.get("market_cap") or 0, reverse=True)
    return names[:UNIVERSE_CAP]


# ───────────────────────── per-name scan ─────────────────────────
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

        inc = fmp("income-statement",
                  f"&symbol={sym}&period=quarter&limit=9") or []
        if len(inc) < 5:
            return None
        # quarterly revenue / EPS, newest first
        rev = [f(r.get("revenue")) for r in inc]
        eps = [f(r.get("epsdiluted") or r.get("eps")) for r in inc]

        # YoY revenue growth: q0 vs q4, and the prior YoY q1 vs q5
        rev_yoy = pct(rev[0], rev[4]) if len(rev) > 4 else None
        rev_yoy_prior = pct(rev[1], rev[5]) if len(rev) > 5 else None
        rev_accel = (rev_yoy - rev_yoy_prior
                     if rev_yoy is not None and rev_yoy_prior is not None
                     else None)
        eps_yoy = pct(eps[0], eps[4]) if len(eps) > 4 else None
        ttm_eps = (sum(e for e in eps[:4] if e is not None)
                   if all(e is not None for e in eps[:4]) else None)

        # earnings beat streak (graceful if endpoint absent)
        beats, beat_mags = 0, []
        surp = fmp("earnings-surprises", f"&symbol={sym}&limit=8")
        if isinstance(surp, list):
            for s in surp[:6]:
                act = f(s.get("actualEarningResult")
                        or s.get("actualEps") or s.get("eps"))
                est = f(s.get("estimatedEarning")
                        or s.get("estimatedEps") or s.get("epsEstimated"))
                if act is None or est is None:
                    break
                if act > est:
                    beats += 1
                    if est != 0:
                        beat_mags.append((act - est) / abs(est) * 100.0)
                else:
                    break
        avg_beat = (sum(beat_mags) / len(beat_mags)
                    if beat_mags else None)

        # forward EPS estimate (next fiscal year) for the target
        est = fmp("analyst-estimates",
                  f"&symbol={sym}&period=annual&limit=2") or []
        fwd_eps = None
        if isinstance(est, list):
            cand = []
            for e in est:
                v = f(e.get("epsAvg") or e.get("estimatedEpsAvg")
                      or e.get("eps"))
                if v is not None:
                    cand.append(v)
            if cand:
                fwd_eps = max(cand)         # the forward-year estimate

        return {
            "symbol": sym, "name": meta["name"],
            "sector": meta.get("sector"), "exchange": meta.get("exchange"),
            "price": round(price, 2),
            "market_cap_usd_mn": round(mcap / 1e6, 0),
            "rev_growth_yoy_pct": (round(rev_yoy, 1)
                                   if rev_yoy is not None else None),
            "rev_accel_pp": (round(rev_accel, 1)
                             if rev_accel is not None else None),
            "eps_growth_yoy_pct": (round(eps_yoy, 1)
                                   if eps_yoy is not None else None),
            "beat_streak": beats,
            "avg_beat_pct": round(avg_beat, 1) if avg_beat is not None
            else None,
            "ttm_eps": round(ttm_eps, 2) if ttm_eps is not None else None,
            "fwd_eps": round(fwd_eps, 2) if fwd_eps is not None else None,
            "pe_ttm": (round(price / ttm_eps, 1)
                       if ttm_eps and ttm_eps > 0 else None),
            "fwd_pe": (round(price / fwd_eps, 1)
                       if fwd_eps and fwd_eps > 0 else None),
        }
    except Exception:
        return None


# ───────────────────────── score + target ─────────────────────────
def score_and_target(x):
    """Boom score 0-100 + a growth-justified price target + reasoning."""
    rev_g = x.get("rev_growth_yoy_pct")
    accel = x.get("rev_accel_pp")
    eps_g = x.get("eps_growth_yoy_pct")
    streak = x.get("beat_streak") or 0
    avg_beat = x.get("avg_beat_pct")
    fwd_eps = x.get("fwd_eps")
    fwd_pe = x.get("fwd_pe")
    price = x.get("price")

    comps = {}
    score = 0.0
    # 1. beat streak & magnitude — 25
    s_beat = clamp(streak / 4.0, 0, 1) * 16
    if avg_beat is not None:
        s_beat += clamp(avg_beat / 15.0, 0, 1) * 9
    comps["beat_streak"] = round(s_beat, 1); score += s_beat
    # 2. revenue growth — 20
    s_rev = clamp((rev_g or 0) / 40.0, 0, 1) * 20
    comps["revenue_growth"] = round(s_rev, 1); score += s_rev
    # 3. revenue acceleration — 22 (the re-rating trigger)
    s_acc = clamp((accel or 0) / 12.0, 0, 1) * 22
    comps["revenue_acceleration"] = round(s_acc, 1); score += s_acc
    # 4. earnings growth — 13
    s_eps = clamp((eps_g or 0) / 45.0, 0, 1) * 13
    comps["earnings_growth"] = round(s_eps, 1); score += s_eps
    # 5. valuation gap — 20  (cheap relative to growth = low PEG)
    peg = None
    growth_for_peg = eps_g if (eps_g and eps_g > 0) else rev_g
    if fwd_pe and growth_for_peg and growth_for_peg > 0:
        peg = fwd_pe / growth_for_peg
        # PEG 0.5 -> full marks, PEG 2.0+ -> nothing
        s_val = clamp((2.0 - peg) / 1.5, 0, 1) * 20
    else:
        s_val = 0.0
    comps["valuation_gap"] = round(s_val, 1); score += s_val
    score = round(clamp(score, 0, 100), 1)

    # ── price target: a growth-justified forward multiple ──
    target = upside = fair_pe = None
    if fwd_eps and fwd_eps > 0 and price:
        # Lynch-style: a fair P/E tracks the growth rate, capped for sanity.
        g = max(eps_g or 0, rev_g or 0, 10)
        fair_pe = clamp(g, 14, 42)
        # reward a proven beat streak with a small multiple premium
        if streak >= 3:
            fair_pe = min(fair_pe * 1.10, 46)
        target = round(fair_pe * fwd_eps, 2)
        upside = round((target / price - 1) * 100, 1)

    # ── plain-English reasoning ──
    bits = []
    if streak >= 2:
        bits.append(
            f"has beaten EPS estimates {streak} quarters running"
            + (f" (avg +{avg_beat:.0f}%)" if avg_beat else ""))
    if rev_g is not None:
        if accel is not None and accel > 1:
            bits.append(
                f"revenue growth is accelerating — {rev_g:.0f}% YoY, up "
                f"{accel:.0f}pp from the prior quarter")
        else:
            bits.append(f"revenue is growing {rev_g:.0f}% YoY")
    if peg is not None:
        bits.append(
            f"yet it trades at a forward P/E of {fwd_pe:.0f} on roughly "
            f"{growth_for_peg:.0f}% growth — a PEG of {peg:.2f}"
            + (", well below the 1.0 'fair-for-growth' line"
               if peg < 1.0 else ""))
    reason = (x["name"] + " " + "; ".join(bits) + "."
              if bits else x["name"] + " — limited data.")
    if target and upside is not None:
        reason += (f" A growth-justified {fair_pe:.0f}x on forward EPS of "
                   f"${fwd_eps:.2f} implies a target of ${target:.2f} — "
                   f"about {upside:+.0f}% from ${price:.2f}.")
    return score, comps, target, upside, fair_pe, peg, reason


def grade(score):
    if score >= 75:
        return "PRIME"
    if score >= 60:
        return "STRONG"
    if score >= 45:
        return "BUILDING"
    return "WATCH"


# ───────────────────────── handler ─────────────────────────
def lambda_handler(event, context):
    t0 = time.time()
    now = datetime.now(timezone.utc)

    universe = get_universe()
    if not universe:
        out = {"schema_version": "1.0", "ok": False,
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
        score, comps, target, upside, fair_pe, peg, reason = \
            score_and_target(x)
        # qualifier: must actually be growing — drop the flat / shrinking
        if (x.get("rev_growth_yoy_pct") or 0) < 8 and score < 45:
            continue
        x["boom_score"] = score
        x["score_components"] = comps
        x["grade"] = grade(score)
        x["peg"] = round(peg, 2) if peg is not None else None
        x["fair_pe"] = round(fair_pe, 1) if fair_pe is not None else None
        x["price_target"] = target
        x["upside_pct"] = upside
        x["reasoning"] = reason
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
        "schema_version": "1.0",
        "method": "hypergrowth_breakout_radar",
        "generated_at": now.isoformat(),
        "elapsed_s": round(time.time() - t0, 1),
        "ok": len(picks) > 0,
        "headline": headline,
        "universe_size": len(universe),
        "n_scanned": len(scanned),
        "n_qualified": len(picks),
        "n_prime": n_prime, "n_strong": n_strong,
        "scoring": ("0-100 boom score — beat streak & magnitude (25), "
                    "revenue growth (20), revenue acceleration (22), "
                    "earnings growth (13), valuation gap / low PEG (20). "
                    "Price target = a growth-justified P/E (Lynch-style, "
                    "capped 14-46x) on the forward-year EPS estimate."),
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
