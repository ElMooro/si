"""
justhodl-ai-rerating-radar — FIND THE NEXT MU BEFORE IT PUMPS
=============================================================
Thesis (the user's MU/SanDisk insight, formalized): a name re-rates violently when
it has HIGH forward growth on a LOW multiple — the market hasn't yet repriced the
multiple to match the AI-driven growth coming through the P&L. That gap — projected
growth minus PRICED-IN growth — is the alpha.

CORE (novel): across the AI-infra cohort, regress EV/Sales on forward revenue growth.
The names with the largest NEGATIVE residual are cheapest relative to what their growth
deserves = the MU-shaped setups ("not priced for the growth").

FIVE PILLARS per name:
  1. AI beneficiary   — membership in ai-infra-stack (+ bottleneck flag)
  2. Laggard gap      — 3M return below its layer's median (hasn't run yet)
  3. Unpriced growth  — negative EV/Sales-vs-growth residual (the core)
  4. Inflection guard — fwd growth >=15% AND not decelerating (else value trap)
  5. Accumulation     — flow signals already firing (smart money early)

INPUTS  ai-infra-stack.json (universe+returns+flow+cap+mktcap+bottleneck)
        revenue-acceleration.json (accel tiers)  +  FMP /stable analyst-estimates,
        income-statement (clean EV/Sales = marketCap / latest revenue)
OUTPUT  data/ai-rerating-radar.json   SCHEDULE daily 14:15 UTC. Real data, research only.
"""
import json
import time
import urllib.request
import urllib.parse
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed

import boto3

VERSION = "1.0.0"
S3_BUCKET = "justhodl-dashboard-live"
OUT_KEY = "data/ai-rerating-radar.json"
FMP = "wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb"
s3 = boto3.client("s3", region_name="us-east-1")

CAP_BOOST = {"nano": 18, "micro": 16, "small": 12, "mid": 8, "large": 3, "mega": 0}
SMALL_MID = {"nano", "micro", "small", "mid"}
MIN_FWD_GROWTH = 0.15   # value-trap guard floor


def _num(x):
    try:
        v = float(x)
        return v if v == v else None
    except (TypeError, ValueError):
        return None


def _fmp(path):
    url = f"https://financialmodelingprep.com/stable/{path}{'&' if '?' in path else '?'}apikey={FMP}"
    try:
        raw = urllib.request.urlopen(
            urllib.request.Request(url, headers={"User-Agent": "jh-rerate"}), timeout=15).read()
        return json.loads(raw)
    except Exception:
        return None


def _read(key):
    try:
        return json.loads(s3.get_object(Bucket=S3_BUCKET, Key=key)["Body"].read())
    except Exception as e:
        print(f"[read] {key}: {e}")
        return None


def universe_from_stack(stack_doc):
    """Flatten ai-infra-stack names -> base records with returns/flow/cap/mktcap."""
    out, by_layer = {}, {}
    for layer in (stack_doc or {}).get("stack", []):
        lk = layer.get("layer")
        for n in layer.get("names", []):
            sym = n.get("symbol")
            if not sym or sym in out:
                continue
            out[sym] = {
                "symbol": sym, "name": n.get("name"), "layer": lk,
                "cap_bucket": n.get("cap_bucket") or "", "market_cap": n.get("market_cap"),
                "ret_1m": n.get("ret_1m_pct"), "ret_3m": n.get("ret_3m_pct"),
                "flow_signals": n.get("flow_signals") or [], "bottleneck": bool(n.get("bottleneck")),
            }
            by_layer.setdefault(lk, []).append(n.get("ret_3m_pct"))
    layer_median = {}
    for lk, rets in by_layer.items():
        vals = sorted(r for r in rets if r is not None)
        layer_median[lk] = vals[len(vals) // 2] if vals else 0.0
    return out, layer_median


def fundamentals(symbol):
    """latest + prior revenue (trailing growth) and forward 2yr revenue CAGR."""
    inc = _fmp(f"income-statement?symbol={urllib.parse.quote(symbol)}&limit=2")
    if not (isinstance(inc, list) and inc):
        return None
    latest_rev = _num(inc[0].get("revenue"))
    if not latest_rev or latest_rev <= 0:
        return None
    trailing_growth = None
    if len(inc) >= 2 and _num(inc[1].get("revenue")):
        prev = _num(inc[1].get("revenue"))
        if prev and prev > 0:
            trailing_growth = latest_rev / prev - 1
    est = _fmp(f"analyst-estimates?symbol={urllib.parse.quote(symbol)}&limit=10")
    fwd_growth = None
    if isinstance(est, list) and est:
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        fut = sorted([e for e in est if (e.get("date") or "") > today and _num(e.get("revenueAvg"))],
                     key=lambda e: e["date"])
        if fut:
            r1 = _num(fut[0]["revenueAvg"])
            if len(fut) >= 2 and _num(fut[1].get("revenueAvg")):
                r2 = _num(fut[1]["revenueAvg"])
                fwd_growth = (r2 / latest_rev) ** 0.5 - 1     # 2yr forward CAGR
            elif r1:
                fwd_growth = r1 / latest_rev - 1
    return {"latest_rev": latest_rev, "trailing_growth": trailing_growth, "fwd_growth": fwd_growth}


def regress(pts):
    """OLS EV/Sales ~ fwd_growth; return (a, b, residual{}, z{})."""
    n = len(pts)
    if n < 8:
        return None
    xs = [p[1] for p in pts]
    ys = [p[2] for p in pts]
    mx = sum(xs) / n
    my = sum(ys) / n
    sxx = sum((x - mx) ** 2 for x in xs) or 1e-9
    b = sum((xs[i] - mx) * (ys[i] - my) for i in range(n)) / sxx
    a = my - b * mx
    resid = {p[0]: p[2] - (a + b * p[1]) for p in pts}
    rv = list(resid.values())
    mr = sum(rv) / len(rv)
    sd = (sum((r - mr) ** 2 for r in rv) / len(rv)) ** 0.5 or 1e-9
    z = {k: (v - mr) / sd for k, v in resid.items()}
    return a, b, resid, z


def lambda_handler(event, context):
    t0 = time.time()
    stack = _read("data/ai-infra-stack.json")
    uni, layer_median = universe_from_stack(stack)
    if not uni:
        return {"statusCode": 500, "body": "no ai-infra-stack universe"}
    ra = _read("data/revenue-acceleration.json") or {}
    accel = {}
    for q in ra.get("all_qualifying", []) or []:
        if isinstance(q, dict) and q.get("symbol"):
            accel[q["symbol"]] = q.get("tier") or "ACCEL"

    syms = list(uni.keys())[:220]
    fund = {}
    with ThreadPoolExecutor(max_workers=18) as ex:
        fut = {ex.submit(fundamentals, s): s for s in syms}
        for f in as_completed(fut):
            r = f.result()
            if r:
                fund[fut[f]] = r

    # build regression points: names with fwd_growth + ev_sales
    pts = []
    for s in syms:
        u, fd = uni[s], fund.get(s)
        if not fd or fd.get("fwd_growth") is None or not u.get("market_cap"):
            continue
        ev_sales = u["market_cap"] / fd["latest_rev"]
        if ev_sales <= 0 or ev_sales > 80:   # drop garbage/outliers
            continue
        u["_ev_sales"] = ev_sales
        u["_fwd_growth"] = fd["fwd_growth"]
        u["_trailing_growth"] = fd.get("trailing_growth")
        pts.append((s, fd["fwd_growth"], ev_sales))

    reg = regress(pts)
    a = b = None
    z = {}
    if reg:
        a, b, _resid, z = reg

    rows = []
    for s in syms:
        u = uni[s]
        if "_ev_sales" not in u:
            continue
        evs = u["_ev_sales"]
        fg = u["_fwd_growth"]
        tg = u["_trailing_growth"]
        zz = z.get(s)
        implied = (a + b * fg) if (a is not None) else None
        discount_pct = round((1 - evs / implied) * 100, 1) if (implied and implied > 0) else None
        accel_flag = s in accel
        bkt = u["cap_bucket"]
        lag_gap = None
        if u["ret_3m"] is not None:
            lag_gap = round((layer_median.get(u["layer"]) or 0) - u["ret_3m"], 1)
        # value-trap guard
        not_decel = accel_flag or (tg is None) or (fg >= 0.7 * tg)
        underpriced = (zz is not None and zz < -0.2)
        is_candidate = (fg >= MIN_FWD_GROWTH) and underpriced and not_decel
        # pillar points
        unpriced_pts = max(0.0, -(zz or 0)) * 20
        laggard_pts = max(0.0, min(lag_gap or 0, 60)) * 0.6
        infl_pts = (15 if accel_flag else 0) + max(0.0, min(((fg - (tg or 0)) * 100), 30))
        accum_pts = min(len(u["flow_signals"]) * 8, 32)
        bn_pts = 10 if u["bottleneck"] else 0
        cap_pts = CAP_BOOST.get(bkt, 5)
        composite = round(unpriced_pts + laggard_pts + infl_pts + accum_pts + bn_pts + cap_pts, 1)
        why = []
        why.append(f"{fg*100:.0f}% fwd rev growth on {evs:.1f}x EV/Sales"
                   + (f" vs ~{implied:.1f}x growth-implied ({discount_pct:.0f}% below)" if discount_pct is not None else ""))
        if lag_gap and lag_gap > 0:
            why.append(f"lags {u['layer']} peers by {lag_gap:.0f}pp")
        why.append("revenue accelerating" if accel_flag else ("forward > trailing growth" if (tg is not None and fg > tg) else "growth intact"))
        if u["flow_signals"]:
            why.append("accumulation: " + ", ".join(u["flow_signals"][:2]))
        rows.append({
            "symbol": s, "name": u["name"], "layer": u["layer"], "cap_bucket": bkt,
            "market_cap": u["market_cap"], "is_small_mid": bkt in SMALL_MID,
            "fwd_growth_pct": round(fg * 100, 1), "trailing_growth_pct": round(tg * 100, 1) if tg is not None else None,
            "ev_sales": round(evs, 2), "ev_sales_implied": round(implied, 2) if implied else None,
            "discount_to_implied_pct": discount_pct, "unpriced_z": round(zz, 2) if zz is not None else None,
            "laggard_gap_pp": lag_gap, "ret_3m_pct": u["ret_3m"],
            "accelerating": accel_flag, "bottleneck": u["bottleneck"],
            "flow_signals": u["flow_signals"], "is_candidate": is_candidate,
            "composite": composite, "why": "; ".join(why),
        })

    rows.sort(key=lambda x: x["composite"], reverse=True)
    candidates = [r for r in rows if r["is_candidate"]]
    out = {
        "engine": "ai-rerating-radar", "version": VERSION,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "thesis": "Find AI-infra names with high forward growth on a low multiple — not yet "
                  "repriced for the AI-driven growth — before the market re-rates them (the MU setup).",
        "regression": {"intercept": round(a, 3) if a is not None else None,
                       "slope_evsales_per_growth": round(b, 3) if b is not None else None,
                       "n_points": len(pts),
                       "reading": "EV/Sales predicted from forward growth; large negative residual = cheap for the growth"},
        "summary": {
            "n_universe": len(syms), "n_priced": len(rows), "n_candidates": len(candidates),
            "n_small_mid_candidates": sum(1 for r in candidates if r["is_small_mid"]),
            "top_setups": candidates[:25],
            "top_small_mid_setups": [r for r in candidates if r["is_small_mid"]][:15],
            "deepest_discounts": sorted([r for r in candidates if r["discount_to_implied_pct"] is not None],
                                        key=lambda x: x["discount_to_implied_pct"], reverse=True)[:15],
        },
        "all_ranked": rows[:120],
        "methodology": {
            "core": "cross-sectional OLS of EV/Sales on forward 2yr revenue CAGR across the AI-infra cohort; "
                    "negative residual (z<-0.2) = priced below its growth-implied multiple",
            "value_trap_guard": f"candidate requires fwd growth >= {int(MIN_FWD_GROWTH*100)}% AND not decelerating "
                                "(accelerating, or fwd >= 0.7x trailing) — cheap+shrinking is excluded",
            "composite": "unpriced residual (35%) + laggard gap + inflection + accumulation + bottleneck + small/mid cap tilt",
            "ev_sales": "marketCap / latest-FY revenue (clean; FMP TTM ev/sales endpoint is unreliable)",
        },
        "sources": ["ai-infra-stack", "revenue-acceleration", "FMP analyst-estimates", "FMP income-statement"],
        "disclaimer": "Pre-re-rating screen. Real data, research only — not investment advice. "
                      "Estimates can be wrong and cheap can stay cheap; the guard reduces but does not remove value-trap risk.",
        "elapsed_s": round(time.time() - t0, 2),
    }
    s3.put_object(Bucket=S3_BUCKET, Key=OUT_KEY, Body=json.dumps(out).encode(), ContentType="application/json")
    print(f"[rerating] priced={len(rows)} candidates={len(candidates)} "
          f"small_mid={out['summary']['n_small_mid_candidates']} slope={out['regression']['slope_evsales_per_growth']} {out['elapsed_s']}s")
    return {"statusCode": 200, "body": json.dumps({"ok": True, "n_priced": len(rows),
            "n_candidates": len(candidates)})}
