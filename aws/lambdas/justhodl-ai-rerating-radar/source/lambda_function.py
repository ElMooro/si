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
import urllib.error
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed

import boto3

VERSION = "1.1.0"
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


def _fmp(path, tries=4):
    url = f"https://financialmodelingprep.com/stable/{path}{'&' if '?' in path else '?'}apikey={FMP}"
    for i in range(tries):
        try:
            raw = urllib.request.urlopen(
                urllib.request.Request(url, headers={"User-Agent": "jh-rerate"}), timeout=15).read()
            return json.loads(raw)
        except urllib.error.HTTPError as e:
            if e.code in (429, 502, 503):      # rate-limited / transient -> backoff
                time.sleep(1.2 * (i + 1))
                continue
            return None
        except Exception:
            time.sleep(0.4)
            continue
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
    """Revenue: latest + trailing growth (reliable). Forward estimate best-effort (endpoint flaky)."""
    inc = _fmp(f"income-statement?symbol={urllib.parse.quote(symbol)}&limit=3")
    if not (isinstance(inc, list) and inc):
        return None
    latest_rev = _num(inc[0].get("revenue"))
    if not latest_rev or latest_rev <= 0:
        return None
    trailing_growth = None
    p1 = _num(inc[1].get("revenue")) if len(inc) >= 2 else None
    if p1 and p1 > 0:
        trailing_growth = latest_rev / p1 - 1
    trailing_cagr2 = None
    p2 = _num(inc[2].get("revenue")) if len(inc) >= 3 else None
    if p2 and p2 > 0:
        trailing_cagr2 = (latest_rev / p2) ** 0.5 - 1
    fwd_growth = None
    est = _fmp(f"analyst-estimates?symbol={urllib.parse.quote(symbol)}&limit=10")
    if isinstance(est, list) and est:
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        fut = sorted([e for e in est if (e.get("date") or "") > today and _num(e.get("revenueAvg"))],
                     key=lambda e: e["date"])
        if fut:
            if len(fut) >= 2 and _num(fut[1].get("revenueAvg")):
                fwd_growth = (_num(fut[1]["revenueAvg"]) / latest_rev) ** 0.5 - 1
            elif _num(fut[0].get("revenueAvg")):
                fwd_growth = _num(fut[0]["revenueAvg"]) / latest_rev - 1
    if fwd_growth is not None:
        growth, basis = fwd_growth, "forward"
    elif trailing_cagr2 is not None:
        growth, basis = trailing_cagr2, "trailing_2y"
    else:
        growth, basis = trailing_growth, "trailing_1y"
    return {"latest_rev": latest_rev, "trailing_growth": trailing_growth, "fwd_growth": fwd_growth,
            "growth": growth, "growth_basis": basis}


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

    # --- revision-velocity inflection (timing trigger) + contagion inputs ---
    erv = {}
    for r in (_read("data/eps-revision-velocity.json") or {}).get("all_qualifying", []) or []:
        est = r.get("estimates", {}) or {}; rb = r.get("ratings_breadth", {}) or {}
        if r.get("symbol"):
            erv[r["symbol"]] = {"vel": _num(r.get("score")), "fy2_lift": _num(est.get("fy2_lift_pct")),
                                "ups": rb.get("n_upgrades") or 0, "downs": rb.get("n_downgrades") or 0}
    revising_up = set(erv.keys())
    for m in (_read("data/estimate-revisions.json") or {}).get("movers_up", []) or []:
        sym = (m.get("symbol") or m.get("ticker")) if isinstance(m, dict) else m
        if sym:
            revising_up.add(sym)
    for r in (_read("data/analyst-consensus.json") or {}).get("strongest_upgrades_30d", []) or []:
        if isinstance(r, dict) and r.get("ticker"):
            revising_up.add(r["ticker"])
    shrt = {}
    for r in (_read("data/finra-short.json") or {}).get("squeeze_candidates", []) or []:
        if isinstance(r, dict) and r.get("symbol"):
            shrt[r["symbol"]] = _num(r.get("squeeze_score"))
    ai_deal_syms = {x.get("symbol") for x in
                    ((_read("data/deal-scanner.json") or {}).get("summary", {}) or {}).get("ai_deals", []) or []
                    if isinstance(x, dict)}
    sm_long = set()
    for _f in (_read("data/smart-money-13f.json") or {}).get("funds", []) or []:
        for _h in _f.get("top_longs", []) or []:
            if _h.get("ticker"):
                sm_long.add(_h["ticker"])
    attn = {}
    for _r in (_read("data/attention-signals.json") or {}).get("tickers", []) or []:
        if _r.get("symbol"):
            attn[_r["symbol"]] = _r
    # AI-infra layer leaders (largest mkt-cap per layer) + which are revising up
    leaders, _lb = {}, {}
    for _s, _u in uni.items():
        _lk, _mc = _u["layer"], _u.get("market_cap") or 0
        if _lk not in _lb or _mc > _lb[_lk][1]:
            _lb[_lk] = (_s, _mc)
    for _lk, (_ls, _m) in _lb.items():
        leaders[_lk] = _ls
    layer_hot = {lk: (leaders[lk] in revising_up) for lk in leaders}

    syms = list(uni.keys())[:220]
    fund = {}
    with ThreadPoolExecutor(max_workers=6) as ex:
        fut = {ex.submit(fundamentals, s): s for s in syms}
        for f in as_completed(fut):
            r = f.result()
            if r:
                fund[fut[f]] = r

    # build regression points: names with fwd_growth + ev_sales
    pts = []
    for s in syms:
        u, fd = uni[s], fund.get(s)
        if not fd or fd.get("growth") is None or not u.get("market_cap"):
            continue
        ev_sales = u["market_cap"] / fd["latest_rev"]
        if ev_sales <= 0 or ev_sales > 80:   # drop garbage/outliers
            continue
        u["_ev_sales"] = ev_sales
        u["_growth"] = fd["growth"]
        u["_growth_basis"] = fd.get("growth_basis")
        u["_fwd_growth"] = fd.get("fwd_growth")
        u["_trailing_growth"] = fd.get("trailing_growth")
        pts.append((s, fd["growth"], ev_sales))

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
        fg = u["_growth"]
        tg = u["_trailing_growth"]
        zz = z.get(s)
        implied = (a + b * fg) if (a is not None) else None
        discount_pct = round((1 - evs / implied) * 100, 1) if (implied and implied > 0) else None
        accel_flag = s in accel
        bkt = u["cap_bucket"]
        lag_gap = None
        if u["ret_3m"] is not None:
            lag_gap = round((layer_median.get(u["layer"]) or 0) - u["ret_3m"], 1)
        # value-trap guard + revision-velocity inflection (the timing trigger)
        not_decel = accel_flag or (tg is None) or (fg >= 0.7 * tg)
        underpriced = (zz is not None and zz < -0.2)
        ev = erv.get(s, {})
        rising = s in revising_up
        falling = ((ev.get("downs") or 0) > (ev.get("ups") or 0)) and (ev.get("ups") or 0) == 0
        contagion = bool(layer_hot.get(u["layer"]) and (not rising) and fg >= MIN_FWD_GROWTH and underpriced)
        is_candidate = (fg >= MIN_FWD_GROWTH) and underpriced and not_decel and not falling
        # pillar points
        unpriced_pts = max(0.0, -(zz or 0)) * 20
        laggard_pts = max(0.0, min(lag_gap or 0, 60)) * 0.6
        infl_pts = (15 if accel_flag else 0) + max(0.0, min(((fg - (tg or 0)) * 100), 30))
        accum_pts = min(len(u["flow_signals"]) * 8, 32)
        bn_pts = 10 if u["bottleneck"] else 0
        cap_pts = CAP_BOOST.get(bkt, 5)
        rev_pts = (min(ev["vel"], 100) * 0.30) if ev.get("vel") else (16 if rising else 0)
        if falling:
            rev_pts = -12                       # estimates being cut -> re-rating DOWN
        contagion_pts = 24 if contagion else 0  # upstream leader rising, this laggard hasn't
        sq = (shrt.get(s) or 0) >= 70
        deal = s in ai_deal_syms
        smbk = s in sm_long
        _att = attn.get(s, {})
        ins_buy = (_att.get("insider_mspr") or 0) >= 30
        anl_up = (_att.get("analyst_upgrade_mom") or 0) > 0.03
        kick_pts = (10 if sq else 0) + (12 if deal else 0) + (14 if smbk else 0) + (12 if ins_buy else 0) + (10 if anl_up else 0)
        composite = round(unpriced_pts + laggard_pts + infl_pts + rev_pts + accum_pts
                          + bn_pts + cap_pts + contagion_pts + kick_pts, 1)
        why = []
        why.append(f"{fg*100:.0f}% rev growth on {evs:.1f}x EV/Sales"
                   + (f" vs ~{implied:.1f}x growth-implied ({discount_pct:.0f}% below)" if discount_pct is not None else ""))
        if lag_gap and lag_gap > 0:
            why.append(f"lags {u['layer']} peers by {lag_gap:.0f}pp")
        why.append("revenue accelerating" if accel_flag else ("forward > trailing growth" if (tg is not None and fg > tg) else "growth intact"))
        if u["flow_signals"]:
            why.append("accumulation: " + ", ".join(u["flow_signals"][:2]))
        if rising:
            why.append("estimates revising UP" + (f" (velocity {ev['vel']:.0f})" if ev.get("vel") else ""))
        elif falling:
            why.append("⚠ estimates being cut")
        else:
            why.append("estimates still flat — not yet re-rated")
        if contagion:
            why.append(f"★ contagion: {leaders[u['layer']]} (layer leader) revising up, this hasn't")
        if deal:
            why.append("fresh AI deal")
        if smbk:
            why.append("★ smart money long (13F)")
        if ins_buy:
            why.append("insider buying")
        if anl_up:
            why.append("analyst upgrades accelerating")
        rows.append({
            "symbol": s, "name": u["name"], "layer": u["layer"], "cap_bucket": bkt,
            "market_cap": u["market_cap"], "is_small_mid": bkt in SMALL_MID,
            "growth_pct": round(fg * 100, 1), "growth_basis": u.get("_growth_basis"),
            "fwd_growth_pct": round(u["_fwd_growth"] * 100, 1) if u.get("_fwd_growth") is not None else None,
            "trailing_growth_pct": round(tg * 100, 1) if tg is not None else None,
            "ev_sales": round(evs, 2), "ev_sales_implied": round(implied, 2) if implied else None,
            "discount_to_implied_pct": discount_pct, "unpriced_z": round(zz, 2) if zz is not None else None,
            "laggard_gap_pp": lag_gap, "ret_3m_pct": u["ret_3m"],
            "accelerating": accel_flag, "bottleneck": u["bottleneck"],
            "flow_signals": u["flow_signals"], "is_candidate": is_candidate,
            "revision_velocity": round(ev["vel"], 1) if ev.get("vel") else None,
            "estimates_rising": rising, "estimates_falling": falling,
            "fy2_eps_lift_pct": ev.get("fy2_lift"), "contagion": contagion,
            "layer_leader": leaders.get(u["layer"]), "layer_leader_rising": layer_hot.get(u["layer"]),
            "short_squeeze": sq, "ai_deal": deal, "smart_money_backed": smbk,
            "insider_buying": ins_buy, "analyst_upgrading": anl_up,
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
            "contagion_candidates": sorted([r for r in rows if r["contagion"]],
                                           key=lambda x: x["composite"], reverse=True)[:20],
            "rising_and_cheap": [r for r in candidates if r["estimates_rising"]][:20],
            "n_contagion": sum(1 for r in rows if r["contagion"]),
            "n_rising": sum(1 for r in rows if r["estimates_rising"]),
            "layer_leaders": leaders, "layer_leader_rising": layer_hot,
        },
        "all_ranked": rows[:120],
        "methodology": {
            "core": "cross-sectional OLS of EV/Sales on forward 2yr revenue CAGR across the AI-infra cohort; "
                    "negative residual (z<-0.2) = priced below its growth-implied multiple",
            "value_trap_guard": f"candidate requires fwd growth >= {int(MIN_FWD_GROWTH*100)}% AND not decelerating "
                                "(accelerating, or fwd >= 0.7x trailing) — cheap+shrinking is excluded",
            "composite": "unpriced residual + laggard gap + revision-velocity inflection + revision contagion + accumulation + bottleneck + kickers(short/AI-deal) + small/mid cap tilt",
            "inflection_trigger": "eps-revision-velocity (analyst estimate-revision velocity + FY2 lift + upgrades): rising estimates BOOST, estimates being cut DISQUALIFY (re-rating down) — separates value-trap from re-rating",
            "contagion": "AI-infra layer leader (largest mkt-cap) revising up while a layer laggard's estimates are still flat — the upstream to downstream revision-lag window",
            "ev_sales": "marketCap / latest-FY revenue (clean; FMP TTM ev/sales endpoint is unreliable)",
        },
        "sources": ["ai-infra-stack", "revenue-acceleration", "eps-revision-velocity", "estimate-revisions", "analyst-consensus", "finra-short", "deal-scanner", "FMP analyst-estimates", "FMP income-statement"],
        "disclaimer": "Pre-re-rating screen. Real data, research only — not investment advice. "
                      "Estimates can be wrong and cheap can stay cheap; the guard reduces but does not remove value-trap risk.",
        "elapsed_s": round(time.time() - t0, 2),
    }
    s3.put_object(Bucket=S3_BUCKET, Key=OUT_KEY, Body=json.dumps(out).encode(), ContentType="application/json")
    print(f"[rerating] priced={len(rows)} candidates={len(candidates)} "
          f"small_mid={out['summary']['n_small_mid_candidates']} slope={out['regression']['slope_evsales_per_growth']} {out['elapsed_s']}s")
    return {"statusCode": 200, "body": json.dumps({"ok": True, "n_priced": len(rows),
            "n_candidates": len(candidates)})}
