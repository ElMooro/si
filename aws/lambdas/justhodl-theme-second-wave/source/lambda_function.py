"""
justhodl-theme-second-wave — THE SECOND-WAVE LAYER (v2 — ALL-CAPS + small-cap tilt)
====================================================================================
For each hot theme, surface what moves AFTER the leaders:
  1. INFRASTRUCTURE / picks-and-shovels gaining momentum (enablers).
  2. LAGGARDS that haven't pumped yet (in the hot theme, lagging the median).
  3. BIG ORDERS / ACCUMULATION — institutional + coiled-spring footprints,
     SMALL-CAP TILTED (smaller = hotter = more upside potential).

v2 fixes v1's large-cap skew:
  - Theme membership now spans the FULL universe by INDUSTRY (not just ETF
    constituents, which hold large/mid caps) -> micro/nano names included.
    Returns for non-constituent members fetched from FMP /stable/stock-price-change.
  - Accumulation index now unifies SMALL-CAP sources: microcap-float-squeeze,
    finra-short squeeze, volatility-squeeze (coiled spring), pre-pump (OBV accum),
    revenue-acceleration — alongside options-flow / 13F / short-covering.
  - CAP TILT: every ranking adds a cap-size boost (nano>micro>small>mid>large>mega).

PURE SYNTHESIS of existing fresh S3 outputs + bounded FMP return fetch.
OUTPUT data/theme-second-wave.json   SCHEDULE daily 14:00 UTC. Real data, research only.
"""
import json
import time
import urllib.request
import urllib.parse
from datetime import datetime, timezone
from statistics import median
from concurrent.futures import ThreadPoolExecutor, as_completed

import boto3

VERSION = "2.0.0"
S3_BUCKET = "justhodl-dashboard-live"
OUT_KEY = "data/theme-second-wave.json"
FMP = "wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb"
s3 = boto3.client("s3", region_name="us-east-1")

INFRA_HINTS = (
    "equipment", "materials", "component", "electronic", "networking", "instrument",
    "machinery", "power", "utilit", "infrastructure", "engineering", "construction",
    "foundry", "fabricat", "tool", "electrical", "communication equipment", "hardware",
    "storage", "connectivity", "specialty industrial", "diagnostic", "research",
    "life science", "medical instrument", "medical device", "laboratory", "supplies",
    "semiconductor", "metals", "mining", "solar", "battery", "grid",
)
SMALL_BUCKETS = {"nano", "micro", "small"}
CAP_BOOST = {"nano": 30, "micro": 25, "small": 18, "mid": 8, "large": 3, "mega": 0}
CAP_RANK = {"nano": 0, "micro": 1, "small": 2, "mid": 3, "large": 4, "mega": 5}


def _read(key):
    try:
        return json.loads(s3.get_object(Bucket=S3_BUCKET, Key=key)["Body"].read())
    except Exception as e:
        print(f"[read] {key}: {e}")
        return None


def _age(o):
    try:
        return round((datetime.now(timezone.utc) - datetime.fromisoformat(
            o["generated_at"].replace("Z", "+00:00"))).total_seconds() / 86400.0, 1)
    except Exception:
        return None


def _num(x):
    try:
        return float(x)
    except (TypeError, ValueError):
        return None


def fmp_1m(symbol):
    url = ("https://financialmodelingprep.com/stable/stock-price-change?symbol=%s&apikey=%s"
           % (urllib.parse.quote(symbol), FMP))
    try:
        raw = urllib.request.urlopen(
            urllib.request.Request(url, headers={"User-Agent": "jh-sw"}), timeout=12).read()
        d = json.loads(raw)
        if isinstance(d, list) and d:
            d = d[0]
        return _num(d.get("1M")) if isinstance(d, dict) else None
    except Exception:
        return None


def fetch_returns(symbols, cap=550):
    syms = list(dict.fromkeys(symbols))[:cap]
    out = {}
    with ThreadPoolExecutor(max_workers=20) as ex:
        futs = {ex.submit(fmp_1m, s): s for s in syms}
        for f in as_completed(futs):
            v = f.result()
            if v is not None:
                out[futs[f]] = v
    return out


def build_universe_index(universe):
    idx, by_industry = {}, {}
    for s in (universe or {}).get("stocks", []):
        sym = s.get("symbol")
        if not sym:
            continue
        meta = {"name": s.get("name"), "industry": s.get("industry") or "",
                "sector": s.get("sector") or "", "market_cap": s.get("market_cap"),
                "cap_bucket": s.get("cap_bucket") or ""}
        idx[sym] = meta
        if meta["industry"]:
            by_industry.setdefault(meta["industry"], []).append(sym)
    return idx, by_industry


def cap_of(meta, fallback_mc=None):
    b = (meta or {}).get("cap_bucket")
    if b:
        return b
    mc = (meta or {}).get("market_cap") or fallback_mc
    if not mc:
        return ""
    if mc < 5e7:
        return "nano"
    if mc < 3e8:
        return "micro"
    if mc < 2e9:
        return "small"
    if mc < 1e10:
        return "mid"
    if mc < 2e11:
        return "large"
    return "mega"


def is_small(bucket):
    return bucket in SMALL_BUCKETS


def is_infra_industry(industry):
    il = (industry or "").lower()
    return any(h in il for h in INFRA_HINTS)


def build_flow_index():
    """symbol -> list of accumulation/big-order signal dicts (incl small-cap sources)."""
    flows = {}

    def add(sym, sig):
        if sym:
            flows.setdefault(sym, []).append(sig)

    def from_qualifying(doc, sig_type, key="all_qualifying", sym_key="symbol"):
        for q in (doc or {}).get(key, []) or []:
            if isinstance(q, dict):
                mc = (q.get("metrics") or {}).get("market_cap")
                add(q.get(sym_key), {"type": sig_type, "tier": q.get("tier"),
                    "score": q.get("score"), "flags": (q.get("flags") or [])[:4], "mc": mc})

    of = _read("data/options-flow.json"); from_qualifying(of, "OPTIONS_UOA")
    st = _read("data/stealth-accumulation.json")
    for r in (st or {}).get("top_smart_money_only", []) or []:
        if (r.get("n_funds_buying") or 0) > 0 or (r.get("score") or 0) >= 70:
            add(r.get("ticker"), {"type": "SMART_MONEY_13F", "n_funds": r.get("n_funds_buying")})
    for r in (st or {}).get("top_short_covering_only", []) or []:
        add(r.get("ticker"), {"type": "SHORT_COVERING", "z_score": r.get("z_score")})
    sp = _read("data/short-pressure.json")
    for n in (sp or {}).get("names", []) or []:
        if "cover" in (n.get("state") or "").lower():
            add(n.get("ticker"), {"type": "SHORT_COVERING", "z_score": n.get("z_score")})
    # ---- small-cap sources ----
    from_qualifying(_read("data/microcap-float-squeeze.json"), "FLOAT_SQUEEZE")
    fs = _read("data/finra-short.json")
    for r in (fs or {}).get("squeeze_candidates", []) or []:
        add(r.get("symbol"), {"type": "SHORT_SQUEEZE", "score": r.get("squeeze_score"),
            "flags": (r.get("squeeze_flags") or [])[:4]})
    from_qualifying(_read("data/volatility-squeeze.json"), "VOL_COILED_SPRING")
    from_qualifying(_read("data/pre-pump-signals.json"), "OBV_ACCUMULATION")
    ra = _read("data/revenue-acceleration.json"); from_qualifying(ra, "REV_ACCELERATION")
    for q in ((ra or {}).get("summary") or {}).get("microcap_picks", []) or []:
        if isinstance(q, dict):
            add(q.get("symbol"), {"type": "REV_ACCEL_MICROCAP", "score": q.get("score")})
    return flows


def build_laggard_enrich():
    enrich = {}
    bl = _read("data/beta-laggards.json")
    for c in (bl or {}).get("top_candidates", []) or []:
        if c.get("symbol"):
            enrich[c["symbol"]] = {"catch_up_score": c.get("catch_up_score"), "beta": c.get("beta"),
                "upside_pct": c.get("upside_pct"), "why": (c.get("why") or "")[:280],
                "risk": (c.get("risk") or "")[:180], "source": "beta-laggard"}
    sm = _read("data/sympathetic-momentum.json")
    for s in (sm or {}).get("top_setups", []) or []:
        sym = s.get("laggard")
        if sym and sym not in enrich:
            enrich[sym] = {"catch_up_score": round((s.get("score") or 0) * 100, 1),
                "expected_catchup_pct": s.get("expected_catchup_pct"),
                "why": f"Lags peer {s.get('leader')} by {s.get('divergence_sigma')}sigma in "
                       f"{s.get('peer_group')}.", "source": "sympathetic-momentum"}
    return enrich


def select_hot_themes(tr):
    all_themes = {t.get("ticker"): t for t in (tr or {}).get("all_themes", [])}
    breadth = (tr or {}).get("breadth_details", {}) or {}
    hot = []
    for etf, bd in breadth.items():
        meta = all_themes.get(etf)
        if not meta:
            continue
        ms = meta.get("momentum_score") or 0
        if not (ms >= 60 or (meta.get("rs_acceleration", 0) > 0 and meta.get("rs_20d", 0) > 0
                             and meta.get("above_ma50"))):
            continue
        cons = bd.get("constituents_perf") or []
        if cons:
            hot.append((meta, cons, bd.get("breadth")))
    hot.sort(key=lambda x: x[0].get("momentum_score") or 0, reverse=True)
    return hot[:10]


def lambda_handler(event, context):
    t0 = time.time()
    tr = _read("data/theme-rotation.json")
    if not tr:
        return {"statusCode": 500, "body": json.dumps({"err": "no theme-rotation.json"})}
    universe = _read("data/universe.json")
    uni, by_industry = build_universe_index(universe)
    flows = build_flow_index()
    lag_enrich = build_laggard_enrich()
    hot = select_hot_themes(tr)

    # ---- gather ALL-CAP candidate symbols (universe stocks in each hot theme's industries) ----
    theme_inds = {}
    need_returns = []
    for meta, cons, bp in hot:
        inds = set()
        for c in cons:
            m = uni.get(c.get("symbol"))
            if m and m.get("industry"):
                inds.add(m["industry"])
        theme_inds[meta.get("ticker")] = inds
        for ind in inds:
            need_returns.extend(by_industry.get(ind, []))
    # prioritize SMALL caps in the return fetch (the whole point), then mid, then larger
    have_ret = {c.get("symbol") for _, cons, _ in hot for c in cons}
    fetch_list = sorted(set(s for s in need_returns if s not in have_ret),
                        key=lambda s: CAP_RANK.get(uni.get(s, {}).get("cap_bucket"), 9))
    uni_returns = fetch_returns(fetch_list, cap=550)

    themes_out = []
    for meta, cons, bp in hot:
        etf = meta.get("ticker")
        # build candidate pool: constituents (have ret_20d) + universe-in-industry (fetched)
        cand = {}
        rets_for_med = []
        for c in cons:
            sym = c.get("symbol"); r = _num(c.get("ret_20d"))
            if sym and r is not None:
                cand[sym] = {"ret": r, "above_ma50": c.get("above_ma50"), "src": "constituent"}
                rets_for_med.append(r)
        # normalize constituent scale to %
        scale = 100.0 if (rets_for_med and max(abs(x) for x in rets_for_med) < 3) else 1.0
        for s in cand.values():
            s["ret"] *= scale
        med = median([s["ret"] for s in cand.values()]) if cand else 0.0
        for ind in theme_inds.get(etf, set()):
            for sym in by_industry.get(ind, []):
                if sym in cand:
                    continue
                r = uni_returns.get(sym)
                if r is not None:
                    cand[sym] = {"ret": r, "above_ma50": None, "src": "universe"}

        leaders, infra, laggards, big = [], [], [], []
        for sym, cd in cand.items():
            mu = uni.get(sym, {})
            bucket = cap_of(mu)
            small = is_small(bucket)
            r = round(cd["ret"], 1)
            sig = flows.get(sym, [])
            cb = CAP_BOOST.get(bucket, 5)
            base = {"symbol": sym, "name": mu.get("name"), "industry": mu.get("industry"),
                    "market_cap": mu.get("market_cap"), "cap_bucket": bucket,
                    "is_small_cap": small, "ret_1m_pct": r, "src": cd["src"]}
            if r >= med and r > 5 and cd["src"] == "constituent":
                leaders.append({**base, "note": "already extended — context"})
            if is_infra_industry(mu.get("industry")) and (r > 0 or cd.get("above_ma50")):
                infra.append({**base, "infra_score": round(r + cb, 1),
                    "big_order_signals": [s["type"] for s in sig],
                    "momentum_note": f"enabler industry, +{r}%/1m — infrastructure catching the theme"})
            if med >= 4 and r < med - 4 and r > -25:
                e = lag_enrich.get(sym, {})
                laggards.append({**base, "gap_vs_theme_pp": round(med - r, 1),
                    "lag_score": round((med - r) + cb, 1), "catch_up_score": e.get("catch_up_score"),
                    "upside_pct": e.get("upside_pct"), "big_order_signals": [s["type"] for s in sig],
                    "why": e.get("why") or (f"In {meta.get('name')} (theme +{round(med,1)}% median/1m) "
                        f"but {sym} is up only {r}% — a {round(med-r,1)}pp gap not yet closed."),
                    "risk": e.get("risk") or "Catch-up is a tendency, not a certainty.",
                    "confirmed_by": e.get("source")})
            if sig:
                comp = len(sig) * 10 + cb + sum(8 for s in sig if s["type"] in
                    ("OPTIONS_UOA", "SMART_MONEY_13F", "FLOAT_SQUEEZE", "REV_ACCEL_MICROCAP"))
                big.append({**base, "signals": [s["type"] for s in sig], "n_signals": len(sig),
                    "composite": comp, "why": ("small-cap " if small else "") +
                    "theme name with accumulation / coiled-spring footprints — front-runs the move"})

        leaders.sort(key=lambda x: x["ret_1m_pct"], reverse=True)
        infra.sort(key=lambda x: x["infra_score"], reverse=True)       # cap-tilted
        laggards.sort(key=lambda x: x["lag_score"], reverse=True)      # cap-tilted
        big.sort(key=lambda x: x["composite"], reverse=True)           # cap-tilted via cb
        themes_out.append({
            "etf": etf, "name": meta.get("name"), "category": meta.get("category"),
            "momentum_score": meta.get("momentum_score"), "rs_acceleration": meta.get("rs_acceleration"),
            "ret_20d_pct": meta.get("ret_20d"), "breadth_pct": bp,
            "theme_median_ret_pct": round(med, 1), "n_candidates": len(cand),
            "leaders": leaders[:5], "infrastructure": infra[:10],
            "laggards": laggards[:12], "big_orders": big[:12]})

    n_infra = sum(len(t["infrastructure"]) for t in themes_out)
    n_lag = sum(len(t["laggards"]) for t in themes_out)
    n_big = sum(len(t["big_orders"]) for t in themes_out)
    n_small_big = sum(1 for t in themes_out for b in t["big_orders"] if b["is_small_cap"])
    n_small_lag = sum(1 for t in themes_out for b in t["laggards"] if b["is_small_cap"])

    # TOP PICKS — multi-bucket, small-cap tilted
    pm = {}
    for t in themes_out:
        for bucket_name, items in (("laggard", t["laggards"]), ("infrastructure", t["infrastructure"]),
                                   ("big_order", t["big_orders"])):
            for b in items:
                e = pm.setdefault(b["symbol"], {"symbol": b["symbol"], "name": b["name"],
                    "theme": t["name"], "cap_bucket": b["cap_bucket"], "is_small_cap": b["is_small_cap"],
                    "ret_1m_pct": b.get("ret_1m_pct"), "buckets": set(),
                    "signals": b.get("big_order_signals") or b.get("signals") or []})
                e["buckets"].add(bucket_name)
    top = []
    for sym, e in pm.items():
        e["buckets"] = sorted(e["buckets"])
        e["conviction"] = len(e["buckets"]) * 10 + CAP_BOOST.get(e["cap_bucket"], 5) + \
            (8 if e["signals"] else 0)
        if len(e["buckets"]) >= 2 or (e["is_small_cap"] and e["signals"]):
            top.append(e)
    top.sort(key=lambda x: x["conviction"], reverse=True)

    out = {"engine": "theme-second-wave", "version": VERSION,
        "generated_at": datetime.now(timezone.utc).isoformat(), "as_of": tr.get("generated_at"),
        "freshness": {"theme_rotation_age_d": _age(tr),
            "universe_age_d": _age(universe) if universe else None,
            "n_universe_returns_fetched": len(uni_returns)},
        "summary": {"n_hot_themes": len(themes_out), "n_infrastructure": n_infra,
            "n_laggards": n_lag, "n_smallcap_laggards": n_small_lag, "n_big_orders": n_big,
            "n_smallcap_big_orders": n_small_big, "n_top_picks": len(top), "top_picks": top[:20]},
        "hot_themes": themes_out,
        "methodology": {
            "membership": "ETF constituents + FULL universe mapped by industry (all caps); "
                          "non-constituent returns from FMP /stable/stock-price-change 1M",
            "cap_tilt": "every ranking adds cap-size boost nano+30/micro+25/small+18/mid+8/large+3/mega+0 "
                        "— smaller ranks hotter (more upside potential)",
            "infrastructure": "enabler-industry members gaining momentum",
            "laggards": "members lagging theme median by >4pp while theme works",
            "big_orders": "options UOA / 13F / short-covering / FLOAT_SQUEEZE / SHORT_SQUEEZE / "
                          "VOL_COILED_SPRING / OBV_ACCUMULATION / REV_ACCELERATION (small-cap sources)"},
        "sources": ["theme-rotation", "universe", "FMP price-change", "beta-laggards",
            "sympathetic-momentum", "options-flow", "stealth-accumulation", "short-pressure",
            "microcap-float-squeeze", "finra-short", "volatility-squeeze", "pre-pump-signals",
            "revenue-acceleration"],
        "disclaimer": "Second-wave rotation is a tendency, not a certainty. Real data, research only.",
        "elapsed_s": round(time.time() - t0, 2)}
    s3.put_object(Bucket=S3_BUCKET, Key=OUT_KEY, Body=json.dumps(out).encode(),
                  ContentType="application/json")
    print(f"[second-wave v2] themes={len(themes_out)} infra={n_infra} lag={n_lag}(sm {n_small_lag}) "
          f"big={n_big}(sm {n_small_big}) picks={len(top)} fetched={len(uni_returns)} {out['elapsed_s']}s")
    return {"statusCode": 200, "body": json.dumps({"ok": True, "n_hot_themes": len(themes_out),
        "n_laggards": n_lag, "n_smallcap_laggards": n_small_lag, "n_big_orders": n_big,
        "n_smallcap_big_orders": n_small_big, "n_top_picks": len(top)})}
