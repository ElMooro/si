"""
justhodl-theme-second-wave — THE SECOND-WAVE LAYER
==================================================

User insight (extends justhodl-theme-cascade): when capital rotates into a theme,
the obvious leaders run FIRST. The durable edge is the *second wave* — the names
that move AFTER the leaders:

  1. INFRASTRUCTURE / picks-and-shovels of the theme that are themselves GAINING
     momentum (the enablers: equipment, materials, power, networking, tools).
  2. LAGGARDS that haven't pumped yet — members of the hot theme trading well
     below the theme's leaders, healthy enough to close the gap (industry
     information diffusion; Hou 2007, Cohen-Frazzini 2008, Asness 1995).
  3. BIG ORDERS — quiet institutional accumulation (bullish options UOA, 13F
     adds, short-covering) on theme names, *weighted toward small caps* where a
     block order is far more informative and front-runs the move.

theme-cascade scores names already moving; this engine finds what moves NEXT.

PURE SYNTHESIS — reads only existing fresh S3 outputs, no external API calls:
  data/theme-rotation.json     hot themes + per-theme constituents (ret_20d, weight)
  data/universe.json           symbol -> industry / market_cap / cap_bucket
  data/beta-laggards.json      sector catch-up candidates (enrich laggard cards)
  data/sympathetic-momentum.json  sub-industry laggard setups
  data/options-flow.json       bullish options unusual activity
  data/stealth-accumulation.json  13F adds / short-covering / options convergence
  data/short-pressure.json     short-volume z-score (building vs covering)

OUTPUT data/theme-second-wave.json   SCHEDULE daily 14:00 UTC (after inputs refresh).
Real data only. Research, not advice.
"""
import json
import time
from datetime import datetime, timezone
from statistics import median

import boto3

VERSION = "1.0.0"
S3_BUCKET = "justhodl-dashboard-live"
OUT_KEY = "data/theme-second-wave.json"
s3 = boto3.client("s3", region_name="us-east-1")

# enabler / picks-and-shovels industry hints (lowercased substring match)
INFRA_HINTS = (
    "equipment", "materials", "component", "electronic", "networking", "instrument",
    "machinery", "power", "utilit", "infrastructure", "engineering", "construction",
    "foundry", "fabricat", "tool", "electrical", "communication equipment", "hardware",
    "storage", "connectivity", "specialty industrial", "diagnostic", "research",
    "life science", "medical instrument", "medical device", "laboratory", "supplies",
    "semiconductor equipment", "metals", "mining",
)
SMALL_BUCKETS = {"nano", "micro", "small"}


def _read(key):
    try:
        return json.loads(s3.get_object(Bucket=S3_BUCKET, Key=key)["Body"].read())
    except Exception as e:
        print(f"[read] {key}: {e}")
        return None


def _age_days(obj):
    try:
        return round((datetime.now(timezone.utc) - datetime.fromisoformat(
            obj["generated_at"].replace("Z", "+00:00"))).total_seconds() / 86400.0, 1)
    except Exception:
        return None


def _num(x):
    try:
        return float(x)
    except (TypeError, ValueError):
        return None


def build_universe_index(universe):
    idx = {}
    for s in (universe or {}).get("stocks", []):
        sym = s.get("symbol")
        if sym:
            idx[sym] = {
                "name": s.get("name"), "industry": s.get("industry") or "",
                "sector": s.get("sector") or "", "market_cap": s.get("market_cap"),
                "cap_bucket": s.get("cap_bucket") or "",
            }
    return idx


def is_small(meta):
    if not meta:
        return False
    if meta.get("cap_bucket") in SMALL_BUCKETS:
        return True
    mc = meta.get("market_cap")
    return bool(mc and mc < 2_000_000_000)


def is_infra_industry(industry):
    il = (industry or "").lower()
    return any(h in il for h in INFRA_HINTS)


def build_flow_index(options_flow, stealth, short_pressure):
    """symbol -> list of big-order signal dicts."""
    flows = {}

    def add(sym, sig):
        if not sym:
            return
        flows.setdefault(sym, []).append(sig)

    for q in (options_flow or {}).get("all_qualifying", []):
        add(q.get("symbol"), {
            "type": "OPTIONS_UOA", "tier": q.get("tier"),
            "score": q.get("score"), "flags": q.get("flags", []),
        })
    for r in (stealth or {}).get("top_smart_money_only", []):
        if (r.get("n_funds_buying") or 0) > 0 or (r.get("score") or 0) >= 70:
            add(r.get("ticker"), {
                "type": "SMART_MONEY_13F", "n_funds": r.get("n_funds_buying"),
                "score": r.get("score"),
            })
    for r in (stealth or {}).get("top_short_covering_only", []):
        add(r.get("ticker"), {"type": "SHORT_COVERING", "z_score": r.get("z_score")})
    for r in (stealth or {}).get("top_options_flow_only", []):
        if isinstance(r, dict):
            add(r.get("ticker") or r.get("symbol"), {"type": "OPTIONS_CONVERGENCE"})
    for n in (short_pressure or {}).get("names", []):
        st = (n.get("state") or "").lower()
        if "cover" in st:
            add(n.get("ticker"), {"type": "SHORT_COVERING", "z_score": n.get("z_score")})
    return flows


def build_laggard_enrich(beta_laggards, sympathetic):
    """symbol -> enrichment (catch_up_score, upside, why, risk, source)."""
    enrich = {}
    for c in (beta_laggards or {}).get("top_candidates", []):
        sym = c.get("symbol")
        if sym:
            enrich[sym] = {
                "catch_up_score": c.get("catch_up_score"), "beta": c.get("beta"),
                "upside_pct": c.get("upside_pct"), "gap_vs_leader_pp": c.get("gap_vs_leader_pp"),
                "why": (c.get("why") or "")[:300], "risk": (c.get("risk") or "")[:200],
                "source": "beta-laggard",
            }
    for s in (sympathetic or {}).get("top_setups", []):
        sym = s.get("laggard")
        if sym and sym not in enrich:
            enrich[sym] = {
                "catch_up_score": round((s.get("score") or 0) * 100, 1),
                "expected_catchup_pct": s.get("expected_catchup_pct"),
                "divergence_sigma": s.get("divergence_sigma"),
                "why": f"Lags peer {s.get('leader')} by {s.get('divergence_sigma')}sigma "
                       f"in {s.get('peer_group')} (corr {s.get('correlation_90d')}).",
                "source": "sympathetic-momentum",
            }
    return enrich


def select_hot_themes(theme_rotation):
    """Themes that gained momentum AND have constituent breadth data."""
    all_themes = {t.get("ticker"): t for t in (theme_rotation or {}).get("all_themes", [])}
    breadth = (theme_rotation or {}).get("breadth_details", {}) or {}
    hot = []
    for etf, bd in breadth.items():
        meta = all_themes.get(etf)
        if not meta:
            continue
        ms = meta.get("momentum_score") or 0
        rs_acc = meta.get("rs_acceleration") or 0
        rs20 = meta.get("rs_20d") or 0
        gained = ms >= 60 or (rs_acc > 0 and rs20 > 0 and meta.get("above_ma50"))
        if not gained:
            continue
        cons = bd.get("constituents_perf") or []
        if not cons:
            continue
        hot.append((meta, cons, bd.get("breadth")))
    hot.sort(key=lambda x: x[0].get("momentum_score") or 0, reverse=True)
    return hot[:10]


def classify_theme(meta, cons, breadth_pct, uni, flows, lag_enrich):
    rets = [_num(c.get("ret_20d")) for c in cons if _num(c.get("ret_20d")) is not None]
    if not rets:
        return None
    # ret_20d in this feed is a fraction-ish; normalize to % for readability
    scale = 100.0 if max(abs(r) for r in rets) < 3 else 1.0
    med = median(rets) * scale

    def card(c):
        sym = c.get("symbol")
        meta_u = uni.get(sym, {})
        r20 = _num(c.get("ret_20d"))
        r20p = round(r20 * scale, 1) if r20 is not None else None
        return sym, meta_u, r20p

    leaders, infra, laggards, big_orders = [], [], [], []
    for c in cons:
        sym, mu, r20p = card(c)
        if not sym or r20p is None:
            continue
        small = is_small(mu)
        sig = flows.get(sym, [])
        base = {
            "symbol": sym, "name": mu.get("name"), "industry": mu.get("industry"),
            "market_cap": mu.get("market_cap"), "cap_bucket": mu.get("cap_bucket"),
            "is_small_cap": small, "ret_20d_pct": r20p,
        }
        # LEADER (already pumped — context)
        if r20p >= med and r20p > 5:
            leaders.append({**base, "note": "already extended — reference, not entry"})
        # INFRASTRUCTURE: enabler industry + gaining momentum
        if is_infra_industry(mu.get("industry")) and (r20p > 0 or c.get("above_ma50")):
            infra.append({**base, "above_ma50": c.get("above_ma50"),
                          "momentum_note": f"enabler industry, +{r20p}% / 20d — infrastructure catching the theme",
                          "big_order_signals": [s["type"] for s in sig]})
        # LAGGARD: lags the theme median, theme is working, not deeply broken
        if med >= 4 and r20p < med - 4 and r20p > -20:
            e = lag_enrich.get(sym, {})
            laggards.append({**base, "gap_vs_theme_pp": round(med - r20p, 1),
                             "catch_up_score": e.get("catch_up_score"),
                             "beta": e.get("beta"), "upside_pct": e.get("upside_pct"),
                             "big_order_signals": [s["type"] for s in sig],
                             "why": e.get("why") or (
                                 f"In {meta.get('name')} (theme +{round(med,1)}% median/20d, "
                                 f"breadth {breadth_pct}%) but {sym} is up only {r20p}% — "
                                 f"a {round(med - r20p,1)}pp gap the tape hasn't closed yet."),
                             "risk": e.get("risk") or "Catch-up is a rotation tendency, not a certainty; "
                                                      "if the theme rolls over laggards can stay laggards.",
                             "confirmed_by": e.get("source")})
        # BIG ORDERS (esp small caps)
        if sig:
            composite = len(sig) * 10 + (15 if small else 0) + \
                sum(8 for s in sig if s["type"] in ("OPTIONS_UOA", "SMART_MONEY_13F"))
            big_orders.append({**base, "signals": sig, "n_signals": len(sig),
                               "composite": composite,
                               "why": ("small-cap " if small else "") +
                                      "theme name with quiet institutional accumulation — "
                                      "block/option footprints front-run the move"})

    leaders.sort(key=lambda x: x["ret_20d_pct"], reverse=True)
    infra.sort(key=lambda x: x["ret_20d_pct"], reverse=True)
    laggards.sort(key=lambda x: x["gap_vs_theme_pp"], reverse=True)
    # small caps first, then by composite
    big_orders.sort(key=lambda x: (x["is_small_cap"], x["composite"]), reverse=True)

    return {
        "etf": meta.get("ticker"), "name": meta.get("name"), "category": meta.get("category"),
        "momentum_score": meta.get("momentum_score"), "rs_acceleration": meta.get("rs_acceleration"),
        "rs_20d": meta.get("rs_20d"), "ret_20d_pct": meta.get("ret_20d"),
        "breadth_pct": breadth_pct, "n_constituents": len(cons),
        "theme_median_ret20d_pct": round(med, 1),
        "leaders": leaders[:5], "infrastructure": infra[:8],
        "laggards": laggards[:8], "big_orders": big_orders[:10],
    }


def lambda_handler(event, context):
    t0 = time.time()
    tr = _read("data/theme-rotation.json")
    if not tr:
        return {"statusCode": 500, "body": json.dumps({"err": "no theme-rotation.json"})}
    universe = _read("data/universe.json")
    uni = build_universe_index(universe)
    flows = build_flow_index(_read("data/options-flow.json"),
                             _read("data/stealth-accumulation.json"),
                             _read("data/short-pressure.json"))
    lag_enrich = build_laggard_enrich(_read("data/beta-laggards.json"),
                                      _read("data/sympathetic-momentum.json"))

    hot = select_hot_themes(tr)
    themes_out = []
    for meta, cons, bp in hot:
        c = classify_theme(meta, cons, bp, uni, flows, lag_enrich)
        if c:
            themes_out.append(c)

    n_infra = sum(len(t["infrastructure"]) for t in themes_out)
    n_lag = sum(len(t["laggards"]) for t in themes_out)
    n_big = sum(len(t["big_orders"]) for t in themes_out)
    n_small_big = sum(1 for t in themes_out for b in t["big_orders"] if b["is_small_cap"])

    # TOP PICKS = the trifecta: names hitting >=2 of {laggard, infra, big_order}, small-cap boosted
    pick_map = {}
    for t in themes_out:
        for b in t["laggards"]:
            pm = pick_map.setdefault(b["symbol"], {"symbol": b["symbol"], "name": b["name"],
                 "theme": t["name"], "cap_bucket": b["cap_bucket"], "is_small_cap": b["is_small_cap"],
                 "buckets": [], "big_order_signals": b.get("big_order_signals", [])})
            pm["buckets"].append("laggard")
        for b in t["infrastructure"]:
            pm = pick_map.setdefault(b["symbol"], {"symbol": b["symbol"], "name": b["name"],
                 "theme": t["name"], "cap_bucket": b["cap_bucket"], "is_small_cap": b["is_small_cap"],
                 "buckets": [], "big_order_signals": b.get("big_order_signals", [])})
            pm["buckets"].append("infrastructure")
        for b in t["big_orders"]:
            pm = pick_map.setdefault(b["symbol"], {"symbol": b["symbol"], "name": b["name"],
                 "theme": t["name"], "cap_bucket": b["cap_bucket"], "is_small_cap": b["is_small_cap"],
                 "buckets": [], "big_order_signals": [s["type"] for s in b["signals"]]})
            pm["buckets"].append("big_order")
    top_picks = []
    for sym, pm in pick_map.items():
        pm["buckets"] = sorted(set(pm["buckets"]))
        pm["conviction"] = len(pm["buckets"]) * 10 + (10 if pm["is_small_cap"] else 0)
        if len(pm["buckets"]) >= 2:
            top_picks.append(pm)
    top_picks.sort(key=lambda x: x["conviction"], reverse=True)

    out = {
        "engine": "theme-second-wave", "version": VERSION,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "as_of": tr.get("generated_at"),
        "freshness": {
            "theme_rotation_age_d": _age_days(tr),
            "universe_age_d": _age_days(universe) if universe else None,
            "notes": ["universe (market-cap/industry) may lag if universe-builder is paused; "
                      "cap classification is slow-moving so still usable."],
        },
        "summary": {
            "n_hot_themes": len(themes_out), "n_infrastructure": n_infra,
            "n_laggards": n_lag, "n_big_orders": n_big, "n_smallcap_big_orders": n_small_big,
            "n_top_picks": len(top_picks),
            "top_picks": top_picks[:15],
        },
        "hot_themes": themes_out,
        "methodology": {
            "hot_theme": "theme-rotation momentum_score>=60 or (rs_acceleration>0 & rs_20d>0 & above_ma50), "
                         "restricted to themes with constituent breadth data",
            "infrastructure": "constituent in enabler industry (equipment/materials/power/networking/tools/"
                              "diagnostics) AND gaining momentum (ret_20d>0 or above_ma50)",
            "laggard": "constituent ret_20d < theme median - 4pp while theme median >= 4% (theme working) "
                       "and not deeply broken; enriched with beta-laggard / sympathetic-momentum cards",
            "big_orders": "options UOA / 13F adds / short-covering on theme names; small caps (<$2B) boosted",
            "top_picks": "names hitting >=2 of {laggard, infrastructure, big_order}; small-cap boosted",
        },
        "sources": ["theme-rotation", "universe", "beta-laggards", "sympathetic-momentum",
                    "options-flow", "stealth-accumulation", "short-pressure"],
        "disclaimer": "Second-wave rotation is a tendency, not a certainty. Real data, research only — not advice.",
        "elapsed_s": round(time.time() - t0, 2),
    }
    s3.put_object(Bucket=S3_BUCKET, Key=OUT_KEY, Body=json.dumps(out).encode(),
                  ContentType="application/json")
    print(f"[second-wave] themes={len(themes_out)} infra={n_infra} lag={n_lag} "
          f"big={n_big} smallbig={n_small_big} picks={len(top_picks)} {out['elapsed_s']}s")
    return {"statusCode": 200, "body": json.dumps({"ok": True, "n_hot_themes": len(themes_out),
            "n_laggards": n_lag, "n_infrastructure": n_infra, "n_big_orders": n_big,
            "n_top_picks": len(top_picks)})}
