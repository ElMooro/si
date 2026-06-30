"""
justhodl-attention-confluence — SMART ACCUMULATION vs CROWD ATTENTION
=====================================================================
Hedge-fund-style fusion engine. The thesis (per the academic + practitioner
literature): the edge is the DIVERGENCE between informed accumulation and crowd
attention.

  • Informed/"smart" side (early, durable): insider cluster buying (Form 4),
    insider MSPR, unusual options flow (sweeps / Vol>OI), 13F fund accumulation,
    smart-money clusters (legend funds), dark-pool off-exchange accumulation,
    Congress buys, analyst upgrade clusters.
  • Crowd/"attention" side (late, mean-reverts — Da/Engelberg/Gao; Barber/Odean):
    Stocktwits retail bull%, theme/narrative attention, search interest, short
    crowding.

Each name is scored 0-100 on BOTH sides, then classified by STAGE:
  STEALTH      — smart firing, crowd quiet        → accumulation before the crowd (alpha)
  IGNITING     — smart firing + crowd waking up   → confirmation, still early
  CROWDED/LATE — crowd loud, no smart confirm     → chase / reversal risk (avoid)
  DISTRIBUTION — insiders/funds selling into hype → avoid
  WATCH        — partial / inconclusive

confluence_smart = number of INDEPENDENT informed families firing (the "2+
sources" institutional bar). Output is graded downstream via the closed loop.

OUTPUT  data/attention-confluence.json     SCHEDULE  daily 15:10 UTC
Real data only. Research, not investment advice.
"""
import json, math, os, datetime
import boto3

S3_BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
OUT_KEY = "data/attention-confluence.json"
s3 = boto3.client("s3")

# ----- io -----
def _read(key):
    try:
        return json.loads(s3.get_object(Bucket=S3_BUCKET, Key=key)["Body"].read())
    except Exception:
        return {}

# ----- math helpers -----
def clamp(x, lo=0.0, hi=100.0):
    try:
        x = float(x)
    except Exception:
        return lo
    return max(lo, min(hi, x))

def safe(x, d=0.0):
    try:
        if x is None:
            return d
        return float(x)
    except Exception:
        return d

def up(t):
    return (t or "").upper().strip()

def log_scale(v, full):
    """0..100 by log of value vs a 'full credit' anchor."""
    v = safe(v)
    if v <= 0:
        return 0.0
    return clamp(math.log10(1 + v) / math.log10(1 + full) * 100.0)

# ============================================================
# EXTRACTORS — each returns { TICKER: {sub-signal fields} }
# ============================================================
def x_attention(att):
    out = {}
    for r in att.get("tickers", []) or []:
        sym = up(r.get("symbol"))
        if not sym:
            continue
        out[sym] = {
            "name": r.get("name"),
            "layer": r.get("layer"),
            "insider_mspr": safe(r.get("insider_mspr"), None),
            "insider_net_change": r.get("insider_net_change"),
            "analyst_buy_pct": r.get("analyst_buy_pct"),
            "analyst_upgrade_mom": safe(r.get("analyst_upgrade_mom"), 0.0),
            "retail_bull_pct": r.get("retail_bull_pct"),
            "retail_msgs": safe(r.get("retail_msgs"), 0.0),
            "trending": bool(r.get("trending")),
        }
    return out

def x_insider_clusters(j):
    out = {}
    for c in j.get("clusters", []) or []:
        t = up(c.get("ticker"))
        if not t:
            continue
        # cluster strength: # insiders, CEO conviction, $ size
        n = safe(c.get("n_insiders"))
        raw = clamp(n * 14 + (35 if c.get("has_ceo") else 0) + log_scale(c.get("total_value"), 5_000_000) * 0.45)
        out[t] = {"n_insiders": int(n), "has_ceo": bool(c.get("has_ceo")),
                  "total_value": safe(c.get("total_value")), "highest_role": c.get("highest_role"),
                  "last_buy": c.get("last_buy"), "score": round(raw, 1)}
    return out

def x_options(j):
    out = {}
    qual = j.get("all_qualifying", []) or []
    scores = [safe(q.get("score")) for q in qual if q.get("score") is not None]
    smax = max(scores) if scores else 1.0
    for q in qual:
        sym = up(q.get("symbol"))
        if not sym:
            continue
        sc = safe(q.get("score"))
        norm = clamp(sc / smax * 100.0) if smax > 0 else 0.0
        out[sym] = {"score": round(sc, 1), "norm": round(norm, 1), "tier": q.get("tier"),
                    "flags": q.get("flags"), "metrics": q.get("metrics")}
    return out

def x_13f(j):
    out = {}
    rows = (j.get("most_bought", []) or [])
    nas = [safe(r.get("net_action_score")) for r in rows if r.get("net_action_score") is not None]
    nmax = max(nas) if nas else 1.0
    for r in rows:
        t = up(r.get("ticker"))
        if not t:
            continue
        na = safe(r.get("net_action_score"))
        norm = clamp(na / nmax * 100.0) if nmax > 0 else 0.0
        out[t] = {"n_funds_holding": r.get("n_funds_holding"), "n_funds_adding": r.get("n_funds_adding"),
                  "n_new": r.get("n_funds_new_position"), "net_action_score": round(na, 2),
                  "norm": round(norm, 1), "fund_actions": r.get("fund_actions")}
    # mark sellers (for distribution)
    for r in (j.get("most_sold", []) or []):
        t = up(r.get("ticker"))
        if t and t not in out:
            out[t] = {"selling": True, "net_action_score": safe(r.get("net_action_score")),
                      "n_funds_exiting": r.get("n_funds_exiting"), "norm": 0.0}
        elif t:
            out[t]["selling"] = True
    return out

def x_smart_money(j):
    out = {}
    for c in j.get("clusters", []) or []:
        t = up(c.get("ticker"))
        if not t:
            continue
        out[t] = {"score": clamp(c.get("score")), "flag": c.get("flag"),
                  "signal_types": c.get("signal_types"), "n_buyers": c.get("n_buyers"),
                  "n_new": c.get("n_new"), "legend_buyers": c.get("legend_buyers") or [],
                  "name": c.get("name")}
    return out

def x_dark_pool(j):
    out = {}
    for r in (j.get("board", []) or []):
        t = up(r.get("ticker"))
        if not t:
            continue
        state = (r.get("state") or "").lower()
        sc = clamp(r.get("score"))
        out[t] = {"state": r.get("state"), "score": round(sc, 1),
                  "dark_pool_pct": r.get("dark_pool_pct"), "dark_accel": r.get("dark_accel"),
                  "accumulating": "accum" in state}
    return out

def x_congress(j):
    out = {}
    cong = j.get("congress", {}) or {}
    for r in (cong.get("top_buys", []) or []):
        t = up(r.get("ticker") or r.get("Ticker") or r.get("symbol"))
        if not t:
            continue
        prev = out.get(t, {"n_buys": 0})
        out[t] = {"n_buys": prev["n_buys"] + 1, "last": r}
    return out

def x_analyst_clusters(j):
    out = {}
    for r in (j.get("buy_picks", []) or []):
        t = up(r.get("ticker") or r.get("symbol"))
        if t:
            out[t] = {"cluster": True, "detail": r}
    return out

def x_short(j):
    out = {}
    bt = j.get("by_ticker", {}) or {}
    for t, v in bt.items():
        if isinstance(v, dict):
            out[up(t)] = {"short_pct": v.get("latest_short_pct"), "dtc": v.get("days_to_cover"),
                          "signal": v.get("signal"), "score": v.get("score")}
    for r in (j.get("top_squeeze_risk", []) or []):
        t = up(r.get("ticker"))
        if t:
            out.setdefault(t, {})
            out[t]["squeeze_risk"] = True
            out[t]["score"] = r.get("score")
    return out

def theme_map(att):
    """layer -> attention_trend_pct from theme_pulse (best-effort fuzzy match on layer/theme words)."""
    tp = att.get("theme_pulse", []) or []
    return tp

# ============================================================
# MAIN
# ============================================================
def lambda_handler(event=None, context=None):
    att = _read("data/attention-signals.json")
    A = x_attention(att)
    IC = x_insider_clusters(_read("data/insider-clusters.json"))
    OP = x_options(_read("data/options-flow.json"))
    F13 = x_13f(_read("data/13f-positions.json"))
    SM = x_smart_money(_read("data/smart-money-clusters.json"))
    DP = x_dark_pool(_read("data/dark-pool.json"))
    CG = x_congress(_read("data/political-stocks.json"))
    AC = x_analyst_clusters(_read("data/rating-change-cluster.json"))
    SH = x_short(_read("data/short-interest.json"))
    SA = (_read("data/search-attention.json") or {}).get("by_ticker", {}) or {}
    gdelt = _read("data/gdelt-news.json")
    gamma = _read("data/options-gamma.json")
    themes = theme_map(att)

    # build universe = union of all informed + attention names (drop junk/CUSIP)
    def junk(t):
        return (not t) or t in {"NONE", "NULL", "NAN", "N/A", "NA"} or t.isdigit() or len(t) > 5
    universe = set()
    for d in (A, IC, OP, F13, SM, DP, CG, AC):
        universe |= set(d.keys())
    universe = {t for t in universe if not junk(t)}

    def theme_trend_for(layer):
        if not layer:
            return None
        lw = set(str(layer).lower().replace("_", " ").split())
        best = None
        for tp in themes:
            th = str(tp.get("theme", "")).lower()
            if lw & set(th.replace("/", " ").split()):
                tr = safe(tp.get("attention_trend_pct"), None)
                if tr is not None and (best is None or tr > best[1]):
                    best = (tp.get("theme"), tr)
        return best

    tickers = {}
    for t in sorted(universe):
        a = A.get(t, {})
        ic = IC.get(t, {})
        op = OP.get(t, {})
        f13 = F13.get(t, {})
        sm = SM.get(t, {})
        dp = DP.get(t, {})
        cg = CG.get(t, {})
        ac = AC.get(t, {})
        sh = SH.get(t, {})
        sa = SA.get(t, {})
        layer = a.get("layer") or sm.get("name")

        # ---------- INFORMED sub-scores (0-100) ----------
        # insider family = max(cluster strength, MSPR-derived)
        mspr = a.get("insider_mspr")
        mspr_s = clamp(safe(mspr)) if mspr is not None else 0.0
        insider_s = max(ic.get("score", 0.0), mspr_s)
        options_s = op.get("norm", 0.0)
        funds_s = max(f13.get("norm", 0.0), sm.get("score", 0.0))
        darkpool_s = dp.get("score", 0.0) if dp.get("accumulating") else dp.get("score", 0.0) * 0.4
        congress_s = clamp(40 + 20 * (cg.get("n_buys", 0) - 1)) if cg.get("n_buys") else 0.0
        analyst_s = (clamp(45 + safe(a.get("analyst_upgrade_mom")) * 600)
                     if safe(a.get("analyst_upgrade_mom")) > 0 else 0.0)
        if ac.get("cluster"):
            analyst_s = max(analyst_s, 70.0)

        fam = [("insider", insider_s, 0.26, 35),
               ("options", options_s, 0.20, 30),
               ("funds", funds_s, 0.22, 35),
               ("darkpool", darkpool_s, 0.14, 40),
               ("congress", congress_s, 0.08, 40),
               ("analyst", analyst_s, 0.10, 45)]
        num = sum(w * s for _, s, w, _ in fam if s > 0)
        den = sum(w for _, s, w, _ in fam if s > 0)
        smart_score = round(num / den, 1) if den > 0 else 0.0
        confluence_smart = sum(1 for _, s, _, thr in fam if s >= thr)
        fams_firing = [n for n, s, _, thr in fam if s >= thr]

        # ---------- CROWD sub-scores (0-100) ----------
        rb = a.get("retail_bull_pct")
        retail_s = 0.0
        if rb is not None:
            vol_w = min(1.0, math.log10(1 + a.get("retail_msgs", 0)) / math.log10(31))
            retail_s = clamp(safe(rb) * 100.0 * (0.55 + 0.45 * vol_w))
        if a.get("trending"):
            retail_s = max(retail_s, 55.0)
        tt = theme_trend_for(layer)
        theme_s = clamp(50 + safe(tt[1]) * 0.6) if tt else 0.0
        # search attention (Wikipedia pageview velocity via justhodl-search-attention)
        svi = sa.get("svi")
        search_has = svi is not None
        if search_has:
            accel = clamp(50 + safe(sa.get("trend_pct")) * 0.5) if sa.get("trend_pct") is not None else 50.0
            search_s = clamp(0.6 * safe(svi) + 0.4 * accel)
        else:
            search_s = 0.0
        cfam = [("retail", retail_s, 0.42), ("theme", theme_s, 0.23), ("search", search_s, 0.35)]
        cnum = sum(w * s for _, s, w in cfam if s > 0)
        cden = sum(w for _, s, w in cfam if s > 0)
        crowd_score = round(cnum / cden, 1) if cden > 0 else 0.0

        divergence = round(smart_score - crowd_score, 1)

        # ---------- distribution flags ----------
        distributing = bool(f13.get("selling")) or ("distrib" in dp.get("state", "").lower()) \
            or (a.get("insider_net_change") is not None and safe(a.get("insider_net_change")) < 0 and not ic)

        # ---------- STAGE ----------
        crowd_has_data = (rb is not None) or (theme_s > 0) or bool(a.get("trending")) or search_has
        smart_fire = smart_score >= 45 and confluence_smart >= 2
        smart_some = smart_score >= 38 and confluence_smart >= 1
        crowd_loud = crowd_score >= 55
        crowd_warm = crowd_score >= 38
        if distributing and crowd_warm:
            stage = "DISTRIBUTION"
        elif smart_fire and crowd_has_data and crowd_score < 40:
            stage = "STEALTH"          # verified: smart firing while retail is demonstrably quiet
        elif smart_fire and not crowd_has_data:
            stage = "UNDISCOVERED"     # smart firing but no retail/attention read yet (search-attention will resolve)
        elif (smart_fire or smart_some) and crowd_warm:
            stage = "IGNITING"
        elif crowd_loud and not smart_some:
            stage = "CROWDED"
        elif smart_some:
            stage = "WATCH"
        else:
            stage = "NEUTRAL"

        # ---------- why ----------
        bits = []
        if ic:
            bits.append(f"{ic.get('n_insiders')} insiders buying" + (" incl CEO" if ic.get("has_ceo") else ""))
        elif mspr_s >= 30:
            bits.append(f"insider MSPR {mspr}")
        if options_s >= 30:
            bits.append(f"unusual options ({op.get('tier','')})")
        if funds_s >= 35:
            lg = sm.get("legend_buyers") or []
            bits.append(("legend funds: " + ", ".join(lg[:2])) if lg else f"{f13.get('n_funds_adding',0)} funds adding")
        if darkpool_s >= 40 and dp.get("accumulating"):
            bits.append(f"dark-pool accumulation ({dp.get('dark_pool_pct')}%)")
        if congress_s:
            bits.append(f"{cg.get('n_buys')} congress buy(s)")
        if analyst_s >= 45:
            bits.append("analyst upgrades")
        cw = []
        if retail_s >= 50:
            cw.append(f"retail {round(safe(rb)*100)}% bull" + (" + trending" if a.get("trending") else ""))
        if theme_s >= 55:
            cw.append(f"hot theme: {tt[0]}")
        if search_s >= 55:
            cw.append(f"search +{sa.get('trend_pct')}%")
        why = "; ".join(bits) + ((" · CROWD: " + ", ".join(cw)) if cw else "")

        tickers[t] = {
            "symbol": t, "name": a.get("name") or sm.get("name") or f13.get("name"), "layer": layer,
            "smart_score": smart_score, "crowd_score": crowd_score, "divergence": divergence,
            "confluence_smart": confluence_smart, "families_firing": fams_firing, "stage": stage,
            "distributing": distributing, "crowd_has_data": crowd_has_data,
            "signals": {
                "insider": ic or ({"mspr": mspr} if mspr is not None else None),
                "options": op or None, "funds": (sm or f13) or None, "darkpool": dp or None,
                "congress": cg or None, "analyst": ac or ({"upgrade_mom": a.get("analyst_upgrade_mom")} if a.get("analyst_upgrade_mom") else None),
                "retail": ({"bull_pct": rb, "msgs": a.get("retail_msgs"), "trending": a.get("trending")} if rb is not None or a.get("trending") else None),
                "theme": ({"theme": tt[0], "trend_pct": tt[1]} if tt else None),
                "search": ({"svi": svi, "trend_pct": sa.get("trend_pct"), "wiki": sa.get("wiki_title")} if search_has else None),
                "short": sh or None,
            },
            "subscores": {"insider": round(insider_s, 1), "options": round(options_s, 1),
                          "funds": round(funds_s, 1), "darkpool": round(darkpool_s, 1),
                          "congress": round(congress_s, 1), "analyst": round(analyst_s, 1),
                          "retail": round(retail_s, 1), "theme": round(theme_s, 1), "search": round(search_s, 1)},
            "why": why,
        }

    # ---------- stage buckets (sorted) ----------
    def bucket(stage, key=lambda r: (r["divergence"], r["smart_score"]), rev=True, n=40):
        rows = [v for v in tickers.values() if v["stage"] == stage]
        rows.sort(key=key, reverse=rev)
        return rows[:n]

    stealth = bucket("STEALTH")
    igniting = bucket("IGNITING", key=lambda r: (r["smart_score"], r["confluence_smart"]))
    crowded = bucket("CROWDED", key=lambda r: (r["crowd_score"],))
    distribution = bucket("DISTRIBUTION", key=lambda r: (r["crowd_score"],))
    undiscovered = bucket("UNDISCOVERED", key=lambda r: (r["smart_score"], r["confluence_smart"]))

    # ---------- section feeds (raw, for dedicated panels) ----------
    options_panel = sorted([{"symbol": k, **v} for k, v in OP.items()],
                           key=lambda r: r.get("norm", 0), reverse=True)[:20]
    insider_panel = sorted([{"ticker": k, **v} for k, v in IC.items()],
                           key=lambda r: r.get("score", 0), reverse=True)[:20]
    funds_panel = sorted([{"ticker": k, **v} for k, v in SM.items()],
                         key=lambda r: r.get("score", 0), reverse=True)[:20]
    darkpool_panel = sorted([{"ticker": k, **v} for k, v in DP.items() if v.get("accumulating")],
                            key=lambda r: r.get("score", 0), reverse=True)[:20]
    congress_panel = sorted([{"ticker": k, "n_buys": v.get("n_buys")} for k, v in CG.items()],
                            key=lambda r: r.get("n_buys", 0), reverse=True)[:20]

    # ---------- market context ----------
    asset_sent = gdelt.get("asset_sentiment", {}) if isinstance(gdelt, dict) else {}
    spy_tone = (asset_sent.get("SPY") or {})
    ctx = {
        "gamma_regime": gamma.get("regime") if isinstance(gamma, dict) else None,
        "vix": gamma.get("vix") if isinstance(gamma, dict) else None,
        "news_tone_spy": spy_tone.get("avg_tone") if isinstance(spy_tone, dict) else None,
        "as_of_13f": _read("data/13f-positions.json").get("as_of_quarter") if False else None,
    }

    counts = {s: len([v for v in tickers.values() if v["stage"] == s])
              for s in ["STEALTH", "IGNITING", "UNDISCOVERED", "CROWDED", "DISTRIBUTION", "WATCH", "NEUTRAL"]}

    out = {
        "engine": "attention-confluence",
        "version": "1.0.0",
        "generated_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "thesis": ("The edge is the divergence between informed accumulation and crowd attention. "
                   "Smart money positions before the crowd arrives (STEALTH); when both fire it is "
                   "confirmation (IGNITING); loud retail with no smart confirmation is a chase/reversal "
                   "risk (CROWDED); insiders/funds selling into hype is DISTRIBUTION."),
        "universe_n": len(universe),
        "n_scored": len(tickers),
        "counts": counts,
        "market_context": ctx,
        "stages": {"stealth": stealth, "igniting": igniting, "undiscovered": undiscovered, "crowded": crowded, "distribution": distribution},
        "panels": {"unusual_options": options_panel, "insider_clusters": insider_panel,
                   "smart_money": funds_panel, "dark_pool_accumulation": darkpool_panel,
                   "congress_buys": congress_panel,
                   "theme_attention": att.get("theme_pulse", []),
                   "stocktwits_trending": att.get("stocktwits_trending", [])},
        "tickers": tickers,
        "scoring": {
            "smart_families": {"insider": 0.26, "options": 0.20, "funds": 0.22, "darkpool": 0.14,
                               "congress": 0.08, "analyst": 0.10},
            "crowd_families": {"retail": 0.42, "theme": 0.23, "search": 0.35},
            "stage_rules": {"STEALTH": "smart>=45 & confluence>=2 & crowd<40",
                            "IGNITING": "smart firing & crowd>=38",
                            "CROWDED": "crowd>=55 & smart weak",
                            "DISTRIBUTION": "fund/insider selling & crowd>=38"},
        },
        "sources": ["Finnhub insider/analyst (via attention-signals)", "Form 4 insider clusters",
                    "options-flow-scanner", "13F + smart-money clusters", "FINRA dark-pool",
                    "Quiver Congress", "rating-change clusters", "GDELT/Stocktwits/Wikipedia attention"],
        "caveats": ("Attention leads ~2 weeks then mean-reverts (Da/Engelberg/Gao). STEALTH is the "
                    "asymmetric setup; CROWDED is shown to be avoided, not chased. Confluence>=2 means "
                    "two independent informed families agree. Research only, not investment advice."),
    }

    s3.put_object(Bucket=S3_BUCKET, Key=OUT_KEY, Body=json.dumps(out).encode(),
                  ContentType="application/json", CacheControl="public, max-age=300")
    return {"ok": True, "n_scored": len(tickers), "counts": counts}


if __name__ == "__main__":
    print(json.dumps(lambda_handler(), indent=2)[:1500])
