"""justhodl-upside-thesis v2 — per-ticker multibagger decision engine.

Joins the discovery fleet into a per-name dossier, then ENRICHES each candidate with:
  • Smart-money convergence — 13F famous funds (Berkshire/Bridgewater/Renaissance/…),
    smart-money-clusters, insider cluster buys ($ + #insiders), ARK funds, Congress.
  • Real valuation — PE / P/S / EV-Sales / FCF-yield (FCF-yield = the single strongest
    empirical multibagger predictor, Yartseva 2025) — also fills the SQGLP "Price" check.
  • Multibagger DNA — matches each name to one of 5 empirically-grounded archetypes
    (Stealth Microcap, Quiet Compounder, Hypergrowth Disruptor, Turnaround, Margin-Inflection).
  • Catalyst countdown — days to next earnings.
  • Chart pattern — double-bottom / 200DMA reclaim / gap-and-go with quality.
Scores each against CAN SLIM (O'Neil) / SQGLP 100-baggers (Mayer) / Lynch tenbagger,
writes a sourced deterministic "why", and an AI thesis for the top N.
Output: data/upside-theses.json -> click-to-drill-down panel on upside-radar.html.
"""
import json, os, time
from datetime import datetime, timezone
import boto3

s3 = boto3.client("s3", "us-east-1")
BUCKET = "justhodl-dashboard-live"
OUT_KEY = "data/upside-theses.json"
TOP_AI = int(os.environ.get("TOP_AI", "16"))

def rd(k):
    try: return json.loads(s3.get_object(Bucket=BUCKET, Key=f"data/{k}.json")["Body"].read())
    except Exception: return {}
def _up(t): return (t or "").upper().strip()
def g(d, *ks, default=None):
    for k in ks:
        if isinstance(d, dict) and d.get(k) is not None: return d[k]
    return default

FUND_NAMES = {"BERKSHIRE": "Berkshire", "BRIDGEWATER": "Bridgewater", "AQR": "AQR", "RENAISSANCE": "Renaissance",
    "TWO_SIGMA": "Two Sigma", "PERSHING": "Pershing Square", "GREENLIGHT": "Greenlight", "CITADEL": "Citadel",
    "MILLENNIUM": "Millennium", "TIGER_GLOBAL": "Tiger Global", "SOROS": "Soros", "BAUPOST": "Baupost",
    "COATUE": "Coatue", "DURATION": "Duration", "SCION": "Scion (Burry)", "LONE_PINE": "Lone Pine", "POINT72": "Point72"}

QUOTES = {
    "oneil": ("William O'Neil", "Buy stocks coming out of broad bases and beginning to make new highs — you're trying to find the beginning of a major move."),
    "mayer": ("Chris Mayer", "Margin of safety comes primarily from the quality of the business, combined with a good entry price."),
    "lynch": ("Peter Lynch", "Big companies have small moves; small companies have big moves."),
}

# ── Multibagger DNA archetypes (empirically grounded) ──
def dna_match(m, eng):
    rev = m.get("rev_growth_pct") or 0; mcap = m.get("mcap_bn"); gm = m.get("gross_margin_now") or 0
    gmt = m.get("gross_margin_then"); ps = m.get("ps"); fcfy = m.get("fcf_y"); pe = m.get("pe")
    dist = m.get("dist_hi_pct"); ret252 = m.get("ret252"); inst = any(e in eng for e in ("dark_pool","institutional_13f","ark"))
    A = []
    # 1 Stealth Microcap (Yartseva: median 10-bagger started ~$348M, P/S<1, under the radar, profitable)
    s = 0
    if mcap is not None and mcap < 1: s += 40
    elif mcap is not None and mcap < 3: s += 22
    if ps is not None and ps < 1.5: s += 25
    if rev > 10: s += 15
    if not inst: s += 20
    A.append(("Stealth Microcap", s, "464-winner study (Yartseva 2025): the median 10-bagger started at ~$348M with P/S<1, profitable and under-followed.", "Lynch's undiscovered small caps"))
    # 2 Quiet Compounder (Mayer SQGLP: high margin, steady growth, high ROIC, reinvestment)
    s = 0
    if gm >= 50: s += 30
    elif gm >= 40: s += 18
    if 15 <= rev <= 40: s += 28
    if (m.get("bagger_score") or 0) >= 60: s += 22
    if mcap is not None and mcap < 20: s += 12
    if m.get("share_chg_4y_pct") is not None and m["share_chg_4y_pct"] <= 3: s += 8
    A.append(("Quiet Compounder", s, "Mayer's 100-baggers: high ROIC + reinvestment + persistent high margins; avg ~26% CAGR over ~17-25 years.", "Monster Beverage, Copart, Pool Corp"))
    # 3 Hypergrowth Disruptor (scalable, rising margins, leader)
    s = 0
    if rev >= 40: s += 38
    elif rev >= 25: s += 20
    if gm >= 50: s += 22
    if ("breakout" in eng or "rs_leader" in eng): s += 25
    if "momentum" in eng: s += 12
    A.append(("Hypergrowth Disruptor", s, "Operating leverage: high fixed / low marginal cost means margins expand as revenue scales — the engine behind early NVIDIA, Shopify, Netflix.", "NVIDIA, Shopify, Netflix"))
    # 4 Turnaround / Down-but-not-out (Yartseva: many 10-baggers near 12m lows, cheap, improving)
    s = 0
    if dist is not None and dist <= -25: s += 30
    if ret252 is not None and ret252 < 0: s += 20
    if ps is not None and ps < 1: s += 22
    if fcfy is not None and fcfy > 0.05: s += 18
    if "cyclical_bagger" in eng: s += 18
    A.append(("Turnaround / Down-but-not-out", s, "Yartseva 2025: many 10-baggers traded near 12-month lows with P/S<1 and high FCF yield before launching — value the crowd had given up on.", "deep cyclical & post-crash recoveries"))
    # 5 Margin-Inflection (triple engine: sales × margin × re-rating)
    s = 0
    if gmt is not None and gm > gmt: s += 34
    if rev >= 20: s += 24
    if (m.get("beat_streak") or 0) >= 2 or "est_revisions" in eng: s += 22
    if gm >= 45: s += 12
    A.append(("Margin-Inflection", s, "Lynch's 'growth in all dimensions': sales × expanding margin × valuation re-rating compound together into the biggest moves.", "operating-leverage inflections"))
    A.sort(key=lambda x: -x[1])
    best = A[0]
    fit = min(100, best[1])
    return {"archetype": best[0], "fit_pct": fit, "anchor": best[2], "examples": best[3],
            "runner_up": A[1][0] if A[1][1] >= 40 else None}

def lambda_handler(event=None, context=None):
    t0 = time.time()
    F = {k: rd(k) for k in ["upside-radar","flow-confluence","equity-confluence","bagger-engine",
        "cyclical-bagger","momentum-breakout","pead-signals","dark-pool","capital-flow","ark-holdings",
        "short-interest","estimate-revisions","sector-emergence","master-ranker","best-setups","risk-regime",
        "smart-money-clusters","insider-buys-enriched","insider-clusters-names","13f-positions",
        "political-stocks","stock-valuations","chart-patterns","eps-revision-velocity","earnings-quality"]}

    # ── lookups for enrichment ──
    val_map = {}
    for tbl in ("sp_table", "hp"):
        for r in F["stock-valuations"].get(tbl, []) or []:
            t = _up(r.get("t"))
            if t and t not in val_map:
                val_map[t] = {"pe": r.get("pe"), "ps": r.get("ps"), "ev_s": r.get("ev_s"),
                              "ev_ebitda": r.get("ev_ebitda"), "fcf_y": r.get("fcf_y"), "p_fcf": r.get("p_fcf"),
                              "sector": r.get("sector"), "roe": r.get("roe")}
    f13_map = {_up(k): v for k, v in (F["13f-positions"].get("aggregate_by_ticker") or {}).items()}
    sm_map = {_up(c.get("ticker")): c for c in (F["smart-money-clusters"].get("clusters") or [])}
    ins_map = {_up(s.get("ticker")): s for s in (F["insider-buys-enriched"].get("top_setups") or [])}
    insn_map = {_up(n.get("ticker")): n for n in (F["insider-clusters-names"].get("names") or [])}
    ark_map = {_up(a.get("ticker")): a for a in (F["ark-holdings"].get("cross_fund_top") or [])}
    epsv_map = {_up(r.get("symbol")): r for r in (F["eps-revision-velocity"].get("all_qualifying") or [])}
    eq_map = {_up(r.get("ticker")): r for r in (F["earnings-quality"].get("all_ranked") or F["earnings-quality"].get("top_20_high_quality") or [])}
    er_date = {}
    for r in F["estimate-revisions"].get("estimate_strength_leaders", []) or []:
        t = _up(r.get("ticker"))
        if t: er_date[t] = {"earnings_date": r.get("earnings_date"), "days": r.get("days_to_earnings")}
    # congress tickers
    congress_t = set()
    cg = F["political-stocks"].get("congress")
    def _scan_tickers(obj):
        if isinstance(obj, dict):
            for k, v in obj.items():
                if k in ("ticker", "symbol") and isinstance(v, str): congress_t.add(_up(v))
                else: _scan_tickers(v)
        elif isinstance(obj, list):
            for x in obj[:500]: _scan_tickers(x)
    _scan_tickers(cg)
    # chart patterns
    pat_map = {}
    for cat in ("gap_and_gos", "volume_breakouts", "cross_up_200dma", "double_bottoms"):
        for r in F["chart-patterns"].get(cat, []) or []:
            t = _up(r.get("symbol"))
            if t and t not in pat_map:
                pat_map[t] = {"pattern": r.get("pattern") or cat, "status": r.get("status"),
                              "quality": r.get("quality"), "days_since": r.get("days_since_cross")}

    dossier = {}
    def D(t):
        t = _up(t)
        if not t: return None
        return dossier.setdefault(t, {"ticker": t, "engines": [], "metrics": {}})

    ur = F["upside-radar"]; scans = ur.get("scans", {})
    for row in scans.get("breakout", []) or []:
        d = D(row.get("t"))
        if d: d["engines"].append("breakout"); d["metrics"].update({"px": row.get("c"), "dist_hi_pct": row.get("dist_hi_pct"), "ret63": row.get("ret63"), "ret252": row.get("ret252"), "dvol_x": row.get("dvol_x")})
    for row in scans.get("rs_leaders", []) or []:
        d = D(row.get("t"))
        if d: d["engines"].append("rs_leader"); d["metrics"]["rs_pctile"] = row.get("rs_pctile"); d["metrics"].setdefault("px", row.get("c"))
    for row in scans.get("coiled", []) or []:
        d = D(row.get("t"))
        if d: d["engines"].append("coiled")
    for row in scans.get("footprint", []) or []:
        d = D(row.get("t"))
        if d: d["engines"].append("footprint"); d["metrics"].setdefault("dvol_x", row.get("dvol_x"))
    for row in ur.get("anatomy", []) or []:
        d = D(row.get("ticker"))
        if d: d["engines"].append("anatomy"); d["metrics"].update({"rev_growth_pct": row.get("rev_growth_pct"), "share_chg_4y_pct": row.get("share_chg_4y_pct"), "gross_margin_now": row.get("gross_margin_now"), "gross_margin_then": row.get("gross_margin_then"), "mcap_bn": row.get("mcap_bn"), "anatomy_score": row.get("anatomy_score")})
    for row in F["flow-confluence"].get("multi_engine_confluence", []) or []:
        d = D(row.get("ticker"))
        if d: d["engines"].append("flow_confluence"); d["metrics"].update({"confluence_score": row.get("score"), "n_flow_engines": row.get("n_engines"), "flow_tags": row.get("tags")})
    for row in F["equity-confluence"].get("confluence_book", []) or []:
        d = D(row.get("ticker"))
        if d: d["engines"].append("equity_confluence")
    for row in (F["bagger-engine"].get("top_100", []) or [])[:60]:
        d = D(row.get("symbol"))
        if d: d["engines"].append("bagger_engine"); d["metrics"].update({"bagger_score": row.get("bagger_score"), "cap_bucket": row.get("cap_bucket"), "sector": row.get("sector"), "name": row.get("name"), "mcap_bn": d["metrics"].get("mcap_bn") or (round(row.get("market_cap",0)/1e9,2) if row.get("market_cap") else None)})
    for row in (F["cyclical-bagger"].get("cyclical_only_book", []) or [])[:30]:
        d = D(row.get("ticker"))
        if d: d["engines"].append("cyclical_bagger")
    for row in (F["momentum-breakout"].get("all_qualifying", []) or [])[:60]:
        d = D(row.get("symbol"))
        if d: d["engines"].append("momentum"); mm = row.get("metrics") or {}; d["metrics"].update({"mom_tier": row.get("tier"), "ret_20d_pct": mm.get("ret_20d_pct"), "is_parabolic": row.get("is_parabolic")})
    for row in (F["pead-signals"].get("all_qualifying", []) or [])[:60]:
        d = D(row.get("symbol"))
        if d: d["engines"].append("pead"); mm = row.get("metrics") or {}; d["metrics"].update({"pead_tier": row.get("tier"), "beat_streak": mm.get("streak"), "avg_beat_pct": mm.get("avg_beat_pct")})
    for row in (F["dark-pool"].get("top_accumulation", []) or []):
        d = D(row.get("ticker"))
        if d: d["engines"].append("dark_pool"); d["metrics"].update({"dark_pool_pct": row.get("dark_pool_pct")})
    for row in (F["capital-flow"].get("accumulating", []) or []):
        d = D(row.get("ticker"))
        if d: d["engines"].append("institutional_13f"); d["metrics"].setdefault("sector", row.get("sector"))
    for row in (F["ark-holdings"].get("cross_fund_top", []) or []):
        d = D(row.get("ticker"))
        if d: d["engines"].append("ark")
    for row in (F["short-interest"].get("top_squeeze_risk", []) or []):
        d = D(row.get("ticker"))
        if d: d["engines"].append("squeeze"); d["metrics"].update({"short_pct": row.get("latest_short_pct"), "days_to_cover": row.get("days_to_cover")})
    for row in (F["estimate-revisions"].get("estimate_strength_leaders", []) or []):
        d = D(row.get("ticker"))
        if d: d["engines"].append("est_revisions"); d["metrics"].setdefault("name", row.get("company"))
    for row in (F["master-ranker"].get("top_tickers", []) or []):
        d = D(row.get("ticker"))
        if d: d["engines"].append("master_ranker"); d["metrics"]["ranker_score"] = row.get("score")
    for row in (F["best-setups"].get("top_setups", []) or []):
        d = D(row.get("ticker"))
        if d: d["engines"].append("best_setup"); d["metrics"].update({"setup_conviction": row.get("conviction"), "name": d["metrics"].get("name") or row.get("name")})

    rr = F["risk-regime"]; reg = (rr.get("risk_regime") or "").upper()
    mkt_ok = "RISK_ON" in reg or reg == "NEUTRAL" or "MILD" in reg
    mkt_state = rr.get("risk_regime") or "UNKNOWN"

    def chk(c): return 1.0 if c is True else (0.5 if c == "partial" else 0.0)

    out = {}
    for t, d in dossier.items():
        m = d["metrics"]; eng = sorted(set(d["engines"]))
        # enrich metrics with valuation
        v = val_map.get(t, {})
        for kk in ("pe", "ps", "ev_s", "fcf_y", "p_fcf"):
            if v.get(kk) is not None: m.setdefault(kk, v[kk])
        if v.get("sector"): m.setdefault("sector", v["sector"])
        ev = epsv_map.get(t, {})
        if ev: m.setdefault("eps_rev_velocity", ev.get("score"))
        rev = m.get("rev_growth_pct"); mcap = m.get("mcap_bn"); gm = m.get("gross_margin_now")
        dil = m.get("share_chg_4y_pct"); dvx = m.get("dvol_x"); rs = m.get("rs_pctile")
        ps = m.get("ps"); fcfy = m.get("fcf_y"); pe = m.get("pe")
        inst = any(e in eng for e in ("dark_pool", "institutional_13f", "ark"))

        # ── smart-money convergence ──
        f13 = f13_map.get(t, {}); smc = sm_map.get(t, {}); ins = ins_map.get(t, {}); insn = insn_map.get(t, {})
        fa = f13.get("fund_actions") or []
        famous = []
        for a in fa:
            fk = (a.get("fund") or "").upper()
            for key, nm in FUND_NAMES.items():
                if fk.startswith(key[:6]) and nm not in famous: famous.append(nm)
        sm = {
            "n_funds": f13.get("n_funds_holding") or smc.get("n_funds_holding"),
            "n_adding": f13.get("n_funds_adding"), "n_new": f13.get("n_funds_new_position") or smc.get("n_new"),
            "famous_funds": famous[:6], "cluster_flag": smc.get("flag"), "cluster_score": smc.get("score"),
            "insider": ({"n_insiders": ins.get("n_insiders"), "value_usd": ins.get("total_value_usd"),
                         "signal": ins.get("signal_type"), "last_buy": ins.get("last_buy")} if ins else None),
            "insider_thesis": insn.get("one_liner") if insn else None,
            "ark_funds": (ark_map.get(t) or {}).get("n_funds"),
            "congress": t in congress_t,
        }
        sm_score = (min(40, (sm["n_funds"] or 0) * 3) + (15 if famous else 0) + (20 if sm["insider"] else 0)
                    + (12 if sm["ark_funds"] else 0) + (8 if sm["congress"] else 0)
                    + (10 if (smc.get("flag") or "").startswith("STRONG") or (smc.get("flag") or "").startswith("HIGH") else 0))
        sm["score"] = min(100, sm_score)

        # ── valuation block + verdict ──
        val = {"pe": pe, "ps": ps, "ev_s": m.get("ev_s"), "fcf_yield_pct": round(fcfy*100, 1) if fcfy is not None else None, "p_fcf": m.get("p_fcf")}
        cheap = (ps is not None and ps < 1.5) or (fcfy is not None and fcfy > 0.05) or (pe is not None and 0 < pe < 18)
        rich = (ps is not None and ps > 10) or (pe is not None and pe > 60)
        val["verdict"] = "attractive entry" if cheap else ("rich — priced for growth" if rich else "fair")

        # ── DNA archetype ──
        dna = dna_match(m, eng)
        # ── catalyst ──
        ed = er_date.get(t, {})
        cat = {"next_earnings": ed.get("earnings_date"), "days_to_earnings": ed.get("days")} if ed.get("earnings_date") else None
        # ── pattern ──
        pat = pat_map.get(t)

        # ── CAN SLIM ──
        cs = {
            "C": chk(("pead" in eng and (m.get("beat_streak") or 0) >= 2) or "est_revisions" in eng or (m.get("avg_beat_pct") or 0) > 0),
            "A": chk(True if (rev or 0) >= 25 else ("partial" if (rev or 0) >= 15 else False)),
            "N": chk("breakout" in eng or (m.get("dist_hi_pct") is not None and m.get("dist_hi_pct") >= -1)),
            "S": chk(True if ((dvx or 0) >= 3 and (mcap or 99) < 10) else ("partial" if (mcap or 99) < 10 or (dvx or 0) >= 3 else False)),
            "L": chk(True if (("rs_leader" in eng) or (rs or 0) >= 80) else ("partial" if (rs or 0) >= 70 else False)),
            "I": chk(True if (inst or (sm["n_funds"] or 0) >= 8 or sm["insider"]) else ("partial" if (sm["n_funds"] or 0) >= 3 else False)),
            "M": chk(True if mkt_ok else "partial"),
        }
        canslim_score = round(sum(cs.values()) / 7 * 100)
        # ── SQGLP — P now uses real valuation ──
        sq = {
            "S_small": chk(True if (mcap or 99) < 3 else ("partial" if (mcap or 99) < 10 else False)),
            "Q_quality": chk(True if ((gm or 0) >= 40 and (dil if dil is not None else 99) <= 3) else ("partial" if (gm or 0) >= 35 or (dil if dil is not None else 99) <= 5 else False)),
            "G_growth": chk(True if ((rev or 0) >= 20 or (m.get("bagger_score") or 0) >= 60) else ("partial" if (rev or 0) >= 12 else False)),
            "L_runway": chk(True if (m.get("cap_bucket") in ("micro", "small") and (rev or 0) > 0) else ("partial" if (rev or 0) > 0 else False)),
            "P_price": chk(True if cheap else ("partial" if not rich else False)),
        }
        sqglp_score = round(sum(sq.values()) / 5 * 100)
        # ── Lynch ──
        ly = {
            "small_cap": (mcap or 99) < 5,
            "fast_grower_not_hyper": 20 <= (rev or 0) <= 60,
            "low_dilution_or_buyback": (dil is not None and dil <= 2),
            "scalable_margins": (gm or 0) >= 40,
            "undiscovered_or_accumulating": inst or ("flow_confluence" in eng) or bool(sm["insider"]),
        }
        lynch_score = round(sum(1 for x in ly.values() if x) / len(ly) * 100)

        # deterministic why
        bits = []
        if "breakout" in eng: bits.append("pressing 52-wk highs" + (f" on {dvx}× volume" if dvx else "") + " (CAN SLIM 'N'+'S')")
        if "rs_leader" in eng or (rs or 0) >= 80: bits.append("RS leader" + (f" at {rs} pctile" if rs else "") + " (O'Neil 'L')")
        if (rev or 0) >= 15: bits.append(f"{rev}% revenue growth")
        if gm: bits.append(f"{gm}% gross margins" + (f" (up from {m.get('gross_margin_then')}%)" if m.get("gross_margin_then") else ""))
        if sm["n_funds"]: bits.append(f"{sm['n_funds']} institutions holding" + (f" incl. {famous[0]}" if famous else ""))
        if sm["insider"]: bits.append(f"{sm['insider']['n_insiders']} insiders bought")
        if ps is not None: bits.append(f"P/S {ps}" + (" (cheap)" if ps < 1.5 else ""))
        if mcap: bits.append(f"${mcap}B base")
        why = (f"{t}: " + "; ".join(bits[:6]) + ".") if bits else f"{t} flagged by {len(eng)} engine(s)."

        disc = (len(eng) * 9 + canslim_score * 0.35 + sqglp_score * 0.25 + sm["score"] * 0.35
                + dna["fit_pct"] * 0.25 + (m.get("anatomy_score") or 0) * 0.2 + (m.get("confluence_score") or 0) * 4)

        out[t] = {
            "ticker": t, "name": m.get("name"), "sector": m.get("sector"),
            "engines_firing": eng, "n_engines": len(eng), "metrics": m,
            "canslim": {"score": canslim_score, "checks": cs},
            "sqglp": {"score": sqglp_score, "checks": sq},
            "lynch": {"score": lynch_score, "checks": ly},
            "smart_money": sm, "valuation": val, "dna": dna, "catalyst": cat, "pattern": pat,
            "market_regime": mkt_state, "why": why, "discovery_score": round(disc, 1), "ai": None,
        }

    ranked = sorted(out.values(), key=lambda x: -x["discovery_score"])
    n_ai = 0
    try:
        from llm_router import complete
        from concurrent.futures import ThreadPoolExecutor, as_completed
        SYS = ("You are an institutional growth-equity analyst in the lineage of William O'Neil (CAN SLIM), "
               "Chris Mayer (100-baggers/SQGLP) and Peter Lynch (tenbaggers). You are handed a rich quantitative "
               "dossier for ONE stock — price signals, fundamentals, framework scorecards, smart-money flows "
               "(13F funds, insiders, Congress, ARK), valuation, a multibagger-DNA archetype match, catalysts and "
               "chart pattern. Write a crisp, decision-grade explanation a retail investor could act on WITHOUT "
               "leaving the page. Be specific and grounded ONLY in the dossier — never invent fundamentals. "
               "Return STRICT JSON only, no markdown: {\"headline\":str (<=95 chars, punchy), "
               "\"why_boom\":str (3-4 sentences — the bull case tying signals together), "
               "\"multibagger_case\":str (the DNA archetype + the realistic path and rough magnitude, 2-3 sentences), "
               "\"smart_money_read\":str (what the institutional/insider flow says, 1-2 sentences, or 'No notable smart-money signal.'), "
               "\"catalysts\":[2-3 short strings], \"risks\":[2-3 short strings], \"what_breaks_it\":str (1 sentence), "
               "\"bull_target\":str (a plausible upside scenario in plain words, 1 sentence), "
               "\"best_framework\":\"CAN SLIM\"|\"100-bagger (SQGLP)\"|\"Lynch tenbagger\", \"conviction\":1-5}. "
               "This is research, not financial advice.")

        def gen(d):
            doss = {k: d[k] for k in ("ticker","name","sector","engines_firing","metrics","canslim","sqglp",
                    "lynch","smart_money","valuation","dna","catalyst","pattern","market_regime")}
            prompt = "DOSSIER:\n" + json.dumps(doss, default=str) + "\n\nReturn the JSON object now."
            try:
                raw = complete(prompt, tier="critical", max_tokens=900, system=SYS)
                txt = (raw or "").strip()
                if txt.startswith("```"): txt = txt.split("```")[1].replace("json", "", 1).strip()
                i, j = txt.find("{"), txt.rfind("}")
                return d["ticker"], (json.loads(txt[i:j+1]) if i >= 0 else None)
            except Exception as e:
                print(f"  AI {d['ticker']} err: {str(e)[:70]}"); return d["ticker"], None

        with ThreadPoolExecutor(max_workers=6) as ex:
            for f in as_completed([ex.submit(gen, d) for d in ranked[:TOP_AI]]):
                tk, ai = f.result()
                if ai: out[tk]["ai"] = ai; n_ai += 1
    except Exception as e:
        print(f"[upside-thesis] LLM unavailable: {str(e)[:80]}")

    payload = {
        "engine": "upside-thesis", "version": "2.0.0",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "elapsed_s": round(time.time() - t0, 1), "market_regime": mkt_state,
        "n_candidates": len(out), "n_ai": n_ai, "top_ranked": [d["ticker"] for d in ranked[:50]],
        "frameworks": {
            "CAN SLIM": {"author": "William O'Neil", "legend": {"C": "Current quarterly earnings ≥25% YoY", "A": "Annual earnings growth ≥25%/3y", "N": "New high / new product", "S": "Supply & demand — small float, volume surge", "L": "Leader (RS ≥80 pctile)", "I": "Institutional sponsorship", "M": "Market in uptrend"}, "quote": QUOTES["oneil"][1]},
            "100-bagger (SQGLP)": {"author": "Chris Mayer", "legend": {"S_small": "Small size — room to compound", "Q_quality": "Quality business + low dilution", "G_growth": "High growth / ROIC + reinvestment", "L_runway": "Long runway", "P_price": "Reasonable entry price"}, "quote": QUOTES["mayer"][1], "note": "Twin engines: earnings growth × multiple expansion; avg 100-bagger took ~17-25y at ~26% CAGR."},
            "Lynch tenbagger": {"author": "Peter Lynch", "legend": {"small_cap": "Small cap — big moves", "fast_grower_not_hyper": "Fast grower 20-60%", "low_dilution_or_buyback": "Low dilution / buybacks", "scalable_margins": "Scalable, high margins", "undiscovered_or_accumulating": "Under-owned / smart money entering"}, "quote": QUOTES["lynch"][1]},
        },
        "dna_archetypes": ["Stealth Microcap", "Quiet Compounder", "Hypergrowth Disruptor", "Turnaround / Down-but-not-out", "Margin-Inflection"],
        "theses": out,
        "methodology": "Per-ticker dossier across 18+ discovery engines, enriched with smart-money (13F famous funds, insiders, Congress, ARK), real valuation (incl. FCF-yield), a grounded multibagger-DNA archetype match, catalysts & chart pattern; scored vs CAN SLIM / SQGLP / Lynch; AI narrative on the top-ranked names. Research, not advice.",
    }
    s3.put_object(Bucket=BUCKET, Key=OUT_KEY, Body=json.dumps(payload, default=str).encode(),
                  ContentType="application/json", CacheControl="public, max-age=900")
    print(f"[upside-thesis v2] {len(out)} candidates · {n_ai} AI · {round(time.time()-t0,1)}s · regime {mkt_state}")
    return {"statusCode": 200, "body": f"{len(out)} candidates, {n_ai} AI"}
