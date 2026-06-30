"""justhodl-upside-thesis — per-ticker multibagger thesis engine.

Joins the discovery fleet (upside-radar scans + anatomy, flow/equity confluence,
bagger-engine, momentum, PEAD, dark-pool / 13F / ARK accumulation, short-interest,
estimate-revisions, RS leaders) into a per-name dossier, scores each candidate
against the three canonical multibagger frameworks computed from REAL signals —
  • CAN SLIM (William O'Neil): C·A·N·S·L·I·M
  • SQGLP 100-baggers (Chris Mayer / Phelps): Small·Quality·Growth·Longevity·Price
  • Lynch tenbagger checklist (Peter Lynch)
— writes a deterministic, sourced "why", and (for the top N) an AI thesis via the
LLM router. Output: data/upside-theses.json keyed by ticker, consumed by the
click-to-drill-down panel on upside-radar.html.
"""
import json, os, time
from datetime import datetime, timezone
import boto3

s3 = boto3.client("s3", "us-east-1")
BUCKET = "justhodl-dashboard-live"
OUT_KEY = "data/upside-theses.json"
TOP_AI = int(os.environ.get("TOP_AI", "14"))   # how many get the AI narrative

def rd(k):
    try: return json.loads(s3.get_object(Bucket=BUCKET, Key=f"data/{k}.json")["Body"].read())
    except Exception: return {}

def _up(t): return (t or "").upper().strip()

# investor wisdom surfaced alongside the matching setup
QUOTES = {
    "oneil": ("William O'Neil", "Buy stocks coming out of broad bases and beginning to make new highs — you're trying to find the beginning of a major move."),
    "mayer": ("Chris Mayer", "Margin of safety comes primarily from the quality of the business, combined with a good entry price."),
    "lynch_small": ("Peter Lynch", "Big companies have small moves; small companies have big moves."),
    "lynch_inst": ("Peter Lynch", "If you find a stock with little or no institutional ownership, you've found a potential winner."),
}

def lambda_handler(event=None, context=None):
    t0 = time.time()
    F = {k: rd(k) for k in ["upside-radar","flow-confluence","equity-confluence","bagger-engine",
        "cyclical-bagger","momentum-breakout","pead-signals","dark-pool","capital-flow","ark-holdings",
        "short-interest","estimate-revisions","sector-emergence","master-ranker","best-setups","risk-regime"]}

    dossier = {}  # ticker -> dict
    def D(t):
        t = _up(t)
        if not t: return None
        return dossier.setdefault(t, {"ticker": t, "engines": [], "metrics": {}})

    # ── upside-radar price scans + anatomy ──
    ur = F["upside-radar"]; scans = ur.get("scans", {})
    for row in scans.get("breakout", []) or []:
        d = D(row.get("t"));
        if d: d["engines"].append("breakout"); d["metrics"].update({"px": row.get("c"), "dist_hi_pct": row.get("dist_hi_pct"), "ret63": row.get("ret63"), "ret252": row.get("ret252"), "dvol_x": row.get("dvol_x")})
    for row in scans.get("rs_leaders", []) or []:
        d = D(row.get("t"));
        if d: d["engines"].append("rs_leader"); d["metrics"]["rs_pctile"] = row.get("rs_pctile"); d["metrics"].setdefault("px", row.get("c"))
    for row in scans.get("coiled", []) or []:
        d = D(row.get("t"));
        if d: d["engines"].append("coiled")
    for row in scans.get("footprint", []) or []:
        d = D(row.get("t"));
        if d: d["engines"].append("footprint"); d["metrics"].setdefault("dvol_x", row.get("dvol_x"))
    for row in ur.get("anatomy", []) or []:
        d = D(row.get("ticker"));
        if d: d["engines"].append("anatomy"); d["metrics"].update({"rev_growth_pct": row.get("rev_growth_pct"), "share_chg_4y_pct": row.get("share_chg_4y_pct"), "gross_margin_now": row.get("gross_margin_now"), "gross_margin_then": row.get("gross_margin_then"), "mcap_bn": row.get("mcap_bn"), "anatomy_score": row.get("anatomy_score")})

    # ── confluence ──
    for row in F["flow-confluence"].get("multi_engine_confluence", []) or []:
        d = D(row.get("ticker"));
        if d: d["engines"].append("flow_confluence"); d["metrics"].update({"confluence_score": row.get("score"), "n_flow_engines": row.get("n_engines"), "flow_tags": row.get("tags")})
    for row in F["equity-confluence"].get("confluence_book", []) or []:
        d = D(row.get("ticker"));
        if d: d["engines"].append("equity_confluence"); d["metrics"]["super_families"] = row.get("super_families")
    # ── parallel bagger models ──
    for row in (F["bagger-engine"].get("top_100", []) or [])[:60]:
        d = D(row.get("symbol"));
        if d: d["engines"].append("bagger_engine"); d["metrics"].update({"bagger_score": row.get("bagger_score"), "cap_bucket": row.get("cap_bucket"), "sector": row.get("sector"), "name": row.get("name"), "mcap_bn": d["metrics"].get("mcap_bn") or (round(row.get("market_cap",0)/1e9,2) if row.get("market_cap") else None)})
    for row in (F["cyclical-bagger"].get("cyclical_only_book", []) or [])[:30]:
        d = D(row.get("ticker"));
        if d: d["engines"].append("cyclical_bagger"); d["metrics"]["cyclical_20x_score"] = row.get("cyclical_20x_score")
    for row in (F["momentum-breakout"].get("all_qualifying", []) or [])[:60]:
        d = D(row.get("symbol"));
        if d: d["engines"].append("momentum"); m = row.get("metrics") or {}; d["metrics"].update({"mom_tier": row.get("tier"), "ret_20d_pct": m.get("ret_20d_pct"), "is_parabolic": row.get("is_parabolic")})
    for row in (F["pead-signals"].get("all_qualifying", []) or [])[:60]:
        d = D(row.get("symbol"));
        if d: d["engines"].append("pead"); m = row.get("metrics") or {}; d["metrics"].update({"pead_tier": row.get("tier"), "beat_streak": m.get("streak"), "avg_beat_pct": m.get("avg_beat_pct")})
    # ── accumulation ──
    for row in (F["dark-pool"].get("top_accumulation", []) or []):
        d = D(row.get("ticker"));
        if d: d["engines"].append("dark_pool"); d["metrics"].update({"dark_pool_pct": row.get("dark_pool_pct"), "dark_pool_score": row.get("score")})
    for row in (F["capital-flow"].get("accumulating", []) or []):
        d = D(row.get("ticker"));
        if d: d["engines"].append("institutional_13f"); d["metrics"]["flow_score"] = row.get("flow_score"); d["metrics"].setdefault("sector", row.get("sector"))
    for row in (F["ark-holdings"].get("cross_fund_top", []) or []):
        d = D(row.get("ticker"));
        if d: d["engines"].append("ark"); d["metrics"]["ark_funds"] = row.get("n_funds")
    for row in (F["short-interest"].get("top_squeeze_risk", []) or []):
        d = D(row.get("ticker"));
        if d: d["engines"].append("squeeze"); d["metrics"].update({"short_pct": row.get("latest_short_pct"), "days_to_cover": row.get("days_to_cover")})
    for row in (F["estimate-revisions"].get("estimate_strength_leaders", []) or []):
        d = D(row.get("ticker"));
        if d: d["engines"].append("est_revisions"); d["metrics"].setdefault("name", row.get("company"))
    for row in (F["master-ranker"].get("top_tickers", []) or []):
        d = D(row.get("ticker"));
        if d: d["engines"].append("master_ranker"); d["metrics"]["ranker_score"] = row.get("score")
    for row in (F["best-setups"].get("top_setups", []) or []):
        d = D(row.get("ticker"));
        if d: d["engines"].append("best_setup"); d["metrics"].update({"setup_conviction": row.get("conviction"), "setup_why": row.get("why"), "name": d["metrics"].get("name") or row.get("name")})

    # market regime (CAN SLIM "M")
    rr = F["risk-regime"]; reg = (rr.get("risk_regime") or "").upper()
    mkt_ok = "RISK_ON" in reg or reg == "NEUTRAL" or "MILD" in reg
    mkt_state = rr.get("risk_regime") or "UNKNOWN"

    def chk(cond):  # ✓ / ~ / ✗ helper -> 1.0 / 0.5 / 0.0
        return 1.0 if cond is True else (0.5 if cond == "partial" else 0.0)

    out = {}
    for t, d in dossier.items():
        m = d["metrics"]; eng = sorted(set(d["engines"]))
        rev = m.get("rev_growth_pct"); mcap = m.get("mcap_bn"); gm = m.get("gross_margin_now")
        dil = m.get("share_chg_4y_pct"); dvx = m.get("dvol_x"); rs = m.get("rs_pctile")
        inst = any(e in eng for e in ("dark_pool", "institutional_13f", "ark"))
        # ── CAN SLIM ──
        cs = {
            "C": chk(("pead" in eng and (m.get("beat_streak") or 0) >= 2) or "est_revisions" in eng or (m.get("avg_beat_pct") or 0) > 0),
            "A": chk(True if (rev or 0) >= 25 else ("partial" if (rev or 0) >= 15 else False)),
            "N": chk("breakout" in eng or (m.get("dist_hi_pct") is not None and m.get("dist_hi_pct") >= -1)),
            "S": chk(True if ((dvx or 0) >= 3 and (mcap or 99) < 10) else ("partial" if (mcap or 99) < 10 or (dvx or 0) >= 3 else False)),
            "L": chk(True if (("rs_leader" in eng) or (rs or 0) >= 80) else ("partial" if (rs or 0) >= 70 else False)),
            "I": chk(inst),
            "M": chk(True if mkt_ok else "partial"),
        }
        canslim_score = round(sum(cs.values()) / 7 * 100)
        # ── SQGLP (100-baggers) ──
        sq = {
            "S_small": chk(True if (mcap or 99) < 3 else ("partial" if (mcap or 99) < 10 else False)),
            "Q_quality": chk(True if ((gm or 0) >= 40 and (dil if dil is not None else 99) <= 3) else ("partial" if (gm or 0) >= 35 or (dil if dil is not None else 99) <= 5 else False)),
            "G_growth": chk(True if ((rev or 0) >= 20 or (m.get("bagger_score") or 0) >= 60) else ("partial" if (rev or 0) >= 12 else False)),
            "L_runway": chk(True if (m.get("cap_bucket") in ("micro", "small") and (rev or 0) > 0) else ("partial" if (rev or 0) > 0 else False)),
            "P_price": chk("partial"),  # valuation not in feed set yet
        }
        sqglp_score = round(sum(sq.values()) / 5 * 100)
        # ── Lynch tenbagger checklist ──
        ly = {
            "small_cap": (mcap or 99) < 5,
            "fast_grower_not_hyper": 20 <= (rev or 0) <= 60,
            "low_dilution_or_buyback": (dil is not None and dil <= 2),
            "scalable_margins": (gm or 0) >= 40,
            "undiscovered_or_accumulating": inst or ("flow_confluence" in eng),
        }
        lynch_score = round(sum(1 for v in ly.values() if v) / len(ly) * 100)

        # ── deterministic, sourced "why" ──
        bits = []
        if "breakout" in eng: bits.append(f"pressing 52-wk highs" + (f" on {dvx}× volume" if dvx else "") + " (CAN SLIM 'N'+'S')")
        if "rs_leader" in eng or (rs or 0) >= 80: bits.append(f"RS leader" + (f" at {rs} pctile" if rs else "") + " (O'Neil 'L')")
        if (rev or 0) >= 15: bits.append(f"{rev}% revenue growth")
        if gm: bits.append(f"{gm}% gross margins" + (f" (up from {m.get('gross_margin_then')}%)" if m.get("gross_margin_then") else ""))
        if dil is not None and dil <= 3: bits.append(f"minimal dilution ({dil}% shares/4y) — Mayer 'Quality'")
        if mcap: bits.append(f"${mcap}B base" + (" (small = room to run, Lynch)" if mcap < 5 else ""))
        if inst: bits.append("institutional accumulation (" + ", ".join([x for x in ("dark-pool" if "dark_pool" in eng else "", "13F" if "institutional_13f" in eng else "", "ARK" if "ark" in eng else "") if x]) + ") — CAN SLIM 'I'")
        if m.get("beat_streak"): bits.append(f"{m['beat_streak']}-qtr earnings-beat streak (CAN SLIM 'C')")
        if m.get("bagger_score"): bits.append(f"bagger-engine score {m['bagger_score']} (high ROIC/reinvestment)")
        why = (f"{t} is confirmed by {len(eng)} independent engines: " + "; ".join(bits[:6]) + ".") if bits else f"{t} flagged by {len(eng)} engine(s)."

        # discovery score (for ranking / AI selection)
        disc = len(eng) * 10 + canslim_score * 0.4 + sqglp_score * 0.3 + (m.get("anatomy_score") or 0) * 0.3 + (m.get("confluence_score") or 0) * 5

        out[t] = {
            "ticker": t, "name": m.get("name"), "sector": m.get("sector"),
            "engines_firing": eng, "n_engines": len(eng), "metrics": m,
            "canslim": {"score": canslim_score, "checks": cs},
            "sqglp": {"score": sqglp_score, "checks": sq},
            "lynch": {"score": lynch_score, "checks": ly},
            "market_regime": mkt_state,
            "why": why, "discovery_score": round(disc, 1),
            "ai": None,
        }

    ranked = sorted(out.values(), key=lambda x: -x["discovery_score"])
    # ── AI narrative for the top N ──
    n_ai = 0
    try:
        from llm_router import complete
        SYS = ("You are an institutional growth-equity analyst in the lineage of William O'Neil (CAN SLIM), "
               "Chris Mayer (100-baggers/SQGLP) and Peter Lynch (tenbaggers). Given a quantitative dossier of "
               "live signals for ONE stock, explain crisply why it could be a multibagger and what would break "
               "the thesis. Be specific and grounded ONLY in the dossier — never invent fundamentals. "
               "Return STRICT JSON only, no markdown: {\"headline\":str (<=90 chars), \"why_boom\":str (2-3 sentences), "
               "\"multibagger_case\":str (which framework it best fits and the path to a multi-bag, 2-3 sentences), "
               "\"catalysts\":[2-3 short strings], \"risks\":[2-3 short strings], \"what_breaks_it\":str (1 sentence), "
               "\"best_framework\":\"CAN SLIM\"|\"100-bagger (SQGLP)\"|\"Lynch tenbagger\", \"conviction\":1-5}. "
               "This is research, not advice.")
        for d in ranked[:TOP_AI]:
            doss = {"ticker": d["ticker"], "name": d.get("name"), "sector": d.get("sector"),
                    "engines_firing": d["engines_firing"], "metrics": d["metrics"],
                    "canslim": d["canslim"], "sqglp": d["sqglp"], "lynch": d["lynch"],
                    "market_regime": d["market_regime"]}
            prompt = "DOSSIER:\n" + json.dumps(doss, default=str) + "\n\nReturn the JSON object now."
            try:
                raw = complete(prompt, tier="reason", max_tokens=700, system=SYS)
                txt = (raw or "").strip()
                if txt.startswith("```"): txt = txt.split("```")[1].replace("json", "", 1).strip()
                i, j = txt.find("{"), txt.rfind("}")
                d["ai"] = json.loads(txt[i:j+1]) if i >= 0 else None
                if d["ai"]: n_ai += 1
            except Exception as e:
                print(f"  AI {d['ticker']} err: {str(e)[:70]}")
    except Exception as e:
        print(f"[upside-thesis] LLM router unavailable: {str(e)[:80]} — deterministic theses only")

    payload = {
        "engine": "upside-thesis", "version": "1.0.0",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "elapsed_s": round(time.time() - t0, 1),
        "market_regime": mkt_state, "n_candidates": len(out), "n_ai": n_ai,
        "top_ranked": [d["ticker"] for d in ranked[:40]],
        "frameworks": {
            "CAN SLIM": {"author": "William O'Neil", "legend": {"C": "Current quarterly earnings ≥25% YoY", "A": "Annual earnings growth ≥25% / 3y", "N": "New high / new product", "S": "Supply & demand — small float, volume surge", "L": "Leader (RS ≥80 pctile)", "I": "Institutional sponsorship", "M": "Market in uptrend"}, "quote": QUOTES["oneil"][1]},
            "100-bagger (SQGLP)": {"author": "Chris Mayer", "legend": {"S_small": "Small size — room to compound", "Q_quality": "Quality business + low dilution (owner mindset)", "G_growth": "High growth / ROIC + reinvestment", "L_runway": "Long runway", "P_price": "Reasonable entry price"}, "quote": QUOTES["mayer"][1], "note": "Twin engines: earnings growth × multiple expansion; avg 100-bagger took ~17-25y at ~26% CAGR."},
            "Lynch tenbagger": {"author": "Peter Lynch", "legend": {"small_cap": "Small cap — big moves", "fast_grower_not_hyper": "Fast grower 20-60% (not hyper >100%)", "low_dilution_or_buyback": "Low dilution / buybacks", "scalable_margins": "Scalable, high margins", "undiscovered_or_accumulating": "Under-owned / smart money entering"}, "quote": QUOTES["lynch_small"][1]},
        },
        "theses": out,
        "methodology": "Per-ticker dossier joined across 15 discovery engines; CAN SLIM / SQGLP / Lynch scored from live signals; AI narrative on the top-ranked names via LLM router. Research, not advice.",
    }
    s3.put_object(Bucket=BUCKET, Key=OUT_KEY, Body=json.dumps(payload, default=str).encode(),
                  ContentType="application/json", CacheControl="public, max-age=900")
    print(f"[upside-thesis] {len(out)} candidates · {n_ai} AI theses · {round(time.time()-t0,1)}s · regime {mkt_state}")
    return {"statusCode": 200, "body": f"{len(out)} candidates, {n_ai} AI"}
