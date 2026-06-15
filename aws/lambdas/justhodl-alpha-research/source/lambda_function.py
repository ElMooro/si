"""
justhodl-alpha-research — per-ticker research + scorecard for the Alpha Scoreboard.
================================================================================
The Alpha Scoreboard surfaces stocks flagged by multiple independent alpha
systems at once (asymmetric setups, EPS-revision velocity, smart-money 13F,
deep value, insider clusters, theme leadership, sector rotation). This engine
turns that flat cross-system table into a research terminal: for each top
convergence name it assembles a plain-English thesis + bear case, 10-year
financials, valuation (vs industry AND its own history), quality/solvency,
confirmation signals, and an HONEST bull/bear scorecard — the alpha systems on
one side, the fundamental red flags on the other. It also forward-tests its own
calls vs SPY (no look-ahead). Output: data/alpha-scoreboard-research.json.
"""
import json
import time
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed
from decimal import Decimal

import boto3
import equity_enrich as EE

VERSION = "1.0.0"
S3 = boto3.client("s3", region_name="us-east-1")
BUCKET = "justhodl-dashboard-live"
SRC_KEY = "data/compound-signals.json"
OUT_KEY = "data/alpha-scoreboard-research.json"
TOP_N = 35
THESIS_CACHE_HRS = 20
THESIS_VER = "alpha-1"
MAX_NEW = 35

SYS_LABELS = {
    "asymmetric": "asymmetric setup", "nobrainers": "asymmetric setup",
    "eps_velocity": "EPS revisions accelerating", "eps": "EPS revisions accelerating",
    "smart_money": "smart-money funds buying", "smart": "smart-money funds buying",
    "deep_value": "deep value", "value": "deep value",
    "insider": "insider cluster buying", "insiders": "insider cluster buying",
    "theme": "theme leader", "theme_tiers": "theme leader", "themes": "theme leader",
    "sector_rotation": "sector-rotation tailwind", "sectors": "sector-rotation tailwind",
    "macro": "macro regime support", "compound": "multi-system",
}

SYSTEM = (
    "You are a sharp, honest equity analyst explaining to a smart beginner why several independent "
    "stock-screening systems are flagging the same stock at once, and whether that convergence is a "
    "real opportunity. Plain English, zero jargon, no hype, no price targets, never promise gains. "
    "Output EXACTLY two labeled parts.\n"
    "THESIS: 3-4 short sentences — what this combination of systems means in plain terms (e.g. cheap "
    "valuation + improving estimates + funds buying = an early re-rating setup), tied to the specific "
    "numbers given, then one sentence on the main risk.\n"
    "BEAR: 1-2 sentences — the strongest argument against owning it, plus the one specific number or "
    "event that would prove the bull case wrong.\n"
    "Use the exact labels 'THESIS:' and 'BEAR:'. No headings, no markdown, no bullet points, no "
    "numbered steps, no 'Draft', never restate these instructions."
)


def label_systems(systems):
    out = []
    for s in (systems or []):
        lab = SYS_LABELS.get(str(s).lower().replace(" ", "_"), str(s).replace("_", " "))
        if lab not in out:
            out.append(lab)
    return out


def signals_block(name, syslabels, rec):
    parts = [f"{len(syslabels)} independent alpha systems flag this at once: {', '.join(syslabels)}."]
    if rec.get("pe") is not None:
        pe_own = (f"; that P/E sits at the {rec['pe_pctile']}th percentile of its own 10-year range"
                  if rec.get("pe_pctile") is not None else "")
        parts.append(f"- valuation: P/E {round(rec['pe'],1)} vs industry {rec.get('industry_pe')}{pe_own}")
    if rec.get("rev_growth_yoy") is not None:
        parts.append(f"- revenue growth {rec['rev_growth_yoy']}% YoY, gross margin {rec.get('gm_latest')}%, "
                     f"free-cash-flow margin {rec.get('fcfm_latest')}%")
    if rec.get("cash_conv") is not None:
        parts.append(f"- cash conversion {rec['cash_conv']}% (free cash flow vs reported profit), "
                     f"net debt/EBITDA {rec.get('net_debt_ebitda')}")
    if rec.get("ret_3m") is not None:
        parts.append(f"- recent price action: {rec.get('ret_1m')}% over 1 month, {rec['ret_3m']}% over 3 months")
    return "\n".join(parts)


def log_signals(rows):
    try:
        tbl = boto3.resource("dynamodb", region_name="us-east-1").Table("justhodl-signals")
        now = datetime.now(timezone.utc); d0 = now.strftime("%Y-%m-%d"); n = 0
        for r in rows[:15]:
            px = r.get("price")
            if not px:
                continue
            tbl.put_item(Item={
                "signal_id": f"alpha-scoreboard#{r['ticker']}#{d0}", "signal_type": "alpha_scoreboard",
                "predicted_direction": "UP", "baseline_price": str(px), "benchmark": "SPY",
                "measure_against": "ticker", "check_windows": [f"day_{w}" for w in (5, 21, 63)],
                "logged_at": now.isoformat(), "logged_epoch": int(now.timestamp()),
                "status": "pending", "schema_version": "2", "horizon_days_primary": 21,
                "ttl": int(now.timestamp()) + 120 * 86400,
                "signal_value": str(r.get("compound_score")),
                "metadata": {"n_systems": str(r.get("n_systems")), "engine": "alpha-scoreboard"},
            })
            n += 1
        return n
    except Exception as e:
        print(f"[signals] {str(e)[:90]}"); return 0


def compute_changes(ranked, status_by_tk):
    PK = "data/alpha-prev-state.json"
    try:
        prev = json.loads(S3.get_object(Bucket=BUCKET, Key=PK)["Body"].read())
    except Exception:
        prev = {}
    prev_ranked = set(prev.get("ranked") or [])
    new_entrants = [t for t in ranked if t not in prev_ranked] if prev_ranked else []
    dropped = [t for t in prev_ranked if t not in set(ranked)] if prev_ranked else []
    try:
        S3.put_object(Bucket=BUCKET, Key=PK,
                      Body=json.dumps({"date": datetime.now(timezone.utc).date().isoformat(),
                                       "ranked": ranked, "status": status_by_tk}).encode(),
                      ContentType="application/json")
    except Exception:
        pass
    if not prev_ranked:
        return {"first_run": True}
    return {"new": new_entrants[:10], "dropped": dropped[:10]}


def lambda_handler(event=None, context=None):
    t0 = time.time()
    try:
        src = json.loads(S3.get_object(Bucket=BUCKET, Key=SRC_KEY)["Body"].read())
    except Exception as e:
        return {"statusCode": 500, "body": json.dumps({"err": f"no source: {e}"})}
    comp = src.get("compound") or src.get("rows") or []
    comp = [c for c in comp if c.get("symbol")]
    comp.sort(key=lambda c: (c.get("compound_score") if c.get("compound_score") is not None else -1,
                             c.get("n_systems") or 0), reverse=True)
    comp = comp[:TOP_N]
    tickers = [c["symbol"] for c in comp]
    try:
        cache = json.loads(S3.get_object(Bucket=BUCKET, Key=OUT_KEY)["Body"].read()).get("by_ticker", {})
    except Exception:
        cache = {}
    now = datetime.now(timezone.utc)

    ind_pe, sec_pe = EE.fetch_peer_pe()
    si_f, f13_f, fwd_f, chain_f = EE.load_confirmation_feeds()

    fin_map = {}
    with ThreadPoolExecutor(max_workers=8) as ex:
        futs = {ex.submit(EE.fetch_financials, tk): tk for tk in tickers}
        for f in as_completed(futs):
            try:
                fin_map[futs[f]] = f.result()
            except Exception:
                fin_map[futs[f]] = {}

    out = {}; need = []
    for c in comp:
        tk = c["symbol"]; fin = fin_map.get(tk, {}) or {}
        ind = fin.get("industry")
        industry_pe = ind_pe.get(ind) or sec_pe.get(fin.get("sector"))
        syslabels = label_systems(c.get("systems"))
        rec = {
            "name": fin.get("name") or c.get("name"), "industry": ind, "sector": fin.get("sector"),
            "desc": fin.get("desc"), "website": fin.get("website"), "employees": fin.get("employees"),
            "compound_score": c.get("compound_score"), "n_systems": c.get("n_systems") or len(syslabels),
            "systems": syslabels, "sys_scores": c.get("scores") or {},
            "mkt_cap": fin.get("mkt_cap"), "price": fin.get("price"),
            "ret_1m": fin.get("ret_1m"), "ret_3m": fin.get("ret_3m"), "price_spark": fin.get("price_spark"),
            "pe": fin.get("pe"), "ps": fin.get("ps"), "pb": fin.get("pb"), "peg": fin.get("peg"),
            "ev_ebitda": fin.get("ev_ebitda"), "industry_pe": industry_pe,
            "pe_low": fin.get("pe_low"), "pe_high": fin.get("pe_high"), "pe_pctile": fin.get("pe_pctile"),
            "div_yield": fin.get("div_yield"), "beta": fin.get("beta"), "range_52w": fin.get("range_52w"),
            "financials": fin.get("financials") or [],
            "rev_growth_yoy": (fin.get("financials") or [{}])[-1].get("revenue") and None,
            "gm_latest": fin.get("gm_latest"), "om_latest": fin.get("om_latest"),
            "fcfm_latest": fin.get("fcfm_latest"), "gm_trend": fin.get("gm_trend"),
            "share_chg_pct": fin.get("share_chg_pct"),
            "cash_conv": fin.get("cash_conv"), "accruals": fin.get("accruals"),
            "cur_ratio": fin.get("cur_ratio"), "int_cov": fin.get("int_cov"),
            "net_debt_ebitda": fin.get("net_debt_ebitda"),
            "off_52w_high": fin.get("off_52w_high"), "next_earnings": fin.get("next_earnings"),
            "beat_rate": fin.get("beat_rate"), "beats_n": fin.get("beats_n"),
            "nq_eps_est": fin.get("nq_eps_est"), "nq_rev_est": fin.get("nq_rev_est"),
            "acq_driven": fin.get("acq_driven"), "acq_pct": fin.get("acq_pct"),
            "seg_conc": fin.get("seg_conc"), "seg_n": fin.get("seg_n"),
            "insider_sig": fin.get("insider_sig"), "insider_buys": fin.get("insider_buys"),
            "insider_sells": fin.get("insider_sells"),
        }
        # revenue growth from financials (latest vs prior)
        fins = fin.get("financials") or []
        if len(fins) >= 2 and fins[-2].get("revenue"):
            rec["rev_growth_yoy"] = round((fins[-1]["revenue"] / fins[-2]["revenue"] - 1) * 100, 1)
        # confirmation
        s = si_f.get(tk) or {}
        if s:
            rec["short_pct"] = s.get("latest_short_pct"); rec["short_signal"] = s.get("signal")
        ff = f13_f.get(tk) or {}
        if ff:
            rec["sm_funds"] = ff.get("n_funds_holding")
            rec["sm_net"] = (ff.get("n_funds_adding") or 0) - (ff.get("n_funds_trimming") or 0)
            rec["sm_value"] = ff.get("total_value")
        if tk in fwd_f:
            rec["fwd_rev_growth"] = fwd_f.get(tk)
        if tk in chain_f:
            rec["chain"] = chain_f.get(tk)

        # --- scorecard: alpha systems (bull) + fundamental red flags (bear) ---
        bull = [f"system: {s}" for s in syslabels]
        bearf = []
        pp = rec.get("pe_pctile")
        if pp is not None and pp < 40: bull.append("cheap vs own history")
        if pp is not None and pp > 80: bearf.append("expensive vs own history")
        cc = rec.get("cash_conv")
        if cc is not None and cc >= 80: bull.append("strong cash conversion")
        if cc is not None and cc < 50: bearf.append("weak cash conversion")
        ac = rec.get("accruals")
        if ac is not None and ac > 15: bearf.append("high accruals / low earnings quality")
        nde = rec.get("net_debt_ebitda")
        if nde is not None and nde > 4: bearf.append("high leverage")
        if nde is not None and 0 <= nde < 2: bull.append("low leverage")
        if rec.get("insider_sig") == "selling": bearf.append("insiders selling")
        if rec.get("beat_rate") is not None and rec["beat_rate"] >= 70: bull.append("consistent earnings beats")
        if rec.get("acq_driven"): bearf.append("acquisition-driven growth")
        if rec.get("seg_conc") is not None and rec["seg_conc"] > 70: bearf.append("revenue concentration")
        gmt = rec.get("gm_trend")
        if gmt is not None and gmt > 0.5: bull.append("gross margins expanding")
        if gmt is not None and gmt < -0.5: bearf.append("gross margins compressing")
        rec["score_bull"] = len(bull); rec["score_bear"] = len(bearf)
        rec["flags_bull"] = bull; rec["flags_bear"] = bearf

        cached = cache.get(tk, {})
        ts = cached.get("thesis_at"); fresh = False
        if ts:
            try:
                fresh = (now - datetime.fromisoformat(ts)).total_seconds() < THESIS_CACHE_HRS * 3600
            except Exception:
                fresh = False
        rec["thesis"], rec["thesis_at"], rec["bear"] = cached.get("thesis"), cached.get("thesis_at"), cached.get("bear")
        if not (fresh and cached.get("thesis") and cached.get("thesis_ver") == THESIS_VER):
            need.append(tk)
        else:
            rec["thesis_ver"] = THESIS_VER
        out[tk] = rec

    # theses in parallel
    new_theses = 0
    targets = need[:MAX_NEW]

    def _gen(tk):
        rec = out[tk]
        sb = signals_block(rec.get("name") or tk, rec.get("systems") or [], rec)
        return tk, EE.make_thesis(rec.get("name") or tk, tk, rec.get("industry"), sb, SYSTEM)

    if targets:
        with ThreadPoolExecutor(max_workers=6) as ex:
            for tk, res in ex.map(_gen, targets):
                th, be = res
                if th:
                    out[tk]["thesis"], out[tk]["thesis_at"] = th, now.isoformat()
                    out[tk]["bear"] = be; out[tk]["thesis_ver"] = THESIS_VER
                    new_theses += 1

    # concentration by sector
    from collections import Counter
    sect = Counter((out.get(c["symbol"], {}) or {}).get("sector") for c in comp[:10]
                   if (out.get(c["symbol"], {}) or {}).get("sector"))
    dom_s, dom_n = (sect.most_common(1)[0] if sect else (None, 0))
    concentration = {"dominant_sector": dom_s, "count": dom_n, "of": min(10, len(comp)), "sectors": dict(sect)}

    # changes + logging + track record
    status_by_tk = {tk: out[tk].get("n_systems") for tk in tickers}
    changes = compute_changes(tickers, status_by_tk)
    n_logged = log_signals([{"ticker": tk, **out[tk]} for tk in tickers])
    track = EE.grade_track_record("alpha_scoreboard", "data/alpha-track-record.json")

    payload = {
        "engine": "alpha-research", "version": VERSION, "generated_at": now.isoformat(),
        "source_generated_at": src.get("generated_at"), "n": len(out), "new_theses": new_theses,
        "signals_logged": n_logged, "duration_s": round(time.time() - t0, 1),
        "concentration": concentration, "changes": changes, "track_record": track,
        "by_ticker": out,
    }
    S3.put_object(Bucket=BUCKET, Key=OUT_KEY, Body=json.dumps(payload, default=str).encode(),
                  ContentType="application/json", CacheControl="public, max-age=1800")
    print(f"[alpha-research] {len(out)} tickers, {new_theses} theses, {n_logged} logged, {round(time.time()-t0,1)}s")
    return {"statusCode": 200, "body": json.dumps({"n": len(out), "new_theses": new_theses})}
