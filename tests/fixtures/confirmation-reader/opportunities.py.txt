"""
justhodl-opportunities-research — adds narrative + deep-research depth to the
flagship S&P-500 Opportunities screener.
================================================================================
opportunities.json already scores every S&P 500 stock into a plain verdict with
3-way valuation, growth-vs-industry, peer comparison, guru metrics, cycle, and
an existing verdict-tier track record. This engine does NOT duplicate any of
that. For the actionable opportunity-tier names it adds the layer the screener
lacks: a connected plain-English thesis + bear case (consistent with the model's
own reasons), 10-year financials, quality/solvency trap-killers (cash conversion,
accruals, net-debt/EBITDA, coverage), confirmation signals (13F, short interest,
insiders), price momentum, and an honest bull/bear conviction scorecard.
Output: data/opportunities-research.json (keyed by ticker). No competing track
record — the page's verdict-tier record stays canonical.
"""
import json
import time
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed

import boto3
import equity_enrich as EE

VERSION = "1.0.0"
S3 = boto3.client("s3", region_name="us-east-1")
BUCKET = "justhodl-dashboard-live"
SRC_KEY = "data/opportunities.json"
OUT_KEY = "data/opportunities-research.json"
TOP_N = 40
THESIS_CACHE_HRS = 20
THESIS_VER = "opp-1"
MAX_NEW = 40
OPP_VERDICTS = {"STRONG OPPORTUNITY", "OPPORTUNITY"}

SYSTEM = (
    "You are a sharp, honest equity analyst explaining to a smart beginner why a stock has screened as "
    "an undervalued opportunity in a disciplined value+quality+growth model, and whether it's a real "
    "one. Plain English, zero jargon, no hype, no price targets, never promise gains. You are given the "
    "model's own reasons and the company's numbers. Output EXACTLY two labeled parts.\n"
    "THESIS: 3-4 short sentences — the plain-English case for why it could be mispriced (tie to the "
    "specific valuation, growth and quality numbers given), then one sentence naming the main risk.\n"
    "BEAR: 1-2 sentences — the strongest argument it's cheap for a good reason, plus the one specific "
    "number or event that would confirm the bear case.\n"
    "Use the exact labels 'THESIS:' and 'BEAR:'. No headings, no markdown, no bullet points, no "
    "numbered steps, no 'Draft', never restate these instructions."
)


def signals_block(name, r, rec):
    ops = [o for o in (r.get("opportunities") or [])][:4]
    risks = [o for o in (r.get("risks") or [])][:3]
    g = r.get("growth_intel") or {}
    parts = [f"The model's verdict is '{r.get('verdict')}' (opportunity score {r.get('opportunity_score')}/100)."]
    if ops:
        parts.append("Why it screened: " + "; ".join(ops) + ".")
    if risks:
        parts.append("Risks the model flagged: " + "; ".join(risks) + ".")
    if rec.get("pe") is not None:
        own = (f"; P/E at the {rec['pe_pctile']}th percentile of its own 10-year range"
               if rec.get("pe_pctile") is not None else "")
        parts.append(f"- valuation: P/E {round(rec['pe'],1)} vs industry {rec.get('industry_pe')}{own}")
    if g.get("company_rev_growth_pct") is not None:
        parts.append(f"- growth: revenue {g.get('company_rev_growth_pct')}% now, {g.get('expected_company_growth_pct')}% "
                     f"expected vs industry {g.get('industry_growth_pct')}%")
    if rec.get("cash_conv") is not None:
        parts.append(f"- quality: cash conversion {rec['cash_conv']}% (free cash flow vs profit), "
                     f"net debt/EBITDA {rec.get('net_debt_ebitda')}, gross-margin {rec.get('gm_latest')}%")
    if rec.get("ret_3m") is not None:
        parts.append(f"- recent price: {rec.get('ret_1m')}% over 1 month, {rec['ret_3m']}% over 3 months")
    return "\n".join(parts)


def lambda_handler(event=None, context=None):
    t0 = time.time()
    try:
        src = json.loads(S3.get_object(Bucket=BUCKET, Key=SRC_KEY)["Body"].read())
    except Exception as e:
        return {"statusCode": 500, "body": json.dumps({"err": f"no source: {e}"})}
    allrows = [r for r in (src.get("all") or []) if r.get("ticker")]
    pool = [r for r in allrows if r.get("verdict") in OPP_VERDICTS]
    pool.sort(key=lambda r: (r.get("opportunity_score") or 0), reverse=True)
    pool = pool[:TOP_N]
    tickers = [r["ticker"] for r in pool]
    by_src = {r["ticker"]: r for r in pool}
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
    for tk in tickers:
        r = by_src[tk]; fin = fin_map.get(tk, {}) or {}
        ind = fin.get("industry")
        industry_pe = ind_pe.get(ind) or sec_pe.get(fin.get("sector"))
        fins = fin.get("financials") or []
        rec = {
            "name": fin.get("name") or r.get("company"), "industry": ind, "sector": fin.get("sector"),
            "desc": fin.get("desc"), "verdict": r.get("verdict"), "opportunity_score": r.get("opportunity_score"),
            "mkt_cap": fin.get("mkt_cap"), "price": fin.get("price"),
            "ret_1m": fin.get("ret_1m"), "ret_3m": fin.get("ret_3m"), "price_spark": fin.get("price_spark"),
            "pe": fin.get("pe"), "ps": fin.get("ps"), "peg": fin.get("peg"), "ev_ebitda": fin.get("ev_ebitda"),
            "industry_pe": industry_pe, "pe_low": fin.get("pe_low"), "pe_high": fin.get("pe_high"),
            "pe_pctile": fin.get("pe_pctile"), "off_52w_high": fin.get("off_52w_high"),
            "financials": fins, "gm_latest": fin.get("gm_latest"), "om_latest": fin.get("om_latest"),
            "fcfm_latest": fin.get("fcfm_latest"), "gm_trend": fin.get("gm_trend"),
            "share_chg_pct": fin.get("share_chg_pct"), "cash_conv": fin.get("cash_conv"),
            "accruals": fin.get("accruals"), "cur_ratio": fin.get("cur_ratio"), "int_cov": fin.get("int_cov"),
            "net_debt_ebitda": fin.get("net_debt_ebitda"), "next_earnings": fin.get("next_earnings"),
            "beat_rate": fin.get("beat_rate"), "beats_n": fin.get("beats_n"),
            "acq_driven": fin.get("acq_driven"), "acq_pct": fin.get("acq_pct"),
            "seg_conc": fin.get("seg_conc"), "seg_n": fin.get("seg_n"),
            "insider_sig": fin.get("insider_sig"),
        }
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

        # --- conviction scorecard (bull vs bear) ---
        bull = []; bearf = []
        pp = rec.get("pe_pctile")
        if pp is not None and pp < 40: bull.append("cheap vs its own history")
        if pp is not None and pp > 80: bearf.append("expensive vs its own history")
        if industry_pe and rec.get("pe") and rec["pe"] < industry_pe: bull.append("cheaper than industry")
        if rec.get("peg") is not None and rec["peg"] < 1: bull.append("PEG under 1 (growth-adjusted cheap)")
        cc = rec.get("cash_conv")
        if cc is not None and cc >= 80: bull.append("strong cash conversion")
        if cc is not None and cc < 50: bearf.append("weak cash conversion")
        if rec.get("accruals") is not None and rec["accruals"] > 15: bearf.append("high accruals / low earnings quality")
        nde = rec.get("net_debt_ebitda")
        if nde is not None and nde > 4: bearf.append("high leverage")
        if nde is not None and 0 <= nde < 2: bull.append("low leverage")
        if rec.get("int_cov") is not None and rec["int_cov"] < 3: bearf.append("thin interest coverage")
        if rec.get("insider_sig") == "selling": bearf.append("insiders selling")
        if rec.get("insider_sig") == "buying": bull.append("insiders buying")
        if rec.get("beat_rate") is not None and rec["beat_rate"] >= 70: bull.append("consistent earnings beats")
        if (rec.get("sm_net") or 0) > 0: bull.append("institutions adding (13F)")
        if rec.get("fwd_rev_growth") is not None:
            (bull if rec["fwd_rev_growth"] > 0 else bearf).append(
                "forward revenue revised up" if rec["fwd_rev_growth"] > 0 else "forward revenue revised down")
        g = r.get("growth_intel") or {}
        if g.get("expected_to_outgrow_industry"): bull.append("expected to outgrow its industry")
        gmt = rec.get("gm_trend")
        if gmt is not None and gmt > 0.5: bull.append("gross margins expanding")
        if gmt is not None and gmt < -0.5: bearf.append("gross margins compressing")
        if rec.get("acq_driven"): bearf.append("acquisition-driven growth")
        if rec.get("seg_conc") is not None and rec["seg_conc"] > 70: bearf.append("revenue concentration")
        ed = (r.get("estimate_revision") or {}).get("direction")
        if ed == "UP": bull.append("estimate revisions trending up")
        if ed == "DOWN": bearf.append("estimate revisions trending down")
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

    new_theses = 0
    targets = need[:MAX_NEW]

    def _gen(tk):
        rec = out[tk]
        sb = signals_block(rec.get("name") or tk, by_src[tk], rec)
        return tk, EE.make_thesis(rec.get("name") or tk, tk, rec.get("industry"), sb, SYSTEM)

    if targets:
        with ThreadPoolExecutor(max_workers=6) as ex:
            for tk, res in ex.map(_gen, targets):
                th, be = res
                if th:
                    out[tk]["thesis"], out[tk]["thesis_at"] = th, now.isoformat()
                    out[tk]["bear"] = be; out[tk]["thesis_ver"] = THESIS_VER
                    new_theses += 1

    payload = {
        "engine": "opportunities-research", "version": VERSION, "generated_at": now.isoformat(),
        "source_generated_at": src.get("generated_at"), "n": len(out), "new_theses": new_theses,
        "enriched_verdicts": sorted(OPP_VERDICTS), "duration_s": round(time.time() - t0, 1),
        "by_ticker": out,
    }
    S3.put_object(Bucket=BUCKET, Key=OUT_KEY, Body=json.dumps(payload, default=str).encode(),
                  ContentType="application/json", CacheControl="public, max-age=1800")
    print(f"[opportunities-research] {len(out)} tickers, {new_theses} theses, {round(time.time()-t0,1)}s")
    return {"statusCode": 200, "body": json.dumps({"n": len(out), "new_theses": new_theses})}
