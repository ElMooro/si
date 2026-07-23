"""
justhodl-chokepoint  ·  v1.0  —  INDUSTRY-CRITICALITY / CHOKEPOINT SCORER
================================================================================
Finds the "make-it-or-break-it" companies — the structural chokepoints their
industry CAN'T route around. Not "good company"; IRREPLACEABLE. TSM (everything
in AI fabbing), ASML (the only EUV maker), the EDA duopoly (CDNS/SNPS), the
ratings/index oligopolies (MCO/SPGI/MSCI), the payment rails (V/MA).

The clean signal — market share / "is there a substitute" — isn't in free data.
So criticality is triangulated from computable proxies, CALIBRATED so textbook
chokepoints (ASML/TSM/NVDA/V/MA/SNPS/CDNS/KLAC, avg 64.9) cleanly separate from
commodity/substitutable names (GT/UAL/DOW/X/GNK/CCL, avg 21.9 — min chokepoint
54.6 > max commodity 34.0, no overlap):

  criticality = 0.30*margin_level + 0.22*margin_STABILITY + 0.20*ROIC
              + 0.13*R&D_intensity + 0.15*supply_chain_centrality

The standout proxy is MARGIN STABILITY: a company that holds an 80% gross margin
through every cycle (V=1.0 std, CDNS=1.4) is, by definition, something customers
can't walk away from. Commodities swing wildly (CCL=56.9 std). Stable pricing
power IS the fingerprint of irreplaceability.

THE EDGE (criticality alone is a quality watchlist the market already prices):
  • HIDDEN chokepoints  — high criticality + low profile (small/mid sole-suppliers
                          nobody watches). Where the engine earns its keep.
  • CHEAP chokepoints   — criticality fused with rerating-radar discount /
                          cyclical-bagger trough: an indispensable company on sale.
  • per-INDUSTRY leader — the single most-critical name in each industry.

Honest scope (v1): supply-chain centrality covers only the curated graph's sectors
(semis/AI); financial/other chokepoints lean on margins+stability. Pool = curated
chokepoint seed + existing engine universes. v2 = broad-universe scan for hidden
small-cap sole-suppliers. Cheap-chokepoint picks logged to the harvester (the
gradeable edge); pure criticality is a quality screen, not a timing signal.
"""
import json, time, statistics, urllib.request, csv, io
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed
import boto3

VERSION = "3.1"
BUCKET = "justhodl-dashboard-live"
OUT_KEY = "data/chokepoint.json"
FMP = "wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb"
s3 = boto3.client("s3", "us-east-1")

CHOKE_THRESHOLD = 50.0   # calibrated gap: commodity max 34, chokepoint min 54.6

# curated chokepoint seed — genuine cross-industry "make-it-or-break-it" candidates
CURATED_SEED = [
    # EDA / silicon IP / litho / semicap (upstream of all chips)
    "SNPS","CDNS","ASML","KLAC","LRCX","AMAT","ACLS","ONTO","CAMT","ARM","TSM","NVDA","AVGO","MPWR","ENTG","COHR",
    # ratings / index / exchanges / data oligopolies
    "MCO","SPGI","MSCI","FICO","ICE","CME","NDAQ","CBOE","FDS","VRSK","TRU","MORN",
    # payment rails
    "V","MA","FIS","FI","ADYEY",
    # mission-critical med / life-science tools
    "ISRG","IDXX","RMD","WST","MTD","WAT","TMO","DHR","A","RGEN","TECH","BIO",
    # aerospace aftermarket / specialty industrial monopolies
    "TDG","HEI","ROP","AME","FAST","GGG","NDSN","ITW","DOV",
    # industrial gas / specialty materials oligopolies
    "LIN","APD","SHW","ECL","ALB",
    # software platforms with switching-cost moats
    "ADBE","INTU","NOW","CRM","ORCL","PANW","CRWD","WDAY","ANSS","TYL",
]


def _read(key):
    try: return json.loads(s3.get_object(Bucket=BUCKET, Key=key)["Body"].read())
    except Exception: return None

def _gj(url, tries=2):
    for _ in range(tries):
        try:
            with urllib.request.urlopen(url, timeout=22) as r: return json.loads(r.read())
        except Exception: time.sleep(1)
    return None

def clamp(v, lo=0.0, hi=1.0): return max(lo, min(hi, v))


# chokepoint-prone industries (where structural pricing power / sole-supplier dynamics live).
# substring match, lowercase — a commodity industry (autos, airlines, steel) never qualifies.
CHOKE_INDUSTRIES = [
    "software", "semiconductor", "medical", "diagnostic", "instrument", "scientific",
    "specialty chemical", "specialty industrial", "aerospace", "defense", "electronic",
    "communication equipment", "computer hardware", "information technology", "biotechnology",
    "drug manufacturer", "security & protection", "life sciences", "technology hardware",
    "capital markets", "financial data", "exchange", "consumer electronics", "data",
]

def _gj_bytes(url, tries=2):
    for _ in range(tries):
        try:
            with urllib.request.urlopen(url, timeout=60) as r: return r.read()
        except Exception: time.sleep(1)
    return None

def fetch_bulk_universe():
    """One call -> {SYMBOL: profile} for the whole market (Stage-1 of the funnel)."""
    raw = _gj_bytes(f"https://financialmodelingprep.com/stable/profile-bulk?part=0&apikey={FMP}")
    if not raw: return {}
    out = {}
    try:
        rdr = csv.DictReader(io.StringIO(raw.decode("utf-8", "replace")))
        for row in rdr:
            sym = (row.get("symbol") or "").upper()
            if not sym or "." in sym or "-" in sym: continue   # skip foreign/preferred share classes
            try: mc = float(row.get("marketCap") or 0)
            except Exception: mc = 0
            out[sym] = {
                "market_cap": mc, "sector": row.get("sector"), "industry": row.get("industry"),
                "name": row.get("companyName"),
                "exchange": row.get("exchangeShortName") or row.get("exchange"),
                "active": str(row.get("isActivelyTrading")).lower() in ("true", "1"),
                "is_etf": str(row.get("isEtf")).lower() in ("true", "1"),
                "is_fund": str(row.get("isFund")).lower() in ("true", "1"),
            }
    except Exception:
        pass
    return out


def evaluate(sym, centrality, rerate_lookup, bulk):
    inc = _gj(f"https://financialmodelingprep.com/stable/income-statement?symbol={sym}&period=annual&limit=10&apikey={FMP}")
    if not isinstance(inc, list) or len(inc) < 4: return None
    gms, oms, rds = [], [], []
    for r in inc:
        rev = r.get("revenue") or 0
        if rev <= 0: continue
        gms.append((r.get("grossProfit") or 0) / rev * 100)
        oms.append((r.get("operatingIncome") or 0) / rev * 100)
        rds.append((r.get("researchAndDevelopmentExpenses") or 0) / rev * 100)
    if len(gms) < 4: return None
    gm_level = statistics.mean(gms); gm_stab = statistics.pstdev(gms)
    om_level = statistics.mean(oms); rd_int = statistics.mean(rds)

    # ROIC (fixed: key-metrics, then ratios fallback)
    roic = None
    km = _gj(f"https://financialmodelingprep.com/stable/key-metrics?symbol={sym}&period=annual&limit=1&apikey={FMP}")
    if isinstance(km, list) and km:
        roic = km[0].get("returnOnInvestedCapital")
    if roic is None:
        rt = _gj(f"https://financialmodelingprep.com/stable/ratios?symbol={sym}&period=annual&limit=1&apikey={FMP}")
        if isinstance(rt, list) and rt:
            roic = rt[0].get("returnOnInvestedCapital") or rt[0].get("returnOnCapitalEmployed") or rt[0].get("returnOnEquity")
    if isinstance(roic, (int, float)):
        roic = roic * 100 if abs(roic) < 3 else roic

    # profile from the bulk universe (no per-name profile call); fallback for names not in bulk
    bp = bulk.get(sym) or {}
    mcap = bp.get("market_cap"); sector = bp.get("sector"); industry = bp.get("industry"); name = bp.get("name")
    if not mcap:
        prof = _gj(f"https://financialmodelingprep.com/stable/profile?symbol={sym}&apikey={FMP}")
        if isinstance(prof, list) and prof:
            mcap = prof[0].get("marketCap"); sector = sector or prof[0].get("sector")
            industry = industry or prof[0].get("industry"); name = name or prof[0].get("companyName")
    cap_bucket = None
    if isinstance(mcap, (int, float)):
        cap_bucket = ("nano" if mcap < 3e8 else "micro" if mcap < 2e9 else "small" if mcap < 1e10 else
                      "mid" if mcap < 5e10 else "large" if mcap < 2e11 else "mega")
    ctr = centrality.get(sym, 0)

    # ---- calibrated criticality ----
    s_gm = clamp(gm_level / 70.0)
    s_stab = clamp(1.0 - gm_stab / 15.0)               # the fingerprint: stable pricing power
    s_roic = clamp((roic or 0) / 30.0) if roic is not None else 0.3
    s_rd = clamp(rd_int / 20.0)
    s_ctr = clamp(ctr / 8.0)
    crit = round(100 * (0.30 * s_gm + 0.22 * s_stab + 0.20 * s_roic + 0.13 * s_rd + 0.15 * s_ctr), 1)
    is_choke = crit >= CHOKE_THRESHOLD

    # cheap overlay (the actionable edge)
    rr = rerate_lookup.get(sym, {})
    disc = rr.get("discount_to_implied_pct")
    cheap = isinstance(disc, (int, float)) and disc >= 25

    reasons = []
    if gm_level >= 50: reasons.append(f"{gm_level:.0f}% gross margin")
    if gm_stab <= 4: reasons.append(f"ultra-stable margins (\u00b1{gm_stab:.1f}pp \u2014 pricing power through cycles)")
    if ctr >= 3: reasons.append(f"supply-chain hub (centrality {ctr})")
    if rd_int >= 15: reasons.append(f"{rd_int:.0f}% R&D barrier")
    if roic and roic >= 20: reasons.append(f"{roic:.0f}% ROIC")
    if cheap: reasons.append(f"trading {disc:.0f}% below fair (cheap chokepoint)")

    return {
        "ticker": sym, "name": name, "sector": sector, "industry": industry,
        "criticality": crit, "is_chokepoint": is_choke,
        "cap_bucket": cap_bucket, "market_cap": mcap,
        "gm_level": round(gm_level, 1), "gm_stability": round(gm_stab, 1),
        "om_level": round(om_level, 1), "rd_intensity": round(rd_int, 1),
        "roic": round(roic, 1) if isinstance(roic, (int, float)) else None,
        "centrality": ctr, "discount_to_fair_pct": disc, "cheap_chokepoint": bool(is_choke and cheap),
        "hidden_chokepoint": bool(is_choke and cap_bucket in ("nano", "micro", "small", "mid")),
        "reasons": reasons,
    }


def _parse_json_array(txt):
    if not txt: return []
    import re
    t = txt.strip()
    if "```" in t:
        m = re.search(r"```(?:json)?\s*(.*?)```", t, re.S)
        if m: t = m.group(1).strip()
    a, b = t.find("["), t.rfind("]")
    if a >= 0 and b > a:
        try: return json.loads(t[a:b + 1])
        except Exception: pass
    # per-object fallback: one malformed entry shouldn't kill the batch
    out = []
    for m in re.finditer(r"\{[^{}]*\}", t):
        try: out.append(json.loads(m.group(0)))
        except Exception: continue
    return out


def verify_irreplaceability(candidates, max_n=56, budget_s=190):
    """LLM judgment: is each a TRUE chokepoint the industry can't route around, or just a
    high-margin business? Margins alone can't tell ADSK (CAD standard) from DUOL (a high-margin
    app). Cached in S3 (only uncached names judged); fault-tolerant (LLM down -> verdicts skipped).
    Small batches + retry + wall-clock budget so it converges in ONE run, not over several days."""
    cache_key = "data/_cache/chokepoint-irreplaceability.json"
    cache = _read(cache_key) or {}
    todo = [c for c in candidates[:max_n] if c["ticker"] not in cache]
    if not todo:
        return cache
    try:
        from llm_router import complete
    except Exception:
        return cache
    SYS = ("You are a hedge-fund analyst judging STRUCTURAL industry criticality. A CHOKEPOINT is a company "
           "its industry genuinely cannot route around: a sole or dominant provider of something essential where "
           "switching away is impossible or prohibitively costly (ASML = only EUV litho; TSMC = leading-edge "
           "foundry; Visa/Mastercard = payment rails; Autodesk = the CAD/AEC standard with deep lock-in; Cadence/"
           "Synopsys = the EDA duopsony chip designers must use). A company is NOT a chokepoint merely because it "
           "has high margins — most software and single-drug biotech businesses have high margins BY DEFAULT yet "
           "face real substitutes (Duolingo, Dropbox, an ordinary SaaS app, a one-drug biotech). Be strict.")

    def judge_batch(batch):
        lst = "\n".join(f"- {c['ticker']} {c.get('name') or ''} ({c.get('industry') or ''})" for c in batch)
        prompt = ("Classify each company. verdict = CHOKEPOINT (its industry cannot route around it), "
                  "NOT (real substitutes exist / ordinary high-margin business), or UNCERTAIN (you do not know it "
                  "well enough). When in doubt use NOT or UNCERTAIN. Return ONLY a JSON array, no prose:\n"
                  '[{"ticker":"X","verdict":"CHOKEPOINT|NOT|UNCERTAIN","reason":"<=8 words"}]\n\nCompanies:\n' + lst)
        try:
            return _parse_json_array(complete(prompt, tier="reason", max_tokens=1200, system=SYS))
        except Exception as e:
            print("[verify] batch error:", str(e)[:70]); return []

    deadline = time.time() + budget_s
    judged = 0
    for i in range(0, len(todo), 8):
        if time.time() > deadline:
            print("[verify] budget reached; %d names left for next run (cached)" % (len(todo) - i)); break
        batch = todo[i:i + 8]
        arr = judge_batch(batch) or judge_batch(batch)   # one retry on empty
        got = set()
        for o in arr:
            tk = str(o.get("ticker", "")).upper()
            v = str(o.get("verdict", "")).upper()
            if tk and v in ("CHOKEPOINT", "NOT", "UNCERTAIN"):
                cache[tk] = {"verdict": v, "reason": str(o.get("reason", ""))[:60],
                             "judged_at": datetime.now(timezone.utc).isoformat()[:10]}
                got.add(tk); judged += 1
    if judged:
        try:
            s3.put_object(Bucket=BUCKET, Key=cache_key, Body=json.dumps(cache).encode(), ContentType="application/json")
        except Exception: pass
    return cache


def lambda_handler(event, context):
    t0 = time.time(); diag = []

    # centrality from the curated supply-chain graph (out-edges = hub-ness)
    centrality = {}
    g = _read("data/supply-chain-graph.json") or {}
    for e in (g.get("edges") or []):
        sup = e.get("supplier") or e.get("source")
        if sup: centrality[sup] = centrality.get(sup, 0) + 1
    for n in (g.get("nodes") or []):
        s = n.get("symbol") or n.get("id")
        if s and isinstance(n.get("centrality"), (int, float)):
            centrality[s] = max(centrality.get(s, 0), n["centrality"] * 10)
    diag.append("centrality_nodes=%d" % len(centrality))

    rerate = _read("data/ai-rerating-radar.json") or {}
    rerate_lookup = {r["symbol"].upper(): r for r in rerate.get("all_ranked", []) if r.get("symbol")}

    # ---- v1 curated pool: seed + hubs + existing engine universes ----
    v1_pool = set(CURATED_SEED) | set(centrality.keys())
    for r in rerate.get("all_ranked", []):
        if r.get("symbol"): v1_pool.add(r["symbol"].upper())
    mr = _read("data/master-ranker.json") or {}
    for r in (mr.get("top_tickers") or [])[:25]:
        s = r.get("ticker") or r.get("symbol")
        if s: v1_pool.add(s.upper())
    bag = _read("data/bagger-engine.json") or {}
    for r in (bag.get("candidates") or bag.get("all_ranked") or [])[:30]:
        s = r.get("symbol") or r.get("ticker")
        if s: v1_pool.add(s.upper())

    # ---- v2 funnel: one bulk call -> high-margin-prone small/mid discovery candidates ----
    bulk = fetch_bulk_universe()
    diag.append("bulk_universe=%d" % len(bulk))
    funnel = []
    for sym, p in bulk.items():
        if sym in v1_pool: continue
        if p["is_etf"] or p["is_fund"] or not p["active"]: continue
        if (p.get("exchange") or "") not in ("NASDAQ", "NYSE", "AMEX"): continue
        mc = p["market_cap"]
        if not (8e8 <= mc <= 5e10): continue                       # $0.8B-$50B established small/mid
        ind = (p.get("industry") or "").lower()
        if not any(w in ind for w in CHOKE_INDUSTRIES): continue
        funnel.append((sym, mc))
    funnel.sort(key=lambda x: -x[1])                               # established niche leaders first
    funnel_syms = [s for s, _ in funnel[:380]]
    diag.append("funnel_candidates=%d -> deep-dive %d" % (len(funnel), len(funnel_syms)))

    pool = list(v1_pool) + funnel_syms
    diag.append("pool=%d (v1=%d + funnel=%d)" % (len(pool), len(v1_pool), len(funnel_syms)))

    results = []
    with ThreadPoolExecutor(max_workers=18) as ex:
        futs = {ex.submit(evaluate, s, centrality, rerate_lookup, bulk): s for s in pool}
        for f in as_completed(futs):
            try:
                r = f.result()
                if r: results.append(r)
            except Exception: pass
    for r in results:
        r["discovered"] = r["ticker"] not in v1_pool      # found by the broad scan, not the curated seed
    diag.append("evaluated=%d" % len(results))

    results.sort(key=lambda r: r["criticality"], reverse=True)
    chokepoints = [r for r in results if r["is_chokepoint"]]
    hidden = [r for r in chokepoints if r["hidden_chokepoint"]]
    cheap = [r for r in chokepoints if r["cheap_chokepoint"]]
    cheap.sort(key=lambda r: (r.get("discount_to_fair_pct") or 0), reverse=True)
    # the v2 payoff: chokepoints the broad scan found that the curated seed/engines missed
    discovered = [r for r in chokepoints if r.get("discovered")]
    discovered.sort(key=lambda r: -r["criticality"])

    # v2.1: LLM irreplaceability filter — separate TRUE chokepoints from "just high-margin"
    verdicts = {}
    try:
        verdicts = verify_irreplaceability(discovered)
    except Exception as e:
        diag.append("verify_failed:%s" % str(e)[:40])
    for r in results:
        v = verdicts.get(r["ticker"])
        if v:
            r["irreplaceability"] = v["verdict"]; r["irreplaceability_reason"] = v.get("reason")
    confirmed = [r for r in discovered if r.get("irreplaceability") == "CHOKEPOINT"]
    rejected_hi_margin = [r for r in discovered if r.get("irreplaceability") == "NOT"]
    diag.append("verified=%d confirmed=%d rejected=%d" % (len(verdicts), len(confirmed), len(rejected_hi_margin)))

    # ---- STRUCTURAL set: the names the decision engines should treat as chokepoints ----
    # verified-structural = curated genuine chokepoint OR LLM-confirmed discovery OR supply-chain hub.
    # (High-criticality-but-unverified discoveries like high-margin SaaS/biotech are excluded.)
    for r in results:
        r["curated"] = r["ticker"] in CURATED_SEED
        r["structural"] = bool(r["is_chokepoint"] and (
            r["curated"] or r.get("irreplaceability") == "CHOKEPOINT" or (r.get("centrality") or 0) >= 3))
    structural = [r for r in chokepoints if r["structural"]]
    structural.sort(key=lambda r: -r["criticality"])
    structural_names = {r["ticker"]: {"criticality": r["criticality"], "curated": r["curated"],
                                      "confirmed": r.get("irreplaceability") == "CHOKEPOINT",
                                      "hub": r.get("centrality", 0)} for r in structural}

    # ---- FUSION: a structural chokepoint AT a cyclical trough (or cheap) = the highest-quality setup ----
    cyc = _read("data/cyclical-bagger.json") or {}
    trough_map = {}
    for v in cyc.values():
        if isinstance(v, list):
            for rr in v:
                if isinstance(rr, dict) and rr.get("twenty_x_shape") and rr.get("stage") in ("EARLY", "CONFIRMING"):
                    tk = (rr.get("ticker") or "").upper()
                    if tk: trough_map[tk] = {"om_trough": rr.get("om_trough"), "om_swing_pp": rr.get("om_swing_pp"),
                                             "stage": rr.get("stage"), "cyc_score": rr.get("cyclical_20x_score")}
    structural_set = {r["ticker"] for r in structural}
    highest_conviction = []
    for r in structural:
        at_trough = r["ticker"] in trough_map
        is_cheap = bool(r.get("cheap_chokepoint"))
        if not (at_trough or is_cheap): continue
        setup_type = "TROUGH+CHEAP" if (at_trough and is_cheap) else ("AT_CYCLICAL_TROUGH" if at_trough else "CHEAP")
        highest_conviction.append({
            "ticker": r["ticker"], "name": r.get("name"), "criticality": r["criticality"],
            "setup_type": setup_type, "confirmed": r.get("irreplaceability") == "CHOKEPOINT",
            "curated": r["curated"], "discount_to_fair_pct": r.get("discount_to_fair_pct"),
            "trough": trough_map.get(r["ticker"]),
            "why": ("A structurally indispensable chokepoint " + (
                "at a cyclical trough AND trading cheap" if setup_type == "TROUGH+CHEAP" else
                "at a cyclical-earnings trough (deep-trough + snapback shape)" if at_trough else
                "trading below fair value")) + " — the rarest, highest-quality setup the system expresses.",
        })
    # trough fusion first (rarer), then cheap
    order = {"TROUGH+CHEAP": 0, "AT_CYCLICAL_TROUGH": 1, "CHEAP": 2}
    highest_conviction.sort(key=lambda x: (order.get(x["setup_type"], 9), -x["criticality"]))
    diag.append("structural=%d trough_names=%d highest_conviction=%d" % (
        len(structural), len(trough_map), len(highest_conviction)))

    # per-industry "make-it-or-break-it" leader
    by_industry = {}
    for r in chokepoints:
        ind = r.get("industry") or r.get("sector") or "Other"
        if ind not in by_industry or r["criticality"] > by_industry[ind]["criticality"]:
            by_industry[ind] = {"ticker": r["ticker"], "name": r["name"], "criticality": r["criticality"],
                                "gm_level": r["gm_level"], "gm_stability": r["gm_stability"]}
    industry_leaders = sorted(by_industry.values(), key=lambda x: -x["criticality"])

    ea = _read("data/engine-alpha.json") or {}
    proven = set(str(x).lower() for x in (ea.get("alpha_proven_signals") or []))
    mode = "PROVEN" if "eng:chokepoint" in proven else "PROVISIONAL"



    # ══════════════════════════════════════════════════════════════════════
    # v3.0 CAPTURE GAP — value-creation (criticality) vs value-capture (mcap)
    # ══════════════════════════════════════════════════════════════════════
    # Khalid's TSMC/ASML thesis, made computable. A company can be the single
    # point of failure for its entire industry and still carry a small slice of
    # that industry's market cap. That gap is the mispricing.
    #
    # Denominator = fetch_bulk_universe() (the WHOLE market, already in memory
    # for the funnel — zero extra API calls). Industry totals therefore include
    # names we never deep-dived, which is correct: the denominator must be the
    # full industry, not just our scored sample.
    capture = {}
    try:
        MIN_PEERS = 5          # industries thinner than this have meaningless shares
        ind_total, ind_members = {}, {}
        for _s, _p in bulk.items():
            if _p.get("is_etf") or _p.get("is_fund") or not _p.get("active"):
                continue
            _ind = (_p.get("industry") or "").strip()
            _mc = _p.get("market_cap") or 0
            if not _ind or _mc <= 0:
                continue
            ind_total[_ind] = ind_total.get(_ind, 0.0) + _mc
            ind_members.setdefault(_ind, []).append((_s, _mc))

        # backlog join (G0-verified key: "by_ticker")
        _bk = _read("data/backlog.json") or {}
        _bk_by = _bk.get("by_ticker") or {}

        def _pctile(vals, v):
            if not vals:
                return None
            n = sum(1 for x in vals if x < v)
            return round(100.0 * n / len(vals), 1)

        # group SCORED rows by industry so percentiles are cross-sectional
        scored_by_ind = {}
        for _r in results:
            _i = (_r.get("industry") or "").strip()
            if _i:
                scored_by_ind.setdefault(_i, []).append(_r)

        cap_rows = []
        for _ind, _rows in scored_by_ind.items():
            _tot = ind_total.get(_ind, 0.0)
            _npeers = len(ind_members.get(_ind, []))
            if _tot <= 0 or _npeers < MIN_PEERS:
                continue
            _crits = [x.get("criticality") or 0 for x in _rows]
            _shares_all = [mc for _, mc in ind_members[_ind]]
            for _r in _rows:
                _mc = _r.get("market_cap") or 0
                if _mc <= 0:
                    continue
                _share = 100.0 * _mc / _tot
                _crit_p = _pctile(_crits, _r.get("criticality") or 0)
                _share_p = _pctile(_shares_all, _mc)
                if _crit_p is None or _share_p is None:
                    continue
                _gap = round(_crit_p - _share_p, 1)

                # backlog leg
                _b = _bk_by.get(_r["ticker"]) or {}
                # producer-verified keys (ops 3767): rpo_yoy / demand_accelerating /
                # deferred_accelerating. 3766 consumed rpo_growth_yoy + accelerating,
                # which the producer never writes -> leg silently dead (0 joins).
                _rpo_g = _b.get("rpo_yoy")
                _bk_accel = bool(_b.get("demand_accelerating") or _b.get("deferred_accelerating"))

                cap_rows.append({
                    "ticker": _r["ticker"], "name": _r.get("name"),
                    "industry": _ind, "sector": _r.get("sector"),
                    "market_cap": _mc,
                    "industry_mcap_total": round(_tot, 0),
                    "industry_peers": _npeers,
                    "mcap_share_pct": round(_share, 3),
                    "criticality": _r.get("criticality"),
                    "criticality_pctile": _crit_p,
                    "mcap_share_pctile": _share_p,
                    "capture_gap": _gap,
                    "gm_stability": _r.get("gm_stability"),
                    "gm_level": _r.get("gm_level"),
                    "roic": _r.get("roic"),
                    "discount_to_fair_pct": _r.get("discount_to_fair_pct"),
                    "rpo_yoy": _rpo_g,
                    "backlog_deferred_accel": bool(_b.get("deferred_accelerating")),
                    "backlog_accelerating": _bk_accel,
                    "cap_bucket": _r.get("cap_bucket"),
                    "is_chokepoint": _r.get("is_chokepoint"),
                })

        # ── 5-leg confirmation ladder ──────────────────────────────────────
        # One leg is noise. Institutional practice: require independent
        # confirmation across creation, price, quality, growth and visibility.
        for _c in cap_rows:
            legs, why = 0, []
            if (_c["capture_gap"] or 0) >= 20:
                legs += 1; why.append("capture gap %.0fpp" % _c["capture_gap"])
            if isinstance(_c.get("discount_to_fair_pct"), (int, float)) and _c["discount_to_fair_pct"] >= 15:
                legs += 1; why.append("%.0f%% below fair" % _c["discount_to_fair_pct"])
            if isinstance(_c.get("gm_stability"), (int, float)) and _c["gm_stability"] <= 5:
                legs += 1; why.append("margin stability ±%.1fpp" % _c["gm_stability"])
            if isinstance(_c.get("roic"), (int, float)) and _c["roic"] >= 15:
                legs += 1; why.append("%.0f%% ROIC" % _c["roic"])
            if _c.get("backlog_accelerating"):
                legs += 1; why.append("backlog accelerating")
            _c["legs"] = legs
            _c["legs_why"] = why
            _c["tier"] = ("STRUCTURALLY_UNDERVALUED" if legs >= 3 and _c["capture_gap"] >= 20
                          else "WATCH" if legs >= 1 else "NONE")

        _tots = ind_total
        _bygi = {}
        cap_rows.sort(key=lambda x: -(x.get("capture_gap") or 0))
        _under = [c for c in cap_rows if c["tier"] == "STRUCTURALLY_UNDERVALUED"]
        _hidden_cap = [c for c in _under if c.get("cap_bucket") in ("nano", "micro", "small", "mid")]

        capture = {
            "marker": "capture_gap_v3",
            "thesis": ("Value CREATION vs value CAPTURE. A company can be the single "
                       "point of failure for its industry and still hold a small slice "
                       "of that industry's market cap. capture_gap = criticality "
                       "percentile minus market-cap-share percentile, within industry. "
                       "Positive = the market underpays for indispensability."),
            "method": {
                "denominator": "full market from profile-bulk (all active non-ETF names)",
                "min_peers": MIN_PEERS,
                "ladder": "3 of 5 legs AND capture_gap>=20pp => STRUCTURALLY_UNDERVALUED",
                "legs": ["capture_gap>=20pp", "discount_to_fair>=15%",
                         "gm_stability<=5pp", "roic>=15%", "backlog_accelerating"],
                "honest_limit": ("mcap share is a proxy for value capture, not a "
                                 "measured revenue share; industries with <5 listed "
                                 "peers are excluded rather than guessed."),
            },
            "stats": {
                "scored": len(cap_rows),
                "industries": len(set(c["industry"] for c in cap_rows)),
                "structurally_undervalued": len(_under),
                "hidden": len(_hidden_cap),
                "backlog_joined": sum(1 for c in cap_rows if c.get("rpo_growth_yoy") is not None),
            },
            "structurally_undervalued": _under[:40],
            "hidden_capture_gaps": _hidden_cap[:25],
            "widest_gaps": cap_rows[:60],
            "all_rows": cap_rows,
        }

        # ── v3.1 CROSS-INDUSTRY (GLOBAL) CAPTURE GAP ──────────────────────
        # Khalid's original framing was cross-industry, not intra-industry:
        # "TSMC at $1T when every other company is about a trillion". The
        # within-industry gap answers "is it cheap vs its own peers"; this
        # answers "is the whole industry underweighted in the market". Both
        # ship — they are different questions and disagreeing is informative.
        try:
            _g_crit = [c["criticality"] for c in cap_rows if c.get("criticality") is not None]
            _g_mc = [c["market_cap"] for c in cap_rows if (c.get("market_cap") or 0) > 0]
            for _c in cap_rows:
                _gc = _pctile(_g_crit, _c.get("criticality") or 0)
                _gm = _pctile(_g_mc, _c.get("market_cap") or 0)
                _c["global_criticality_pctile"] = _gc
                _c["global_mcap_pctile"] = _gm
                _c["global_capture_gap"] = (round(_gc - _gm, 1)
                                            if (_gc is not None and _gm is not None) else None)
                # divergence: cheap globally but fair within industry (or vice
                # versa) = the whole industry is being repriced, not the name
                if _c.get("capture_gap") is not None and _c["global_capture_gap"] is not None:
                    _c["gap_divergence"] = round(_c["global_capture_gap"] - _c["capture_gap"], 1)
                else:
                    _c["gap_divergence"] = None
            for _c in cap_rows:
                if _c.get("global_capture_gap") is not None:
                    _bygi.setdefault(_c["industry"], []).append(_c["global_capture_gap"])
            _gsorted = sorted([c for c in cap_rows if c.get("global_capture_gap") is not None],
                              key=lambda x: -x["global_capture_gap"])
            capture["global_marker"] = "global_capture_gap_v31"
            capture["global_method"] = (
                "criticality percentile vs market-cap percentile across the WHOLE scored "
                "cross-section (not within industry). Answers 'is this business "
                "under-capitalised relative to everything listed', which is the "
                "cross-industry version of the question. gap_divergence = global minus "
                "within-industry: large positive means the entire industry is "
                "under-capitalised, not just the name.")
            capture["widest_global_gaps"] = _gsorted[:40]
            capture["industry_underweight"] = sorted(
                [{"industry": _i,
                  "n": len(_v),
                  "median_global_gap": round(sorted(_v)[len(_v) // 2], 1),
                  "industry_mcap_total": _tots.get(_i)}
                 for _i, _v in _bygi.items() if len(_v) >= 3],
                key=lambda x: -x["median_global_gap"])[:25]
            capture["stats"]["global_scored"] = len(_gsorted)
            diag.append("global_capture_gap: %d scored" % len(_gsorted))
        except Exception as _ge:
            capture["global_error"] = str(_ge)[:200]
            diag.append("global_capture_gap FAILED: %s" % str(_ge)[:160])

        diag.append("capture_gap: %d scored / %d ind / %d undervalued" % (
            len(cap_rows), len(set(c["industry"] for c in cap_rows)), len(_under)))
    except Exception as _e:
        capture = {"marker": "capture_gap_v3", "error": str(_e)[:300]}
        diag.append("capture_gap FAILED: %s" % str(_e)[:160])

    out = {
        "engine": "chokepoint", "version": VERSION, "generated_at": datetime.now(timezone.utc).isoformat(),
        "duration_s": round(time.time() - t0, 1), "mode": mode, "scope": "broad_v2",
        "thesis": ("Industry-criticality: the make-it-or-break-it companies their industry can't route around. "
                   "Criticality is a quality attribute the market usually prices; the edge is in HIDDEN chokepoints "
                   "(small sole-suppliers) and CHEAP chokepoints (indispensable on sale)."),
        "scoring": {"formula": "0.30*margin_level + 0.22*margin_STABILITY + 0.20*ROIC + 0.13*R&D + 0.15*centrality",
                    "calibration": {"chokepoints_avg": 64.9, "commodity_avg": 21.9, "clean_gap": "54.6 > 34.0"},
                    "standout_proxy": "margin stability = the fingerprint of irreplaceable pricing power",
                    "threshold": CHOKE_THRESHOLD},
        "stats": {"pool": len(pool), "evaluated": len(results), "chokepoints": len(chokepoints),
                  "hidden": len(hidden), "cheap": len(cheap), "discovered": len(discovered),
                  "verified": len(verdicts), "confirmed": len(confirmed), "structural": len(structural),
                  "highest_conviction": len(highest_conviction),
                  "rejected_high_margin": len(rejected_hi_margin), "industries": len(industry_leaders)},
        "highest_conviction_book": highest_conviction[:25],
        "structural_names": structural_names,
        "confirmed_chokepoint_book": confirmed[:30],
        "discovered_chokepoint_book": discovered[:30],
        "rejected_high_margin_sample": [{"ticker": r["ticker"], "name": r.get("name"),
                                         "criticality": r["criticality"], "reason": r.get("irreplaceability_reason")}
                                        for r in rejected_hi_margin[:15]],
        "cheap_chokepoint_book": cheap[:20],
        "hidden_chokepoint_book": hidden[:25],
        "industry_leaders": industry_leaders[:40],
        "all_chokepoints": chokepoints[:80],
        "capture_gap": capture,
        "methodology": ("TWO-STAGE FUNNEL. Stage 1: one FMP profile-bulk call returns the whole universe; "
                        "filter to actively-trading US small/mid-caps ($0.8-50B) in chokepoint-prone industries "
                        "(commodity industries never qualify). Stage 2: deep-dive survivors + curated seed + engine "
                        "universes for 10y margins (level + STABILITY), ROIC, R&D, supply-chain centrality. "
                        "Calibrated vs known chokepoints AND commodity controls. 'discovered' = found by the broad "
                        "scan, not the curated seed. Cheap-chokepoint picks graded forward; pure criticality is a "
                        "quality screen, not a timing signal. Research, not investment advice."),
        "disclaimer": "Criticality measures business indispensability, not stock cheapness — most chokepoints are expensive. Not investment advice.",
        "next": "v2.1: multi-part bulk pagination + LLM irreplaceability pass on the discovered names.",
        "diagnostics": diag,
        "top_picks": [{"ticker": r["ticker"], "score": r["criticality"], "cheap": True,
                       "discount_pct": r.get("discount_to_fair_pct")} for r in cheap[:12]],
    }
    s3.put_object(Bucket=BUCKET, Key=OUT_KEY, Body=json.dumps(out, default=str).encode(),
                  ContentType="application/json", CacheControl="no-cache, max-age=0")
    print("[chokepoint v2.1] pool=%d eval=%d chokepoints=%d discovered=%d verified=%d CONFIRMED=%d rejected=%d" % (
        len(pool), len(results), len(chokepoints), len(discovered), len(verdicts), len(confirmed), len(rejected_hi_margin)))
    if confirmed:
        print("  CONFIRMED true chokepoints (LLM: industry can't route around):")
        for r in confirmed[:14]:
            print("   %s %s crit=%s  %s" % (r["ticker"], (r.get("name") or "")[:24], r["criticality"], r.get("irreplaceability_reason", "")))
    if rejected_hi_margin:
        print("  REJECTED (high-margin but NOT chokepoints):", [(r["ticker"], r["criticality"]) for r in rejected_hi_margin[:12]])
    if not verdicts:
        print("  (verification did not run — LLM unavailable; criticality still output)")
    print("  STRUCTURAL names exported for decision engines: %d" % len(structural))
    if highest_conviction:
        print("  ⭐ HIGHEST-CONVICTION (structural chokepoint at trough/cheap):")
        for r in highest_conviction[:10]:
            print("   %s %s crit=%s [%s]" % (r["ticker"], (r.get("name") or "")[:22], r["criticality"], r["setup_type"]))
    else:
        print("  ⭐ HIGHEST-CONVICTION: empty — no structural chokepoint is currently at a cyclical trough or cheap (these are rare; the machinery surfaces them when one appears)")
    if hidden: print("  HIDDEN chokepoints:", [(r["ticker"], r["criticality"], r["cap_bucket"]) for r in hidden[:8]])
    return {"statusCode": 200, "body": json.dumps({"mode": mode, "chokepoints": len(chokepoints), "cheap": len(cheap)})}
