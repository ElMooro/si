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

VERSION = "1.0"
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
                  "industries": len(industry_leaders)},
        "discovered_chokepoint_book": discovered[:30],
        "cheap_chokepoint_book": cheap[:20],
        "hidden_chokepoint_book": hidden[:25],
        "industry_leaders": industry_leaders[:40],
        "all_chokepoints": chokepoints[:80],
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
    print("[chokepoint v2] pool=%d eval=%d chokepoints=%d hidden=%d cheap=%d DISCOVERED=%d industries=%d" % (
        len(pool), len(results), len(chokepoints), len(hidden), len(cheap), len(discovered), len(industry_leaders)))
    print("  top criticality:", [(r["ticker"], r["criticality"]) for r in results[:10]])
    if discovered: print("  NEWLY DISCOVERED (broad scan found, curated missed):",
                         [(r["ticker"], r["criticality"], r["cap_bucket"], (r.get("industry") or "")[:18]) for r in discovered[:12]])
    if cheap: print("  CHEAP chokepoints:", [(r["ticker"], r["criticality"], f"{r.get('discount_to_fair_pct')}%") for r in cheap[:6]])
    if hidden: print("  HIDDEN chokepoints:", [(r["ticker"], r["criticality"], r["cap_bucket"]) for r in hidden[:8]])
    return {"statusCode": 200, "body": json.dumps({"mode": mode, "chokepoints": len(chokepoints), "cheap": len(cheap)})}
