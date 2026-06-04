"""justhodl-dislocation-detector — full-universe relative-value "Buy the Laggard" engine

THE THESIS (the X-post logic, done institutionally):
  Within a tight peer cohort (same industry, comparable cap), find names that
  are CHEAP on valuation but STRONG on growth + quality + balance sheet — the
  mispriced laggard to buy — and the rich, weaker peer it's dislocated against.

WHY THIS SCALES (vs peer-comparison's per-ticker FMP /peers lookups):
  Reuses the PROTECTED screener/data.json (~1,800 names, already has marketCap,
  psRatio, evEbitda, roe/roic, all margins, revenueGrowth, epsGrowth,
  debtToEquity, interestCoverage, revenue, ebitda, fcf, piotroski/altmanZ,
  instOwnership, insider signals) + data/universe.json (cap buckets). So the
  whole universe is screened with ZERO extra FMP calls for the base metrics;
  we only fetch FMP /stable/ for EV/Sales + forward revenue on the TOP
  candidates (cheap to do).

CLUSTERING: by INDUSTRY cohort (≥5 members) within the universe — each name is
  z-scored / percentiled vs its real industry peers, cap-aware.

DISLOCATION SCORE (0-100):
    cheapness  = percentile-rank (cheap end) of EV/Sales, EV/EBITDA, P/S,
                 and growth-adjusted EV/Sales (EV/S ÷ rev-growth) within cohort
    quality    = Rule-of-40 (rev growth% + FCF/op margin%), ROIC, margin level,
                 margin durability, Piotroski, moat proxy
    risk_pen   = leverage (debt/EBITDA, EV>>mcap), dilution (share growth),
                 customer/instituional concentration, Altman-Z distress
    score = 100 * cheapness^0.5 * quality^0.5 * (1 - risk_pen)
  A high score = "cheap for its fundamentals" = BUY THE LAGGARD.
  We also surface the RICH peer (expensive + weak) it's dislocated against.

OUTPUT: data/dislocations.json — ranked buy-the-laggard list + cohort context.
SCHEDULE: daily 14:30 UTC (after screener + fundamentals refresh).
"""
import io, json, os, time, math, statistics
import urllib.request, urllib.error
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
import boto3

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
OUT_KEY = "data/dislocations.json"
FMP_KEY = os.environ.get("FMP_KEY", "wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb")
FMP_BASE = "https://financialmodelingprep.com/stable"
s3 = boto3.client("s3", region_name=REGION)


def read_json(key, default=None):
    try:
        return json.loads(s3.get_object(Bucket=BUCKET, Key=key)["Body"].read())
    except Exception as e:
        print(f"[read] {key}: {str(e)[:80]}")
        return default


def http_json(url, timeout=12):
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "JustHodl/1.0"})
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return json.loads(r.read().decode())
    except Exception:
        return None


def sf(v):
    try:
        if v is None: return None
        f = float(v)
        return f if math.isfinite(f) else None
    except Exception:
        return None


def pctile_rank(value, arr, low_is_good):
    """Percentile rank of value within arr (0..1). If low_is_good, cheaper=higher."""
    vals = [x for x in arr if x is not None]
    if value is None or len(vals) < 3:
        return None
    below = sum(1 for x in vals if x < value)
    p = below / len(vals)
    return (1 - p) if low_is_good else p


def clamp(v, lo=0.0, hi=1.0):
    return max(lo, min(hi, v))


def lambda_handler(event=None, context=None):
    t0 = time.time()
    print("[dislocation] start")

    screener = read_json("screener/data.json") or {}
    rows = screener.get("stocks") or screener.get("data") or screener.get("results") or []
    if isinstance(screener, list):
        rows = screener
    universe = read_json("data/universe.json") or {}
    cap_by_sym = {}
    for s in (universe.get("stocks") or []):
        cap_by_sym[(s.get("symbol") or "").upper()] = s.get("cap_bucket")

    # Build base records from the screener (already-computed metrics)
    recs = {}
    for r in rows:
        sym = (r.get("symbol") or r.get("ticker") or "").upper()
        if not sym:
            continue
        industry = r.get("industry") or ""
        sector = r.get("sector") or ""
        mcap = sf(r.get("marketCap"))
        if not mcap or mcap < 30e6:   # skip sub-$30M illiquids
            continue
        rev = sf(r.get("revenue"))
        revG = sf(r.get("revenueGrowth"))
        # growth may be fractional (0.25) or pct (25); normalize to %
        if revG is not None and abs(revG) < 3:
            revG *= 100
        opM = sf(r.get("operatingMargin")); fcfM = sf(r.get("fcfYieldCalc"))
        nM = sf(r.get("netMargin")); gM = sf(r.get("grossMargin"))
        for m in ("operatingMargin","netMargin","grossMargin"):
            pass
        # margins may be fractional too
        def asp(x):
            if x is None: return None
            return x*100 if abs(x) < 3 else x
        opM, nM, gM = asp(opM), asp(nM), asp(gM)
        ps = sf(r.get("psRatio"))
        evEbitda = sf(r.get("evEbitda"))
        roic = sf(r.get("roic")); roe = sf(r.get("roe"))
        if roic is not None and abs(roic) < 3: roic *= 100
        if roe is not None and abs(roe) < 3: roe *= 100
        de = sf(r.get("debtToEquity"))
        ic = sf(r.get("interestCoverage"))
        ebitda = sf(r.get("ebitda"))
        pe = sf(r.get("peRatio"))
        piotroski = sf(r.get("piotroski"))
        altman = sf(r.get("altmanZ"))
        instPct = sf(r.get("instOwnershipPct"))
        # EV/Sales: approximate EV ~ mcap + netDebt; netDebt ≈ de*equity unknown,
        # use evEbitda*ebitda as EV proxy when available, else mcap (refined w/ FMP later)
        ev = (evEbitda * ebitda) if (evEbitda and ebitda and ebitda > 0) else mcap
        ev_sales = (ev / rev) if (rev and rev > 0) else None
        # Rule of 40 = rev growth% + best available margin%
        margin_for_r40 = opM if opM is not None else (fcfM*100 if (fcfM is not None and abs(fcfM)<3) else nM)
        rule40 = (revG + margin_for_r40) if (revG is not None and margin_for_r40 is not None) else None
        # growth-adjusted EV/Sales (PEG-for-sales): EV/S divided by growth
        gaes = (ev_sales / revG) if (ev_sales is not None and revG and revG > 5) else None
        recs[sym] = {
            "ticker": sym, "name": r.get("name") or r.get("companyName") or sym,
            "sector": sector, "industry": industry, "cap_bucket": cap_by_sym.get(sym),
            "market_cap": mcap, "revenue": rev, "rev_growth_pct": revG,
            "eps_growth_pct": asp(sf(r.get("epsGrowth"))),
            "gross_margin": gM, "op_margin": opM, "net_margin": nM,
            "ps": ps, "pe": pe, "ev_ebitda": evEbitda, "ev_sales": ev_sales,
            "growth_adj_ev_sales": gaes, "rule_of_40": rule40,
            "roic": roic, "roe": roe, "debt_to_equity": de, "interest_coverage": ic,
            "piotroski": piotroski, "altman_z": altman, "inst_own_pct": instPct,
            "insider_cluster": bool(r.get("insiderClusterBuy")),
            "beat_streak": sf(r.get("beatStreak")),
        }

    print(f"[dislocation] base records from screener: {len(recs)}")

    # ── Supplement with the FULL universe (all caps) via FMP for names not
    # already covered by the screener. key-metrics-ttm + ratios-ttm per name. ──
    def cap_from_mcap(m):
        if not m: return None
        if m < 300e6: return "micro"
        if m < 2e9: return "small"
        if m < 10e9: return "mid"
        if m < 200e9: return "large"
        return "mega"

    uni_stocks = universe.get("stocks") or []
    missing = [s for s in uni_stocks if (s.get("symbol") or "").upper() not in recs
               and sf(s.get("market_cap")) and sf(s.get("market_cap")) >= 30e6]
    missing = missing[:1500]  # ceiling per run
    print(f"[dislocation] deep-fetching {len(missing)} additional universe names")

    def deep_fetch(stock):
        sym = (stock.get("symbol") or "").upper()
        try:
            km = http_json(f"{FMP_BASE}/key-metrics-ttm?symbol={sym}&apikey={FMP_KEY}")
            rt = http_json(f"{FMP_BASE}/ratios-ttm?symbol={sym}&apikey={FMP_KEY}")
            km = (km[0] if isinstance(km, list) and km else km) or {}
            rt = (rt[0] if isinstance(rt, list) and rt else rt) or {}
            def kp(*keys):
                for k in keys:
                    if km.get(k) is not None: return sf(km[k])
                    if rt.get(k) is not None: return sf(rt[k])
                return None
            mcap = sf(stock.get("market_cap"))
            ev_sales = kp("evToSalesTTM","enterpriseValueOverSalesTTM","evToSales")
            ev_ebitda = kp("evToEBITDATTM","enterpriseValueOverEBITDATTM","evToOperatingCashFlowTTM")
            roic = kp("returnOnInvestedCapitalTTM","roicTTM","returnOnCapitalEmployedTTM")
            gm = kp("grossProfitMarginTTM","grossMarginTTM")
            om = kp("operatingProfitMarginTTM","operatingMarginTTM")
            nm = kp("netProfitMarginTTM","netMarginTTM")
            de = kp("debtToEquityRatioTTM","debtToEquityTTM")
            ic = kp("interestCoverageRatioTTM","interestCoverageTTM")
            ps = kp("priceToSalesRatioTTM","priceToSalesTTM")
            pe = kp("priceToEarningsRatioTTM","peRatioTTM")
            # growth not in TTM endpoints reliably → leave None (still scored on cheapness/quality available)
            def aspf(x):
                if x is None: return None
                return x*100 if abs(x) < 3 else x
            gm, om, nm, roic = aspf(gm), aspf(om), aspf(nm), aspf(roic)
            return sym, {
                "ticker": sym, "name": stock.get("name") or sym,
                "sector": stock.get("sector") or "", "industry": stock.get("industry") or "",
                "cap_bucket": stock.get("cap_bucket") or cap_from_mcap(mcap),
                "market_cap": mcap, "revenue": None, "rev_growth_pct": None,
                "eps_growth_pct": None, "gross_margin": gm, "op_margin": om, "net_margin": nm,
                "ps": ps, "pe": pe, "ev_ebitda": ev_ebitda, "ev_sales": ev_sales,
                "growth_adj_ev_sales": None, "rule_of_40": None,
                "roic": roic, "roe": None, "debt_to_equity": de, "interest_coverage": ic,
                "piotroski": None, "altman_z": None, "inst_own_pct": None,
                "insider_cluster": False, "beat_streak": None,
            }
        except Exception:
            return sym, None

    if missing:
        with ThreadPoolExecutor(max_workers=24) as ex:
            for fut in as_completed([ex.submit(deep_fetch, s) for s in missing]):
                sym, rc = fut.result()
                if rc and (rc.get("ev_sales") is not None or rc.get("ps") is not None):
                    recs[sym] = rc

    # cap-bucket fallback for screener names missing it
    for rc in recs.values():
        if not rc.get("cap_bucket"):
            rc["cap_bucket"] = cap_from_mcap(rc.get("market_cap"))
        # sanity-cap absurd growth (tiny-base distortions) → keeps Rule-of-40 sane
        if rc.get("rev_growth_pct") is not None:
            rc["rev_growth_pct"] = max(-95.0, min(200.0, rc["rev_growth_pct"]))
        # recompute rule_of_40 cleanly (growth% + op margin%), capped
        rg, om2 = rc.get("rev_growth_pct"), rc.get("op_margin")
        if rg is not None and om2 is not None:
            rc["rule_of_40"] = round(max(-100.0, min(150.0, rg + om2)), 1)
        # recompute growth-adjusted EV/Sales with sane guard
        if rc.get("ev_sales") is not None and rg and rg > 8:
            rc["growth_adj_ev_sales"] = round(rc["ev_sales"]/rg, 3)
        else:
            rc["growth_adj_ev_sales"] = None

    print(f"[dislocation] total records after universe supplement: {len(recs)}")

    # Cluster by industry (>=5 members); fallback to sector for thin industries
    by_industry = {}
    for sym, rc in recs.items():
        key = rc["industry"] or rc["sector"] or "Other"
        by_industry.setdefault(key, []).append(sym)
    # merge thin industries into sector cohort
    cohorts = {}
    for key, syms in by_industry.items():
        if len(syms) >= 5:
            cohorts[key] = syms
        else:
            for sym in syms:
                sec = recs[sym]["sector"] or "Other"
                cohorts.setdefault("SECTOR:" + sec, []).append(sym)

    # Score each name vs its cohort
    scored = []
    for cohort, syms in cohorts.items():
        if len(syms) < 5:
            continue
        members = [recs[s] for s in syms]
        col = lambda k: [m.get(k) for m in members]
        cohort_med = {k: (statistics.median([x for x in col(k) if x is not None]) if any(x is not None for x in col(k)) else None)
                      for k in ("ev_sales","ev_ebitda","ps","pe","rule_of_40","roic","gross_margin","rev_growth_pct","growth_adj_ev_sales")}
        for rc in members:
            # cheapness: avg percentile-rank (cheap end) across valuation metrics
            ch = [pctile_rank(rc["ev_sales"], col("ev_sales"), True),
                  pctile_rank(rc["ev_ebitda"], col("ev_ebitda"), True),
                  pctile_rank(rc["ps"], col("ps"), True),
                  pctile_rank(rc["growth_adj_ev_sales"], col("growth_adj_ev_sales"), True)]
            ch = [x for x in ch if x is not None]
            cheapness = sum(ch)/len(ch) if ch else None
            # quality: percentile of growth, rule40, roic, gross margin + piotroski/8
            qu = [pctile_rank(rc["rule_of_40"], col("rule_of_40"), False),
                  pctile_rank(rc["roic"], col("roic"), False),
                  pctile_rank(rc["gross_margin"], col("gross_margin"), False),
                  pctile_rank(rc["rev_growth_pct"], col("rev_growth_pct"), False)]
            qu = [x for x in qu if x is not None]
            quality = sum(qu)/len(qu) if qu else None
            if rc["piotroski"] is not None and quality is not None:
                quality = clamp(0.8*quality + 0.2*(rc["piotroski"]/8.0))
            # moat proxy: high ROIC + high gross margin + larger relative size
            moat = []
            if rc["roic"] is not None: moat.append(clamp(rc["roic"]/30.0))
            if rc["gross_margin"] is not None: moat.append(clamp(rc["gross_margin"]/80.0))
            moat_score = sum(moat)/len(moat) if moat else None
            if moat_score is not None and quality is not None:
                quality = clamp(0.85*quality + 0.15*moat_score)
            # risk penalty: leverage, distress, dilution(proxy via altman), concentration
            risk = 0.0
            if rc["debt_to_equity"] is not None and rc["debt_to_equity"] > 2: risk += 0.20
            if rc["interest_coverage"] is not None and rc["interest_coverage"] < 2: risk += 0.20
            if rc["altman_z"] is not None and rc["altman_z"] < 1.8: risk += 0.25  # distress zone
            if rc["ev_ebitda"] is not None and rc["ev_ebitda"] < 0: risk += 0.10  # negative EBITDA
            if rc["inst_own_pct"] is not None and rc["inst_own_pct"] > 0.9: risk += 0.05  # crowded
            risk = clamp(risk)
            if cheapness is None or quality is None:
                continue
            score = round(100 * math.sqrt(cheapness) * math.sqrt(quality) * (1 - risk), 1)
            # leverage caveat
            caveats = []
            if rc["debt_to_equity"] is not None and rc["debt_to_equity"] > 2: caveats.append("high leverage")
            if rc["altman_z"] is not None and rc["altman_z"] < 1.8: caveats.append("distress zone (Altman-Z<1.8)")
            if rc["interest_coverage"] is not None and rc["interest_coverage"] < 2: caveats.append("thin interest coverage")
            if rc["rev_growth_pct"] is not None and rc["rev_growth_pct"] < 0: caveats.append("revenue declining")
            scored.append({**rc, "cohort": cohort, "cohort_size": len(members),
                           "cheapness": round(cheapness,3), "quality": round(quality,3),
                           "moat_score": round(moat_score,3) if moat_score is not None else None,
                           "risk_penalty": round(risk,3), "dislocation_score": score,
                           "cohort_median": {k: (round(v,2) if v is not None else None) for k,v in cohort_med.items()},
                           "caveats": caveats})

    scored.sort(key=lambda x: -x["dislocation_score"])

    # ── MOMENTUM / REGIME OVERLAY: "cheap AND inflecting" ──
    # The backtest showed cheap-value lagged momentum. So we fetch recent price
    # trend for the TOP cheap candidates and require momentum to be turning UP
    # before a name is a true buy. Cheap + downtrending = falling knife (penalize);
    # cheap + inflecting = the real setup (boost).
    import concurrent.futures as cf
    polygon_key = os.environ.get("POLYGON_KEY", "zvEY_KYYMHoAN0JqY7n2Ze6q0kBuJX_d")
    top_for_mom = scored[:80]

    def momentum(rc):
        tk = rc["ticker"]
        try:
            from datetime import timedelta
            to = datetime.now(timezone.utc); frm = to - timedelta(days=120)
            url = (f"https://api.polygon.io/v2/aggs/ticker/{tk}/range/1/day/"
                   f"{frm.strftime('%Y-%m-%d')}/{to.strftime('%Y-%m-%d')}?adjusted=true&sort=asc&limit=200&apiKey={polygon_key}")
            req = urllib.request.Request(url, headers={"User-Agent": "JustHodl/1.0"})
            with urllib.request.urlopen(req, timeout=12) as r:
                data = json.loads(r.read().decode())
            bars = data.get("results") or []
            if len(bars) < 30:
                return tk, None
            closes = [b["c"] for b in bars]
            last = closes[-1]
            sma20 = sum(closes[-20:]) / 20
            sma50 = sum(closes[-50:]) / 50 if len(closes) >= 50 else sma20
            ret_20d = (last / closes[-20] - 1) * 100 if closes[-20] else 0
            ret_5d = (last / closes[-5] - 1) * 100 if closes[-5] else 0
            # inflecting = price above 20d SMA, 20d SMA rising vs 50d, recent green
            above_20 = last > sma20
            sma_rising = sma20 > sma50
            recent_up = ret_5d > 0
            inflecting = above_20 and (sma_rising or recent_up)
            # momentum score 0-1
            ms = 0.0
            if above_20: ms += 0.35
            if sma_rising: ms += 0.30
            if recent_up: ms += 0.20
            if ret_20d > 0: ms += 0.15
            return tk, {"inflecting": inflecting, "mom_score": round(ms, 2),
                        "ret_20d": round(ret_20d, 1), "ret_5d": round(ret_5d, 1),
                        "above_20d_sma": above_20, "sma_rising": sma_rising}
        except Exception:
            return tk, None

    mom_map = {}
    with cf.ThreadPoolExecutor(max_workers=16) as ex:
        for fut in cf.as_completed([ex.submit(momentum, rc) for rc in top_for_mom]):
            tk, m = fut.result()
            if m: mom_map[tk] = m
    # apply overlay: boost inflecting, penalize falling knives
    for rc in scored:
        m = mom_map.get(rc["ticker"])
        if not m:
            continue
        rc["momentum"] = m
        if m["inflecting"]:
            rc["dislocation_score"] = round(rc["dislocation_score"] * 1.15, 1)
            rc["cheap_and_inflecting"] = True
        elif m["ret_20d"] < -10 and not m["above_20d_sma"]:
            rc["dislocation_score"] = round(rc["dislocation_score"] * 0.65, 1)  # falling knife
            rc.setdefault("caveats", []).append("downtrend — not yet inflecting (falling knife)")
            rc["cheap_and_inflecting"] = False
        else:
            rc["cheap_and_inflecting"] = False
    scored.sort(key=lambda x: -x["dislocation_score"])

    # expensive (top-quartile EV/S) AND weaker quality (the dislocation pair)
    by_cohort = {}
    for s in scored: by_cohort.setdefault(s["cohort"], []).append(s)
    for s in scored[:60]:
        peers = by_cohort.get(s["cohort"], [])
        cap_order = ["nano","micro","small","mid","large","mega"]
        sci = cap_order.index(s["cap_bucket"]) if s.get("cap_bucket") in cap_order else -1
        def comparable(p):
            if p["ticker"] == s["ticker"]: return False
            if not (p.get("ev_sales") and s.get("ev_sales")): return False
            prem = p["ev_sales"]/s["ev_sales"] - 1
            if prem < 0.3 or prem > 8.0: return False         # 30%–800% premium band
            if (p.get("quality") or 1) > (s.get("quality") or 0): return False  # peer must be weaker/equal
            # cap proximity: within one adjacent bucket
            pci = cap_order.index(p["cap_bucket"]) if p.get("cap_bucket") in cap_order else -1
            if sci >= 0 and pci >= 0 and abs(sci - pci) > 1: return False
            # revenue within ~12x when both known
            if s.get("revenue") and p.get("revenue") and p["revenue"]>0 and s["revenue"]>0:
                ratio = p["revenue"]/s["revenue"]
                if ratio > 12 or ratio < 1/12: return False
            return True
        rich = [p for p in peers if comparable(p)]
        rich.sort(key=lambda p: -(p.get("ev_sales") or 0))
        if rich:
            rp = rich[0]
            s["dislocated_vs"] = {
                "ticker": rp["ticker"], "name": rp["name"],
                "ev_sales": round(rp["ev_sales"],2) if rp.get("ev_sales") else None,
                "ev_sales_premium_pct": round((rp["ev_sales"]/s["ev_sales"]-1)*100) if s.get("ev_sales") else None,
                "rev_growth_pct": rp.get("rev_growth_pct"), "quality": rp.get("quality"),
                "cap_bucket": rp.get("cap_bucket"),
            }

    top = scored[:120]
    # buy-the-laggard shortlist: strong score + has a richer weaker peer + acceptable risk
    laggards = [s for s in top if s.get("dislocated_vs") and s["risk_penalty"] < 0.45][:40]
    # The highest-conviction subset: cheap AND inflecting (momentum confirming)
    cheap_inflecting = [s for s in scored if s.get("cheap_and_inflecting")
                        and s["risk_penalty"] < 0.45][:25]

    output = {
        "engine": "dislocation-detector",
        "version": "1.0",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "universe_scored": len(scored),
        "n_cohorts": len([c for c in cohorts.values() if len(c) >= 5]),
        "methodology": (
            "Per name vs its INDUSTRY cohort (>=5 peers, cap-aware): cheapness "
            "= percentile-rank on EV/Sales, EV/EBITDA, P/S, growth-adjusted EV/S; "
            "quality = Rule-of-40 + ROIC + gross margin + growth + Piotroski + moat "
            "proxy; risk penalty = leverage, distress (Altman-Z), coverage, "
            "concentration. Score = 100*sqrt(cheap)*sqrt(quality)*(1-risk). "
            "Reuses screener/data.json so the full ~1,800-name universe is scored."),
        "buy_the_laggard": laggards,
        "cheap_and_inflecting": cheap_inflecting,
        "top_dislocations": top,
    }
    s3.put_object(Bucket=BUCKET, Key=OUT_KEY,
                  Body=json.dumps(output, default=str).encode(),
                  ContentType="application/json", CacheControl="public, max-age=3600")
    print(f"[dislocation] DONE {round(time.time()-t0,1)}s — scored {len(scored)}, "
          f"{len(laggards)} buy-the-laggard, {output['n_cohorts']} cohorts")
    return {"statusCode": 200, "headers": {"Content-Type": "application/json"},
            "body": json.dumps({"ok": True, "scored": len(scored), "laggards": len(laggards),
                                 "cohorts": output["n_cohorts"],
                                 "top5": [{"t": s["ticker"], "score": s["dislocation_score"],
                                            "vs": (s.get("dislocated_vs") or {}).get("ticker")} for s in top[:5]]})}
