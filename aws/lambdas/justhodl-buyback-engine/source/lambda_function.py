"""
justhodl-buyback-engine — UNIFIED BUYBACK INTELLIGENCE
======================================================
Large buybacks are a durable catalyst, but the headline number lies: many
"buybacks" only offset stock-comp dilution. This engine fuses five layers into
one institutional read per name:

  1. CATALYST  — fresh 8-K repurchase authorizations + size vs market cap and
                 expected post-announcement drift (reused from justhodl-buyback-scanner;
                 academic priors Ikenberry-Lakonishok-Vermaelen / Peyer-Vermaelen).
  2. EXECUTION — actual cash repurchases from the cash-flow statement (TTM, and
                 whether the company is buying in the most recent quarter).
  3. NET-OF-DILUTION — repurchases minus issuance (net buyback), because gross
                 buybacks frequently just neutralize SBC (O'Shaughnessy / GuruFocus).
  4. SHARE SHRINK — the ground truth: is the actual share count falling YoY?
                 (enterprise-values numberOfShares). Confirms a real return of capital.
  5. VALUATION  — buybacks create value when the stock is cheap (FCF yield); penalize
                 expensive-multiple repurchasers (O'Shaughnessy filter #2).

Per name → buyback_score 0-100 + a class:
  🚀 FRESH_LARGE_AUTH   fresh authorization >=5% of mcap (+ execution/shrink) — pump setup
  💪 NET_SHRINKER       share count down >=2% YoY with positive net buyback (durable)
  💰 HIGH_SHAREHOLDER_YIELD  net buyback + dividend yield >=6%
  🎯 CHEAP_REPURCHASER  actively buying while cheap (high FCF yield) — value conviction
  ⚠️ DILUTION_OFFSET    gross buyback but share count flat/rising (fake buyback)
  ACTIVE / NEUTRAL

high_conviction_pumps = large fresh authorization confirmed by execution or shrink —
the user's "large buybacks almost always cause pumps" list.

OUTPUT  data/buyback-engine.json     SCHEDULE  daily 13:30 UTC
Real data only (FMP /stable/ cash-flow + key-metrics + enterprise-values, SEC 8-K via
buyback-scanner). Research, not investment advice.
"""
import json, os, time, datetime, urllib.request

import boto3

S3_BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
FMP_KEY = os.environ.get("FMP_API_KEY", "wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb")
OUT_KEY = "data/buyback-engine.json"
s3 = boto3.client("s3")

# ── Security-master exclusion ───────────────────────────────────────────────
# Closed-end funds / BDCs do "buybacks" via fund mechanics (not operating-company
# returns of capital), and heavy-ATM crypto miners report repurchase/issuance
# churn that corrupts net-buyback yield. These are excluded from the buyback board.
# The isFund/isEtf profile flag is the general backstop for anything not listed.
EXCLUDE_TICKERS = {
    # closed-end funds / BDCs
    "GLV", "GLO", "GLQ", "GLU", "GAB", "GGZ", "GDV", "GUT", "GGT", "ECC", "OXLC",
    "PDI", "PTY", "PCN", "PHK", "UTF", "RVT", "ADX", "BST", "BSTZ", "ETV", "ETY",
    "QQQX", "JEPI", "JEPQ", "SPXX", "RQI", "USA", "CET", "HQH", "THQ",
    # heavy-ATM crypto miners / digital-asset treasuries (buyback data unreliable)
    "CLSK", "MARA", "RIOT", "CIFR", "WULF", "BITF", "HUT", "BTBT", "BTDR", "HIVE",
    "IREN", "CORZ", "APLD", "SDIG", "GREE", "MSTR", "SMLR", "BTCS",
}


def is_excluded_profile(prof):
    """Return an exclusion reason if the FMP profile marks this as a non-operating
    structure (fund / ETF), else None."""
    if not isinstance(prof, dict):
        return None
    if prof.get("isFund"):
        return "closed_end_fund"
    if prof.get("isEtf"):
        return "etf"
    nm = (prof.get("companyName") or "").lower()
    if any(w in nm for w in (" closed-end", "closed end fund", "income fund", " term trust")):
        return "fund_name_pattern"
    return None


def _read(key, default=None):
    try:
        return json.loads(s3.get_object(Bucket=S3_BUCKET, Key=key)["Body"].read())
    except Exception:
        return {} if default is None else default


def fmp(path, retries=2):
    sep = "&" if "?" in path else "?"
    url = f"https://financialmodelingprep.com/stable/{path}{sep}apikey={FMP_KEY}"
    for a in range(retries):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "JustHodl/1.0"})
            with urllib.request.urlopen(req, timeout=18) as r:
                return json.loads(r.read().decode("utf-8", "ignore"))
        except Exception:
            if a < retries - 1:
                time.sleep(0.4)
    return None


def clamp(x, lo=0.0, hi=100.0):
    try:
        return max(lo, min(hi, float(x)))
    except Exception:
        return lo


def up(t):
    return (t or "").upper().strip()


def analyze_ticker(t):
    """Pull FMP and compute the buyback dossier for one ticker.
    Returns None (no data), {"_excluded": reason}, or the dossier dict."""
    if t in EXCLUDE_TICKERS:
        return {"_excluded": "denylist"}
    pr = fmp(f"profile?symbol={t}")
    prof = pr[0] if isinstance(pr, list) and pr else {}
    exr = is_excluded_profile(prof)
    if exr:
        return {"_excluded": exr}
    cf = fmp(f"cash-flow-statement?symbol={t}&period=quarter&limit=5")
    if not isinstance(cf, list) or len(cf) < 2:
        return None
    km = fmp(f"key-metrics?symbol={t}&period=quarter&limit=1")
    ev = fmp(f"enterprise-values?symbol={t}&period=quarter&limit=5")

    q4 = cf[:4]
    # TTM gross repurchases (positive $), issuance, dividends, net buyback
    gross_repo = sum(abs(float(q.get("commonStockRepurchased") or 0)) for q in q4)
    issuance = sum(float(q.get("commonStockIssuance") or 0) for q in q4)
    net_issuance = sum(float(q.get("netCommonStockIssuance") or q.get("netStockIssuance") or 0) for q in q4)
    net_buyback = -net_issuance  # negative net issuance = net buyback
    ttm_div = sum(abs(float(q.get("commonDividendsPaid") or q.get("netDividendsPaid") or 0)) for q in q4)
    ttm_sbc = sum(float(q.get("stockBasedCompensation") or 0) for q in q4)
    debt_issuance = sum(float(q.get("netDebtIssuance") or 0) for q in q4)
    last_q_repo = abs(float(q4[0].get("commonStockRepurchased") or 0))
    active = last_q_repo > 0

    mcap = None
    if isinstance(km, list) and km:
        mcap = float(km[0].get("marketCap") or 0) or None
    # FCF yield SELF-COMPUTED from the same 4 quarters (FMP's derived
    # freeCashFlowYield x4 printed DCGO at -32,762,564% -- never trust
    # a vendor-derived ratio we can compute from primaries already paid for)
    fcf_yield = None
    fcf_nm = False
    sec = (prof.get("sector") or "")
    ind = (prof.get("industry") or "")
    fin_like = ("Financial" in sec or any(w in ind for w in
                ("Bank", "Insurance", "Capital Markets", "Credit",
                 "Asset Management", "Mortgage", "REIT",
                 "Healthcare Plans", "Financial")))
    if fin_like:
        # OCF for banks/insurers is float + balance-sheet motion --
        # FCF yield is not a cheapness signal there (BAC 47% class)
        fcf_nm = True

    # share count trend (YoY): newest vs ~4 quarters back
    sr_pct = None
    shares_now = shares_prior = None
    if isinstance(ev, list) and len(ev) >= 4:
        try:
            shares_now = float(ev[0].get("numberOfShares") or 0)
            shares_prior = float(ev[min(4, len(ev) - 1)].get("numberOfShares") or 0)
            if shares_now > 0 and shares_prior > 0:
                sr_pct = round((shares_prior - shares_now) / shares_prior * 100, 2)
        except Exception:
            pass

    if not mcap or mcap <= 0:
        return None
    if not fin_like:
        try:
            ocf = sum(float(q.get("operatingCashFlow") or
                            q.get("netCashProvidedByOperatingActivities")
                            or 0) for q in q4)
            capex = sum(abs(float(q.get("capitalExpenditure") or 0))
                        for q in q4)
            fy = (ocf - capex) / mcap * 100
            if -150 <= fy <= 150:
                fcf_yield = round(fy, 2)
        except Exception:
            pass
    gross_yield = round(gross_repo / mcap * 100, 2)
    net_yield = round(net_buyback / mcap * 100, 2)
    div_yield = round(ttm_div / mcap * 100, 2)
    shareholder_yield = round(net_yield + div_yield, 2)
    debt_funded = debt_issuance > 0 and gross_repo > 0
    # Net-issuer sanity gate: shares rising materially (>3% YoY) means the company is
    # diluting regardless of any gross repurchases — its "net buyback yield" is noise.
    net_issuer = sr_pct is not None and sr_pct <= -3.0

    return {
        "symbol": t, "market_cap": mcap,
        "sector": prof.get("sector"), "industry": prof.get("industry"),
        "company_name": prof.get("companyName"),
        "gross_repurchases_ttm": round(gross_repo, 0), "net_buyback_ttm": round(net_buyback, 0),
        "issuance_ttm": round(issuance, 0), "sbc_ttm": round(ttm_sbc, 0),
        "gross_buyback_yield": gross_yield, "net_buyback_yield": net_yield,
        "dividend_yield": div_yield, "shareholder_yield": shareholder_yield,
        "active_execution": active, "last_q_repurchase": round(last_q_repo, 0),
        "share_count_reduction_yoy": sr_pct, "shares_now": shares_now,
        "fcf_yield_annualized": fcf_yield, "fcf_nm": fcf_nm,
        "extreme": (sr_pct is not None and abs(sr_pct) >= 80),
        "debt_funded": debt_funded, "net_issuer": net_issuer,
    }


def classify_and_score(d, auth_pct, insider):
    nbY = d["net_buyback_yield"]
    grY = d["gross_buyback_yield"]
    srP = d["share_count_reduction_yoy"]
    shY = d["shareholder_yield"]
    fcfY = d["fcf_yield_annualized"]
    active = d["active_execution"]

    comps = []
    comps.append(("net_buyback", clamp(nbY / 6.0 * 100), 0.30))
    if srP is not None:
        comps.append(("share_reduction", clamp(srP / 3.0 * 100), 0.25))
    comps.append(("active", 100.0 if active else 0.0, 0.12))
    comps.append(("shareholder_yield", clamp(shY / 8.0 * 100), 0.10))
    if fcfY is not None:
        comps.append(("cheapness", clamp(fcfY / 8.0 * 100), 0.10))
    if auth_pct:
        comps.append(("fresh_auth", clamp(auth_pct / 8.0 * 100), 0.13))
    num = sum(s * w for _, s, w in comps)
    den = sum(w for _, s, w in comps)
    score = round(num / den, 1) if den else 0.0
    if d.get("debt_funded"):
        score = round(max(0.0, score - 8.0), 1)

    cheap = fcfY is not None and fcfY >= 6.0
    net_issuer = d.get("net_issuer")
    if net_issuer:
        # diluting regardless of gross repurchases — never a buyback star
        klass = "⚠️ DILUTION_OFFSET"
    elif auth_pct and auth_pct >= 5 and (active or (srP or 0) >= 1.5 or insider):
        klass = "🚀 FRESH_LARGE_AUTH"
    elif (srP or 0) >= 2 and d["net_buyback_ttm"] > 0:
        klass = "💪 NET_SHRINKER"
    elif shY >= 6 and (srP is None or srP >= 0):
        klass = "💰 HIGH_SHAREHOLDER_YIELD"
    elif active and cheap:
        klass = "🎯 CHEAP_REPURCHASER"
    elif grY > 0.5 and (srP is not None and srP <= 0):
        klass = "⚠️ DILUTION_OFFSET"
    elif active or d["net_buyback_ttm"] > 0:
        klass = "ACTIVE"
    else:
        klass = "NEUTRAL"
    high_conviction_pump = bool(not net_issuer and auth_pct and auth_pct >= 5
                                and (active or (srP or 0) >= 1.5))
    return score, klass, high_conviction_pump, cheap


def lambda_handler(event=None, context=None):
    # ---- universe: scanner authorizations (catalyst) + attention-confluence universe ----
    scanner = _read("data/buyback-scanner.json")
    auths = {}
    for o in (scanner.get("top_opportunities", []) or []):
        tk = up(o.get("ticker") or o.get("symbol"))
        if not tk:
            continue
        mcap = float(o.get("market_cap") or 0)
        auth = float(o.get("authorization_usd") or 0)
        auths[tk] = {
            "authorization_usd": auth, "market_cap": mcap,
            "auth_pct_mcap": round(auth / mcap * 100, 2) if mcap > 0 and auth > 0 else None,
            "announcement_date": o.get("announcement_date"), "company": o.get("company"),
            "asr": o.get("asr_accelerated"), "expected_drift": o.get("expected_return_basis") or o.get("forward_expectations"),
            "insider_n_buyers": o.get("insider_n_buyers") or o.get("n_buyers"),
            "filing_url": o.get("filing_url"),
        }

    conf = _read("data/attention-confluence.json")
    univ = set(auths.keys())
    univ |= set(up(k) for k in (conf.get("tickers", {}) or {}).keys())
    # cap to bound FMP calls / runtime
    universe = sorted(univ)[:190]
    # ensure all authorization names are kept even if cap trims
    for tk in auths:
        if tk not in universe:
            universe.append(tk)

    # ── blackout join: market-wide FMP earnings calendar, chunked 7d
    # (calendar truncates long ranges -- earnings-blackout engine lesson),
    # same street-proxy window [T-30d, T+2d]. ~8 calls per run total.
    import datetime as _dt
    _today = _dt.datetime.now(_dt.timezone.utc).date()
    next_earn = {}
    cur = _today
    to = _today + _dt.timedelta(days=45)
    while cur <= to:
        nxt = min(cur + _dt.timedelta(days=6), to)
        j = fmp("earnings-calendar?from=%s&to=%s&limit=3000"
                % (cur.isoformat(), nxt.isoformat()))
        for r in j if isinstance(j, list) else []:
            tt = up(r.get("symbol"))
            dd = r.get("date")
            if tt and dd and (tt not in next_earn or dd < next_earn[tt]):
                next_earn[tt] = dd
        cur = nxt + _dt.timedelta(days=1)
    bo_agg = (_read("data/earnings-blackout.json") or {}).get("now") or {}

    # ── share-flows join: P/E + insider $ from the sibling desk, composed
    sfl = (_read("data/share-flows.json") or {}).get("tickers") or {}

    def blackout_fields(t):
        d0 = next_earn.get(t)
        if not d0:
            return {}
        try:
            e = _dt.date.fromisoformat(d0)
        except Exception:
            return {}
        start = e - _dt.timedelta(days=30)
        f = {"next_earnings": d0}
        if start <= _today <= e + _dt.timedelta(days=2):
            f["in_blackout"] = True
        elif _today < start:
            f["days_to_blackout"] = (start - _today).days
        return f

    tickers = {}
    n_fmp_ok = 0
    excluded = []
    for i, t in enumerate(universe):
        d = analyze_ticker(t)
        if i % 30 == 0:
            time.sleep(0.25)
        if isinstance(d, dict) and d.get("_excluded"):
            excluded.append({"ticker": t, "reason": d["_excluded"]})
            continue
        if not d:
            continue
        n_fmp_ok += 1
        a = auths.get(t, {})
        auth_pct = a.get("auth_pct_mcap")
        insider = bool(a.get("insider_n_buyers"))
        score, klass, pump, cheap = classify_and_score(d, auth_pct, insider)
        bits = []
        if auth_pct:
            bits.append(f"fresh authorization {auth_pct}% of mcap")
        if d["net_buyback_yield"] > 0:
            bits.append(f"net buyback yield {d['net_buyback_yield']}%")
        if d["share_count_reduction_yoy"] is not None and d["share_count_reduction_yoy"] > 0:
            bits.append(f"shares -{d['share_count_reduction_yoy']}% YoY")
        if d["active_execution"]:
            bits.append("buying this quarter")
        if cheap:
            bits.append(f"cheap (FCF yield {d['fcf_yield_annualized']}%)")
        if d["debt_funded"]:
            bits.append("⚠ debt-funded")
        bo = blackout_fields(t)
        if pump and bo.get("in_blackout"):
            bits.append("🔇 in blackout — corporate bid off until "
                        "~2d post-earnings")
        elif pump and (bo.get("days_to_blackout") or 99) <= 7:
            bits.append("⏳ blackout starts in %dd"
                        % bo["days_to_blackout"])
        sf = sfl.get(t) or {}
        sfj = {k2: sf[k2] for k2 in
               ("pe_ttm", "insider_buy_usd_90d", "insider_n_buyers",
                "insider_sell_usd_recent", "insider_n_sellers")
               if sf.get(k2) is not None}
        tickers[t] = {**d, **bo, **sfj,
                      "auth_pct_mcap": auth_pct, "buyback_score": score, "class": klass,
                      "high_conviction_pump": pump, "cheap": cheap,
                      "company": a.get("company"), "announcement_date": a.get("announcement_date"),
                      "asr": a.get("asr"), "filing_url": a.get("filing_url"),
                      "why": "; ".join(bits)}

    # dual-class collapse: same company under two tickers (FOX/FOXA)
    # double-counts one capital-return program on every board -- keep
    # the larger class on boards, chip the sibling, keep both in map
    byname = {}
    for t, v in tickers.items():
        nm = (v.get("company_name") or v.get("company") or "").strip().lower()
        if nm:
            byname.setdefault(nm, []).append(t)
    for nm, ts in byname.items():
        if len(ts) > 1:
            ts.sort(key=lambda x: -(tickers[x].get("market_cap") or 0))
            keep = ts[0]
            tickers[keep]["dual_class_with"] = ts[1:]
            for o in ts[1:]:
                tickers[o]["dual_class_with"] = [keep]
                tickers[o]["board_suppressed"] = True

    rows = list(tickers.values())

    def top(pred, key, n=25):
        r = [x for x in rows if pred(x)
             and not x.get("board_suppressed")]
        r.sort(key=key, reverse=True)
        return r[:n]

    out = {
        "engine": "buyback-engine", "version": "1.1.0",
        "generated_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "thesis": ("Unified buyback intelligence: fresh authorizations (catalyst) + actual execution "
                   "+ net-of-dilution + share-count shrink + valuation. Net buyback yield and a "
                   "genuinely shrinking share count separate real returns of capital from SBC offset."),
        "universe_n": len(universe), "n_scored": len(tickers), "n_fmp_resolved": n_fmp_ok,
        "n_excluded": len(excluded), "excluded_sample": excluded[:40],
        "market_blackout": {"pct": bo_agg.get("blackout_mktcap_pct"),
                            "state": bo_agg.get("state")},
        "counts": {k: len([x for x in rows if x["class"] == k]) for k in
                   ["🚀 FRESH_LARGE_AUTH", "💪 NET_SHRINKER", "💰 HIGH_SHAREHOLDER_YIELD",
                    "🎯 CHEAP_REPURCHASER", "⚠️ DILUTION_OFFSET", "ACTIVE", "NEUTRAL"]},
        "scanner_state": scanner.get("state"),
        "high_conviction_pumps": top(lambda x: x["high_conviction_pump"],
                                     lambda x: (x.get("auth_pct_mcap") or 0, x["buyback_score"])),
        "fresh_authorizations": top(lambda x: x.get("auth_pct_mcap")
                                    and not x.get("extreme"),
                                    lambda x: (x.get("auth_pct_mcap") or 0)),
        "net_shrinkers": top(lambda x: (x["share_count_reduction_yoy"] or 0) >= 1
                             and x["net_buyback_ttm"] > 0 and not x.get("net_issuer"),
                             lambda x: x["share_count_reduction_yoy"] or 0),
        "high_shareholder_yield": top(lambda x: x["shareholder_yield"] >= 3
                                      and x["net_buyback_yield"] > 0.5 and not x.get("net_issuer"),
                                      lambda x: x["shareholder_yield"]),
        "cheap_repurchasers": top(lambda x: x["cheap"] and not x.get("net_issuer")
                                  and (x["active_execution"] or x["net_buyback_ttm"] > 0),
                                  lambda x: x["buyback_score"]),
        "dilution_offset_warnings": top(lambda x: x["class"] == "⚠️ DILUTION_OFFSET",
                                        lambda x: x["gross_buyback_yield"]),
        "tickers": tickers,
        "scoring": {"weights": {"net_buyback": 0.30, "share_reduction": 0.25, "active": 0.12,
                                "shareholder_yield": 0.10, "cheapness": 0.10, "fresh_auth": 0.13},
                    "notes": "net buyback yield = (repurchases - issuance)/mcap TTM; share reduction = YoY numberOfShares."},
        "sources": ["SEC 8-K via buyback-scanner", "FMP /stable/ cash-flow-statement",
                    "FMP key-metrics (mcap, FCF yield)", "FMP enterprise-values (share count)"],
        "caveats": ("Authorization != execution; quarters are lumpy. Net buyback yield + a falling "
                    "share count are the real signal; gross repurchases that only offset SBC are flagged "
                    "DILUTION_OFFSET. Debt-funded buybacks flagged. Research only, not investment advice."),
    }
    s3.put_object(Bucket=S3_BUCKET, Key=OUT_KEY, Body=json.dumps(out).encode(),
                  ContentType="application/json", CacheControl="public, max-age=600")
    return {"ok": True, "n_scored": len(tickers), "n_fmp": n_fmp_ok,
            "pumps": len(out["high_conviction_pumps"]), "counts": out["counts"]}


if __name__ == "__main__":
    print(json.dumps(lambda_handler(), indent=2)[:1500])
