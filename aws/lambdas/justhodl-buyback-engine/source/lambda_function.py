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
    """Pull FMP and compute the buyback dossier for one ticker. None if no data."""
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
    fcf_yield = None
    if isinstance(km, list) and km:
        mcap = float(km[0].get("marketCap") or 0) or None
        fcfq = km[0].get("freeCashFlowYield")
        fcf_yield = float(fcfq) * 4 * 100 if fcfq is not None else None  # quarterly→annualized %

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
    gross_yield = round(gross_repo / mcap * 100, 2)
    net_yield = round(net_buyback / mcap * 100, 2)
    div_yield = round(ttm_div / mcap * 100, 2)
    shareholder_yield = round(net_yield + div_yield, 2)
    debt_funded = debt_issuance > 0 and gross_repo > 0

    return {
        "symbol": t, "market_cap": mcap,
        "gross_repurchases_ttm": round(gross_repo, 0), "net_buyback_ttm": round(net_buyback, 0),
        "issuance_ttm": round(issuance, 0), "sbc_ttm": round(ttm_sbc, 0),
        "gross_buyback_yield": gross_yield, "net_buyback_yield": net_yield,
        "dividend_yield": div_yield, "shareholder_yield": shareholder_yield,
        "active_execution": active, "last_q_repurchase": round(last_q_repo, 0),
        "share_count_reduction_yoy": sr_pct, "shares_now": shares_now,
        "fcf_yield_annualized": round(fcf_yield, 2) if fcf_yield is not None else None,
        "debt_funded": debt_funded,
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

    cheap = fcfY is not None and fcfY >= 6.0
    if auth_pct and auth_pct >= 5 and (active or (srP or 0) >= 1.5 or insider):
        klass = "🚀 FRESH_LARGE_AUTH"
    elif (srP or 0) >= 2 and d["net_buyback_ttm"] > 0:
        klass = "💪 NET_SHRINKER"
    elif shY >= 6:
        klass = "💰 HIGH_SHAREHOLDER_YIELD"
    elif active and cheap:
        klass = "🎯 CHEAP_REPURCHASER"
    elif grY > 0.5 and (srP is not None and srP <= 0):
        klass = "⚠️ DILUTION_OFFSET"
    elif active or d["net_buyback_ttm"] > 0:
        klass = "ACTIVE"
    else:
        klass = "NEUTRAL"
    high_conviction_pump = bool(auth_pct and auth_pct >= 5 and (active or (srP or 0) >= 1.5))
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

    tickers = {}
    n_fmp_ok = 0
    for i, t in enumerate(universe):
        d = analyze_ticker(t)
        if i % 30 == 0:
            time.sleep(0.25)
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
        tickers[t] = {**d, "auth_pct_mcap": auth_pct, "buyback_score": score, "class": klass,
                      "high_conviction_pump": pump, "cheap": cheap,
                      "company": a.get("company"), "announcement_date": a.get("announcement_date"),
                      "asr": a.get("asr"), "filing_url": a.get("filing_url"),
                      "why": "; ".join(bits)}

    rows = list(tickers.values())

    def top(pred, key, n=25):
        r = [x for x in rows if pred(x)]
        r.sort(key=key, reverse=True)
        return r[:n]

    out = {
        "engine": "buyback-engine", "version": "1.0.0",
        "generated_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "thesis": ("Unified buyback intelligence: fresh authorizations (catalyst) + actual execution "
                   "+ net-of-dilution + share-count shrink + valuation. Net buyback yield and a "
                   "genuinely shrinking share count separate real returns of capital from SBC offset."),
        "universe_n": len(universe), "n_scored": len(tickers), "n_fmp_resolved": n_fmp_ok,
        "counts": {k: len([x for x in rows if x["class"] == k]) for k in
                   ["🚀 FRESH_LARGE_AUTH", "💪 NET_SHRINKER", "💰 HIGH_SHAREHOLDER_YIELD",
                    "🎯 CHEAP_REPURCHASER", "⚠️ DILUTION_OFFSET", "ACTIVE", "NEUTRAL"]},
        "scanner_state": scanner.get("state"),
        "high_conviction_pumps": top(lambda x: x["high_conviction_pump"],
                                     lambda x: (x.get("auth_pct_mcap") or 0, x["buyback_score"])),
        "fresh_authorizations": top(lambda x: x.get("auth_pct_mcap"),
                                    lambda x: (x.get("auth_pct_mcap") or 0)),
        "net_shrinkers": top(lambda x: (x["share_count_reduction_yoy"] or 0) >= 1 and x["net_buyback_ttm"] > 0,
                             lambda x: x["share_count_reduction_yoy"] or 0),
        "high_shareholder_yield": top(lambda x: x["shareholder_yield"] >= 3,
                                      lambda x: x["shareholder_yield"]),
        "cheap_repurchasers": top(lambda x: x["cheap"] and (x["active_execution"] or x["net_buyback_ttm"] > 0),
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
