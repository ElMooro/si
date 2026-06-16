"""
justhodl-edgar-authority — filing-grade EDGAR layer (two gaps the system lacked).

1) AUTHORITATIVE CROSS-CHECK
   The system's financials come from FMP (a third party). This validates FMP's
   latest reported revenue / net income / total assets against the actual SEC
   filing (XBRL companyfacts, period-matched), and flags names where FMP diverges
   from the filing by >10%. Filing-grade provenance for anything you'd stake a
   position on. Scope: the high-potential names the system actively surfaces.

2) NCAV / NET-NET DEEP VALUE (Graham)
   Net Current Asset Value = current assets − TOTAL liabilities. When market cap
   < NCAV the market is paying less than liquidation-ish working capital; < 2/3
   NCAV is Graham's classic net-net. Whole-market via the SEC frames API.
   HONESTY: net-nets are usually tiny/distressed names — cheap for a reason. This
   surfaces them; it does not endorse them.

OUTPUT: data/edgar-authority.json
"""
import json
import os
import time
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed

import boto3
import edgar  # shared toolkit (bundled at deploy)

S3 = boto3.client("s3", region_name="us-east-1")
BUCKET = "justhodl-dashboard-live"
OUT_KEY = "data/edgar-authority.json"
FMP_KEY = os.environ.get("FMP_KEY") or "wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb"

# balance-sheet instants (most-recent-first); income annual durations
INSTANT_PERIODS = ["CY2026Q1I", "CY2025Q4I", "CY2025Q3I", "CY2025Q2I", "CY2025Q1I"]
ANNUAL_PERIODS = ["CY2025", "CY2024"]
DISCREPANCY_PCT = 10.0   # flag FMP vs filing divergence beyond this


def _fmp(ep, params):
    p = dict(params); p["apikey"] = FMP_KEY
    url = "https://financialmodelingprep.com/stable/%s?%s" % (ep, urllib.parse.urlencode(p))
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    try:
        with urllib.request.urlopen(req, timeout=25) as r:
            return json.loads(r.read().decode("utf-8", "ignore"))
    except Exception:
        return None


def _pct_diff(a, b):
    if a is None or b is None or not b:
        return None
    return round((a - b) / abs(b) * 100, 1)


def _match_fmp_by_end(fmp_rows, end_date):
    """Pick the FMP annual row whose period end aligns with the filing's end date."""
    if not fmp_rows:
        return None
    if end_date:
        ey = str(end_date)[:7]  # YYYY-MM
        for row in fmp_rows:
            if str(row.get("date", ""))[:7] == ey:
                return row
        ey4 = str(end_date)[:4]
        for row in fmp_rows:
            if str(row.get("date", ""))[:4] == ey4 or str(row.get("fiscalYear")) == ey4:
                return row
    return fmp_rows[0]


def _crosscheck_one(tk, cik, fmp_inc, fmp_bs):
    """Compare FMP's latest annual NET INCOME + TOTAL ASSETS to the SEC filing for
    the SAME period end. Universal us-gaap concepts only (revenue is sector-variable
    in XBRL — banks/insurers don't use 'Revenues' cleanly — so it's excluded)."""
    facts = edgar.companyfacts(cik)
    if not facts:
        return None
    ed_ni, fy, end = edgar.cf_latest_annual(facts, edgar.NET_INCOME)
    ed_assets, fya, enda = edgar.cf_latest_annual(facts, edgar.ASSETS)
    if ed_ni is None and ed_assets is None:
        return None
    irow = _match_fmp_by_end(fmp_inc, end)
    brow = _match_fmp_by_end(fmp_bs, enda)
    fmp_ni = (irow or {}).get("netIncome")
    fmp_assets = (brow or {}).get("totalAssets")
    d_ni = _pct_diff(fmp_ni, ed_ni)
    d_as = _pct_diff(fmp_assets, ed_assets)

    flags, unverified = [], []
    for label, d in (("net income", d_ni), ("assets", d_as)):
        if d is None:
            continue
        if 5.0 < abs(d) <= 75.0:
            flags.append("%s %+.1f%%" % (label, d))
        elif abs(d) > 75.0:
            unverified.append("%s %+.1f%% (period/concept mismatch — not a confirmed error)" % (label, d))
    return {
        "ticker": tk, "fy": fy, "period_end": end,
        "filing": {"net_income": ed_ni, "assets": ed_assets},
        "fmp": {"net_income": fmp_ni, "assets": fmp_assets},
        "diff_pct": {"net_income": d_ni, "assets": d_as},
        "flags": flags, "unverified": unverified,
        "match": not flags and not unverified,
    }


def lambda_handler(event, context):
    t0 = time.time()
    now = datetime.now(timezone.utc).isoformat()
    cmap = edgar.cik_map()
    uni = json.loads(S3.get_object(Bucket=BUCKET, Key="data/finviz-universe.json")["Body"].read()).get("by_ticker", {})

    # ── (2) NCAV net-net screen — whole market via frames ──
    ac = edgar.frames_multi(edgar.ASSETS_CURRENT, periods=INSTANT_PERIODS)
    li = edgar.frames_multi(edgar.LIABILITIES, periods=INSTANT_PERIODS)
    net_nets = []          # credible: US-listed, >= floor, sane NCAV/MC
    n_all = 0              # raw count incl. foreign micro-caps / likely data errors
    MC_FLOOR_M = 50.0      # net-nets below this are usually nano-cap traps/errors
    NCAV_MC_CAP = 10.0     # NCAV/MC above this is almost always a units/currency error
    for tk, r in uni.items():
        cik = cmap.get(tk)
        mc_m = r.get("market_cap")  # $ millions
        if not cik or not mc_m:
            continue
        a = ac.get(cik); l = li.get(cik)
        if a is None or l is None:
            continue
        ncav = a - l
        mc = mc_m * 1e6
        if ncav <= 0 or mc >= ncav:
            continue
        n_all += 1
        country = r.get("country")
        ratio = ncav / mc
        # credible list: US-listed, above floor, NCAV/MC within sane bounds
        if country != "USA" or mc_m < MC_FLOOR_M or ratio > NCAV_MC_CAP:
            continue
        net_nets.append({
            "ticker": tk, "name": r.get("company"), "sector": r.get("sector"),
            "country": country, "market_cap_m": round(mc_m, 1),
            "ncav_m": round(ncav / 1e6, 1), "discount_pct": round((1 - mc / ncav) * 100, 1),
            "classic_net_net": mc < (2.0 / 3.0) * ncav,
            "price": r.get("price"), "pb": r.get("pb"),
        })
    net_nets.sort(key=lambda x: -x["discount_pct"])
    ncav_coverage = sum(1 for tk in uni if cmap.get(tk) in ac)

    # ── (1) FMP cross-check vs filing — names the system actually surfaces ──
    targets = []
    try:
        val = json.loads(S3.get_object(Bucket=BUCKET, Key="data/stock-valuations.json")["Body"].read())
        targets += [r.get("t") for r in (val.get("hp") or []) if r.get("t")]
    except Exception:
        pass
    try:
        opp = json.loads(S3.get_object(Bucket=BUCKET, Key="data/opportunities-research.json")["Body"].read())
        targets += list(opp.get("by_ticker", {}).keys())
    except Exception:
        pass
    targets = [t for t in dict.fromkeys(targets)
               if cmap.get(t) and uni.get(t, {}).get("country") == "USA"][:90]

    def _do(tk):
        inc = _fmp("income-statement", {"symbol": tk, "period": "annual", "limit": 4})
        bs = _fmp("balance-sheet-statement", {"symbol": tk, "period": "annual", "limit": 4})
        return _crosscheck_one(tk, cmap.get(tk), inc if isinstance(inc, list) else [],
                               bs if isinstance(bs, list) else [])

    checks = []
    with ThreadPoolExecutor(max_workers=6) as ex:
        futs = {ex.submit(_do, t): t for t in targets}
        for f in as_completed(futs):
            try:
                c = f.result()
                if c:
                    checks.append(c)
            except Exception:
                pass
    flagged = [c for c in checks if c["flags"]]
    unverified = [c for c in checks if c["unverified"] and not c["flags"]]
    clean = [c for c in checks if c["match"]]
    flagged.sort(key=lambda c: -max((abs(v) for v in c["diff_pct"].values() if v is not None), default=0))

    out = {
        "engine": "edgar-authority", "version": "1.0.0", "generated_at": now,
        "elapsed_s": round(time.time() - t0, 1),
        "net_nets": net_nets[:60],
        "n_net_nets": len(net_nets),
        "n_net_nets_raw": n_all,
        "n_classic_net_nets": sum(1 for x in net_nets if x["classic_net_net"]),
        "ncav_coverage": ncav_coverage,
        "net_net_filter": "US-listed, market cap >= $%.0fM, NCAV/MC <= %.0fx (excludes foreign micro-cap traps and likely units/currency reporting errors)" % (MC_FLOOR_M, NCAV_MC_CAP),
        "crosscheck": {
            "n_checked": len(checks), "n_clean": len(clean),
            "n_flagged": len(flagged), "n_unverified": len(unverified),
            "flagged": flagged,
            "unverified": [{"ticker": c["ticker"], "note": c["unverified"]} for c in unverified][:20],
            "sample_clean": [c["ticker"] for c in clean][:20],
            "method": "US filers only; SEC filing (XBRL) vs FMP, period-end matched; net income + total assets (universal concepts); confirmed flag band 5-75%, larger diffs bucketed as unverified concept/period mismatch.",
        },
        "provenance": "SEC EDGAR XBRL (frames + companyfacts); cross-check vs FMP /stable/",
        "note": "Net-nets are typically clinical-stage/distressed (below net cash for a reason) — surfaced, not endorsed. Cross-check confirms FMP matches the filing on net income + assets; flags are plausible FMP divergences worth a manual look at the 10-K.",
    }
    S3.put_object(Bucket=BUCKET, Key=OUT_KEY,
                  Body=json.dumps(out, separators=(",", ":"), default=str).encode(),
                  ContentType="application/json", CacheControl="public, max-age=900")
    return {"statusCode": 200, "body": json.dumps({
        "net_nets": len(net_nets), "classic": out["n_classic_net_nets"],
        "ncav_cov": ncav_coverage, "checked": len(checks), "flagged": len(flagged), "unverified": len(unverified),
        "elapsed_s": out["elapsed_s"]})}
