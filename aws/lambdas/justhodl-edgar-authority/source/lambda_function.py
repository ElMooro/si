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


def _crosscheck_one(tk, cik, fmp_inc, fmp_bs):
    """Compare FMP's latest annual figures to the SEC filing for the same fiscal year."""
    facts = edgar.companyfacts(cik)
    if not facts:
        return None
    ed_rev, fy, end = edgar.cf_latest_annual(facts, edgar.REVENUE)
    ed_ni, _, _ = edgar.cf_latest_annual(facts, edgar.NET_INCOME)
    ed_assets, _, _ = edgar.cf_latest_annual(facts, edgar.ASSETS)
    if ed_rev is None and ed_ni is None:
        return None
    # match FMP row to the same fiscal year as the filing
    frow = None
    for row in (fmp_inc or []):
        ry = row.get("fiscalYear") or str(row.get("date", ""))[:4]
        if str(ry) == str(fy):
            frow = row; break
    if frow is None and fmp_inc:
        frow = fmp_inc[0]
    fmp_rev = (frow or {}).get("revenue")
    fmp_ni = (frow or {}).get("netIncome")
    brow = None
    for row in (fmp_bs or []):
        ry = row.get("fiscalYear") or str(row.get("date", ""))[:4]
        if str(ry) == str(fy):
            brow = row; break
    if brow is None and fmp_bs:
        brow = fmp_bs[0]
    fmp_assets = (brow or {}).get("totalAssets")

    d_rev = _pct_diff(fmp_rev, ed_rev)
    d_ni = _pct_diff(fmp_ni, ed_ni)
    d_as = _pct_diff(fmp_assets, ed_assets)
    flags = []
    if d_rev is not None and abs(d_rev) > DISCREPANCY_PCT:
        flags.append("revenue %+.1f%%" % d_rev)
    if d_ni is not None and abs(d_ni) > DISCREPANCY_PCT:
        flags.append("net income %+.1f%%" % d_ni)
    if d_as is not None and abs(d_as) > DISCREPANCY_PCT:
        flags.append("assets %+.1f%%" % d_as)
    return {
        "ticker": tk, "fy": fy, "period_end": end,
        "filing": {"revenue": ed_rev, "net_income": ed_ni, "assets": ed_assets},
        "fmp": {"revenue": fmp_rev, "net_income": fmp_ni, "assets": fmp_assets},
        "diff_pct": {"revenue": d_rev, "net_income": d_ni, "assets": d_as},
        "flags": flags, "match": not flags,
    }


def lambda_handler(event, context):
    t0 = time.time()
    now = datetime.now(timezone.utc).isoformat()
    cmap = edgar.cik_map()
    uni = json.loads(S3.get_object(Bucket=BUCKET, Key="data/finviz-universe.json")["Body"].read()).get("by_ticker", {})

    # ── (2) NCAV net-net screen — whole market via frames ──
    ac = edgar.frames_multi(edgar.ASSETS_CURRENT, periods=INSTANT_PERIODS)
    li = edgar.frames_multi(edgar.LIABILITIES, periods=INSTANT_PERIODS)
    net_nets = []
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
        if ncav > 0 and mc < ncav:
            net_nets.append({
                "ticker": tk, "name": r.get("company"), "sector": r.get("sector"),
                "market_cap_m": round(mc_m, 1), "ncav_m": round(ncav / 1e6, 1),
                "discount_pct": round((1 - mc / ncav) * 100, 1),
                "classic_net_net": mc < (2.0 / 3.0) * ncav,
                "price": r.get("price"), "pb": r.get("pb"),
            })
    net_nets.sort(key=lambda x: -x["discount_pct"])
    ncav_coverage = sum(1 for tk in uni if cmap.get(tk) in ac)

    # ── (1) FMP cross-check vs filing — high-potential set ──
    try:
        val = json.loads(S3.get_object(Bucket=BUCKET, Key="data/stock-valuations.json")["Body"].read())
        hp = [r.get("t") for r in (val.get("hp_out") or []) if r.get("t")]
    except Exception:
        hp = []
    targets = [t for t in dict.fromkeys(hp) if cmap.get(t)][:80]

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
    flagged.sort(key=lambda c: -max(abs(v) for v in c["diff_pct"].values() if v is not None) if any(v is not None for v in c["diff_pct"].values()) else 0)

    out = {
        "engine": "edgar-authority", "version": "1.0.0", "generated_at": now,
        "elapsed_s": round(time.time() - t0, 1),
        "net_nets": net_nets[:60],
        "n_net_nets": len(net_nets),
        "n_classic_net_nets": sum(1 for x in net_nets if x["classic_net_net"]),
        "ncav_coverage": ncav_coverage,
        "crosscheck": {
            "n_checked": len(checks), "n_clean": len(checks) - len(flagged),
            "n_flagged": len(flagged), "flagged": flagged, "sample_clean": [c["ticker"] for c in checks if c["match"]][:20],
        },
        "provenance": "SEC EDGAR XBRL (frames + companyfacts); cross-check vs FMP /stable/",
        "note": "Net-nets are typically tiny/distressed (cheap for a reason). Cross-check flags large FMP-vs-filing divergences; some may be fiscal-period timing — verify against the 10-K.",
    }
    S3.put_object(Bucket=BUCKET, Key=OUT_KEY,
                  Body=json.dumps(out, separators=(",", ":"), default=str).encode(),
                  ContentType="application/json", CacheControl="public, max-age=900")
    return {"statusCode": 200, "body": json.dumps({
        "net_nets": len(net_nets), "classic": out["n_classic_net_nets"],
        "ncav_cov": ncav_coverage, "checked": len(checks), "flagged": len(flagged),
        "elapsed_s": out["elapsed_s"]})}
