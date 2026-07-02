"""justhodl-etf-true-flows — REAL ETF net creation/redemption flows

The true mechanic of ETF flows: when capital comes IN, the issuer CREATES new
shares (shares outstanding rises); when capital leaves, shares are REDEEMED.
So:  net_flow_$ = Δ(shares_outstanding) × price.
This is genuine net flow from public data (FMP shares-float history) — not a
dollar-volume proxy. Computed over 1d / 5d / 20d windows for a curated set of
major broad / sector / thematic / asset-class ETFs.

OUTPUT: data/etf-true-flows.json  {by_etf{}, inflows[], outflows[],
  by_category{}}. Daily 15:45 UTC. capital-flow reads this for real flows.
"""
import json, os, time
import urllib.request
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed
from collections import defaultdict
import boto3

REGION = "us-east-1"; BUCKET = "justhodl-dashboard-live"
OUT_KEY = "data/etf-true-flows.json"
FMP_KEY = os.environ.get("FMP_KEY", "wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb")
FMP = "https://financialmodelingprep.com/stable"
s3 = boto3.client("s3", region_name=REGION)

# Curated ETF universe by category (the ones capital actually rotates through)
ETFS = {
    "BROAD_EQUITY_US": ["SPY", "VOO", "IVV", "QQQ", "VTI", "IWM", "DIA", "RSP"],
    "SECTOR_EQUITY": ["XLK", "XLF", "XLE", "XLV", "XLI", "XLY", "XLP", "XLU", "XLB", "XLRE", "XLC", "SMH", "XBI", "KRE", "ITB", "XOP"],
    "THEMATIC": ["ARKK", "ICLN", "TAN", "BOTZ", "ROBO", "LIT", "URA", "NLR", "IGV", "HACK", "CIBR", "FINX", "DRIV"],
    "INTERNATIONAL": ["EFA", "VEA", "EEM", "VWO", "FXI", "EWZ", "EWJ", "INDA", "ASHR"],
    "COUNTRY": ["MCHI", "EWG", "EWY", "EWT", "EWU", "EWC", "EWA", "EWW", "EZA", "TUR",
                "EPOL", "ARGT", "EIDO", "VNM", "THD", "EWQ", "EWL", "EWI", "EWP", "EWS"],  # hot-money world map (ops 2720)
    "RATES_TREASURIES": ["TLT", "IEF", "SHY", "GOVT", "BIL", "SGOV", "SHV", "VGSH", "IEI", "VGIT", "VGLT", "EDV"],
    "CREDIT": ["LQD", "HYG", "JNK", "AGG", "BND", "VCIT", "VCSH", "USHY", "SJNK", "BKLN", "SRLN", "EMB", "MUB"],
    "TIPS_INFLATION": ["TIP", "SCHP", "VTIP", "STIP"],
    "CRYPTO_ETF": ["IBIT", "FBTC", "ETHA", "BITO"],
    "COMMODITIES": ["GLD", "SLV", "USO", "DBC", "DBA", "UNG", "GDX"],
    "CRYPTO": ["IBIT", "FBTC", "ETHA", "BITO", "GBTC"],
    "VOLATILITY": ["VXX", "UVXY", "SVXY"],
    "DIVIDEND_VALUE": ["SCHD", "VYM", "VTV", "DVY", "VIG"],
    "GROWTH": ["VUG", "IWF", "MGK", "SCHG"],
}


def http_json(url, t=12):
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "JustHodl/1.0"})
        with urllib.request.urlopen(req, timeout=t) as r:
            return json.loads(r.read().decode())
    except Exception:
        return None


def read_json(key, default=None):
    try:
        return json.loads(s3.get_object(Bucket=BUCKET, Key=key)["Body"].read())
    except Exception:
        return default


def num(v):
    try:
        f = float(v); return f if f == f else None
    except Exception:
        return None


def fetch_shares(sym, category):
    """Current shares outstanding + price (FMP shares-float is latest-only, so
    we snapshot daily and diff to get true net flow)."""
    try:
        sf = http_json(f"{FMP}/shares-float?symbol={sym}&apikey={FMP_KEY}")
        rec = (sf[0] if isinstance(sf, list) and sf else sf) if sf else None
        if not rec:
            return None
        so = num(rec.get("outstandingShares")) or num(rec.get("floatShares")) or num(rec.get("freeFloat"))
        if not so:
            return None
        q = http_json(f"{FMP}/quote-short?symbol={sym}&apikey={FMP_KEY}")
        price = None
        if isinstance(q, list) and q: price = num(q[0].get("price"))
        return {"ticker": sym, "category": category, "shares_outstanding": so, "price": price,
                "aum_est_b": round(so * price / 1e9, 2) if (so and price) else None}
    except Exception:
        return None


def lambda_handler(event=None, context=None):
    t0 = time.time()
    tasks = [(s, cat) for cat, syms in ETFS.items() for s in syms]
    today = {}
    with ThreadPoolExecutor(max_workers=20) as ex:
        for fut in as_completed([ex.submit(fetch_shares, s, c) for s, c in tasks]):
            r = fut.result()
            if r: today[r["ticker"]] = r

    # Load prior daily snapshots (we keep a rolling history) to diff Δshares
    prev = read_json("data/etf-shares-snapshots/latest.json") or {}
    prev_shares = prev.get("shares", {})
    hist = read_json("data/etf-shares-history.json") or {"days": []}
    # compute true net flow = Δshares × price (vs yesterday)
    results = []
    for tk, r in today.items():
        so_now = r["shares_outstanding"]; price = r.get("price")
        prev_so = prev_shares.get(tk)
        nf1 = round((so_now - prev_so) * price, 0) if (prev_so and price) else None
        # 5d/20d from history if available
        def flow_over(days):
            days_list = hist.get("days", [])
            if len(days_list) < days:
                return None
            old = days_list[-days].get("shares", {}).get(tk)
            return round((so_now - old) * price, 0) if (old and price) else None
        nf5 = flow_over(5); nf20 = flow_over(20)
        results.append({**r, "net_flow_1d_usd": nf1, "net_flow_5d_usd": nf5 if nf5 is not None else nf1,
                        "net_flow_20d_usd": nf20,
                        "shares_chg_5d_pct": (round((so_now/(prev_so)-1)*100, 2) if prev_so else None)})

    # primary flow metric: 5d if available else 1d
    def fm(r): return r.get("net_flow_5d_usd") if r.get("net_flow_5d_usd") is not None else (r.get("net_flow_1d_usd") or 0)
    results.sort(key=lambda x: -(fm(x) or 0))
    inflows = [r for r in results if (fm(r) or 0) > 0][:25]
    outflows = sorted([r for r in results if (fm(r) or 0) < 0], key=lambda x: fm(x) or 0)[:20]

    by_cat = defaultdict(lambda: {"net_flow_5d_usd": 0.0, "n": 0})
    for r in results:
        v = fm(r)
        if v is not None:
            by_cat[r["category"]]["net_flow_5d_usd"] += v
            by_cat[r["category"]]["n"] += 1
    cat_rotation = sorted([{"category": k, "net_flow_5d_usd": round(v["net_flow_5d_usd"], 0), "n_etfs": v["n"]}
                           for k, v in by_cat.items()], key=lambda x: -x["net_flow_5d_usd"])

    # persist today's snapshot + roll history (keep 25 days)
    today_shares = {tk: r["shares_outstanding"] for tk, r in today.items()}
    day_str = datetime.now(timezone.utc).date().isoformat()
    s3.put_object(Bucket=BUCKET, Key="data/etf-shares-snapshots/latest.json",
                  Body=json.dumps({"date": day_str, "shares": today_shares}).encode(),
                  ContentType="application/json")
    days = hist.get("days", [])
    if not days or days[-1].get("date") != day_str:
        days.append({"date": day_str, "shares": today_shares})
    days = days[-25:]
    s3.put_object(Bucket=BUCKET, Key="data/etf-shares-history.json",
                  Body=json.dumps({"days": days}).encode(), ContentType="application/json")

    bootstrapping = not prev_shares
    out = {
        "engine": "etf-true-flows", "version": "1.1",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "duration_s": round(time.time() - t0, 1),
        "n_etfs": len(results),
        "maturity": "BOOTSTRAPPING" if bootstrapping else ("BUILDING" if len(days) < 5 else "READY"),
        "method": ("True net creation/redemption flow = Δ(shares outstanding) × "
                   "price, snapshotted daily (FMP shares-float is latest-only). "
                   "Bootstraps over a few days; 5d/20d windows fill in as history "
                   "accrues. Creations = inflow, redemptions = outflow."),
        "inflows": inflows, "outflows": outflows,
        "category_rotation": cat_rotation,
        "by_etf": {r["ticker"]: r for r in results},
    }
    s3.put_object(Bucket=BUCKET, Key=OUT_KEY, Body=json.dumps(out, default=str).encode(),
                  ContentType="application/json", CacheControl="public, max-age=3600")
    print(f"[etf-true-flows] DONE {round(time.time()-t0,1)}s — {len(results)} ETFs snapshot, "
          f"maturity {out['maturity']}, history {len(days)}d")
    return {"statusCode": 200, "body": json.dumps({"ok": True, "n_etfs": len(results),
                                                     "maturity": out["maturity"], "history_days": len(days)})}
