"""
justhodl-bottleneck-research — per-stock research + plain-English AI thesis layer
================================================================================
Turns the bottleneck-boom page into a self-contained research terminal. For each
top bottleneck candidate it assembles:
  • What the company does (FMP profile description)
  • Valuation: P/E, P/S, P/B, and the stock's P/E vs its INDUSTRY P/E
  • 10 years of financials (revenue, net income, EPS, gross/op/net margins)
  • A short, HONEST, normie-readable thesis: the mechanism for why it could
    re-rate (bottleneck demand + the numbers) AND the single biggest risk.
Theses are generated via the tiered LLM router (tier="reason" -> GLM-5.1 with
Claude fallback; this is public-data synthesis) and CACHED so we don't pay to
regenerate identical theses every run. Output: data/bottleneck-boom-research.json
keyed by ticker, consumed by /bottleneck-boom.html as expandable research drawers.
"""
import json
import os
import time
import urllib.request
import urllib.parse
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed

import boto3
from llm_router import complete

VERSION = "1.0.0"
S3 = boto3.client("s3", region_name="us-east-1")
BUCKET = "justhodl-dashboard-live"
SRC_KEY = "data/bottleneck-boom.json"
OUT_KEY = "data/bottleneck-boom-research.json"
FMP_KEY = os.environ.get("FMP_KEY") or "wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb"
BASE = "https://financialmodelingprep.com/stable"
TOP_N = 30
THESIS_CACHE_HRS = 20
MAX_NEW_THESES = 30

SYSTEM = (
    "You are a sharp, honest equity analyst explaining a stock to a smart beginner with no "
    "finance background. Plain English, zero jargon, no hype, no price targets, never promise "
    "gains. Write 3-4 short sentences: first explain in simple terms the MECHANISM for why this "
    "stock could re-rate higher (tie it to the demand/supply bottleneck and the specific numbers "
    "given), then end with ONE sentence naming the single biggest risk or the thing that would "
    "prove the thesis wrong. Be concrete and specific to the numbers provided."
)


def fmp(path, params):
    p = dict(params); p["apikey"] = FMP_KEY
    url = f"{BASE}/{path}?" + urllib.parse.urlencode(p)
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "JustHodl/1.0"})
        with urllib.request.urlopen(req, timeout=15) as r:
            return json.loads(r.read().decode())
    except Exception:
        return None


def num(v):
    try:
        return float(v)
    except Exception:
        return None


def fetch_financials(tk):
    prof = fmp("profile", {"symbol": tk})
    inc = fmp("income-statement", {"symbol": tk, "period": "annual", "limit": 10})
    rat = fmp("ratios-ttm", {"symbol": tk})
    p = (prof[0] if isinstance(prof, list) and prof else {}) or {}
    r = (rat[0] if isinstance(rat, list) and rat else {}) or {}
    fins = []
    for row in (inc if isinstance(inc, list) else [])[:10]:
        rev = num(row.get("revenue")); ni = num(row.get("netIncome"))
        gp = num(row.get("grossProfit")); oi = num(row.get("operatingIncome"))
        fins.append({
            "year": (row.get("calendarYear") or str(row.get("date", ""))[:4]),
            "revenue": rev, "netIncome": ni,
            "eps": num(row.get("epsdiluted")) or num(row.get("eps")),
            "gm": round(gp / rev * 100, 1) if (rev and gp is not None) else None,
            "om": round(oi / rev * 100, 1) if (rev and oi is not None) else None,
            "nm": round(ni / rev * 100, 1) if (rev and ni is not None) else None,
        })
    fins.reverse()  # oldest -> newest for charting
    return {
        "desc": (p.get("description") or "")[:650],
        "ceo": p.get("ceo"), "employees": p.get("fullTimeEmployees"),
        "website": p.get("website"), "exchange": p.get("exchangeShortName"),
        "sector": p.get("sector"), "industry": p.get("industry"),
        "mkt_cap": p.get("mktCap") or p.get("marketCap"),
        "price": num(p.get("price")), "range_52w": p.get("range"), "beta": num(p.get("beta")),
        "pe": num(p.get("pe")) or num(r.get("priceToEarningsRatioTTM")),
        "ps": num(r.get("priceToSalesRatioTTM")),
        "pb": num(r.get("priceToBookRatioTTM")),
        "div_yield": (round(num(r.get("dividendYieldTTM")) * 100, 2)
                      if num(r.get("dividendYieldTTM")) is not None else None),
        "financials": fins,
    }


def fetch_peer_pe():
    """Best-effort industry + sector P/E snapshots for the vs-peers comparison."""
    today = datetime.now(timezone.utc).date().isoformat()
    ind, sec = {}, {}
    isnap = fmp("industry-pe-snapshot", {"date": today})
    for row in (isnap if isinstance(isnap, list) else []):
        k = row.get("industry"); v = num(row.get("pe"))
        if k and v:
            ind[k] = round(v, 1)
    ssnap = fmp("sector-pe-snapshot", {"date": today})
    for row in (ssnap if isinstance(ssnap, list) else []):
        k = row.get("sector"); v = num(row.get("pe"))
        if k and v:
            sec[k] = round(v, 1)
    return ind, sec


def make_thesis(name, tk, ind, m):
    prompt = (
        f"Stock: {name} ({tk}); industry: {ind}.\n"
        f"What it does: {(m.get('desc') or '')[:320]}\n"
        f"Signals:\n"
        f"- revenue growth {m.get('rev_growth_yoy')}% year-over-year\n"
        f"- revenue ACCELERATION {m.get('rev_accel_pp')}pp (this quarter's growth minus last quarter's — positive means speeding up)\n"
        f"- revenue-to-market-cap {m.get('rev_to_mcap_pct')}% (higher = cheaper for the sales it generates)\n"
        f"- valuation: P/E {m.get('pe')} vs its industry's typical P/E {m.get('industry_pe')}\n"
        f"- its industry's supply-bottleneck pressure is {m.get('group_pressure')}/100 "
        f"(high = orders/backlog piling up faster than companies can ship)\n"
        f"Write the plain-English thesis for why this stock could re-rate higher, then the single biggest risk."
    )
    try:
        out = complete(prompt, tier="reason", max_tokens=320, system=SYSTEM)
        return (out or "").strip() or None
    except Exception as e:
        print(f"[research] thesis fail {tk}: {str(e)[:80]}")
        return None


def lambda_handler(event=None, context=None):
    t0 = time.time()
    try:
        src = json.loads(S3.get_object(Bucket=BUCKET, Key=SRC_KEY)["Body"].read())
    except Exception as e:
        return {"statusCode": 500, "body": json.dumps({"err": f"no source: {e}"})}
    ranks = (src.get("ranks") or [])[:TOP_N]
    top_calls = set(src.get("top_calls") or [])
    try:
        cache = json.loads(S3.get_object(Bucket=BUCKET, Key=OUT_KEY)["Body"].read()).get("by_ticker", {})
    except Exception:
        cache = {}
    now = datetime.now(timezone.utc)
    ind_pe, sec_pe = fetch_peer_pe()

    tickers = [r["ticker"] for r in ranks]
    fin_map = {}
    with ThreadPoolExecutor(max_workers=8) as ex:
        futs = {ex.submit(fetch_financials, tk): tk for tk in tickers}
        for f in as_completed(futs):
            try:
                fin_map[futs[f]] = f.result()
            except Exception:
                fin_map[futs[f]] = {}

    out = {}
    need = []
    for r in ranks:
        tk = r["ticker"]
        fin = fin_map.get(tk, {}) or {}
        ind = fin.get("industry") or r.get("industry")
        industry_pe = ind_pe.get(ind) or sec_pe.get(fin.get("sector") or r.get("sector"))
        rec = {
            "name": r.get("name"), "industry": ind, "sector": fin.get("sector") or r.get("sector"),
            "desc": fin.get("desc"), "ceo": fin.get("ceo"), "employees": fin.get("employees"),
            "website": fin.get("website"), "exchange": fin.get("exchange"),
            "mkt_cap": fin.get("mkt_cap") or r.get("mkt_cap"), "price": fin.get("price"),
            "range_52w": fin.get("range_52w"), "beta": fin.get("beta"),
            "pe": fin.get("pe"), "ps": fin.get("ps") or r.get("ps_ttm"), "pb": fin.get("pb"),
            "div_yield": fin.get("div_yield"), "industry_pe": industry_pe,
            "financials": fin.get("financials") or [],
            "rev_growth_yoy": r.get("rev_growth_yoy"), "rev_accel_pp": r.get("rev_accel_pp"),
            "rev_to_mcap_pct": r.get("rev_to_mcap_pct"), "boom_score": r.get("boom_score"),
            "group_pressure": r.get("group_pressure"), "is_top_call": tk in top_calls,
        }
        cached = cache.get(tk, {})
        ts = cached.get("thesis_at")
        fresh = False
        if ts:
            try:
                fresh = (now - datetime.fromisoformat(ts)).total_seconds() < THESIS_CACHE_HRS * 3600
            except Exception:
                fresh = False
        rec["thesis"], rec["thesis_at"] = cached.get("thesis"), cached.get("thesis_at")
        if not (fresh and cached.get("thesis")):
            need.append(tk)
        out[tk] = rec

    # generate stale/new theses IN PARALLEL (independent LLM calls) to fit the timeout
    new_theses = 0
    targets = need[:MAX_NEW_THESES]

    def _gen(tk):
        rec = out[tk]
        return tk, make_thesis(rec.get("name") or tk, tk, rec.get("industry"), rec)

    if targets:
        with ThreadPoolExecutor(max_workers=6) as ex:
            for tk, th in ex.map(_gen, targets):
                if th:
                    out[tk]["thesis"], out[tk]["thesis_at"] = th, now.isoformat()
                    new_theses += 1

    payload = {
        "engine": "bottleneck-research", "version": VERSION,
        "generated_at": now.isoformat(), "source_generated_at": src.get("generated_at"),
        "n": len(out), "new_theses": new_theses, "duration_s": round(time.time() - t0, 1),
        "by_ticker": out,
    }
    S3.put_object(Bucket=BUCKET, Key=OUT_KEY, Body=json.dumps(payload, default=str).encode(),
                  ContentType="application/json", CacheControl="public, max-age=1800")
    print(f"[research] enriched {len(out)} tickers, {new_theses} new theses in {round(time.time()-t0,1)}s")
    return {"statusCode": 200, "body": json.dumps({"n": len(out), "new_theses": new_theses})}
