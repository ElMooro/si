"""
justhodl-fast-filings — FAST CAPITAL-FLOW FILINGS (13D/13G + Form 4 clusters)
=============================================================================
13F is 45 days stale. The fast, free tells that front-run moves:
  • 13D / 13G  — an investor crossing 5% ownership, filed within ~10 days. 13D
    (activist intent) is the stronger signal; 13G (passive) still flags accumulation.
  • Form 4 CLUSTER buys — multiple distinct insiders making OPEN-MARKET PURCHASES
    (code P) in the same name inside a short window. One insider buy is noise; a
    cluster is conviction. Pulled from Finnhub insider-transactions over our universe.

Activist targets + cluster-bought names feed the truth ledger via the harvester.
OUTPUT data/fast-filings.json   SCHEDULE daily 12:00 UTC. Real filings, research only.
"""
import json
import time
import urllib.request
import urllib.parse
from datetime import datetime, timezone, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

import boto3

VERSION = "1.0.0"
S3_BUCKET = "justhodl-dashboard-live"
OUT_KEY = "data/fast-filings.json"
FINNHUB = "d8qlt5pr01qrf6e278d0d8qlt5pr01qrf6e278dg"
SEC_UA = {"User-Agent": "JustHodl Research contact@justhodl.ai"}
MAX_F4 = 55
s3 = boto3.client("s3", region_name="us-east-1")


def _read(key):
    try:
        return json.loads(s3.get_object(Bucket=S3_BUCKET, Key=key)["Body"].read())
    except Exception:
        return None


def _get(url, hdr=None, timeout=14):
    try:
        r = urllib.request.urlopen(urllib.request.Request(url, headers=hdr or {"User-Agent": "jh-ff"}), timeout=timeout)
        return r.getcode(), r.read().decode("utf-8", "ignore")
    except Exception:
        return 0, ""


def cik_ticker_map():
    c, b = _get("https://www.sec.gov/files/company_tickers.json", SEC_UA, 20)
    m = {}
    if c == 200 and b:
        try:
            for row in json.loads(b).values():
                if row.get("cik_str") and row.get("ticker"):
                    m[int(row["cik_str"])] = row["ticker"].upper()
        except Exception:
            pass
    return m


def fts(form, days=16):
    """EDGAR full-text search for a form type; recent filings."""
    end = datetime.now(timezone.utc).date()
    start = end - timedelta(days=days)
    url = ("https://efts.sec.gov/LATEST/search-index?forms=" + urllib.parse.quote(form)
           + f"&startdt={start}&enddt={end}")
    c, b = _get(url, SEC_UA, 16)
    hits = []
    if c == 200 and b:
        try:
            hits = (json.loads(b).get("hits", {}) or {}).get("hits", []) or []
        except Exception:
            hits = []
    if not hits:  # fallback: no date filter (relevance/recent)
        c, b = _get("https://efts.sec.gov/LATEST/search-index?forms=" + urllib.parse.quote(form), SEC_UA, 16)
        if c == 200 and b:
            try:
                hits = (json.loads(b).get("hits", {}) or {}).get("hits", []) or []
            except Exception:
                hits = []
    return hits


def finnhub_clusters(sym):
    frm = (datetime.now(timezone.utc) - timedelta(days=50)).date()
    to = datetime.now(timezone.utc).date()
    c, b = _get(f"https://finnhub.io/api/v1/stock/insider-transactions?symbol={sym}&from={frm}&to={to}&token={FINNHUB}")
    if c != 200 or not b:
        return None
    try:
        rows = json.loads(b).get("data", []) or []
    except Exception:
        return None
    buyers, shares = set(), 0
    last = None
    for r in rows:
        code = (r.get("transactionCode") or "").upper()
        chg = r.get("change") or 0
        if code == "P" and chg > 0:                      # open-market purchase
            buyers.add(r.get("name"))
            shares += chg
            d = r.get("transactionDate") or r.get("filingDate")
            if d and (last is None or d > last):
                last = d
    if len(buyers) >= 2:
        return {"symbol": sym, "n_buyers": len(buyers), "shares_bought": int(shares), "latest": last}
    return None


def build_universe():
    u = {}
    rr = _read("data/ai-rerating-radar.json") or {}
    for r in ((rr.get("summary", {}) or {}).get("top_setups", []) or []):
        if r.get("symbol"):
            u[r["symbol"]] = r.get("name")
    stk = _read("data/ai-infra-stack.json") or {}
    for layer in stk.get("stack", []):
        for n in layer.get("names", []):
            if n.get("symbol") and n.get("is_small_cap"):
                u.setdefault(n["symbol"], n.get("name"))
    rot = _read("data/theme-rotation.json") or {}
    for p in ((rot.get("summary", {}) or {}).get("top_picks", []) or []):
        if p.get("symbol"):
            u.setdefault(p["symbol"], None)
    return list(u.keys())[:MAX_F4], u


def lambda_handler(event, context):
    t0 = time.time()
    cmap = cik_ticker_map()

    # --- 13D / 13G activist crossings ---
    activist = []
    seen = set()
    for form in ("SC 13D", "SC 13D/A", "SC 13G"):
        for h in fts(form, 16):
            src = h.get("_source", {}) or {}
            fdate = src.get("file_date")
            ciks = src.get("ciks", []) or []
            names = src.get("display_names", []) or []
            tkr = None
            for ck in ciks:
                try:
                    t = cmap.get(int(ck))
                except Exception:
                    t = None
                if t:
                    tkr = t
                    break
            key = (form, fdate, tuple(ciks))
            if key in seen:
                continue
            seen.add(key)
            activist.append({
                "form": form, "date": fdate, "subject_ticker": tkr,
                "filer": names[0] if names else None,
                "parties": names[:3],
                "is_activist": form.startswith("SC 13D"),
            })
        time.sleep(0.4)
    activist.sort(key=lambda x: (x.get("date") or ""), reverse=True)
    activist = activist[:60]

    # --- Form 4 cluster buys over the universe ---
    universe, names = build_universe()
    clusters = []
    for s in universe:
        if time.time() - t0 > 210:
            break
        cl = finnhub_clusters(s)
        if cl:
            cl["name"] = names.get(s)
            clusters.append(cl)
        time.sleep(1.05)
    clusters.sort(key=lambda x: (x["n_buyers"], x["shares_bought"]), reverse=True)

    # picks for the harvester: activist subjects (with tickers) + cluster names
    picks, pseen = [], set()
    for a in activist:
        t = a.get("subject_ticker")
        if t and t not in pseen and a["is_activist"]:
            pseen.add(t)
            picks.append({"symbol": t, "reason": "13D activist stake", "date": a.get("date")})
    for c in clusters:
        if c["symbol"] not in pseen:
            pseen.add(c["symbol"])
            picks.append({"symbol": c["symbol"], "reason": f"{c['n_buyers']} insiders buying", "date": c.get("latest")})

    out = {
        "engine": "fast-filings", "version": VERSION,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "thesis": "13D/13G 5% crossings (filed within ~10d) + Form 4 open-market purchase clusters — "
                  "the fast capital-flow tells, vs 45-day-stale 13F.",
        "n_activist": len(activist), "n_clusters": len(clusters), "universe_scanned": len(universe),
        "activist_filings": activist,
        "activist_with_ticker": [a for a in activist if a.get("subject_ticker")][:30],
        "form4_clusters": clusters,
        "picks": picks[:40],
        "sources": ["SEC EDGAR full-text search (free)", "SEC company_tickers.json", "Finnhub insider-transactions (free)"],
        "caveats": "EDGAR display_names may list filer and subject; subject ticker is best-effort via CIK map and "
                   "can be blank for funds/foreign filers. 13G is passive accumulation (weaker than 13D). Form 4 "
                   "clusters use code-P open-market buys only (grants excluded) over our universe, not all listings. "
                   "Real filings, research only — not advice.",
        "elapsed_s": round(time.time() - t0, 2),
    }
    s3.put_object(Bucket=S3_BUCKET, Key=OUT_KEY, Body=json.dumps(out).encode(), ContentType="application/json")
    print(f"[fast-filings] activist={len(activist)} (w/ticker {sum(1 for a in activist if a.get('subject_ticker'))}) "
          f"clusters={len(clusters)} scanned={len(universe)} {out['elapsed_s']}s")
    return {"statusCode": 200, "body": json.dumps({"ok": True, "activist": len(activist), "clusters": len(clusters)})}
