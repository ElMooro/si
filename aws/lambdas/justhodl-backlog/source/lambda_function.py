"""justhodl-backlog — structured backlog / RPO / deferred-revenue signal from SEC XBRL.

The audit's #1 gap: backlog as a QUANTIFIED signal, not narrative. Pulls free
SEC XBRL company facts and computes the metrics that lead earnings by 1-2 quarters:
  • RPO (RevenueRemainingPerformanceObligation) — SaaS/cloud demand visibility
  • Deferred revenue / contract liabilities (ContractWithCustomerLiability*)
  • RPO growth YoY/QoQ vs revenue growth (RPO > rev growth = accelerating demand)
  • Deferred-revenue acceleration (leads revenue by ~2 quarters)
  • EV/Backlog (cheap + accelerating = early boom setup)

Universe: curated backlog-relevant names (semis, defense, aerospace, SaaS,
cloud, industrials, energy services) where these XBRL tags are populated.

OUTPUT: data/backlog.json — { by_ticker, accelerating[], cheap_vs_backlog[] }
SCHEDULE: daily 11:30 UTC.
"""
import json, os, time
import urllib.request
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed
import boto3

REGION = "us-east-1"; BUCKET = "justhodl-dashboard-live"
OUT_KEY = "data/backlog.json"
FMP_KEY = os.environ.get("FMP_KEY", "wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb")
UA = "JustHodl.AI research raafouis@gmail.com"
s3 = boto3.client("s3", region_name=REGION)

# Backlog-relevant universe by group (where RPO / contract-liability tags matter)
UNIVERSE = {
    "SaaS/Software": ["CRM","NOW","SNOW","DDOG","CRWD","ZS","NET","PANW","WDAY","TEAM","HUBS","MDB","OKTA","PLTR","S","FTNT","ADBE","INTU","ORCL"],
    "Cloud/Mega": ["MSFT","AMZN","GOOGL","META","IBM","ACN"],
    "Semis": ["NVDA","AMD","AVGO","KLAC","AMAT","LRCX","ASML","MU","TSM","ON","MCHP","ADI","TXN","MRVL"],
    "Defense": ["LMT","NOC","RTX","GD","LHX","HII","LDOS","BAH","KTOS","AVAV","CW","MRCY"],
    "Aerospace": ["BA","GE","HON","TDG","HEI","SPR","HWM","TXT"],
    "Industrials": ["CAT","DE","CMI","PCAR","ETN","EMR","ROK","PH","ITW","GNRC","PWR","VRT"],
    "Energy svcs": ["SLB","HAL","BKR","FTI","NOV","WFRD"],
}
ALL = sorted({t for v in UNIVERSE.values() for t in v})
GROUP = {t: g for g, v in UNIVERSE.items() for t in v}

RPO_TAGS = ["RevenueRemainingPerformanceObligation"]
DEF_TAGS = ["ContractWithCustomerLiability", "ContractWithCustomerLiabilityCurrent",
            "DeferredRevenueCurrent", "ContractWithCustomerLiabilityNoncurrent"]


def http_json(url, t=15):
    try:
        req = urllib.request.Request(url, headers={"User-Agent": UA, "Accept-Encoding": "gzip, deflate"})
        with urllib.request.urlopen(req, timeout=t) as r:
            raw = r.read()
            if r.headers.get("Content-Encoding") == "gzip":
                import gzip; raw = gzip.decompress(raw)
            return json.loads(raw.decode())
    except Exception:
        return None


def load_cik_map():
    d = http_json("https://www.sec.gov/files/company_tickers.json")
    out = {}
    if isinstance(d, dict):
        for v in d.values():
            t = (v.get("ticker") or "").upper()
            if t:
                out[t] = str(v.get("cik_str")).zfill(10)
    return out


def concept_series(cik, tag):
    """Return time-ordered USD values for a us-gaap concept."""
    d = http_json(f"https://data.sec.gov/api/xbrl/companyconcept/CIK{cik}/us-gaap/{tag}.json")
    if not d:
        return []
    units = (d.get("units") or {}).get("USD") or []
    rows = []
    for u in units:
        v = u.get("val"); end = u.get("end")
        if v is None or not end:
            continue
        rows.append({"end": end, "val": float(v), "fp": u.get("fp"), "form": u.get("form")})
    rows.sort(key=lambda x: x["end"])
    # dedupe by end date keeping last
    seen = {}
    for r in rows:
        seen[r["end"]] = r
    return sorted(seen.values(), key=lambda x: x["end"])


def first_series(cik, tags):
    for tag in tags:
        s = concept_series(cik, tag)
        if len(s) >= 2:
            return tag, s
    return None, []


def fetch_fmp(sym, ep):
    d = http_json(f"https://financialmodelingprep.com/stable/{ep}?symbol={sym}&apikey={FMP_KEY}")
    return d if isinstance(d, list) else (d if isinstance(d, dict) else None)


def pct(cur, prev):
    if prev and prev != 0:
        return round((cur / prev - 1) * 100, 1)
    return None


def analyze(sym, cik_map):
    cik = cik_map.get(sym)
    if not cik:
        return None
    rpo_tag, rpo = first_series(cik, RPO_TAGS)
    def_tag, defr = first_series(cik, DEF_TAGS)
    if not rpo and not defr:
        return None
    rec = {"ticker": sym, "group": GROUP.get(sym), "cik": cik}
    # RPO metrics
    if rpo:
        latest = rpo[-1]["val"]
        rec["rpo"] = latest
        rec["rpo_qoq"] = pct(latest, rpo[-2]["val"]) if len(rpo) >= 2 else None
        # YoY: find ~4 quarters back
        rec["rpo_yoy"] = pct(latest, rpo[-5]["val"]) if len(rpo) >= 5 else None
        rec["rpo_tag"] = rpo_tag
    # Deferred revenue metrics
    if defr:
        rec["deferred_rev"] = defr[-1]["val"]
        rec["deferred_qoq"] = pct(defr[-1]["val"], defr[-2]["val"]) if len(defr) >= 2 else None
        rec["deferred_yoy"] = pct(defr[-1]["val"], defr[-5]["val"]) if len(defr) >= 5 else None
        # acceleration: latest QoQ vs prior QoQ
        if len(defr) >= 3:
            q1 = pct(defr[-1]["val"], defr[-2]["val"]); q0 = pct(defr[-2]["val"], defr[-3]["val"])
            rec["deferred_accelerating"] = (q1 is not None and q0 is not None and q1 > q0)
    # Revenue growth (FMP) for the RPO-vs-revenue divergence
    try:
        q = fetch_fmp(sym, "income-statement?period=quarter&limit=6")
        if isinstance(q, list) and len(q) >= 5:
            qs = sorted(q, key=lambda r: r.get("date", ""))
            rev_now = qs[-1].get("revenue"); rev_yoy = qs[-5].get("revenue")
            rec["rev_yoy"] = pct(rev_now, rev_yoy) if (rev_now and rev_yoy) else None
        prof = fetch_fmp(sym, "key-metrics-ttm?")
        ev = None
        if isinstance(prof, list) and prof:
            ev = prof[0].get("enterpriseValueTTM") or prof[0].get("enterpriseValue")
        if ev and rec.get("rpo"):
            rec["ev_to_rpo"] = round(ev / rec["rpo"], 2)
    except Exception:
        pass
    # Signal: RPO growth materially > revenue growth = accelerating demand / sandbagging
    if rec.get("rpo_yoy") is not None and rec.get("rev_yoy") is not None:
        rec["rpo_minus_rev_growth"] = round(rec["rpo_yoy"] - rec["rev_yoy"], 1)
        rec["demand_accelerating"] = rec["rpo_yoy"] > rec["rev_yoy"] + 5
    return rec


def lambda_handler(event=None, context=None):
    t0 = time.time()
    cik_map = load_cik_map()
    print(f"[backlog] CIK map: {len(cik_map)}")
    results = []
    with ThreadPoolExecutor(max_workers=10) as ex:
        futs = {ex.submit(analyze, s, cik_map): s for s in ALL}
        for f in as_completed(futs):
            try:
                r = f.result()
                if r: results.append(r)
            except Exception:
                pass

    accelerating = sorted([r for r in results if r.get("demand_accelerating") or r.get("deferred_accelerating")],
                          key=lambda r: -(r.get("rpo_minus_rev_growth") or r.get("deferred_yoy") or 0))[:30]
    cheap_vs_backlog = sorted([r for r in results if r.get("ev_to_rpo") is not None],
                              key=lambda r: r.get("ev_to_rpo"))[:25]

    out = {
        "engine": "backlog", "version": "1.0",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "duration_s": round(time.time() - t0, 1),
        "n_covered": len(results),
        "by_ticker": {r["ticker"]: r for r in results},
        "accelerating": accelerating,
        "cheap_vs_backlog": cheap_vs_backlog,
        "method": ("RPO (RevenueRemainingPerformanceObligation) + deferred revenue/"
                   "contract liabilities from free SEC XBRL company facts; YoY/QoQ "
                   "growth, RPO-vs-revenue-growth divergence (demand accelerating), "
                   "deferred-revenue acceleration, EV/RPO. Leads earnings 1-2 quarters."),
        "sources": {"backlog": "SEC XBRL (data.sec.gov)", "revenue/EV": "FMP"},
    }
    s3.put_object(Bucket=BUCKET, Key=OUT_KEY, Body=json.dumps(out, default=str).encode(),
                  ContentType="application/json", CacheControl="public, max-age=3600")
    print(f"[backlog] DONE {round(time.time()-t0,1)}s — {len(results)} covered, "
          f"{len(accelerating)} accelerating")
    return {"statusCode": 200, "body": json.dumps({"ok": True, "covered": len(results),
                                                     "accelerating": len(accelerating)})}
