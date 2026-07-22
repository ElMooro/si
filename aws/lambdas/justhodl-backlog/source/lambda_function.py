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
from datetime import datetime, timedelta, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed
import boto3

REGION = "us-east-1"; BUCKET = "justhodl-dashboard-live"
OUT_KEY = "data/backlog.json"
FMP_KEY = os.environ.get("FMP_KEY", "wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb")
UA = "JustHodl.AI research raafouis@gmail.com"
s3 = boto3.client("s3", region_name=REGION)

# Backlog-relevant groups are still tagged for context, but coverage is now the
# FULL universe (data/universe.json) — any name exposing RPO/contract-liability
# XBRL tags is captured, across mega/large/mid/small/micro.
SECTOR_GROUP = {
    "Technology": "Software/Semis", "Communication Services": "Cloud/Media",
    "Industrials": "Industrials", "Energy": "Energy", "Health Care": "Healthcare",
    "Consumer Cyclical": "Consumer", "Financial Services": "Financials",
}
# A small curated seed guarantees the marquee backlog names are always covered
# even if they're missing from the universe snapshot.
SEED = ["CRM","NOW","SNOW","DDOG","CRWD","ZS","NET","PANW","WDAY","MDB","OKTA","PLTR","S","FTNT",
        "MSFT","AMZN","GOOGL","META","IBM","ACN","NVDA","AMD","AVGO","KLAC","AMAT","LRCX","MU","ON","MCHP","ADI","MRVL",
        "LMT","NOC","RTX","GD","LHX","HII","LDOS","BAH","KTOS","AVAV","CW","MRCY",
        "BA","GE","HON","TDG","HEI","HWM","TXT","CAT","DE","CMI","PCAR","ETN","EMR","ROK","PH","VRT","PWR","GNRC",
        "SLB","HAL","BKR","FTI","NOV"]

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


def read_json(key, default=None):
    try:
        return json.loads(s3.get_object(Bucket=BUCKET, Key=key)["Body"].read())
    except Exception:
        return default


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
        rows.append({"end": end, "val": float(v), "fp": u.get("fp"),
                     "form": u.get("form"), "filed": u.get("filed")})
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
    sep = "&" if "?" in ep else "?"
    d = http_json(f"https://financialmodelingprep.com/stable/{ep}{sep}symbol={sym}&apikey={FMP_KEY}")
    return d if isinstance(d, list) else (d if isinstance(d, dict) else None)


def pct(cur, prev):
    if prev and prev != 0:
        return round((cur / prev - 1) * 100, 1)
    return None


def analyze(sym, cik_map, meta):
    cik = cik_map.get(sym)
    if not cik:
        return None
    rpo_tag, rpo = first_series(cik, RPO_TAGS)
    def_tag, defr = first_series(cik, DEF_TAGS)
    if not rpo and not defr:
        return None   # no backlog disclosure → skip (this is the natural filter)
    m = meta.get(sym, {})
    rec = {"ticker": sym, "sector": m.get("sector"), "cap_bucket": m.get("cap_bucket"),
           "group": SECTOR_GROUP.get(m.get("sector"), m.get("sector")), "cik": cik}
    if rpo:
        latest = rpo[-1]["val"]
        rec["rpo"] = latest
        rec["rpo_qoq"] = pct(latest, rpo[-2]["val"]) if len(rpo) >= 2 else None
        rec["rpo_yoy"] = pct(latest, rpo[-5]["val"]) if len(rpo) >= 5 else None
        rec["rpo_tag"] = rpo_tag
        rec["rpo_asof"] = rpo[-1].get("end")      # period the figure covers
        rec["rpo_filed"] = rpo[-1].get("filed")   # when it became public
        rec["rpo_form"] = rpo[-1].get("form")
    if defr:
        rec["deferred_rev"] = defr[-1]["val"]
        rec["deferred_asof"] = defr[-1].get("end")
        rec["deferred_filed"] = defr[-1].get("filed")
        rec["deferred_qoq"] = pct(defr[-1]["val"], defr[-2]["val"]) if len(defr) >= 2 else None
        rec["deferred_yoy"] = pct(defr[-1]["val"], defr[-5]["val"]) if len(defr) >= 5 else None
        if len(defr) >= 3:
            q1 = pct(defr[-1]["val"], defr[-2]["val"]); q0 = pct(defr[-2]["val"], defr[-3]["val"])
            rec["deferred_accelerating"] = (q1 is not None and q0 is not None and q1 > q0)
    # Only enrich (extra FMP calls) names that actually disclose backlog → keeps
    # the run fast even across the full universe.
    try:
        q = fetch_fmp(sym, "income-statement?period=quarter&limit=6")
        if isinstance(q, list) and len(q) >= 5:
            qs = sorted(q, key=lambda r: r.get("date", ""))
            rev_now = qs[-1].get("revenue"); rev_yoy = qs[-5].get("revenue")
            rec["rev_yoy"] = pct(rev_now, rev_yoy) if (rev_now and rev_yoy) else None
        prof = fetch_fmp(sym, "key-metrics-ttm")
        ev = None
        if isinstance(prof, list) and prof:
            ev = prof[0].get("enterpriseValueTTM") or prof[0].get("enterpriseValue")
        if ev and rec.get("rpo"):
            rec["ev_to_rpo"] = round(ev / rec["rpo"], 2)
    except Exception:
        pass
    if rec.get("rpo_yoy") is not None and rec.get("rev_yoy") is not None:
        rec["rpo_minus_rev_growth"] = round(rec["rpo_yoy"] - rec["rev_yoy"], 1)
        rec["demand_accelerating"] = rec["rpo_yoy"] > rec["rev_yoy"] + 5
    return rec


def _capdist(rows):
    out = {}
    for r in rows:
        cb = r.get("cap_bucket") or "?"
        out[cb] = out.get(cb, 0) + 1
    return out


def lambda_handler(event=None, context=None):
    t0 = time.time()
    cik_map = load_cik_map()
    # Full universe (all caps) restricted to backlog-relevant SECTORS (where
    # RPO/contract-liability tags actually exist), SEED first, plus a persistent
    # coverage cache so confirmed-empty names are skipped on future runs — this
    # converges to full relevant-universe coverage over daily runs without
    # exceeding SEC's ~10 req/s rate limit in a single invocation.
    BACKLOG_SECTORS = {"Technology", "Communication Services", "Industrials",
                       "Energy", "Health Care", "Healthcare", "Consumer Cyclical"}
    cache = read_json("data/backlog-coverage-cache.json") or {"has_backlog": [], "no_backlog": []}
    has_set = set(cache.get("has_backlog", []))
    no_set = set(cache.get("no_backlog", []))
    uni = read_json("data/universe.json") or {}
    meta = {}
    for s in (uni.get("stocks") or []):
        tk = (s.get("symbol") or "").upper()
        if tk:
            meta[tk] = {"sector": s.get("sector"), "cap_bucket": s.get("cap_bucket")}
    for tk in SEED:
        meta.setdefault(tk, {"sector": "Technology", "cap_bucket": None})
    # candidate order: SEED → known-has-backlog → relevant-sector unknowns
    relevant = [t for t, m in meta.items()
                if (m.get("sector") in BACKLOG_SECTORS or t in SEED) and t in cik_map]
    seed_first = [t for t in SEED if t in cik_map]
    known = [t for t in relevant if t in has_set and t not in seed_first]
    unknown = [t for t in relevant if t not in has_set and t not in no_set and t not in seed_first]
    # cap the per-run workload so we finish inside the timeout; unknowns fill in
    # over successive days.
    candidates = seed_first + known + unknown[:600]
    print(f"[backlog] candidates {len(candidates)} (seed {len(seed_first)}, known {len(known)}, new {min(600,len(unknown))})")
    results = []
    new_has, new_no = set(), set()
    with ThreadPoolExecutor(max_workers=12) as ex:
        futs = {ex.submit(analyze, s, cik_map, meta): s for s in candidates}
        for f in as_completed(futs):
            sym = futs[f]
            try:
                r = f.result()
                if r:
                    results.append(r); new_has.add(sym)
                else:
                    new_no.add(sym)
            except Exception:
                pass
    # update coverage cache — throttle-safe:
    #  • never demote a known-has-backlog name to no_backlog (transient empties)
    #  • if we captured far fewer than the known-has set, the run was likely
    #    SEC-rate-limited — do NOT overwrite the cache (avoid poisoning it).
    expected_known = len(known) + len(seed_first)
    throttled = expected_known > 10 and len(new_has) < 0.3 * expected_known
    if not throttled:
        merged_has = has_set | new_has
        merged_no = (no_set | new_no) - merged_has   # has always wins
        cache = {"has_backlog": sorted(merged_has), "no_backlog": sorted(merged_no)}
        try:
            s3.put_object(Bucket=BUCKET, Key="data/backlog-coverage-cache.json",
                          Body=json.dumps(cache).encode(), ContentType="application/json")
        except Exception:
            pass
    else:
        print(f"[backlog] suspected SEC throttle ({len(new_has)}/{expected_known}) — cache NOT updated")
    # if THIS run was throttled, keep the prior good output instead of clobbering
    if throttled and len(results) < 10:
        prev = read_json(OUT_KEY)
        if prev and prev.get("n_covered", 0) > len(results):
            print("[backlog] throttled run — preserving prior output")
            return {"statusCode": 200, "body": json.dumps({"ok": True, "throttled": True,
                                                             "kept_prior": prev.get("n_covered")})}

    accelerating = sorted([r for r in results if r.get("demand_accelerating") or r.get("deferred_accelerating")],
                          key=lambda r: -(r.get("rpo_minus_rev_growth") or r.get("deferred_yoy") or 0))[:30]
    cheap_vs_backlog = sorted([r for r in results if r.get("ev_to_rpo") is not None],
                              key=lambda r: r.get("ev_to_rpo"))[:25]

    # ── PERSISTENT LEDGER (ops 3710) ────────────────────────────────────────
    # Each run walks a SLICE of the universe and this engine published only that
    # slice, so data/backlog.json churned instead of converging. Two invokes
    # minutes apart returned entirely different books (ZS/PLTR/DDOG/CRWD, then
    # CETX/OLOX/TPET). Merge the slice into a standing ledger so coverage
    # actually accumulates, and prune anything not re-verified in 60 days so it
    # cannot go stale silently.
    _now = datetime.now(timezone.utc).isoformat()
    _prev = (read_json(OUT_KEY) or {}).get("by_ticker") or {}
    ledger = dict(_prev)
    for _r in results:
        _r["refreshed_at"] = _now
        ledger[_r["ticker"]] = _r
    _cut = (datetime.now(timezone.utc) - timedelta(days=60)).isoformat()
    ledger = {k: v for k, v in ledger.items()
              if not v.get("refreshed_at") or v["refreshed_at"] >= _cut}
    print(f"[backlog] ledger {len(_prev)} + slice {len(results)} -> {len(ledger)}")

    out = {
        "engine": "backlog", "version": "1.0",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "duration_s": round(time.time() - t0, 1),
        "n_covered": len(results),
        "cap_distribution": _capdist(results),
        "by_ticker": ledger,
        "accelerating": accelerating,
        "cheap_vs_backlog": cheap_vs_backlog,
        "method": ("RPO (RevenueRemainingPerformanceObligation) + deferred revenue/"
                   "contract liabilities from free SEC XBRL company facts; YoY/QoQ "
                   "growth, RPO-vs-revenue-growth divergence (demand accelerating), "
                   "deferred-revenue acceleration, EV/RPO. Leads earnings 1-2 quarters."),
        "sources": {"backlog": "SEC XBRL (data.sec.gov)", "revenue/EV": "FMP"},
        "ledger_size": len(ledger),
        "slice_this_run": len(results),
    }
    s3.put_object(Bucket=BUCKET, Key=OUT_KEY, Body=json.dumps(out, default=str).encode(),
                  ContentType="application/json", CacheControl="public, max-age=3600")
    print(f"[backlog] DONE {round(time.time()-t0,1)}s — {len(results)} covered, "
          f"{len(accelerating)} accelerating")
    return {"statusCode": 200, "body": json.dumps({"ok": True, "covered": len(results),
                                                     "accelerating": len(accelerating)})}
