"""
justhodl-share-flows v1.0.0 — capital-structure fleet map
=========================================================
Khalid: "track stock issuing, whether management is buying or
selling and how much, share count shrinking or increasing, dilution,
buybacks — on opportunities and all other engines where it applies."

Design: COMPOSER, extend-never-duplicate. One compact per-name map
any page/engine can join (pattern of ma_state / phases_all):

  data/share-flows.json
    tickers: {T: {sh_yoy_pct, sh_qoq_pct, buyback_ttm_usd,
                  buyback_yield_pct, issuance_ttm_usd,
                  issuance_pct_mcap, insider_buy_usd_90d,
                  insider_n_buyers, insider_sell_usd_recent,
                  read}}
    boards: top_buybacks / top_diluters / insider_conviction
    method: plain-language field guide (page tooltips reuse it)

Sources (all real):
  • FMP /stable/cash-flow-statement (quarterly, TTM buybacks =
    -sum(commonStockRepurchased), issuance = sum(commonStockIssued))
  • FMP /stable/income-statement (quarterly weightedAverageShsOutDil
    -> QoQ / YoY share-count change: the dilution truth)
  • FMP /stable/quote marketCap -> buyback yield / issuance %
  • data/insider-radar.json (Form-4 open-market BUY clusters:
    n_insiders, total_value) + any data/insider-sell* doc present
    (sell clusters) — joined, never re-fetched.

Universe: union of opportunities + master-ranker + IR soldiers +
insider names (~300-500). 7-day per-name cache via prev output keeps
steady state to a handful of calls. Reads:
  BUYBACK_HEAVY  yield>=2% and shares shrinking
  SHRINKING      sh_yoy <= -1%
  NEUTRAL
  DILUTING       sh_yoy >= +2% or issuance >= 2% of mcap
  HEAVY_DILUTION sh_yoy >= +5%
"""
import json
import os
import time
import urllib.request
from datetime import datetime, timezone

import boto3

S3 = boto3.client("s3", region_name="us-east-1")
BUCKET = "justhodl-dashboard-live"
OUT_KEY = "data/share-flows.json"
VERSION = "1.0.0"
FMP = (os.environ.get("FMP_API_KEY") or os.environ.get("FMP_KEY")
       or "")
MAX_FRESH_FETCH = 420          # per-run new-name budget
CACHE_DAYS = 7


def _http(url, timeout=25, tries=2):
    for i in range(tries):
        try:
            req = urllib.request.Request(
                url, headers={"User-Agent": "justhodl-share-flows"})
            with urllib.request.urlopen(req, timeout=timeout) as r:
                return json.loads(r.read().decode())
        except Exception:
            if i == tries - 1:
                return None
            time.sleep(0.6)
    return None


def s3_json(key):
    try:
        return json.loads(S3.get_object(
            Bucket=BUCKET, Key=key)["Body"].read())
    except Exception:
        return None


def build_universe(warns):
    u = set()
    opp = s3_json("data/opportunities.json") or {}
    for r in (opp.get("opportunities") or opp.get("rows") or []):
        t = r.get("ticker") or r.get("symbol")
        if t:
            u.add(t)
    mr = s3_json("data/master-ranker.json") or {}
    for r in (mr.get("rows") or mr.get("rankings") or [])[:250]:
        t = r.get("ticker") or r.get("symbol")
        if t:
            u.add(t)
    ir = s3_json("data/industry-rotation.json") or {}
    for l in (ir.get("leaders") or []):
        for h in (l.get("holdings_top") or []):
            if h.get("ticker"):
                u.add(h["ticker"])
    ins = s3_json("data/insider-radar.json") or {}
    for c in (ins.get("clusters") or []):
        if c.get("ticker"):
            u.add(c["ticker"])
    for b in (ins.get("latest_buys") or [])[:80]:
        if b.get("ticker"):
            u.add(b["ticker"])
    u = {t for t in u if t and t.isupper() and len(t) <= 6
         and "." not in t and "-" not in t}
    warns.append("universe: %d names (opp+ranker+soldiers+insider)"
                 % len(u))
    return sorted(u)


def fetch_name(t):
    """FMP fills: 3 calls -> share trend, buyback/issuance TTM,
    yields. Returns dict or None (honest silence)."""
    cf = _http("https://financialmodelingprep.com/stable/"
               "cash-flow-statement?symbol=%s&period=quarter"
               "&limit=5&apikey=%s" % (t, FMP)) or []
    inc = _http("https://financialmodelingprep.com/stable/"
                "income-statement?symbol=%s&period=quarter"
                "&limit=6&apikey=%s" % (t, FMP)) or []
    q = _http("https://financialmodelingprep.com/stable/"
              "quote?symbol=%s&apikey=%s" % (t, FMP))
    if isinstance(q, list) and q:
        q = q[0]
    if not isinstance(q, dict):
        q = {}
    out = {}
    if isinstance(cf, list) and len(cf) >= 4:
        rep = [c.get("commonStockRepurchased") or 0 for c in cf[:4]]
        iss = [c.get("commonStockIssued") or 0 for c in cf[:4]]
        out["buyback_ttm_usd"] = round(-sum(x for x in rep
                                            if x < 0))
        out["issuance_ttm_usd"] = round(sum(x for x in iss
                                            if x > 0))
    sh = [r.get("weightedAverageShsOutDil")
          or r.get("weightedAverageShsOut")
          for r in inc if isinstance(r, dict)]
    sh = [x for x in sh if x]
    if len(sh) >= 2 and sh[1]:
        out["sh_qoq_pct"] = round((sh[0] / sh[1] - 1) * 100, 2)
    if len(sh) >= 5 and sh[4]:
        out["sh_yoy_pct"] = round((sh[0] / sh[4] - 1) * 100, 2)
    mcap = q.get("marketCap")
    if not mcap and q.get("price") and q.get("sharesOutstanding"):
        mcap = q["price"] * q["sharesOutstanding"]
    if mcap:
        if out.get("buyback_ttm_usd"):
            out["buyback_yield_pct"] = round(
                out["buyback_ttm_usd"] / mcap * 100, 2)
        if out.get("issuance_ttm_usd"):
            out["issuance_pct_mcap"] = round(
                out["issuance_ttm_usd"] / mcap * 100, 2)
    return out or None


def classify(d):
    yoy = d.get("sh_yoy_pct")
    by = d.get("buyback_yield_pct") or 0
    ipm = d.get("issuance_pct_mcap") or 0
    if yoy is not None and yoy >= 5:
        return "HEAVY_DILUTION"
    if (yoy is not None and yoy >= 2) or ipm >= 2:
        return "DILUTING"
    if by >= 2 and (yoy is None or yoy < 0.5):
        return "BUYBACK_HEAVY"
    if yoy is not None and yoy <= -1:
        return "SHRINKING"
    return "NEUTRAL"


def lambda_handler(event=None, context=None):
    warns = []
    t0 = time.time()
    prev = s3_json(OUT_KEY) or {}
    prev_t = prev.get("tickers") or {}
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    # insider joins (composed, not re-fetched)
    ins = s3_json("data/insider-radar.json") or {}
    ibuy = {}
    for c in (ins.get("clusters") or []):
        t = c.get("ticker")
        if t:
            ibuy[t] = {"insider_buy_usd_90d":
                       round(c.get("total_value") or 0),
                       "insider_n_buyers": c.get("n_insiders")}
    for b in (ins.get("latest_buys") or []):
        t = b.get("ticker")
        if t and t not in ibuy:
            ibuy[t] = {"insider_buy_usd_90d":
                       round(b.get("value") or 0) or None,
                       "insider_n_buyers": 1}
    isell = {}
    for key in ("data/insider-sell-clusters.json",
                "data/insider-sells.json",
                "data/insider-sell-cluster.json"):
        doc = s3_json(key)
        if not doc:
            continue
        for c in (doc.get("clusters") or doc.get("rows")
                  or doc.get("sells") or []):
            t = c.get("ticker")
            if t:
                isell[t] = {"insider_sell_usd_recent":
                            round(c.get("total_value")
                                  or c.get("value") or 0),
                            "insider_n_sellers":
                            c.get("n_insiders") or c.get("n")}
        warns.append("sell feed joined: %s (%d names)"
                     % (key, len(isell)))
        break
    if not isell:
        warns.append("no insider-sell doc found -- sells omitted "
                     "honestly")

    tickers = {}
    fresh_used = 0
    for t in build_universe(warns):
        cached = prev_t.get(t)
        keep = False
        if cached and cached.get("as_of"):
            try:
                age = (datetime.fromisoformat(today)
                       - datetime.fromisoformat(
                           cached["as_of"])).days
                keep = age <= CACHE_DAYS
            except Exception:
                pass
        if keep:
            d = {k: v for k, v in cached.items()
                 if not k.startswith("insider")}
        else:
            if fresh_used >= MAX_FRESH_FETCH \
                    or time.time() - t0 > 560:
                if cached:
                    d = {k: v for k, v in cached.items()
                         if not k.startswith("insider")}
                else:
                    continue
            else:
                d = fetch_name(t)
                fresh_used += 1
                if not d:
                    continue
                d["as_of"] = today
                time.sleep(0.04)
        d.update(ibuy.get(t) or {})
        d.update(isell.get(t) or {})
        d["read"] = classify(d)
        tickers[t] = d

    rows = [dict(ticker=t, **v) for t, v in tickers.items()]
    top_bb = sorted([r for r in rows
                     if r.get("buyback_yield_pct")],
                    key=lambda r: -r["buyback_yield_pct"])[:20]
    top_dil = sorted([r for r in rows
                      if (r.get("sh_yoy_pct") or 0) >= 2],
                     key=lambda r: -(r.get("sh_yoy_pct") or 0))[:20]
    conv = sorted([r for r in rows
                   if r.get("insider_buy_usd_90d")
                   and r["read"] in ("BUYBACK_HEAVY", "SHRINKING")],
                  key=lambda r: -r["insider_buy_usd_90d"])[:15]

    out = {
        "version": VERSION,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "n_tickers": len(tickers),
        "fresh_fetched": fresh_used,
        "tickers": tickers,
        "boards": {"top_buybacks": top_bb,
                   "top_diluters": top_dil,
                   "insider_conviction": conv},
        "method": {
            "sh_yoy_pct": "diluted weighted-average share count, "
                          "latest quarter vs a year ago -- negative "
                          "= the company is shrinking the float "
                          "(buybacks retiring shares), positive = "
                          "dilution (issuance, SBC, offerings)",
            "buyback_yield_pct": "trailing-12M cash spent "
                                 "repurchasing stock / market cap "
                                 "-- 2%+ is a heavy repurchaser",
            "issuance_pct_mcap": "TTM stock issued / market cap -- "
                                 "2%+ meaningfully dilutive",
            "insider": "Form-4 open-market activity from the "
                       "insider desk: management buying with their "
                       "own money vs clustered selling",
            "reads": "BUYBACK_HEAVY / SHRINKING / NEUTRAL / "
                     "DILUTING / HEAVY_DILUTION"},
        "warns": warns[:12],
    }
    S3.put_object(Bucket=BUCKET, Key=OUT_KEY,
                  Body=json.dumps(out, default=str).encode(),
                  ContentType="application/json",
                  CacheControl="max-age=900")
    return {"ok": True, "n": len(tickers), "fresh": fresh_used}
