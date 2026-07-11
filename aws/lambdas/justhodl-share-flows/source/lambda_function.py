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
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone

import boto3

S3 = boto3.client("s3", region_name="us-east-1")
BUCKET = "justhodl-dashboard-live"
OUT_KEY = "data/share-flows.json"
VERSION = "1.2.1"
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
    # phase-detector is the widest curated fleet universe (~683
    # dollar-volume-ranked common stocks) -- primary source
    ph = s3_json("data/phase-detector.json") or {}
    u.update((ph.get("tickers") or {}).keys())
    # generic ticker harvest from the richer insider docs
    for key in ("data/insider-clusters.json",
                "data/insider-buys-enriched.json"):
        doc = s3_json(key) or {}
        for lk in ("clusters", "rows", "buys", "strong"):
            for r in (doc.get(lk) or []):
                if isinstance(r, dict) and r.get("ticker"):
                    u.add(r["ticker"])
    opp = s3_json("data/opportunities.json") or {}
    for r in (opp.get("all") or opp.get("opportunities")
              or opp.get("rows") or []):
        t = (r.get("ticker") or r.get("symbol")) \
            if isinstance(r, dict) else (r if isinstance(r, str)
                                         else None)
        if t:
            u.add(t)
    mr = s3_json("data/master-ranker.json") or {}
    for r in (mr.get("top_tickers") or mr.get("rows")
              or mr.get("rankings") or [])[:300]:
        t = (r.get("ticker") or r.get("symbol")) \
            if isinstance(r, dict) else (r if isinstance(r, str)
                                         else None)
        if t:
            u.add(t)
    ir = s3_json("data/industry-rotation.json") or {}
    for l in (ir.get("leaders") or []):
        for h in (l.get("holdings_top") or []):
            if h.get("ticker"):
                u.add(h["ticker"])
    pd = s3_json("data/phase-detector.json") or {}
    for t in (pd.get("tickers") or {}):
        u.add(t)
    sv = s3_json("data/stock-valuations.json") or {}
    hm = sv.get("heatmap") or {}
    for grp in (hm.get("sp") or {}, hm.get("hp") or {}):
        if isinstance(grp, dict):
            for t in grp:
                u.add(t)
    ins = s3_json("data/insider-radar.json") or {}
    for c in (ins.get("clusters") or []):
        if c.get("ticker"):
            u.add(c["ticker"])
    for b in (ins.get("latest_buys") or [])[:80]:
        if b.get("ticker"):
            u.add(b["ticker"])
    u = {t for t in u if t and t.isupper() and len(t) <= 6
         and "." not in t and "-" not in t}
    warns.append("universe: %d names (opp+ranker+soldiers+insider+phase-ring+valuations)"
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
                "&limit=13&apikey=%s" % (t, FMP)) or []
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
        out["buyback_net_ttm_usd"] = (out["buyback_ttm_usd"]
                                      - out["issuance_ttm_usd"])
        sbc = [c.get("stockBasedCompensation") or 0 for c in cf[:4]]
        out["sbc_ttm_usd"] = round(sum(abs(x) for x in sbc))
        dv = [c.get("netDividendsPaid")
              or c.get("dividendsPaid") or 0 for c in cf[:4]]
        out["div_ttm_usd"] = round(sum(abs(x) for x in dv if x < 0)
                                   or sum(abs(x) for x in dv))
    sh = [r.get("weightedAverageShsOutDil")
          or r.get("weightedAverageShsOut")
          for r in inc if isinstance(r, dict)]
    sh = [x for x in sh if x]
    if len(sh) >= 2 and sh[1]:
        out["sh_qoq_pct"] = round((sh[0] / sh[1] - 1) * 100, 2)
    if len(sh) >= 5 and sh[4]:
        out["sh_yoy_pct"] = round((sh[0] / sh[4] - 1) * 100, 2)
    if len(sh) >= 13 and sh[12] and sh[12] > 0:
        out["sh_3y_cagr_pct"] = round(
            ((sh[0] / sh[12]) ** (1.0 / 3) - 1) * 100, 2)
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
        if out.get("buyback_net_ttm_usd") is not None:
            out["buyback_net_yield_pct"] = round(
                out["buyback_net_ttm_usd"] / mcap * 100, 2)
        if out.get("sbc_ttm_usd"):
            out["sbc_pct_mcap"] = round(
                out["sbc_ttm_usd"] / mcap * 100, 2)
        if out.get("buyback_net_ttm_usd") is not None \
                or out.get("div_ttm_usd"):
            out["total_shareholder_yield_pct"] = round(
                ((out.get("buyback_net_ttm_usd") or 0)
                 + (out.get("div_ttm_usd") or 0)) / mcap * 100, 2)
    return out or None


def classify(d):
    yoy = d.get("sh_yoy_pct")
    by = d.get("buyback_yield_pct") or 0
    ipm = d.get("issuance_pct_mcap") or 0
    if (yoy is not None and abs(yoy) >= 80) or by >= 30:
        # death-spiral issuance / reverse-split artifact / tender:
        # real but out-of-band -- carry an explicit extreme flag
        d["extreme"] = True
    if yoy is not None and yoy >= 80:
        return "EXTREME_DILUTION"
    if yoy is not None and yoy >= 5:
        return "HEAVY_DILUTION"
    if (yoy is not None and yoy >= 2) or ipm >= 2:
        return "DILUTING"
    nby = d.get("buyback_net_yield_pct")
    if (nby if nby is not None else by) >= 2 \
            and (yoy is None or yoy < 0.5):
        return "BUYBACK_HEAVY"
    if yoy is not None and yoy <= -1:
        return "SHRINKING"
    return "NEUTRAL"


def flag_row(d):
    """Forensic flags -- from fields already on the row."""
    f = []
    gross = d.get("buyback_yield_pct") or 0
    net = d.get("buyback_net_yield_pct")
    yoy = d.get("sh_yoy_pct")
    if gross >= 1 and net is not None and net < gross * 0.4 \
            and (yoy is None or yoy > -0.5):
        f.append("SBC_WASH")
    if (d.get("sbc_pct_mcap") or 0) >= 3:
        f.append("HEAVY_SBC")
    if gross >= 1.5 and (d.get("insider_sell_usd_recent") or 0) \
            >= 2_000_000 and not d.get("insider_buy_usd_90d"):
        f.append("MGMT_SELLING_INTO_BUYBACK")
    if (d.get("insider_buy_usd_90d") or 0) >= 1_000_000 \
            and (d.get("insider_n_buyers") or 0) >= 2:
        f.append("INSIDER_CONVICTION")
    if d.get("read") in ("DILUTING", "HEAVY_DILUTION") \
            and (d.get("insider_buy_usd_90d") or 0) >= 250_000:
        f.append("MGMT_BUYING_DESPITE_DILUTION")
    return f


def lambda_handler(event=None, context=None):
    warns = []
    t0 = time.time()
    prev = s3_json(OUT_KEY) or {}
    prev_t = prev.get("tickers") or {}
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    # insider joins (composed, not re-fetched).
    # justhodl-insider-trades emits the raw Form-4 tape with
    # side=buy/sell -- the fleet's real management buy/sell truth.
    ibuy, isell = {}, {}
    itr = s3_json("data/insider-trades.json") or {}
    for c in (itr.get("clusters") or []):
        t = c.get("ticker")
        if t:
            ibuy[t] = {"insider_buy_usd_90d":
                       round(c.get("total_value") or 0),
                       "insider_n_buyers":
                       c.get("insider_count")
                       or c.get("n_insiders")}
    bsum, bwho, ssum, swho = {}, {}, {}, {}
    for x in ((itr.get("transactions") or [])
              + (itr.get("sell_transactions") or [])):
        t = x.get("ticker")
        if not t or not isinstance(x, dict):
            continue
        v = x.get("value") or 0
        who = x.get("insider") or x.get("name") or ""
        _sd = str(x.get("side") or x.get("type")
                  or x.get("transactionType")
                  or x.get("acquisitionOrDisposition")
                  or "").strip().lower()
        if _sd in ("sell", "s", "sale", "d", "disposition",
                   "s-sale"):
            ssum[t] = ssum.get(t, 0) + v
            swho.setdefault(t, set()).add(who)
        elif _sd in ("buy", "b", "p", "purchase", "a",
                     "acquisition", "p-purchase"):
            bsum[t] = bsum.get(t, 0) + v
            bwho.setdefault(t, set()).add(who)
    for t, v in bsum.items():
        if t not in ibuy and v:
            ibuy[t] = {"insider_buy_usd_90d": round(v),
                       "insider_n_buyers": len(bwho.get(t) or ())}
    for t, v in ssum.items():
        if v:
            isell[t] = {"insider_sell_usd_recent": round(v),
                        "insider_n_sellers": len(swho.get(t)
                                                 or ())}
    if not ibuy:  # fallback: legacy radar doc
        ins = s3_json("data/insider-radar.json") or {}
        for c in (ins.get("clusters") or []):
            t = c.get("ticker")
            if t:
                ibuy[t] = {"insider_buy_usd_90d":
                           round(c.get("total_value") or 0),
                           "insider_n_buyers": c.get("n_insiders")}
    warns.append("insider join: %d buy names / %d sell names "
                 "(insider-trades tape)" % (len(ibuy), len(isell)))
    if not isell:
        warns.append("no sell rows in tape -- sells omitted "
                     "honestly")

    universe = build_universe(warns)
    cached_ok, need = {}, []
    for t in universe:
        cached = prev_t.get(t)
        keep = False
        if cached and cached.get("as_of"):
            try:
                age = (datetime.fromisoformat(today)
                       - datetime.fromisoformat(
                           cached["as_of"])).days
                keep = (age <= CACHE_DAYS
                        and "buyback_net_ttm_usd" in cached)
            except Exception:
                pass
        (cached_ok.__setitem__(t, cached) if keep
         else need.append(t))

    # threaded fresh fetch -- the sequential 3-call crawl only
    # reached ~65 names inside the time guard (3095 lesson #2)
    fresh = {}
    deadline = t0 + 760
    with ThreadPoolExecutor(max_workers=10) as exe:
        futs = {exe.submit(fetch_name, t): t
                for t in need[:MAX_FRESH_FETCH]}
        for fu in as_completed(futs):
            t2 = futs[fu]
            try:
                r2 = fu.result()
            except Exception:
                r2 = None
            if r2:
                r2["as_of"] = today
                fresh[t2] = r2
            if time.time() > deadline:
                warns.append("deadline at %d fresh" % len(fresh))
                break
    fresh_used = len(fresh)
    warns.append("fresh %d / needed %d (budget %d)"
                 % (fresh_used, len(need), MAX_FRESH_FETCH))

    tickers = {}
    for t in universe:
        if t in fresh:
            d = fresh[t]
        elif t in cached_ok:
            d = {k: v for k, v in cached_ok[t].items()
                 if not k.startswith("insider")}
        elif prev_t.get(t):
            d = {k: v for k, v in prev_t[t].items()
                 if not k.startswith("insider")}
        else:
            continue
        d.update(ibuy.get(t) or {})
        d.update(isell.get(t) or {})
        # split/adjustment-mismatch guard: quarterly weighted-avg
        # share counts across a reverse split produce impossible
        # jumps -- keep the real number (never fake data) but flag
        # it so pages and boards can exclude it honestly
        if abs(d.get("sh_yoy_pct") or 0) > 80 \
                or abs(d.get("sh_qoq_pct") or 0) > 40:
            d["data_suspect"] = True
        d["read"] = classify(d)
        fl = flag_row(d)
        if fl:
            d["flags"] = fl
        tickers[t] = d

    rows = [dict(ticker=t, **v) for t, v in tickers.items()
            if not v.get("data_suspect")]
    top_bb = sorted([r for r in rows
                     if r.get("buyback_yield_pct")
                     and not r.get("extreme")],
                    key=lambda r: -r["buyback_yield_pct"])[:20]
    top_dil = sorted([r for r in rows
                      if (r.get("sh_yoy_pct") or 0) >= 2
                      and not r.get("extreme")],
                     key=lambda r: -(r.get("sh_yoy_pct") or 0))[:20]
    extremes = sorted([r for r in rows if r.get("extreme")],
                      key=lambda r: -abs(r.get("sh_yoy_pct")
                                         or 0))[:20]
    conv = sorted([r for r in rows
                   if r.get("insider_buy_usd_90d")
                   and r["read"] in ("BUYBACK_HEAVY", "SHRINKING")],
                  key=lambda r: -r["insider_buy_usd_90d"])[:15]
    washers = sorted([r for r in rows
                      if "SBC_WASH" in (r.get("flags") or [])
                      and not r.get("extreme")],
                     key=lambda r: -(r.get("buyback_yield_pct")
                                     or 0))[:20]
    selling = sorted([r for r in rows
                      if "MGMT_SELLING_INTO_BUYBACK"
                      in (r.get("flags") or [])],
                     key=lambda r: -(r.get("insider_sell_usd_recent")
                                     or 0))[:20]

    out = {
        "version": VERSION,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "n_tickers": len(tickers),
        "fresh_fetched": fresh_used,
        "tickers": tickers,
        "boards": {"top_buybacks": top_bb,
                   "sbc_washers": washers,
                   "mgmt_selling_into_buyback": selling,
                   "top_diluters": top_dil,
                   "extreme_diluters": extremes,
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
            "buyback_net_yield_pct": "NET buyback (TTM repurchases"
                                     " minus TTM issuance) / mcap --"
                                     " gross spend that only mops up"
                                     " stock-comp issuance is flagged"
                                     " SBC_WASH",
            "sbc_pct_mcap": "TTM stock-based compensation / mcap --"
                            " the silent annual dilution tax; >=3%"
                            " flagged HEAVY_SBC",
            "total_shareholder_yield_pct": "(net buyback + dividends)"
                                           " / mcap",
            "sh_3y_cagr_pct": "3-year CAGR of the diluted share"
                              " count",
            "flags": "SBC_WASH / HEAVY_SBC /"
                     " MGMT_SELLING_INTO_BUYBACK (company buys,"
                     " insiders dump >=$2M) / INSIDER_CONVICTION"
                     " (2+ insiders, >=$1M open-market) /"
                     " MGMT_BUYING_DESPITE_DILUTION",
            "insider": "Form-4 open-market activity from the "
                       "insider desk: management buying with their "
                       "own money vs clustered selling",
            "reads": "BUYBACK_HEAVY / SHRINKING / NEUTRAL / "
                     "DILUTING / HEAVY_DILUTION / "
                     "EXTREME_DILUTION (death-spiral"
                     " issuance, extreme flag)"},
        "warns": warns[:12],
    }
    S3.put_object(Bucket=BUCKET, Key=OUT_KEY,
                  Body=json.dumps(out, default=str).encode(),
                  ContentType="application/json",
                  CacheControl="max-age=900")
    return {"ok": True, "n": len(tickers), "fresh": fresh_used}
