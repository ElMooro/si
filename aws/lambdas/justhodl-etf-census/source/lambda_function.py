"""justhodl-etf-census v1.0.0 (ops 3556) — the Fundamental Census's
capabilities translated to ETFs. Universe = the fleet's flow feeds
(etf-flows/daily.json metrics container, fallback legacy
data/etf-flows.json by_etf) ∪ curated core wrappers. Per ETF:
FMP /stable etf/info (AUM, expense, NAV, asset class) + price series
(windowed stitch) → the shared census kernel (momentum, 52w
distances, RSI, vol, beta vs SPY, golden cross, breakout, double
top/bottom with neckline+extremeness) → tech_score / risk_score /
flow columns → columnar matrix data/etf-census-matrix.json (same
shape as the flagship: tickers/categories/cols) + boards doc
data/etf-census.json. Leveraged/inverse decay-careful board built
from name/class detection. Real data only; self-probing readers print
the flow-record vocabulary they found.
"""
import json
import time
import urllib.request
from datetime import datetime, timezone

import boto3

import sys as _sys
_sys.path.insert(0, "/var/task")
from census_lib import (tech_series, beta_vs, momentum, mom_12_1,
                        cross_pct)

VERSION = "1.0.1"
BUCKET = "justhodl-dashboard-live"
S3 = boto3.client("s3", region_name="us-east-1")
FMP_KEY = __import__("os").environ.get(
    "FMP_API_KEY", "wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb")
MX_KEY = "data/etf-census-matrix.json"
DOC_KEY = "data/etf-census.json"
CORE = ["SPY", "IVV", "VOO", "QQQ", "IWM", "DIA", "VTI", "RSP",
        "XLK", "XLF", "XLE", "XLV", "XLI", "XLY", "XLP", "XLU",
        "XLB", "XLRE", "XLC", "SMH", "SOXX", "XBI", "KRE", "XOP",
        "GDX", "GLD", "SLV", "USO", "UNG", "DBC",
        "EEM", "EFA", "VEA", "VWO", "FXI", "EWJ", "EWZ", "INDA",
        "ARKK", "MTUM", "QUAL", "USMV", "VLUE", "IWF", "IWD",
        "HYG", "JNK", "LQD", "AGG", "TLT", "IEF", "SHY", "TIP",
        "BIL", "EMB", "MUB", "BKLN",
        "TQQQ", "SQQQ", "SPXL", "SPXS", "SOXL", "SOXS", "UVXY",
        "BITO", "IBIT", "ETHA"]
LEV_PAT = ("2X", "3X", "-2X", "-3X", "ULTRA", "BULL", "BEAR",
           "INVERSE", "SHORT", "DAILY")


def _get(url, timeout=40):
    req = urllib.request.Request(url, headers={"User-Agent":
                                               "justhodl-etf-census"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.loads(r.read())


def fmp(qs):
    return _get("https://financialmodelingprep.com/stable/" + qs
                + ("&" if "?" in qs else "?") + "apikey=" + FMP_KEY)


def s3json(key):
    try:
        return json.loads(S3.get_object(Bucket=BUCKET,
                                        Key=key)["Body"].read())
    except Exception:  # noqa: BLE001
        return None


def flow_records():
    """Tolerant reader over the fleet's flow feeds. Returns
    {TICKER: {category, aum_b, flow fields...}} + the vocabulary."""
    out, vocab = {}, set()
    d = s3json("etf-flows/daily.json") or {}
    m = d.get("metrics")
    if isinstance(m, dict) and m:
        for t, r in m.items():
            if isinstance(r, dict):
                out[str(t).upper()] = dict(r)
                vocab |= set(r.keys())
    ptc = s3json("etf-flows/per-ticker-context.json") or {}
    for cand in (ptc, ptc.get("tickers"), ptc.get("by_ticker"),
                 ptc.get("context")):
        if isinstance(cand, dict) and len(cand) > 50:
            for t, r in cand.items():
                if isinstance(r, dict) and str(t).isupper() \
                        and len(str(t)) <= 6:
                    rec = out.setdefault(str(t).upper(), {})
                    for k, v in r.items():
                        rec.setdefault(k, v)
                    vocab |= set(r.keys())
            break
    legacy = (s3json("data/etf-flows.json") or {}).get("by_etf") or {}
    for t, r in legacy.items():
        if isinstance(r, dict):
            rec = out.setdefault(str(t).upper(), {})
            for k, v in r.items():
                rec.setdefault(k, v)
            vocab |= set(r.keys())
    print(f"[etf-census] flow records={len(out)} "
          f"vocab={sorted(vocab)[:30]}")
    return out


def price_weekly(sym):
    """Daily closes via FMP light (5000-row cap ⇒ 2006+), resampled
    to Friday-ish weekly closes [[date, close], ...]."""
    try:
        px = fmp(f"historical-price-eod/light?symbol={sym}"
                 "&from=2004-01-01")
        if isinstance(px, dict):
            px = px.get("historical") or []
        rows = sorted(((str(x.get("date"))[:10],
                        x.get("price") or x.get("close"))
                       for x in px if x.get("date")),
                      key=lambda z: z[0])
        rows = [(d, float(v)) for d, v in rows
                if isinstance(v, (int, float)) and v > 0]
        wk, cur = [], None
        for d, v in rows:
            iso = datetime.strptime(d, "%Y-%m-%d").isocalendar()[:2]
            if cur != iso:
                wk.append([d, v]); cur = iso
            else:
                wk[-1] = [d, v]
        return wk
    except Exception as e:  # noqa: BLE001
        print(f"[px] {sym}: {str(e)[:60]}")
        return []


def lambda_handler(event, context):
    event = event or {}
    t0 = time.time()
    flows = flow_records()
    uni = sorted(set(list(flows.keys()) + CORE))
    if event.get("limit"):
        uni = uni[: int(event["limit"])]
    spx_rows = price_weekly("SPY")
    spx_map = {d: v for d, v in spx_rows}

    rows = {}
    n_info = 0
    for i, sym in enumerate(uni):
        rec = {"t": sym}
        fr = flows.get(sym) or {}
        rec["category"] = (fr.get("category") or fr.get("class")
                           or "UNCLASSIFIED")
        rec["name"] = fr.get("name")
        try:
            info = fmp(f"etf/info?symbol={sym}")
            i0 = info[0] if isinstance(info, list) and info else {}
            if i0:
                n_info += 1
            rec["asset_class"] = i0.get("assetClass") or rec["category"]
            aum = i0.get("assetsUnderManagement")
            rec["aum_usd_m"] = round(aum / 1e6, 1) if \
                isinstance(aum, (int, float)) and aum > 0 else None
            er = i0.get("expenseRatio")
            rec["expense_pct"] = round(er * 100, 2) if \
                isinstance(er, (int, float)) and er < 1 else \
                (round(er, 2) if isinstance(er, (int, float)) else None)
            rec["nav"] = i0.get("nav")
            av = i0.get("avgVolume")
            rec["avg_volume"] = av if isinstance(av, (int, float)) \
                else None
            rec["holdings_n"] = i0.get("holdingsCount")
            nm = (i0.get("name") or rec.get("name") or "").upper()
        except Exception as e:  # noqa: BLE001
            print(f"[info] {sym}: {str(e)[:60]}")
            nm = (rec.get("name") or "").upper()
        rec["leveraged"] = 1 if any(p in nm for p in LEV_PAT) else 0
        if rec.get("aum_usd_m") is None and \
                isinstance(fr.get("aum_b"), (int, float)):
            rec["aum_usd_m"] = round(fr["aum_b"] * 1000, 1)

        wk = price_weekly(sym)
        lv = {"mom_6m_pct": momentum(wk, 26),
              "mom_12_1_pct": mom_12_1(wk)}
        lv.update(tech_series(wk))
        b = beta_vs(wk, spx_map)
        if b is not None:
            lv["beta_spy"] = b
        # flow columns (tolerant): any numeric field mentioning flow /
        # dvol / return copied through with a normalized name
        for k, v in fr.items():
            if not isinstance(v, (int, float)):
                continue
            lk = k.lower()
            if any(w in lk for w in ("flow", "dvol", "return_",
                                     "creation", "redeem")):
                lv["f_" + lk] = round(v, 3)
        rec["_lv"] = lv
        rows[sym] = rec
        if i % 40 == 0:
            print(f"[etf-census] {i}/{len(uni)} {sym} "
                  f"({time.time()-t0:.0f}s)")
        time.sleep(0.12)

    # ── matrix ──
    tickers = sorted(rows.keys())
    n = len(tickers)
    cats = [rows[t]["category"] for t in tickers]
    aclass = [rows[t].get("asset_class") for t in tickers]
    names = [rows[t].get("name") for t in tickers]
    lev = [rows[t].get("leveraged", 0) for t in tickers]
    allk = sorted({k for t in tickers for k in rows[t]["_lv"]})
    cols = {}
    for k in allk:
        col = [rows[t]["_lv"].get(k) for t in tickers]
        if sum(1 for v in col if v is not None) >= max(4, int(n * 0.15)):
            cols[k] = [round(v, 3) if isinstance(v, float) else v
                       for v in col]
    for k in ("aum_usd_m", "expense_pct", "nav", "avg_volume",
              "holdings_n"):
        cols[k] = [rows[t].get(k) for t in tickers]
    cols["leveraged"] = lev

    base = [cross_pct(cols.get(k) or [None] * n, low)
            for k, low in (("mom_12_1_pct", 0), ("mom_6m_pct", 0),
                           ("dist_52w_high_pct", 0), ("rsi_14w", 0))
            if cols.get(k)]
    ts_col = []
    for i in range(n):
        vs = [bp[i] for bp in base if bp[i] is not None]
        if len(vs) < 2:
            ts_col.append(None); continue
        v = sum(vs) / len(vs)
        v += 8 * ((cols.get("double_bottom") or [0] * n)[i] or 0)
        v += 6 * ((cols.get("breakout_20w") or [0] * n)[i] or 0)
        v += 6 * ((cols.get("golden_cross_10_40w") or [0] * n)[i] or 0)
        v -= 8 * ((cols.get("double_top") or [0] * n)[i] or 0)
        ts_col.append(round(max(0.0, min(100.0, v)), 1))
    cols["tech_score"] = ts_col
    rparts = [cross_pct(cols.get(k) or [None] * n, low)
              for k, low in (("vol_52w_pct", 0), ("beta_spy", 0),
                             ("expense_pct", 0),
                             ("dist_52w_high_pct", 1),
                             ("aum_usd_m", 1))
              if cols.get(k)]
    rk = []
    for i in range(n):
        vs = [rp[i] for rp in rparts if rp[i] is not None]
        if len(vs) < 3:
            rk.append(None); continue
        v = sum(vs) / len(vs) + (12 if lev[i] else 0)
        rk.append(round(max(0.0, min(100.0, v)), 1))
    cols["risk_score"] = rk

    now = datetime.now(timezone.utc).isoformat()
    mx = {"generated_at": now, "version": VERSION, "n": n,
          "tickers": tickers, "categories": cats,
          "asset_classes": aclass, "names": names,
          "metrics": sorted(cols.keys()), "cols": cols}
    S3.put_object(Bucket=BUCKET, Key=MX_KEY,
                  Body=json.dumps(mx, default=str).encode(),
                  ContentType="application/json",
                  CacheControl="no-cache")

    def tops(k, nn=10, rev=True):
        pr = [(tickers[i], v) for i, v in enumerate(cols.get(k) or [])
              if isinstance(v, (int, float))]
        pr.sort(key=lambda x: -x[1] if rev else x[1])
        return pr[:nn]
    doc = {"generated_at": now, "version": VERSION, "n": n,
           "boards": {
               "tech_leaders": tops("tech_score"),
               "momentum_12_1": tops("mom_12_1_pct"),
               "double_bottoms": [tickers[i] for i, v in
                                  enumerate(cols.get("double_bottom")
                                            or []) if v == 1],
               "double_tops": [tickers[i] for i, v in
                               enumerate(cols.get("double_top") or [])
                               if v == 1],
               "lowest_risk": tops("risk_score", 10, rev=False),
               "decay_careful": [tickers[i] for i in range(n)
                                 if lev[i] == 1],
           },
           "coverage": {"n_info": n_info,
                        "flow_cols": [k for k in cols
                                      if k.startswith("f_")][:20]},
           "duration_s": round(time.time() - t0, 1)}
    S3.put_object(Bucket=BUCKET, Key=DOC_KEY,
                  Body=json.dumps(doc, default=str).encode(),
                  ContentType="application/json",
                  CacheControl="no-cache")
    print(f"[etf-census] DONE n={n} info={n_info} "
          f"{doc['duration_s']}s")
    return {"ok": True, "n": n}
