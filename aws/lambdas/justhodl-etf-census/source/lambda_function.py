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

VERSION = "1.2.0"
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


def _wret(wk, n=None):
    px = [v for _, v in wk]
    if n:
        px = px[-(n + 1):]
    return [px[i] / px[i - 1] - 1 for i in range(1, len(px))]


def rs_corr_capture(wk, spy_wk):
    """13w relative strength, 52w correlation, up/down capture vs
    SPY on date-matched weekly closes."""
    sm = dict(spy_wk)
    pairs = [(v, sm[d]) for d, v in wk if d in sm]
    out = {}
    if len(pairs) >= 15:
        a13 = pairs[-14:]
        r_s = a13[-1][0] / a13[0][0] - 1
        r_m = a13[-1][1] / a13[0][1] - 1
        out["rs_13w_pct"] = round((r_s - r_m) * 100, 2)
    if len(pairs) >= 40:
        tail = pairs[-53:]
        ra = [tail[i][0] / tail[i-1][0] - 1 for i in range(1, len(tail))]
        rb = [tail[i][1] / tail[i-1][1] - 1 for i in range(1, len(tail))]
        ma = sum(ra) / len(ra); mb = sum(rb) / len(rb)
        cov = sum((ra[i]-ma)*(rb[i]-mb) for i in range(len(ra)))
        va = sum((x-ma)**2 for x in ra); vb = sum((x-mb)**2 for x in rb)
        if va > 0 and vb > 0:
            out["corr_spy_52w"] = round(cov / (va**.5 * vb**.5), 2)
        ups = [(ra[i], rb[i]) for i in range(len(rb)) if rb[i] > 0]
        dns = [(ra[i], rb[i]) for i in range(len(rb)) if rb[i] < 0]
        if len(ups) >= 8:
            out["up_capture_pct"] = round(100 * (sum(u[0] for u in ups)
                                          / sum(u[1] for u in ups)), 1)
        if len(dns) >= 8:
            out["down_capture_pct"] = round(100 * (sum(d[0] for d in dns)
                                            / sum(d[1] for d in dns)), 1)
    return out


def ratio_z(wk_a, wk_b, look=156):
    ma, mb = dict(wk_a), dict(wk_b)
    dates = sorted(set(ma) & set(mb))[-look:]
    if len(dates) < 60:
        return None
    r = [ma[d] / mb[d] for d in dates]
    mu = sum(r) / len(r)
    sd = (sum((x - mu) ** 2 for x in r) / (len(r) - 1)) ** 0.5
    return {"z": round((r[-1] - mu) / sd, 2) if sd > 0 else None,
            "ratio": round(r[-1], 4), "n_weeks": len(dates)}


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
    wk_map = {}
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
        lv.update(rs_corr_capture(wk, spx_rows))
        if wk and isinstance(rec.get("nav"), (int, float)) \
                and rec["nav"] > 0:
            lv["prem_disc_pct"] = round(
                (wk[-1][1] / rec["nav"] - 1) * 100, 2)
        if wk and isinstance(rec.get("avg_volume"), (int, float)):
            lv["dollar_vol_usd_m"] = round(
                rec["avg_volume"] * wk[-1][1] / 1e6, 1)
        wk_map[sym] = wk
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
    # variance-drag estimate for leveraged wrappers:
    # drag ≈ 0.5·(L²−L)·σ_SPY² annualized (weekly σ from SPY 52w)
    spy_r = _wret(spx_rows, 52)
    if len(spy_r) >= 40:
        mu = sum(spy_r) / len(spy_r)
        var_w = sum((x - mu) ** 2 for x in spy_r) / (len(spy_r) - 1)
        var_a = var_w * 52
        drag = []
        for i in range(n):
            if lev[i] == 1 and isinstance(
                    (cols.get("beta_spy") or [None]*n)[i],
                    (int, float)):
                L = abs(round(cols["beta_spy"][i]))
                drag.append(round(50 * (L * L - L) * var_a, 2)
                            if L >= 2 else None)
            else:
                drag.append(None)
        if any(v is not None for v in drag):
            cols["variance_drag_pct_ann"] = drag

    def g(k, i):
        v = (cols.get(k) or [None] * n)[i]
        return v if isinstance(v, (int, float)) else None
    ups, dns, rrs = [], [], []
    for i in range(n):
        dh = g("dist_52w_high_pct", i)
        dl = g("dist_52w_low_pct", i)
        ts2 = g("tech_score", i)
        vol = g("vol_52w_pct", i)
        rk2 = g("risk_score", i)
        if dh is None or vol is None or rk2 is None:
            ups.append(None); dns.append(None); rrs.append(None)
            continue
        up = 0.5 * max(0.0, -dh) + 0.25 * (ts2 if ts2 is not None
                                           else 50.0)
        up += 8 * (g("double_bottom", i) or 0)
        up += 6 * (g("golden_cross_10_40w", i) or 0)
        dn = 0.6 * vol * (rk2 / 100.0) + 0.2 * max(0.0, dl or 0.0)
        dn += 10 * (g("double_top", i) or 0)
        dn += (g("variance_drag_pct_ann", i) or 0)
        dn += 8 if lev[i] else 0
        dn = max(3.0, dn)
        ups.append(round(up, 1)); dns.append(round(dn, 1))
        rrs.append(round(up / dn, 2))
    cols["upside_pct"] = ups
    cols["downside_pct"] = dns
    cols["rr_ratio"] = rrs
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
           "pairs": [dict(pair=lbl, note=note,
                          **(ratio_z(wk_map.get(a) or [],
                                     wk_map.get(b) or []) or {}))
                     for lbl, a, b, note in
                     (("IWD/IWF", "IWD", "IWF", "value vs growth"),
                      ("IWM/SPY", "IWM", "SPY", "small vs large"),
                      ("SMH/SPY", "SMH", "SPY", "semis leadership"),
                      ("XLY/XLP", "XLY", "XLP",
                       "cyclicals vs defensives"),
                      ("EEM/EFA", "EEM", "EFA", "EM vs DM"),
                      ("HYG/IEF", "HYG", "IEF", "credit risk appetite"),
                      ("GLD/SPY", "GLD", "SPY", "gold vs equities"),
                      ("RSP/SPY", "RSP", "SPY", "equal-weight breadth"))
                     if wk_map.get(a) and wk_map.get(b)],
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
