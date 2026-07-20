"""justhodl-fi-census v1.0.0 (ops 3556) — the census translated to
FIXED INCOME. Curated bond-ETF universe (govt ladder, IG/HY credit,
munis, EM, TIPS, loans, agg, cash) through the shared census kernel:
momentum, 52w distances, RSI, vol, double patterns, beta_spy AND
beta_tlt (the duration proxy — TLT-beta ≈ empirical duration /
long-bond sensitivity). Plus RATES & CREDIT blocks: treasury curve
snapshot (FRED), credit OAS + sovereign spreads (data/cds-proxy.json
leaves: oas_bp / change_5d_bp / spread_vs_us_bp / status), regime.
Outputs data/fi-census-matrix.json + data/fi-census.json.
"""
import json
import os
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
FMP_KEY = os.environ.get("FMP_API_KEY",
                         "wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb")
FRED_KEY = os.environ.get("FRED_API_KEY",
                          "2f057499936072679d8843d7fce99989")
MX_KEY = "data/fi-census-matrix.json"
DOC_KEY = "data/fi-census.json"
UNIVERSE = [
    ("BIL", "CASH"), ("SHV", "CASH"), ("SGOV", "CASH"),
    ("SHY", "GOVT_SHORT"), ("VGSH", "GOVT_SHORT"),
    ("IEI", "GOVT_INTermediate".upper()), ("IEF", "GOVT_INTERMEDIATE"),
    ("VGIT", "GOVT_INTERMEDIATE"),
    ("TLH", "GOVT_LONG"), ("TLT", "GOVT_LONG"), ("EDV", "GOVT_LONG"),
    ("ZROZ", "GOVT_LONG"), ("GOVT", "GOVT_BROAD"),
    ("AGG", "AGGREGATE"), ("BND", "AGGREGATE"), ("SCHZ", "AGGREGATE"),
    ("LQD", "CORP_IG"), ("VCIT", "CORP_IG"), ("VCSH", "CORP_IG"),
    ("IGSB", "CORP_IG"), ("IGIB", "CORP_IG"),
    ("HYG", "CORP_HY"), ("JNK", "CORP_HY"), ("SHYG", "CORP_HY"),
    ("SJNK", "CORP_HY"), ("ANGL", "CORP_HY"),
    ("BKLN", "LOANS"), ("SRLN", "LOANS"),
    ("TIP", "TIPS"), ("VTIP", "TIPS"), ("SCHP", "TIPS"),
    ("STIP", "TIPS"),
    ("MUB", "MUNI"), ("HYD", "MUNI"), ("SUB", "MUNI"),
    ("EMB", "EM"), ("PCY", "EM"), ("EMLC", "EM"),
    ("MBB", "MBS"), ("VMBS", "MBS"),
    ("FLOT", "FLOATING"), ("USFR", "FLOATING"), ("TFLO", "FLOATING"),
    ("CWB", "CONVERTS"), ("PFF", "PREFERREDS"),
]


def _get(url, timeout=40):
    req = urllib.request.Request(url, headers={"User-Agent":
                                               "justhodl-fi-census"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.loads(r.read())


def fmp(qs):
    return _get("https://financialmodelingprep.com/stable/" + qs
                + ("&" if "?" in qs else "?") + "apikey=" + FMP_KEY)


def fred_latest(series):
    try:
        d = _get("https://api.stlouisfed.org/fred/series/observations"
                 f"?series_id={series}&api_key={FRED_KEY}"
                 "&file_type=json&sort_order=desc&limit=8")
        for o in d.get("observations") or []:
            try:
                return float(o["value"])
            except Exception:  # noqa: BLE001
                continue
    except Exception as e:  # noqa: BLE001
        print(f"[fred] {series}: {str(e)[:50]}")
    return None


def price_weekly(sym):
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


def s3json(key):
    try:
        return json.loads(S3.get_object(Bucket=BUCKET,
                                        Key=key)["Body"].read())
    except Exception:  # noqa: BLE001
        return None


def credit_blocks():
    c = s3json("data/cds-proxy.json") or {}
    corp = c.get("corporate") or {}
    sov = c.get("sovereigns") or {}
    out = {"regime": c.get("regime"),
           "composite_credit_risk": c.get("composite_credit_risk"),
           "us_10y_yield": c.get("us_10y_yield"),
           "corporate": {k: v for k, v in corp.items()
                         if isinstance(v, dict)},
           "sovereigns": {k: v for k, v in sov.items()
                          if isinstance(v, dict)}}
    return out


def curve_block():
    m = {k: fred_latest(s) for k, s in
         (("y3m", "DGS3MO"), ("y2", "DGS2"), ("y5", "DGS5"),
          ("y10", "DGS10"), ("y30", "DGS30"),
          ("t10y2y", "T10Y2Y"), ("t10y3m", "T10Y3M"),
          ("breakeven_5y", "T5YIE"), ("real_10y", "DFII10"))}
    if m.get("y10") is not None and m.get("y2") is not None:
        m["curve_2s10s_bp"] = round((m["y10"] - m["y2"]) * 100, 1)
    if m.get("y30") is not None and m.get("y5") is not None:
        m["curve_5s30s_bp"] = round((m["y30"] - m["y5"]) * 100, 1)
    return m


def ttm_yield(sym, last_close):
    """TTM distribution yield % from FMP dividends (real payouts)."""
    try:
        d = fmp(f"dividends?symbol={sym}")
        if isinstance(d, dict):
            d = d.get("historical") or []
        from datetime import datetime as _dt, timedelta as _td
        cut = (_dt.utcnow() - _td(days=370)).strftime("%Y-%m-%d")
        tot = sum(float(x.get("adjDividend") or x.get("dividend") or 0)
                  for x in d if str(x.get("date", ""))[:10] >= cut)
        if tot > 0 and last_close:
            return round(100 * tot / last_close, 2)
    except Exception as e:  # noqa: BLE001
        print(f"[yield] {sym}: {str(e)[:50]}")
    return None


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
    uni = UNIVERSE[: int(event["limit"])] if event.get("limit") \
        else UNIVERSE
    spx_map = {d: v for d, v in price_weekly("SPY")}
    tlt_map = {d: v for d, v in price_weekly("TLT")}

    rows = {}
    wk_map = {}
    for i, (sym, seg) in enumerate(uni):
        wk = price_weekly(sym)
        lv = {"mom_6m_pct": momentum(wk, 26),
              "mom_12_1_pct": mom_12_1(wk)}
        lv.update(tech_series(wk))
        bs = beta_vs(wk, spx_map)
        bt = beta_vs(wk, tlt_map)
        if bs is not None:
            lv["beta_spy"] = bs
        if bt is not None:
            lv["beta_tlt"] = bt
        rec = {"t": sym, "segment": seg, "_lv": lv}
        wk_map[sym] = wk
        yy = ttm_yield(sym, wk[-1][1] if wk else None)
        if yy is not None:
            lv["ttm_yield_pct"] = yy
        if isinstance(bt, (int, float)) and abs(bt) >= 0.05 and \
                isinstance(lv.get("mom_12_1_pct"), (int, float)):
            lv["ret_per_duration"] = round(
                lv["mom_12_1_pct"] / abs(bt), 1)
        try:
            info = fmp(f"etf/info?symbol={sym}")
            i0 = info[0] if isinstance(info, list) and info else {}
            aum = i0.get("assetsUnderManagement")
            rec["aum_usd_m"] = round(aum / 1e6, 1) if \
                isinstance(aum, (int, float)) and aum > 0 else None
            er = i0.get("expenseRatio")
            rec["expense_pct"] = round(er * 100, 2) if \
                isinstance(er, (int, float)) and er < 1 else None
            rec["name"] = i0.get("name")
        except Exception as e:  # noqa: BLE001
            print(f"[info] {sym}: {str(e)[:50]}")
        rows[sym] = rec
        if i % 12 == 0:
            print(f"[fi-census] {i}/{len(uni)} {sym}")
        time.sleep(0.12)

    tickers = sorted(rows.keys())
    n = len(tickers)
    segs = [rows[t]["segment"] for t in tickers]
    names = [rows[t].get("name") for t in tickers]
    allk = sorted({k for t in tickers for k in rows[t]["_lv"]})
    cols = {}
    for k in allk:
        col = [rows[t]["_lv"].get(k) for t in tickers]
        if sum(1 for v in col if v is not None) >= max(4,
                                                       int(n * 0.15)):
            cols[k] = [round(v, 3) if isinstance(v, float) else v
                       for v in col]
    for k in ("aum_usd_m", "expense_pct"):
        cols[k] = [rows[t].get(k) for t in tickers]

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
        v += 6 * ((cols.get("golden_cross_10_40w") or [0] * n)[i] or 0)
        v -= 8 * ((cols.get("double_top") or [0] * n)[i] or 0)
        ts_col.append(round(max(0.0, min(100.0, v)), 1))
    cols["tech_score"] = ts_col
    rparts = [cross_pct(cols.get(k) or [None] * n, low)
              for k, low in (("vol_52w_pct", 0), ("beta_tlt", 0),
                             ("dist_52w_high_pct", 1))
              if cols.get(k)]
    rk = []
    for i in range(n):
        vs = [rp[i] for rp in rparts if rp[i] is not None]
        rk.append(round(sum(vs) / len(vs), 1) if len(vs) >= 2
                  else None)
    cols["risk_score"] = rk
    yb = None
    if cols.get("ttm_yield_pct"):
        try:
            yb = cols["ttm_yield_pct"][tickers.index("BIL")]
        except Exception:  # noqa: BLE001
            yb = None
        if isinstance(yb, (int, float)):
            cols["carry_vs_cash_bp"] = [
                round((v - yb) * 100, 0)
                if isinstance(v, (int, float)) else None
                for v in cols["ttm_yield_pct"]]

    def g(k, i):
        v = (cols.get(k) or [None] * n)[i]
        return v if isinstance(v, (int, float)) else None
    ups, dns, rrs = [], [], []
    for i in range(n):
        dh = g("dist_52w_high_pct", i)
        dl = g("dist_52w_low_pct", i)
        vol = g("vol_52w_pct", i)
        rk2 = g("risk_score", i)
        yy2 = g("ttm_yield_pct", i)
        if dh is None or vol is None:
            ups.append(None); dns.append(None); rrs.append(None)
            continue
        up = (yy2 or 0.0) + 0.4 * max(0.0, -dh)
        up += 0.10 * (g("tech_score", i) or 50.0)
        up += 5 * (g("double_bottom", i) or 0)
        dn = 0.6 * vol * ((rk2 if rk2 is not None else 50.0) / 100.0)
        dn += 0.15 * max(0.0, dl or 0.0)
        dn += 5 * (g("double_top", i) or 0)
        dn = max(1.5, dn)
        ups.append(round(up, 1)); dns.append(round(dn, 1))
        rrs.append(round(up / dn, 2))
    cols["upside_pct"] = ups
    cols["downside_pct"] = dns
    cols["rr_ratio"] = rrs
    now = datetime.now(timezone.utc).isoformat()
    mx = {"generated_at": now, "version": VERSION, "n": n,
          "tickers": tickers, "segments": segs, "names": names,
          "metrics": sorted(cols.keys()), "cols": cols}
    S3.put_object(Bucket=BUCKET, Key=MX_KEY,
                  Body=json.dumps(mx, default=str).encode(),
                  ContentType="application/json",
                  CacheControl="no-cache")

    def tops(k, nn=8, rev=True):
        pr = [(tickers[i], v) for i, v in enumerate(cols.get(k) or [])
              if isinstance(v, (int, float))]
        pr.sort(key=lambda x: -x[1] if rev else x[1])
        return pr[:nn]
    spreads = [dict(pair=lbl, note=note,
                    **(ratio_z(wk_map.get(a) or [],
                               wk_map.get(b) or []) or {}))
               for lbl, a, b, note in
               (("HYG/LQD", "HYG", "LQD", "HY vs IG risk appetite"),
                ("LQD/IEF", "LQD", "IEF", "credit vs rates"),
                ("TIP/IEF", "TIP", "IEF", "breakeven direction"),
                ("EMB/IEF", "EMB", "IEF", "EM risk"),
                ("TLT/SHY", "TLT", "SHY",
                 "flattener (falling = steepener)"))
               if wk_map.get(a) and wk_map.get(b)]
    dealer, funding = None, None
    try:
        cs = s3json("data/credit-stress.json") or {}
        dealer = cs.get("dealer_positioning")
    except Exception:  # noqa: BLE001
        pass
    try:
        ep = s3json("data/eurodollar-plumbing.json") or {}
        for mrec in ((ep.get("us_core") or {}).get("metrics") or []):
            if "gcf" in str(mrec.get("id", "")).lower():
                funding = mrec
                break
    except Exception:  # noqa: BLE001
        pass
    doc = {"generated_at": now, "version": VERSION, "n": n,
           "spreads": spreads, "dealer": dealer, "funding": funding,
           "curve": curve_block(),
           "credit": credit_blocks(),
           "boards": {"duration_ladder": tops("beta_tlt", 12),
                      "momentum": tops("mom_12_1_pct"),
                      "tech_leaders": tops("tech_score"),
                      "lowest_risk": tops("risk_score", 8, rev=False),
                      "double_bottoms": [tickers[i] for i, v in
                                         enumerate(cols.get(
                                             "double_bottom") or [])
                                         if v == 1]},
           "duration_s": round(time.time() - t0, 1)}
    S3.put_object(Bucket=BUCKET, Key=DOC_KEY,
                  Body=json.dumps(doc, default=str).encode(),
                  ContentType="application/json",
                  CacheControl="no-cache")
    print(f"[fi-census] DONE n={n} {doc['duration_s']}s")
    return {"ok": True, "n": n}
