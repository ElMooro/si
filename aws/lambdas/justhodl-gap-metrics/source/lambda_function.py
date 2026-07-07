"""justhodl-gap-metrics — the fleet-audit gap matrix, built.

Eleven independent modules, each closing a verified gap from
data/fleet-audit.json (ops 2974-2976). Every module writes its OWN key so
pages can wire granularly via jh-wire.js, plus a combined index at
data/gap-metrics.json. Modules never fabricate: each returns status OK or
DEGRADED with the reason, and one module failing never blocks the rest.

  M1  sloos            data/sloos.json            FRED bank lending standards
  M13 stock_bond_corr  data/stock-bond-corr.json  SPY/TLT rolling 63d corr regime
  M14 global_m2        data/global-m2.json        USD-converted G3 broad money impulse
  M16 ofr_fsi          data/ofr-fsi.json          OFR Financial Stress Index
  M22 muni_ratio       data/muni-ratio.json       MUB yield / 10y UST
  M11 revision_breadth data/revision-breadth.json market-level EPS revision breadth
  M15 miner_margin     data/miner-margin.json     gold-miner operating margin vs gold
  M19 em_carry         data/em-carry.json         EM FX carry-trade P&L proxy
  M20 baltic_dry       data/baltic-dry.json       freight pulse via BDRY (futures ETF)
  M4  bill_share       data/bill-share.json       Treasury bill share of gross issuance
  M9  cor3m            data/implied-corr.json     CBOE 3m implied correlation (best-effort)

Zero LLM. All sources: FRED, Polygon, FMP, OFR public CSV, TreasuryDirect
public API, Yahoo chart (best-effort).
"""
import json
import math
import os
import time
import urllib.request
from datetime import datetime, timedelta, timezone

import boto3

S3 = boto3.client("s3")
BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
FRED = os.environ.get("FRED_KEY") or os.environ.get("FRED_API_KEY", "")
POLY = os.environ.get("POLYGON_API_KEY") or os.environ.get("POLYGON_KEY", "")
FMP = os.environ.get("FMP_API_KEY") or os.environ.get("FMP_KEY", "")
UA = {"User-Agent": "Mozilla/5.0 (JustHodl gap-metrics)"}


def http(url, timeout=25, headers=None):
    req = urllib.request.Request(url, headers=headers or UA)
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.read().decode("utf-8", "replace")


def fred_series(sid, start):
    d = json.loads(http(
        "https://api.stlouisfed.org/fred/series/observations?series_id=%s"
        "&api_key=%s&file_type=json&observation_start=%s" % (sid, FRED,
                                                             start)))
    out = []
    for o in d.get("observations", []):
        v = o.get("value")
        if v not in (".", "", None):
            try:
                out.append((o["date"], float(v)))
            except ValueError:
                pass
    return out


def polygon_daily(tkr, years=3):
    end = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    start = (datetime.now(timezone.utc) - timedelta(days=365 * years)
             ).strftime("%Y-%m-%d")
    d = json.loads(http(
        "https://api.polygon.io/v2/aggs/ticker/%s/range/1/day/%s/%s"
        "?adjusted=true&sort=asc&limit=50000&apiKey=%s"
        % (tkr, start, end, POLY)))
    return d.get("results") or []


def zscore(x, hist):
    h = [v for v in hist if v is not None]
    if len(h) < 12:
        return None
    m = sum(h) / len(h)
    sd = math.sqrt(sum((v - m) ** 2 for v in h) / len(h)) or 1e-9
    return round((x - m) / sd, 2)


def pctile(x, hist):
    h = sorted(v for v in hist if v is not None)
    if len(h) < 12:
        return None
    return round(100.0 * sum(1 for v in h if v <= x) / len(h), 1)


def rets(closes):
    return [closes[i + 1] / closes[i] - 1.0 for i in range(len(closes) - 1)
            if closes[i]]


def pearson(a, b):
    k = min(len(a), len(b))
    if k < 30:
        return None
    a, b = a[-k:], b[-k:]
    ma, mb = sum(a) / k, sum(b) / k
    cov = sum((x - ma) * (y - mb) for x, y in zip(a, b))
    va = sum((x - ma) ** 2 for x in a)
    vb = sum((y - mb) ** 2 for y in b)
    if va <= 0 or vb <= 0:
        return None
    return cov / math.sqrt(va * vb)


# ────────────────────────────── modules ──────────────────────────────

def m_sloos():
    out = {"gap_id": "M1", "source": "FRED SLOOS (quarterly)"}
    series = {"DRTSCILM": "C&I loans, large/medium firms",
              "DRTSCIS": "C&I loans, small firms",
              "DRTSCLCC": "Credit cards"}
    rows = {}
    hist_main = []
    for sid, label in series.items():
        s = fred_series(sid, "1995-01-01")
        if not s:
            continue
        vals = [v for _, v in s]
        latest = vals[-1]
        rows[sid] = {"label": label, "latest_pct_tightening": latest,
                     "prior": vals[-2] if len(vals) > 1 else None,
                     "avg_4q": round(sum(vals[-4:]) / min(4, len(vals)), 1),
                     "z_full_history": zscore(latest, vals),
                     "date": s[-1][0]}
        if sid == "DRTSCILM":
            hist_main = vals
    if not rows:
        return {"status": "DEGRADED", "note": "no SLOOS series returned",
                **out}
    lead = rows.get("DRTSCILM", {})
    lv = lead.get("latest_pct_tightening")
    direction = ("TIGHTENING" if lv is not None and lv > 5 else
                 "EASING" if lv is not None and lv < -5 else "NEUTRAL")
    return {"status": "OK", "series": rows, "direction": direction,
            "headline_pct_tightening": lv,
            "headline_pctile_history": pctile(lv, hist_main)
            if lv is not None else None,
            "read": "Net % of banks tightening C&I standards. Leads HY "
                    "spreads and default rates by 2-3 quarters.", **out}


def m_stock_bond_corr():
    out = {"gap_id": "M13", "source": "Polygon SPY/TLT daily"}
    spy = [b["c"] for b in polygon_daily("SPY", 3)]
    time.sleep(0.15)
    tlt = [b_["c"] for b_ in polygon_daily("TLT", 3)]
    bars = polygon_daily("SPY", 3)  # dates
    if len(spy) < 300 or len(tlt) < 300:
        return {"status": "DEGRADED", "note": "insufficient bars "
                "(spy=%d tlt=%d)" % (len(spy), len(tlt)), **out}
    k = min(len(spy), len(tlt))
    rs, rt = rets(spy[-k:]), rets(tlt[-k:])
    dates = [datetime.fromtimestamp(b["t"] / 1000, tz=timezone.utc)
             .strftime("%Y-%m-%d") for b in bars][-k:][1:]
    series = []
    for i in range(63, len(rs)):
        c = pearson(rs[i - 63:i], rt[i - 63:i])
        if c is not None:
            series.append([dates[i - 1] if i - 1 < len(dates) else None,
                           round(c, 3)])
    if not series:
        return {"status": "DEGRADED", "note": "corr series empty", **out}
    cur = series[-1][1]
    last21 = [c for _, c in series[-21:]]
    avg21 = round(sum(last21) / len(last21), 3)
    regime = ("POSITIVE" if avg21 > 0.15 else
              "NEGATIVE" if avg21 < -0.15 else "TRANSITION")
    yr = [c for _, c in series[-252:]]
    return {"status": "OK", "current_63d_corr": cur, "avg_21d": avg21,
            "regime": regime,
            "share_positive_1y_pct": round(
                100.0 * sum(1 for c in yr if c > 0) / len(yr), 1),
            "series_260d": series[-260:],
            "read": "POSITIVE regime means duration no longer hedges "
                    "equities -- 60/40 and risk-parity hedge ratios "
                    "break. NEGATIVE restores the classic hedge.", **out}


def m_global_m2():
    out = {"gap_id": "M14",
           "source": "FRED M2SL + OECD broad money (EA/JP), FX-converted"}
    legs, notes = {}, []
    try:
        us = fred_series("M2SL", "2005-01-01")           # $B
        legs["US"] = [(d, v) for d, v in us]
    except Exception as e:
        notes.append("US M2SL failed: %s" % str(e)[:60])
    fx_eu = fx_jp = None
    try:
        fx_eu = fred_series("DEXUSEU", "2005-01-01")     # USD per EUR
        fx_jp = fred_series("DEXJPUS", "2005-01-01")     # JPY per USD
    except Exception as e:
        notes.append("FX legs failed: %s" % str(e)[:60])
    for sid, name, conv in (("MABMM301EZM189S", "EA", "eur"),
                            ("MABMM301JPM189S", "JP", "jpy")):
        try:
            s = fred_series(sid, "2005-01-01")
            if s and conv == "eur" and fx_eu:
                fxm = {d[:7]: v for d, v in fx_eu}
                legs[name] = [(d, v * fxm.get(d[:7], list(fxm.values())[-1])
                               / 1e9) for d, v in s]       # units→$B approx
            elif s and conv == "jpy" and fx_jp:
                fxm = {d[:7]: v for d, v in fx_jp}
                legs[name] = [(d, v / fxm.get(d[:7],
                                              list(fxm.values())[-1]) / 1e9)
                              for d, v in s]
        except Exception as e:
            notes.append("%s leg failed: %s" % (name, str(e)[:60]))
    if "US" not in legs or len(legs) < 2:
        return {"status": "DEGRADED",
                "note": "insufficient legs: %s" % list(legs), **out}
    monthly = {}
    for name, s in legs.items():
        for d, v in s:
            monthly.setdefault(d[:7], {})[name] = v
    full = {m: sum(v.values()) for m, v in sorted(monthly.items())
            if len(v) == len(legs)}
    ms = sorted(full)
    if len(ms) < 15:
        return {"status": "DEGRADED", "note": "aligned months < 15", **out}
    latest_m = ms[-1]
    latest = full[latest_m]
    yoy_m = ms[-13]
    yoy = round(100.0 * (latest / full[yoy_m] - 1.0), 2)
    impulse = [round(100.0 * (full[ms[i]] / full[ms[i - 12]] - 1.0), 2)
               for i in range(12, len(ms))]
    return {"status": "OK", "legs": sorted(legs), "as_of_month": latest_m,
            "total_usd_bn": round(latest, 0), "yoy_impulse_pct": yoy,
            "impulse_z_history": zscore(yoy, impulse),
            "impulse_series_36m": [[ms[12 + i], v] for i, v in
                                   enumerate(impulse)][-36:],
            "notes": notes or None,
            "read": "The USD liquidity tide behind risk assets. Rising "
                    "impulse historically leads BTC and QQQ at cycle "
                    "scale.", **out}


def m_ofr_fsi():
    out = {"gap_id": "M16", "source": "OFR public CSV"}
    try:
        csv = http("https://www.financialresearch.gov/"
                   "financial-stress-index/data/fsi.csv", timeout=30)
    except Exception as e:
        return {"status": "DEGRADED", "note": "fetch failed: %s"
                % str(e)[:80], **out}
    lines = [ln for ln in csv.splitlines() if ln.strip()]
    if len(lines) < 30:
        return {"status": "DEGRADED", "note": "csv too short", **out}
    hdr = [h.strip().strip('"') for h in lines[0].split(",")]
    idx = None
    for i, h in enumerate(hdr):
        if "OFR FSI" in h or h.upper() == "OFR_FSI" or h == "FSI":
            idx = i
            break
    if idx is None:
        idx = 1 if len(hdr) > 1 else None
    if idx is None:
        return {"status": "DEGRADED", "note": "no FSI column in %s"
                % hdr[:6], **out}
    series = []
    for ln in lines[1:]:
        parts = [p.strip().strip('"') for p in ln.split(",")]
        if len(parts) <= idx:
            continue
        try:
            series.append((parts[0], float(parts[idx])))
        except ValueError:
            continue
    if len(series) < 100:
        return {"status": "DEGRADED", "note": "parsed %d rows"
                % len(series), **out}
    vals = [v for _, v in series]
    latest_d, latest = series[-1]
    return {"status": "OK", "date": latest_d, "fsi": round(latest, 3),
            "z_1y": zscore(latest, vals[-252:]),
            "pctile_10y": pctile(latest, vals[-2520:]),
            "series_1y": [[d, round(v, 3)] for d, v in series[-252:]],
            "read": "Official daily systemic-stress benchmark. Positive "
                    "= above-average stress; use to sanity-check the "
                    "in-house composite.", **out}


def m_muni_ratio():
    out = {"gap_id": "M22", "source": "Polygon MUB dividends + FRED DGS10"}
    try:
        end = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        divs = json.loads(http(
            "https://api.polygon.io/v3/reference/dividends?ticker=MUB"
            "&limit=50&apiKey=" + POLY)).get("results") or []
        cutoff = (datetime.now(timezone.utc) - timedelta(days=370)
                  ).strftime("%Y-%m-%d")
        ttm = sum(d.get("cash_amount") or 0 for d in divs
                  if (d.get("ex_dividend_date") or "") >= cutoff)
        px_bars = polygon_daily("MUB", 1)
        px = px_bars[-1]["c"] if px_bars else None
        y10s = fred_series("DGS10", (datetime.now(timezone.utc)
                                     - timedelta(days=20)
                                     ).strftime("%Y-%m-%d"))
        y10 = y10s[-1][1] if y10s else None
        if not (ttm and px and y10):
            return {"status": "DEGRADED", "note": "ttm=%s px=%s y10=%s"
                    % (ttm, px, y10), **out}
        muni_y = round(100.0 * ttm / px, 2)
        ratio = round(muni_y / y10, 3)
        band = ("CHEAP vs UST" if ratio > 0.85 else
                "RICH vs UST" if ratio < 0.65 else "FAIR")
        return {"status": "OK", "mub_ttm_yield_pct": muni_y,
                "ust_10y_pct": y10, "muni_treasury_ratio": ratio,
                "band": band,
                "read": "Tax-free muni yield as a share of the taxable "
                        "10y. >0.85 historically cheap, <0.65 rich "
                        "(heuristic bands, flagged as such). Tax-"
                        "equivalent for a 37%% bracket: %.2f%%"
                        % (muni_y / 0.63), **out}
    except Exception as e:
        return {"status": "DEGRADED", "note": str(e)[:100], **out}


def m_revision_breadth():
    out = {"gap_id": "M11",
           "source": "aggregates existing data/estimate-revisions.json"}
    try:
        doc = json.loads(S3.get_object(
            Bucket=BUCKET, Key="data/estimate-revisions.json")
            ["Body"].read())
    except Exception as e:
        return {"status": "DEGRADED", "note": "feed unavailable: %s"
                % str(e)[:70], **out}
    rows = None
    if isinstance(doc, list):
        rows = doc
    elif isinstance(doc, dict):
        best = []
        for v in doc.values():
            if isinstance(v, list) and v and isinstance(v[0], dict) \
                    and len(v) > len(best):
                best = v
        rows = best or None
    if not rows:
        return {"status": "DEGRADED",
                "note": "no row array found; top keys: %s"
                % list(doc)[:8] if isinstance(doc, dict) else "n/a", **out}
    fields = ("eps_rev_pct", "eps_rev_recent_pct", "eps_revision_3m",
              "revision_3m", "rev_3m", "net_revision",
              "eps_change_pct", "revision_pct", "delta_pct", "change_3m",
              "eps_rev", "delta", "change")
    fld = None
    for f in fields:
        if any(isinstance(r.get(f), (int, float)) for r in rows[:25]
               if isinstance(r, dict)):
            fld = f
            break
    if not fld:
        sample = sorted({k for r in rows[:10] if isinstance(r, dict)
                         for k in r})[:14]
        return {"status": "DEGRADED",
                "note": "no numeric revision field; row keys: %s"
                % sample, **out}
    def signed(r):
        for f in (fld, "eps_rev_recent_pct", "eps_rev_pct"):
            v = r.get(f)
            if isinstance(v, (int, float)) and v != 0:
                return 1 if v > 0 else -1
        d = str(r.get("direction") or "").upper()
        if d in ("UP", "POSITIVE", "POS", "RAISED", "HIGHER"):
            return 1
        if d in ("DOWN", "NEGATIVE", "NEG", "CUT", "LOWER"):
            return -1
        return 0
    signs = [signed(r) for r in rows if isinstance(r, dict)]
    pos = sum(1 for x in signs if x > 0)
    neg = sum(1 for x in signs if x < 0)
    tot = pos + neg
    if tot < 10:
        return {"status": "DEGRADED",
                "note": "only %d signed rows of %d in feed (universe too "
                        "thin for market-level breadth)"
                % (tot, len(rows)), **out}
    breadth = round(100.0 * pos / tot, 1)
    return {"status": "OK", "field_used": fld, "names_covered": tot, "universe_rows": len(rows),
            "positive": pos, "negative": neg,
            "breadth_pct_positive": breadth,
            "regime": ("EXPANSION" if breadth > 55 else
                       "CONTRACTION" if breadth < 45 else "NEUTRAL"),
            "read": "%% of covered names with positive EPS revisions. "
                    "Breadth leads the EPS cycle; watch crossings of "
                    "50.", **out}


MINERS = ["NEM", "GOLD", "AEM", "WPM", "FNV", "KGC", "GFI", "RGLD"]


def m_miner_margin():
    out = {"gap_id": "M15", "source": "FMP quarterly income statements "
           "(top GDX names) vs GLD"}
    if not FMP:
        return {"status": "DEGRADED", "note": "no FMP key in env", **out}
    per, errs = {}, []
    for t in MINERS:
        try:
            d = json.loads(http(
                "https://financialmodelingprep.com/stable/"
                "income-statement?symbol=%s&period=quarter&limit=8"
                "&apikey=%s" % (t, FMP), timeout=20))
            if isinstance(d, dict) and d.get("Error Message"):
                errs.append("%s: %s" % (t, str(d["Error Message"])[:60]))
                continue
            if isinstance(d, list) and len(d) >= 5:
                now = d[0].get("operatingIncomeRatio")
                ya = d[4].get("operatingIncomeRatio")
                if isinstance(now, (int, float)) and \
                        isinstance(ya, (int, float)):
                    per[t] = {"margin_now_pct": round(100 * now, 1),
                              "margin_yr_ago_pct": round(100 * ya, 1),
                              "delta_pp": round(100 * (now - ya), 1)}
            time.sleep(0.12)
        except Exception as e:
            errs.append("%s: %s" % (t, str(e)[:60]))
            continue
    if len(per) < 5:
        return {"status": "DEGRADED", "note": "only %d miners parsed; "
                "errors: %s" % (len(per), "; ".join(errs[:3]) or "none"),
                **out}
    deltas = sorted(v["delta_pp"] for v in per.values())
    med = deltas[len(deltas) // 2]
    gld = [b["c"] for b in polygon_daily("GLD", 2)]
    gld_yoy = (round(100.0 * (gld[-1] / gld[-252] - 1.0), 1)
               if len(gld) > 260 else None)
    return {"status": "OK", "miners": per,
            "median_margin_delta_pp": med,
            "gold_yoy_pct": gld_yoy,
            "margin_expansion": med > 0,
            "read": "Miner re-ratings follow MARGIN expansion, not gold "
                    "price alone. Median operating-margin change vs a "
                    "year ago across the top GDX weights, next to "
                    "gold's own move.", **out}


EM_PAIRS = ["C:USDMXN", "C:USDBRL", "C:USDZAR", "C:USDINR"]


def m_em_carry():
    out = {"gap_id": "M19",
           "source": "Polygon FX spot (carry P&L PROXY -- no forwards)"}
    legs = {}
    for p in EM_PAIRS:
        try:
            bars = polygon_daily(p, 1)
            cl = [b["c"] for b in bars]
            if len(cl) > 70:
                legs[p] = cl
            time.sleep(0.12)
        except Exception:
            continue
    if len(legs) < 3:
        return {"status": "DEGRADED", "note": "only %d pairs" % len(legs),
                **out}

    def basket(n):
        r = []
        for cl in legs.values():
            # long EM = short USDxxx → invert the pair return
            r.append(-(cl[-1] / cl[-n - 1] - 1.0))
        return round(100.0 * sum(r) / len(r), 2)
    r21, r63 = basket(21), basket(63)
    return {"status": "OK", "pairs": sorted(legs),
            "long_em_basket_21d_pct": r21,
            "long_em_basket_63d_pct": r63,
            "risk_appetite": "ON" if r63 > 0 else "OFF",
            "read": "Equal-weight long MXN/BRL/ZAR/INR vs USD -- a spot "
                    "proxy for carry-trade P&L (forwards not free-"
                    "sourced; labeled proxy). Carry appetite is the "
                    "purest global risk thermometer.", **out}


def m_baltic_dry():
    out = {"gap_id": "M20",
           "source": "Polygon BDRY (dry-bulk freight-futures ETF proxy)"}
    bars = polygon_daily("BDRY", 2)
    cl = [b["c"] for b in bars]
    if len(cl) < 120:
        return {"status": "DEGRADED", "note": "only %d bars" % len(cl),
                **out}
    last = cl[-1]
    return {"status": "OK", "bdry_close": round(last, 2),
            "chg_21d_pct": round(100.0 * (last / cl[-22] - 1.0), 1),
            "chg_63d_pct": round(100.0 * (last / cl[-64] - 1.0), 1),
            "pctile_52w": pctile(last, cl[-252:]),
            "read": "Physical-economy pulse via dry-bulk freight "
                    "futures. ETF proxy for the Baltic Dry Index "
                    "(labeled as such).", **out}


def m_bill_share():
    out = {"gap_id": "M4", "source": "TreasuryDirect auctions API"}
    try:
        rows = json.loads(http(
            "https://www.treasurydirect.gov/TA_WS/securities/auctioned"
            "?days=95&format=json", timeout=30))
    except Exception as e:
        return {"status": "DEGRADED", "note": "fetch failed: %s"
                % str(e)[:80], **out}
    if not isinstance(rows, list) or len(rows) < 10:
        return {"status": "DEGRADED", "note": "unexpected payload", **out}

    def amt(r):
        v = r.get("offeringAmount") or r.get("totalAccepted") or 0
        try:
            return float(str(v).replace("$", "").replace(",", ""))
        except ValueError:
            return 0.0
    tot = sum(amt(r) for r in rows)
    bills = sum(amt(r) for r in rows
                if (r.get("securityType") or "").lower() == "bill")
    if tot <= 0:
        return {"status": "DEGRADED", "note": "zero total amount", **out}
    share = round(100.0 * bills / tot, 1)
    return {"status": "OK", "window_days": 95, "auctions": len(rows),
            "gross_issuance_usd_bn": round(tot / 1e9, 1),
            "bills_usd_bn": round(bills / 1e9, 1),
            "bill_share_pct": share,
            "regime": "BILL-HEAVY (drains RRP, spares duration)"
                      if share > 55 else
                      "COUPON-HEAVY (duration supply pressure)"
                      if share < 40 else "BALANCED",
            "read": "Bill-heavy issuance drains the RRP rather than "
                    "bank reserves and spares duration; coupon-heavy "
                    "pressures the long end. Same deficit, different "
                    "liquidity.", **out}


def m_cor3m():
    out = {"gap_id": "M9",
           "source": "Yahoo implied-correlation indices (best-effort)"}
    closes, used = [], None
    try:
        d = json.loads(http(
            "https://cdn.cboe.com/api/global/delayed_quotes/charts/"
            "_COR3M.json", timeout=20))
        pts = ((d.get("data") or {}).get("prices")
               or d.get("data") or [])
        cs = []
        for row in pts:
            v = row.get("price") if isinstance(row, dict) else \
                (row[1] if isinstance(row, (list, tuple))
                 and len(row) > 1 else None)
            if isinstance(v, (int, float)):
                cs.append(float(v))
        if len(cs) >= 60:
            closes, used = cs, "CBOE _COR3M"
    except Exception:
        pass
    for sym in (() if closes else
                ("%5ECOR3M", "%5ECOR90D", "%5ECOR1M", "%5ECOR30D")):
        try:
            d = json.loads(http(
                "https://query1.finance.yahoo.com/v8/finance/chart/"
                + sym + "?range=1y&interval=1d", timeout=20))
            res = (d.get("chart") or {}).get("result") or []
            q = ((res[0].get("indicators") or {}).get("quote")
                 or [{}])[0] if res else {}
            cs = [c for c in (q.get("close") or []) if c is not None]
            if len(cs) >= 60:
                closes, used = cs, sym.replace("%5E", "^")
                break
        except Exception:
            continue
    try:
        if len(closes) < 60:
            return {"status": "DEGRADED", "note": "no implied-corr "
                    "symbol returned >=60 closes (tried COR3M/COR90D/"
                    "COR1M/COR30D)", **out}
        out["source"] = (used if used and "CBOE" in used
                         else "Yahoo " + (used or ""))
        cur = round(closes[-1], 2)
        return {"status": "OK", "implied_corr_3m": cur,
                "pctile_1y": pctile(cur, closes),
                "read": "CBOE 3m implied correlation. Low = dispersion / "
                        "single-stock market; spikes mark macro "
                        "takeover of the tape.", **out}
    except Exception as e:
        return {"status": "DEGRADED", "note": str(e)[:90], **out}


MODULES = [
    ("sloos", "data/sloos.json", m_sloos),
    ("stock_bond_corr", "data/stock-bond-corr.json", m_stock_bond_corr),
    ("global_m2", "data/global-m2.json", m_global_m2),
    ("ofr_fsi", "data/ofr-fsi.json", m_ofr_fsi),
    ("muni_ratio", "data/muni-ratio.json", m_muni_ratio),
    ("revision_breadth", "data/revision-breadth.json", m_revision_breadth),
    ("miner_margin", "data/miner-margin.json", m_miner_margin),
    ("em_carry", "data/em-carry.json", m_em_carry),
    ("baltic_dry", "data/baltic-dry.json", m_baltic_dry),
    ("bill_share", "data/bill-share.json", m_bill_share),
    ("cor3m", "data/implied-corr.json", m_cor3m),
]


def put(key, obj):
    S3.put_object(Bucket=BUCKET, Key=key,
                  Body=json.dumps(obj, ensure_ascii=False).encode(),
                  ContentType="application/json",
                  CacheControl="public, max-age=900")


def lambda_handler(event, context):
    t0 = time.time()
    now = datetime.now(timezone.utc).isoformat()
    index = {}
    for name, key, fn in MODULES:
        try:
            r = fn()
        except Exception as e:
            r = {"status": "DEGRADED", "note": "module crashed: %s"
                 % str(e)[:120], "gap_id": "?"}
        r["generated_at"] = now
        r["engine"] = "justhodl-gap-metrics"
        r["module"] = name
        try:
            put(key, r)
        except Exception as e:
            r["status"] = "DEGRADED"
            r["note"] = (r.get("note") or "") + " | put failed: %s" \
                % str(e)[:60]
        head = {k: v for k, v in r.items()
                if not isinstance(v, (list, dict))}
        index[name] = {"key": key, "status": r["status"],
                       "gap_id": r.get("gap_id"), "headline": head}
    ok = sum(1 for m in index.values() if m["status"] == "OK")
    doc = {"generated_at": now, "engine": "justhodl-gap-metrics",
           "elapsed_s": round(time.time() - t0, 1),
           "modules_ok": ok, "modules_total": len(MODULES),
           "modules": index,
           "method": "fleet-audit gap matrix (ops 2974) built; each "
                     "module independent, honest DEGRADED states, "
                     "zero LLM."}
    put("data/gap-metrics.json", doc)
    return {"statusCode": 200,
            "body": json.dumps({"ok": ok, "total": len(MODULES),
                                "elapsed_s": doc["elapsed_s"]})}
