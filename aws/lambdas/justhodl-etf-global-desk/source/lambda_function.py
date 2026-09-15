"""justhodl-etf-global-desk — paid ETF Global (flows + constituents + profiles).

You pay $297/mo for three Massive add-ons. This engine is the desk producer:
it pulls all three into one CDN-visible file so /etf.html, /strong.html and the
chart data panel actually render creations/redemptions, official AUM, fees,
sector/geo mix and holdings — not a dollar-volume z-score proxy.

Writes:
  data/etf-desk.json              compact per-ticker desk payload (GitHub Pages)
  data/etf-global.json            harvest status
  data/etf-global-desk-meta.json  counts / HTTP map
"""
from __future__ import annotations

import json
import os
import time
import urllib.error
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone

import boto3

VERSION = "1.5.1"
BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
KEY = os.environ.get("POLYGON_KEY") or os.environ.get("POLYGON_API_KEY") or os.environ.get("MASSIVE_API_KEY") or ""
HOSTS = ("https://api.massive.com", "https://api.polygon.io")
UA = {"User-Agent": "JustHodl-ETFGlobalDesk/1.1"}
s3 = boto3.client("s3", region_name="us-east-1")

# Liquid wrappers the ETF / Strong desks actually render. Keep this in lockstep
# with jh-etf-engine.js UNIVERSE plus a few flow-critical names.
DESK = [
    "SPY", "VOO", "IVV", "QQQ", "QQQM", "IWM", "VTI", "DIA", "RSP",
    "XLK", "SMH", "XLF", "XLE", "XLV", "XLY", "XLP", "XLI", "XLU", "XLB",
    "XLRE", "XLC", "XBI", "KRE", "SOXX", "ARKK",
    "TLT", "IEF", "SHY", "BIL", "TIP", "GOVT", "TBT", "SGOV",
    "AGG", "BND", "LQD", "HYG", "JNK", "USHY", "EMB", "FALN", "VCIT", "BKLN",
    "GLD", "IAU", "SLV", "GDX", "GDXJ", "PPLT", "CPER",
    "USO", "UNG", "DBC", "DBA",
    "EEM", "VWO", "EFA", "IEFA", "VEA", "FXI", "EWJ", "EWZ", "INDA", "MCHI",
    "IBIT", "FBTC", "ETHA", "BITO",
    "MTUM", "QUAL", "USMV", "MOAT", "VLUE", "IWF", "IWD", "VUG", "VTV",
    "UUP", "UDN", "FXE", "FXY", "KWEB", "XHB", "ITA", "PAVE", "BOTZ", "HACK",
    # Levered / inverse — speculative positioning, not 1:1 underlying.
    "TQQQ", "UPRO", "SOXL", "TNA", "UDOW", "SPXL", "QLD", "SSO", "TECL", "FAS",
    "SQQQ", "SPXU", "SOXS", "TZA", "SDOW", "SPXS", "QID", "SDS", "TECS", "FAZ",
    "SH", "PSQ", "UVXY", "SVXY",
]


def _num(v):
    try:
        if v is None or v == "":
            return None
        n = float(v)
        return n if n == n else None
    except (TypeError, ValueError):
        return None


def _http(url, timeout=25):
    req = urllib.request.Request(url, headers=UA)
    with urllib.request.urlopen(req, timeout=timeout) as r:
        body = json.loads(r.read().decode("utf-8", "replace"))
        return r.status, body


def _with_key(url):
    if not KEY:
        return url
    if "apiKey=" in url:
        return url
    return url + ("&" if "?" in url else "?") + "apiKey=" + urllib.parse.quote(KEY)


def _get(path, extra, timeout=20):
    params = {"limit": extra.pop("limit", "120"), "apiKey": KEY}
    params.update({k: v for k, v in extra.items() if v is not None})
    last = (0, {"error": "no host"}, HOSTS[0])
    for host in HOSTS:
        url = host + path + "?" + urllib.parse.urlencode(params)
        try:
            status, body = _http(url, timeout=timeout)
            rows = body.get("results") if isinstance(body, dict) else None
            if status == 200 and isinstance(rows, list):
                return 200, body, host
            last = (status, body, host)
        except urllib.error.HTTPError as e:
            raw = e.read().decode("utf-8", "replace")[:240]
            last = (e.code, {"error": raw, "status": "HTTP_%s" % e.code}, host)
        except Exception as e:
            last = (0, {"error": str(e)[:180]}, host)
    return last[0], last[1], last[2]


def _pages(path, extra, max_pages=40, timeout=25):
    """Follow next_url until exhausted or max_pages. Returns (http, rows, host, err, pages)."""
    st, body, host = _get(path, dict(extra), timeout=timeout)
    if st != 200:
        return st, [], host, (body or {}).get("error") or (body or {}).get("status"), 0
    rows = list((body or {}).get("results") or [])
    nxt = (body or {}).get("next_url")
    pages = 1
    while nxt and pages < max_pages and KEY:
        try:
            status, more = _http(_with_key(nxt), timeout=timeout)
            if status != 200 or not isinstance(more, dict):
                break
            chunk = more.get("results") or []
            if not chunk:
                break
            rows.extend(chunk)
            nxt = more.get("next_url")
            pages += 1
        except Exception:
            break
    return 200, rows, host, None, pages


def _latest(rows, date_key="processed_date"):
    if not rows:
        return None
    return max(rows, key=lambda r: str(r.get(date_key) or ""))


def _sum_n(rows, n):
    """Sum of the first n flow rows. Incomplete windows are flagged, not silently filled."""
    vals = [_num(r.get("fund_flow")) for r in rows if r.get("fund_flow") is not None]
    if not vals:
        return {"usd": None, "n_observed": 0, "n_requested": n, "complete": False}
    take = vals[:n]
    return {
        "usd": sum(take),
        "n_observed": len(take),
        "n_requested": n,
        "complete": len(vals) >= n,
    }


def _top_exp(obj, n=6):
    if not isinstance(obj, dict):
        return []
    items = []
    for k, v in obj.items():
        nv = _num(v)
        if nv is None:
            continue
        items.append({"k": k, "w": nv})
    items.sort(key=lambda x: abs(x["w"]), reverse=True)
    return items[:n]


def _fee(p):
    """Normalize ETF Global fee fields to percent (0.0945 = 9.45 bps).

    Vendor mixes three encodings: fraction (0.000945), percent (0.0945),
    and occasionally bps (9.45). The v1.0 scaler treated anything < 0.2 as a
    fraction, so SPY printed as 9.0% instead of 0.09%.
    """
    for k in ("net_expense_ratio", "expense_ratio", "net_expenses", "management_fee",
              "net_expense", "total_expense_ratio"):
        v = _num(p.get(k))
        if v is None:
            continue
        if v <= 0.005:
            v = v * 100.0
        elif v >= 1.5:
            v = v / 100.0
        return round(v, 4)
    return None


def _exp_full(obj):
    if not isinstance(obj, dict):
        return {}
    out = {}
    for k, v in obj.items():
        nv = _num(v)
        if nv is None:
            continue
        out[str(k)] = nv
    return out


def _constituents(ticker):
    """One dated snapshot, next_url to the end. sort=constituent_rank.asc 400s — do not use it."""
    asof = None
    st, rows, host, err, pages = _pages(
        "/etf-global/v1/constituents",
        {"composite_ticker": ticker, "sort": "processed_date.desc", "limit": "1"},
        max_pages=1,
    )
    if st == 200 and rows:
        asof = rows[0].get("processed_date") or rows[0].get("effective_date")
    attempts = []
    if asof:
        attempts.append({"composite_ticker": ticker, "processed_date": asof, "limit": "1000"})
    attempts.append({"composite_ticker": ticker, "sort": "processed_date.desc", "limit": "1000"})
    attempts.append({"composite_ticker": ticker, "limit": "1000"})
    last_st, last_err, last_host, last_pages = st, err, host, pages
    for extra in attempts:
        st, rows, host, err, pages = _pages("/etf-global/v1/constituents", extra, max_pages=40)
        last_st, last_err, last_host, last_pages = st, err, host, pages
        if st == 200 and rows:
            return st, rows, host, None, pages, asof
    return last_st, [], last_host, last_err, last_pages, asof


def harvest_one(ticker):
    out = {"ticker": ticker, "ok": {}, "http": {}, "err": {}}
    end = datetime.now(timezone.utc).date()
    start = end - timedelta(days=130)
    st, body, host = _get("/etf-global/v1/fund-flows", {
        "composite_ticker": ticker,
        "sort": "processed_date.desc",
        "processed_date.gte": start.isoformat(),
        "limit": "120",
    })
    out["http"]["flows"] = st
    rows = (body or {}).get("results") if st == 200 else []
    rows = sorted(rows or [], key=lambda r: str(r.get("processed_date") or ""), reverse=True)
    if rows:
        latest = rows[0]
        nav = _num(latest.get("nav"))
        sh = _num(latest.get("shares_outstanding"))
        flows = [_num(r.get("fund_flow")) for r in rows if r.get("fund_flow") is not None]
        f1 = _sum_n(rows, 1)
        f5 = _sum_n(rows, 5)
        f21 = _sum_n(rows, 21)
        f63 = _sum_n(rows, 63)
        out["ok"]["flows"] = True
        out["nav"] = nav
        out["shares"] = sh
        out["aum_from_nav"] = (nav * sh) if (nav is not None and sh is not None) else None
        out["flow_1d"] = f1["usd"]
        out["flow_5d"] = f5["usd"]
        out["flow_21d"] = f21["usd"]
        out["flow_63d"] = f63["usd"]
        out["flow_windows"] = {"1d": f1, "5d": f5, "21d": f21, "63d": f63}
        out["flow_asof"] = latest.get("processed_date")
        out["flow_effective"] = latest.get("effective_date")
        hist = flows[:60]
        if len(hist) >= 15:
            mu = sum(hist) / len(hist)
            var = sum((x - mu) ** 2 for x in hist) / len(hist)
            sd = var ** 0.5
            out["flow_z"] = ((hist[0] - mu) / sd) if sd else None
        else:
            out["flow_z"] = None
        out["flow_hist"] = [
            {"d": r.get("processed_date"), "f": _num(r.get("fund_flow")), "n": _num(r.get("nav"))}
            for r in rows[:40]
        ]
    else:
        out["ok"]["flows"] = False

    st, body, host = _get("/etf-global/v1/profiles", {
        "composite_ticker": ticker,
        "sort": "processed_date.desc",
        "limit": "8",
    })
    out["http"]["profiles"] = st
    prows = (body or {}).get("results") if st == 200 else []
    p = _latest(prows) or {}
    if p:
        out["ok"]["profiles"] = True
        out["name"] = p.get("description") or p.get("fund_name") or p.get("name")
        out["issuer"] = p.get("issuer") or p.get("advisor")
        out["aum"] = _num(p.get("aum")) or out.get("aum_from_nav")
        out["er"] = _fee(p)
        out["asset_class"] = p.get("asset_class")
        out["category"] = p.get("category") or p.get("focus")
        out["benchmark"] = p.get("primary_benchmark")
        out["inception"] = p.get("inception_date")
        out["holdings_n"] = (
            _num(p.get("num_holdings"))
            or _num(p.get("holdings_count"))
            or _num(p.get("number_of_holdings"))
        )
        out["adv"] = _num(p.get("avg_daily_trading_volume"))
        out["spread"] = _num(p.get("bid_ask_spread"))
        out["discount_premium"] = _num(p.get("discount_premium"))
        out["leverage_style"] = p.get("leverage_style") or p.get("leverage") or p.get("leverage_factor")
        out["levered_amount"] = _num(p.get("levered_amount"))
        out["leverage"] = out["leverage_style"]
        out["creation_unit"] = p.get("creation_unit_size")
        out["sector_full"] = _exp_full(p.get("sector_exposure") or {})
        out["industry_full"] = _exp_full(p.get("industry_exposure") or {})
        out["geo_full"] = _exp_full(p.get("geographic_exposure") or p.get("country_exposure") or {})
        out["ccy_full"] = _exp_full(p.get("currency_exposure") or {})
        out["sector"] = _top_exp(out["sector_full"], n=12)
        out["geo"] = _top_exp(out["geo_full"], n=12)
        out["ccy"] = _top_exp(out["ccy_full"], n=12)
        out["profile_asof"] = p.get("processed_date") or p.get("effective_date")
        out["profile_effective"] = p.get("effective_date")
    else:
        out["ok"]["profiles"] = False
        out["aum"] = out.get("aum_from_nav")

    st, crows, host, err, pages, asof_hint = _constituents(ticker)
    out["http"]["constituents"] = st
    out["holdings_pages"] = pages
    if err:
        out["err"]["constituents"] = str(err)[:160]
    if crows:
        asof = max(str(r.get("processed_date") or r.get("effective_date") or "") for r in crows)
        if asof:
            dated = [r for r in crows if str(r.get("processed_date") or r.get("effective_date") or "") == asof]
            if dated:
                crows = dated
        crows.sort(key=lambda r: _num(r.get("weight")) or 0, reverse=True)
        weights = [_num(r.get("weight")) or 0 for r in crows]
        hhi = sum((w * 100.0) ** 2 for w in weights)
        wsum = sum(weights)
        profile_n = out.get("holdings_n")
        n = len(crows)
        complete = True
        if profile_n and n + 5 < float(profile_n):
            complete = False
        if pages >= 40:
            complete = False
        out["ok"]["constituents"] = True
        out["holdings_n"] = int(profile_n) if profile_n else n
        out["holdings_n_received"] = n
        out["holdings_complete"] = complete
        out["holdings_weight_sum"] = round(wsum, 4)
        out["hhi"] = round(hhi, 1) if complete else None
        out["hhi_note"] = None if complete else "partial snapshot — HHI withheld"
        out["all_holdings"] = [{
            "t": r.get("constituent_ticker"),
            "n": r.get("constituent_name"),
            "w": _num(r.get("weight")),
            "mv": _num(r.get("market_value")),
            "sh": _num(r.get("shares_held")),
            "rank": r.get("constituent_rank"),
        } for r in crows if r.get("constituent_ticker")]
        out["top"] = out["all_holdings"][:12]
        out["holdings_asof"] = asof or asof_hint
    else:
        out["ok"]["constituents"] = False
        out["top"] = []
        out["all_holdings"] = []
        out["hhi"] = None
        out["holdings_complete"] = False
    return out


def _label(flow, z):
    if flow is None:
        return "QUIET"
    bn = flow / 1e9
    if bn >= 1.0:
        return "HEAVY INFLOW"
    if bn <= -1.0:
        return "HEAVY OUTFLOW"
    if bn >= 0.15:
        return "INFLOW"
    if bn <= -0.15:
        return "OUTFLOW"
    if z is not None and z >= 1.6:
        return "ELEVATED IN"
    if z is not None and z <= -1.6:
        return "ELEVATED OUT"
    return "QUIET"


def _put(key, obj):
    s3.put_object(
        Bucket=BUCKET,
        Key=key,
        Body=json.dumps(obj, default=str).encode("utf-8"),
        ContentType="application/json",
        CacheControl="public, max-age=120",
    )


def _load_s3(key):
    try:
        return json.loads(s3.get_object(Bucket=BUCKET, Key=key)["Body"].read())
    except Exception:
        return None


def _wfrac(w):
    n = _num(w)
    if n is None:
        return 0.0
    return float(n) if abs(n) <= 1.5 else float(n) / 100.0


def _nav_ret(hist, n=5):
    if not hist or len(hist) < 2:
        return None
    a = _num((hist[0] or {}).get("n"))
    b = _num((hist[min(n - 1, len(hist) - 1)] or {}).get("n"))
    if not a or not b:
        return None
    return round((a / b - 1.0) * 100.0, 3)


def _is_levered(row):
    style = str(row.get("leverage_style") or row.get("leverage") or "").strip().lower()
    amt = _num(row.get("levered_amount"))
    if amt is not None and (abs(amt) >= 1.5 or amt < 0):
        return True
    if not style or style in ("unlevered", "unleveraged", "n/a", "na", "none", "long", "1x", "1.0"):
        return False
    if style in ("leveraged", "inverse", "short", "2x", "3x", "-1x", "-2x", "-3x"):
        return True
    return any(k in style for k in ("inverse", "2x", "3x", "-1", "-2"))


def _member(t, r):
    return {
        "t": t,
        "name": r.get("name"),
        "flow_1d": r.get("flow_1d"),
        "flow_5d": r.get("flow_5d"),
        "aum": r.get("aum"),
        "hhi": r.get("hhi"),
        "holdings_n": r.get("holdings_n"),
        "leverage_style": r.get("leverage_style") or r.get("leverage"),
        "levered_amount": r.get("levered_amount"),
        "flow_label": r.get("flow_label"),
        "top": (r.get("top") or [None])[0],
    }


def _sleeve_sum(desk, tickers):
    members = []
    f1 = f5 = aum = 0.0
    n1 = n5 = na = 0
    for t in tickers:
        r = desk.get(t)
        if not r:
            continue
        members.append(_member(t, r))
        if r.get("flow_1d") is not None:
            f1 += r["flow_1d"]
            n1 += 1
        if r.get("flow_5d") is not None:
            f5 += r["flow_5d"]
            n5 += 1
        if r.get("aum") is not None:
            aum += r["aum"]
            na += 1
    return {
        "tickers": [m["t"] for m in members],
        "n": len(members),
        "flow_1d": round(f1, 2) if n1 else None,
        "flow_5d": round(f5, 2) if n5 else None,
        "aum": round(aum, 2) if na else None,
        "pct_aum_5d": round(f5 / aum * 100.0, 3) if (n5 and na and aum) else None,
        "members": members,
    }


def _family_gross(desk, tickers, window="flow_5d"):
    """Net vs gross on substitute wrappers (SPY/VOO/IVV). Gross >> net = AP plumbing."""
    gin = gout = 0.0
    signed = []
    n_in = n_out = 0
    for t in tickers:
        r = desk.get(t) or {}
        f = r.get(window)
        if f is None:
            continue
        signed.append({"t": t, "flow": f})
        if f > 0:
            gin += f
            n_in += 1
        elif f < 0:
            gout += abs(f)
            n_out += 1
    net = gin - gout
    gross = gin + gout
    plumbing = n_in >= 1 and n_out >= 1 and gross >= 5e9 and abs(net) < 0.45 * gross
    return {
        "tickers": [x["t"] for x in signed],
        "members": signed,
        "gross_in": round(gin, 2),
        "gross_out": round(gout, 2),
        "net": round(net, 2),
        "plumbing": plumbing,
        "window": window,
        "note": (
            "S&P 500 share-class rotation (SPY/VOO/IVV swapping). Net is the risk signal; gross is AP plumbing."
            if plumbing else
            "SPY/VOO/IVV net is the S&P 500 wrapper print."
        ),
    }


def _compact_name(r):
    if not r:
        return None
    return {
        "t": r.get("ticker") or r.get("t"),
        "net_flow_5d_usd": r.get("net_flow_5d_usd"),
        "shares_delta_usd": r.get("shares_delta_usd"),
        "etf_ownership_pct": r.get("etf_ownership_pct"),
        "flow_type": r.get("flow_type"),
        "confirmed": r.get("confirmed"),
        "flow_bps_mcap": r.get("flow_bps_mcap"),
        "flow_bps_adv_day": r.get("flow_bps_adv_day"),
        "industry": r.get("industry"),
        "n_etfs": r.get("n_etfs"),
        "drivers": [
            {"etf": d.get("etf"), "usd": d.get("contrib_5d_usd"), "broad": d.get("broad")}
            for d in (r.get("drivers") or [])[:3]
        ],
    }


def _px_label(flow, ret):
    if flow is None or ret is None:
        return None
    if flow < 0 and ret > 0:
        return "ABSORPTION"
    if flow > 0 and ret < 0:
        return "DISTRIBUTION"
    if flow > 0 and ret > 0:
        return "CONFIRMED_BID"
    if flow < 0 and ret < 0:
        return "CONFIRMED_OFFER"
    return "QUIET"


def _build_derived(desk, complete_hold, generated, lookthrough=None):
    """Ten honest products from ETF Global. Inferred where noted. Never 'smart money'."""
    index_beta = ["SPY", "VOO", "IVV", "VTI", "QQQ", "QQQM", "IWM", "DIA", "RSP"]
    thematic = ["SMH", "SOXX", "XBI", "ARKK", "KRE", "KWEB", "XHB", "ITA", "PAVE", "BOTZ", "HACK"]
    sector = ["XLK", "XLF", "XLE", "XLV", "XLY", "XLP", "XLI", "XLU", "XLB", "XLRE", "XLC", "GDX", "GDXJ"]
    factor_map = {
        "momentum": ["MTUM"],
        "value": ["VLUE", "IWD", "VTV"],
        "quality": ["QUAL", "MOAT"],
        "minvol": ["USMV"],
        "growth": ["IWF", "VUG"],
    }
    credit_map = {
        "hy": ["HYG", "JNK", "USHY", "BKLN"],
        "fallen": ["FALN"],
        "ig": ["LQD", "VCIT"],
        "agg": ["AGG", "BND"],
        "em_sov": ["EMB"],
    }
    size_map = {
        "mega": ["SPY", "VOO", "IVV", "QQQ"],          # cap-weight mega-heavy
        "large": ["DIA", "RSP"],                       # Dow + equal-weight S&P (not VTI)
        "small": ["IWM"],
    }
    bond_q_map = {
        "ust": ["TLT", "IEF", "SHY", "GOVT", "BIL", "SGOV", "TIP"],
        "duration": ["TLT", "IEF", "GOVT"],
        "t_bills": ["BIL", "SGOV", "SHY"],
        "ig": ["LQD", "VCIT"],
        "hy": ["HYG", "JNK", "USHY"],
        "fallen": ["FALN"],
        "em": ["EMB"],
    }
    lev_bull = ["TQQQ", "UPRO", "SOXL", "TNA", "UDOW", "SPXL", "QLD", "SSO", "TECL", "FAS"]
    lev_bear = ["SQQQ", "SPXU", "SOXS", "TZA", "SDOW", "SPXS", "QID", "SDS", "TECS", "FAZ", "TBT", "SH", "PSQ"]
    lev_vol = ["UVXY", "SVXY"]
    rates_map = {
        "long": ["TLT"],
        "intermediate": ["IEF", "GOVT"],
        "short": ["SHY", "BIL", "SGOV"],
        "tips": ["TIP"],
        "inverse": ["TBT"],
    }
    crypto_map = {
        "btc_spot": ["IBIT", "FBTC"],
        "eth_spot": ["ETHA"],
        "btc_futures": ["BITO"],
    }
    bond_funds = ["HYG", "JNK", "USHY", "FALN", "LQD", "VCIT", "EMB", "AGG", "BND", "TLT", "IEF"]

    sleeves = {
        "index_beta": _sleeve_sum(desk, index_beta),
        "thematic": _sleeve_sum(desk, thematic),
        "sector": _sleeve_sum(desk, sector),
        "factor": {k: _sleeve_sum(desk, v) for k, v in factor_map.items()},
        "credit": {k: _sleeve_sum(desk, v) for k, v in credit_map.items()},
        "rates": {k: _sleeve_sum(desk, v) for k, v in rates_map.items()},
        "crypto": {k: _sleeve_sum(desk, v) for k, v in crypto_map.items()},
        "size": {k: _sleeve_sum(desk, v) for k, v in size_map.items()},
        "bond_quality": {k: _sleeve_sum(desk, v) for k, v in bond_q_map.items()},
        "lev_bull": _sleeve_sum(desk, lev_bull),
        "lev_bear": _sleeve_sum(desk, lev_bear),
        "lev_vol": _sleeve_sum(desk, lev_vol),
    }

    idx5 = sleeves["index_beta"].get("flow_5d") or 0.0
    th5 = sleeves["thematic"].get("flow_5d") or 0.0
    if abs(th5) >= 1.5e8 and (abs(th5) > abs(idx5) * 0.6 or (th5 * idx5) < 0):
        tv_verdict = "ROTATION"
        tv_note = "Thematic wrappers are printing while beta is quieter or opposite — not just SPY plumbing."
    elif abs(idx5) >= 1.5e8 and abs(th5) < abs(idx5) * 0.35:
        tv_verdict = "BETA"
        tv_note = "Broad index creations/redemptions dominate. Sector/thematic is not the story."
    else:
        tv_verdict = "MIXED"
        tv_note = "Neither sleeve is large enough, or both are moving together."
    thematic_vs_index = {
        "index_5d": sleeves["index_beta"].get("flow_5d"),
        "thematic_5d": sleeves["thematic"].get("flow_5d"),
        "sector_5d": sleeves["sector"].get("flow_5d"),
        "verdict": tv_verdict,
        "note": tv_note,
        "evidence_tier": "measured_fact",
    }

    factor_ranked = []
    for k, sl in sleeves["factor"].items():
        factor_ranked.append({
            "id": k,
            "label": k.replace("minvol", "min vol").title(),
            "flow_1d": sl.get("flow_1d"),
            "flow_5d": sl.get("flow_5d"),
            "aum": sl.get("aum"),
            "pct_aum_5d": sl.get("pct_aum_5d"),
            "tickers": sl.get("tickers"),
        })
    factor_ranked.sort(key=lambda x: abs(x.get("flow_5d") or 0), reverse=True)
    mom = next((x for x in factor_ranked if x["id"] == "momentum"), {})
    val = next((x for x in factor_ranked if x["id"] == "value"), {})
    mom5, val5 = mom.get("flow_5d") or 0, val.get("flow_5d") or 0
    if abs(mom5) + abs(val5) < 5e7:
        factor_rot = "QUIET"
    elif mom5 > 0 and val5 < 0:
        factor_rot = "MOMENTUM_OVER_VALUE"
    elif val5 > 0 and mom5 < 0:
        factor_rot = "VALUE_OVER_MOMENTUM"
    elif abs(mom5) >= abs(val5):
        factor_rot = "MOMENTUM_LED"
    else:
        factor_rot = "VALUE_LED"

    hy = sleeves["credit"]["hy"]
    ig = sleeves["credit"]["ig"]
    em = sleeves["credit"]["em_sov"]
    fallen = sleeves["credit"]["fallen"]
    lng = sleeves["rates"]["long"]
    hy5, ig5, tlt5, em5 = hy.get("flow_5d") or 0, ig.get("flow_5d") or 0, lng.get("flow_5d") or 0, em.get("flow_5d") or 0
    fal5 = fallen.get("flow_5d") or 0
    if hy5 > 1e8 and tlt5 < -5e7:
        credit_verdict = "RISK_ON"
        credit_note = "HY creations with long-Treasury redemptions — duration sold, credit bid, in wrapper dollars."
    elif hy5 < -1e8 and tlt5 > 5e7:
        credit_verdict = "RISK_OFF"
        credit_note = "HY redemptions with TLT creations — credit offered, duration bid."
    elif em5 < -8e7 and hy5 < 0:
        credit_verdict = "EM_STRESS"
        credit_note = "EM sovereign wrappers redeeming with HY — not a rates-only move."
    elif fal5 < -3e7 and hy5 < 0:
        credit_verdict = "CREDIT_STRESS"
        credit_note = "Fallen angels and HY both redeeming — quality-off inside credit, not just duration."
    else:
        credit_verdict = "MIXED"
        credit_note = "Credit/rates/EM wrappers are not printing a clean risk stack."
    credit_stack = {
        "hy": hy, "fallen": fallen, "ig": ig, "agg": sleeves["credit"]["agg"], "em_sov": em,
        "rates_long": lng, "rates_short": sleeves["rates"]["short"],
        "verdict": credit_verdict, "note": credit_note,
        "evidence_tier": "measured_fact",
    }

    btc = sleeves["crypto"]["btc_spot"]
    eth = sleeves["crypto"]["eth_spot"]
    fut = sleeves["crypto"]["btc_futures"]
    crypto_wrapper = {
        "btc_spot": btc, "eth_spot": eth, "btc_futures": fut,
        "btc_eth_1d": (btc.get("flow_1d") or 0) + (eth.get("flow_1d") or 0),
        "btc_eth_5d": (btc.get("flow_5d") or 0) + (eth.get("flow_5d") or 0),
        "note": "IBIT/FBTC/ETHA creations are the wrapper bid. Not Coinbase, Binance, or CME open interest.",
        "evidence_tier": "measured_fact",
    }

    leverage = []
    for t, r in desk.items():
        if not _is_levered(r):
            continue
        leverage.append({
            "t": t,
            "name": r.get("name"),
            "leverage_style": r.get("leverage_style") or r.get("leverage"),
            "levered_amount": r.get("levered_amount"),
            "flow_1d": r.get("flow_1d"),
            "flow_5d": r.get("flow_5d"),
            "aum": r.get("aum"),
            "note": "Creations are not 1:1 underlying demand. Do not add this flow into beta or duration.",
        })
    leverage.sort(key=lambda x: abs(x.get("flow_5d") or 0), reverse=True)

    # --- Wrapper bid/offer intensity (large cashing in / cashing out) ---
    gross_in_1d = gross_out_1d = gross_in_5d = gross_out_5d = 0.0
    heavy = []
    for t, r in desk.items():
        if _is_levered(r):
            continue  # levered is a separate sentiment tape
        f1, f5 = r.get("flow_1d"), r.get("flow_5d")
        if f1 is not None:
            if f1 > 0:
                gross_in_1d += f1
            else:
                gross_out_1d += abs(f1)
        if f5 is not None:
            if f5 > 0:
                gross_in_5d += f5
            else:
                gross_out_5d += abs(f5)
        if f1 is not None and abs(f1) >= 1e9:
            heavy.append({"t": t, "name": r.get("name"), "flow_1d": f1, "flow_label": r.get("flow_label")})
    heavy.sort(key=lambda x: abs(x["flow_1d"]), reverse=True)
    net_1d = gross_in_1d - gross_out_1d
    net_5d = gross_in_5d - gross_out_5d
    if gross_out_1d > gross_in_1d * 1.4 and gross_out_1d >= 2e9:
        wrap_verdict = "CASHING_OUT"
        wrap_note = "Unlevered wrappers are being redeemed at scale — cash leaving the ETF complex."
    elif gross_in_1d > gross_out_1d * 1.4 and gross_in_1d >= 2e9:
        wrap_verdict = "CASHING_IN"
        wrap_note = "Unlevered wrappers are taking large creations — cash entering the ETF complex."
    else:
        wrap_verdict = "BALANCED"
        wrap_note = "Creations and redemptions are not one-sided enough to call a wrapper bid or offer."
    wrapper_intensity = {
        "gross_in_1d": round(gross_in_1d, 2),
        "gross_out_1d": round(gross_out_1d, 2),
        "net_1d": round(net_1d, 2),
        "gross_in_5d": round(gross_in_5d, 2),
        "gross_out_5d": round(gross_out_5d, 2),
        "net_5d": round(net_5d, 2),
        "heavy_1d": heavy[:12],
        "verdict": wrap_verdict,
        "note": wrap_note,
        "evidence_tier": "measured_fact",
    }

    # --- Levered / inverse positioning (not 1:1 underlying) ---
    bull = sleeves["lev_bull"]
    bear = sleeves["lev_bear"]
    vol = sleeves["lev_vol"]
    bull5, bear5 = bull.get("flow_5d") or 0, bear.get("flow_5d") or 0
    bull1, bear1 = bull.get("flow_1d") or 0, bear.get("flow_1d") or 0
    if bull5 > 8e7 and bear5 < -3e7:
        lev_verdict = "SPEC_GREED"
        lev_note = "Bull levered creating, inverse redeeming — speculative long via 2x/3x wrappers."
    elif bear5 > 8e7 and bull5 < -3e7:
        lev_verdict = "SPEC_FEAR"
        lev_note = "Inverse/short wrappers creating, bull levered redeeming — speculative short."
    elif bull5 > 8e7:
        lev_verdict = "BULL_LEVERED_BID"
        lev_note = "Bull 2x/3x wrappers taking creations. Not 1:1 SPY/QQQ demand."
    elif bear5 > 8e7:
        lev_verdict = "BEAR_LEVERED_BID"
        lev_note = "Inverse wrappers taking creations (TBT/SQQQ/SH). Fear via leverage, not a cash Treasury bid."
    else:
        lev_verdict = "QUIET"
        lev_note = "Levered/inverse complex is not printing a one-sided book."
    levered_sentiment = {
        "bull": bull, "bear": bear, "vol": vol,
        "net_bull_minus_bear_5d": round(bull5 - bear5, 2),
        "verdict": lev_verdict,
        "note": lev_note,
        "evidence_tier": "measured_fact",
        "caveat": "Creations of TQQQ/SQQQ/TBT are not 1:1 QQQ/SPY/TLT demand. Do not add them into beta or duration.",
    }

    # --- Cross-asset rotation: size + stocks vs Treasuries vs junk vs fallen ---
    mega = sleeves["size"]["mega"]
    large = sleeves["size"]["large"]
    small = sleeves["size"]["small"]
    ust = sleeves["bond_quality"]["ust"]
    duration = sleeves["bond_quality"]["duration"]
    t_bills = sleeves["bond_quality"]["t_bills"]
    bq_hy = sleeves["bond_quality"]["hy"]
    bq_ig = sleeves["bond_quality"]["ig"]
    bq_fal = sleeves["bond_quality"]["fallen"]
    spx_family = _family_gross(desk, ["SPY", "VOO", "IVV"], "flow_5d")
    spx_family_1d = _family_gross(desk, ["SPY", "VOO", "IVV"], "flow_1d")
    mega5 = mega.get("flow_5d") or 0
    large5 = large.get("flow_5d") or 0
    small5 = small.get("flow_5d") or 0
    ust5 = ust.get("flow_5d") or 0
    dur5 = duration.get("flow_5d") or 0
    bills5 = t_bills.get("flow_5d") or 0
    eq5 = mega5 + large5 + small5
    credit5 = (bq_hy.get("flow_5d") or 0) + (bq_ig.get("flow_5d") or 0) + (bq_fal.get("flow_5d") or 0)
    mega_pct = mega.get("pct_aum_5d")
    small_pct = small.get("pct_aum_5d")

    score = 0
    reasons = []
    if spx_family.get("plumbing"):
        reasons.append("S&P 500 share-class rotation (SPY/VOO/IVV swapping) — net counts, gross is plumbing")
    if eq5 > 2e8:
        score += 1
        reasons.append("equity wrappers net in")
    elif eq5 < -2e8:
        score -= 1
        reasons.append("equity wrappers net out")
    if small5 > 5e7 and (mega5 <= 0 or (small_pct is not None and mega_pct is not None and small_pct > mega_pct)):
        score += 1
        reasons.append("small-cap bid vs mega")
        size_verdict = "SIZE_ON"
    elif small5 < -5e7 and mega5 > 5e7:
        score -= 1
        reasons.append("small-cap out, mega in — flight to mega")
        size_verdict = "FLIGHT_TO_MEGA"
    elif small5 < -5e7 and mega5 < 0:
        size_verdict = "SIZE_OFF"
        score -= 1
        reasons.append("small and mega both redeeming")
    else:
        size_verdict = "MIXED"
    if hy5 > 1e8:
        score += 1
        reasons.append("junk bid")
    elif hy5 < -1e8:
        score -= 1
        reasons.append("junk offered")
    if fal5 < -2e7:
        score -= 1
        reasons.append("fallen angels redeeming")
    if dur5 < -1e8:
        score += 1
        reasons.append("duration sold")
    elif dur5 > 1e8:
        score -= 1
        reasons.append("duration bid (TLT/IEF)")
    if bills5 > 1.5e8 and eq5 < 0:
        score -= 1
        reasons.append("T-bills bid while stocks redeem — cash parking")
    if lev_verdict in ("SPEC_GREED", "BULL_LEVERED_BID"):
        score += 1
        reasons.append("bull leverage")
    elif lev_verdict in ("SPEC_FEAR", "BEAR_LEVERED_BID"):
        score -= 1
        reasons.append("bear leverage")
    if wrap_verdict == "CASHING_IN":
        score += 1
        reasons.append("wrapper cashing in")
    elif wrap_verdict == "CASHING_OUT":
        score -= 1
        reasons.append("wrapper cashing out")

    if score >= 3:
        risk_verdict, fear_greed = "RISK_ON", "GREED"
    elif score <= -3:
        risk_verdict, fear_greed = "RISK_OFF", "FEAR"
    elif score >= 1:
        risk_verdict, fear_greed = "RISK_ON_SOFT", "NEUTRAL_GREED"
    elif score <= -1:
        risk_verdict, fear_greed = "RISK_OFF_SOFT", "NEUTRAL_FEAR"
    else:
        risk_verdict, fear_greed = "MIXED", "NEUTRAL"

    if eq5 > 2e8 and ust5 < -1e8:
        rotation = "STOCKS_OVER_BONDS"
        rot_note = "Equity wrappers creating while Treasuries redeem — stocks over govvies, in dollars."
    elif eq5 < -2e8 and ust5 > 1e8:
        rotation = "BONDS_OVER_STOCKS"
        rot_note = "Treasuries creating while equity wrappers redeem — duration over stocks."
    elif hy5 > 1e8 and dur5 < -5e7:
        rotation = "CREDIT_OVER_DURATION"
        rot_note = "Junk bid, duration sold — credit over Treasuries."
    elif hy5 < -1e8 and dur5 > 5e7:
        rotation = "DURATION_OVER_CREDIT"
        rot_note = "Junk offered, duration bid — quality-up / Treasuries over credit."
    else:
        rotation = "NO_CLEAN_ROTATION"
        rot_note = "Stocks vs Treasuries vs junk are not printing a one-way book."

    cross_asset = {
        "verdict": risk_verdict,
        "fear_greed": fear_greed,
        "score": score,
        "reasons": reasons,
        "rotation": rotation,
        "rotation_note": rot_note,
        "size": size_verdict,
        "equity_5d": round(eq5, 2),
        "gov_5d": round(ust5, 2),
        "credit_5d": round(credit5, 2),
        "mega": mega, "large": large, "small": small,
        "ust": ust, "duration": duration, "t_bills": t_bills,
        "ig": bq_ig, "hy": bq_hy, "fallen": bq_fal,
        "em": sleeves["bond_quality"]["em"],
        "spx_family": spx_family,
        "spx_family_1d": spx_family_1d,
        "note": "Unlevered wrapper dollars only. Score is mechanical (equity, size, junk, duration, T-bills, leverage, wrapper intensity). Not a fear/greed index from prices. SPY/VOO/IVV gross vs net is AP plumbing when they swap.",
        "evidence_tier": "measured_fact",
    }

    conc_high = []
    hit = []
    for t, r in desk.items():
        hhi = r.get("hhi")
        top = (r.get("top") or [None])[0] or {}
        tw = _wfrac(top.get("w"))
        f1 = r.get("flow_1d")
        rec = {
            "t": t, "name": r.get("name"), "hhi": hhi,
            "holdings_n": r.get("holdings_n"),
            "holdings_complete": r.get("holdings_complete"),
            "top": top.get("t"), "top_w": tw if tw else None,
            "implied_top_1d": round(f1 * tw, 2) if (f1 is not None and tw) else None,
            "flow_1d": f1,
        }
        if hhi is not None:
            conc_high.append(rec)
        if rec["implied_top_1d"] is not None and abs(rec["implied_top_1d"]) >= 2e6:
            hit.append(rec)
    conc_high.sort(key=lambda x: -(x.get("hhi") or 0))
    hit.sort(key=lambda x: abs(x.get("implied_top_1d") or 0), reverse=True)
    concentration = {
        "high_hhi": conc_high[:18],
        "top_holding_hit": hit[:18],
        "note": "HHI ≈ Σ (weight%²). A 30-name ARKK and a 505-name SPY are not the same $1B. Implied top-holding hit = fund flow × top weight — inferred.",
        "evidence_tier": "measured_fact",
    }

    buckets = {"ABSORPTION": [], "DISTRIBUTION": [], "CONFIRMED_BID": [], "CONFIRMED_OFFER": []}
    px_by = {}
    for t, r in desk.items():
        hist = r.get("flow_hist") or []
        ret = _nav_ret(hist, 5)
        flow = r.get("flow_5d")
        if flow is None or abs(flow) < 5e7 or ret is None or abs(ret) < 0.25:
            continue
        lab = _px_label(flow, ret)
        if not lab or lab == "QUIET":
            continue
        rec = {"t": t, "name": r.get("name"), "flow_5d": flow, "nav_5d_pct": ret, "label": lab}
        buckets[lab].append(rec)
        px_by[t] = rec
    for k in buckets:
        buckets[k].sort(key=lambda x: abs(x.get("flow_5d") or 0), reverse=True)
        buckets[k] = buckets[k][:12]
    price_vs_flow = {
        **buckets,
        "note": "NAV 5d vs creation $ 5d. Up on outflow = someone else is buying (buybacks, short cover, active). Down on inflow = absorption. Wrapper NAV, not the stock tape.",
        "evidence_tier": "tier_b_inferred_allocation",
    }

    bond_leaders = []
    bond_by_fund = {}
    for t in bond_funds:
        rec = complete_hold.get(t) or {}
        holds = rec.get("holdings") or []
        drow = desk.get(t) or {}
        f5 = drow.get("flow_5d")
        rows = []
        for h in holds:
            w = _wfrac(h.get("w"))
            if not w or f5 is None:
                continue
            usd = f5 * w
            if abs(usd) < 5e5:
                continue
            rows.append({
                "cusip_or_ticker": h.get("t"),
                "name": h.get("n"),
                "etf": t,
                "w": round(w, 6),
                "implied_5d_usd": round(usd, 2),
            })
        rows.sort(key=lambda x: abs(x["implied_5d_usd"]), reverse=True)
        bond_by_fund[t] = {
            "flow_5d": f5, "n": rec.get("n"), "complete": rec.get("complete"),
            "top": rows[:12],
        }
        bond_leaders.extend(rows[:8])
    bond_leaders.sort(key=lambda x: abs(x.get("implied_5d_usd") or 0), reverse=True)
    bond_lookthrough = {
        "by_fund": bond_by_fund,
        "leaders": bond_leaders[:30],
        "note": "HYG/LQD/EMB/TLT flow × holding weight. Same math as stocks. Inferred allocation, not TRACE prints.",
        "evidence_tier": "tier_b_inferred_allocation",
    }

    crowding = []
    confirmed = []
    disagreed = []
    if lookthrough:
        crowding = [
            _compact_name(r) for r in (lookthrough.get("passive_concentration") or [])[:25]
        ]
        for r in (lookthrough.get("actual_accumulation") or [])[:20]:
            if r.get("confirmed"):
                confirmed.append(_compact_name(r))
        for r in (lookthrough.get("inflow_leaders") or [])[:40]:
            c = _compact_name(r)
            if r.get("confirmed") and c and c["t"] not in {x["t"] for x in confirmed if x}:
                confirmed.append(c)
        for src in (
            lookthrough.get("inflow_leaders") or [],
            lookthrough.get("outflow_leaders") or [],
            lookthrough.get("thematic_rotation_leaders") or [],
        ):
            for r in src:
                if r.get("shares_delta_usd") is None or r.get("confirmed") or abs(r.get("net_flow_5d_usd") or 0) < 5e7:
                    continue
                c = _compact_name(r)
                if c and c["t"] not in {x["t"] for x in disagreed if x}:
                    c["why"] = "flow×weight and share-count delta disagree — cash/custom basket or stale holdings"
                    disagreed.append(c)
        confirmed = [x for x in confirmed if x][:20]
        disagreed = [x for x in disagreed if x][:20]

    by_ticker = {}
    sleeve_of = {}
    for name, sl in (("index_beta", index_beta), ("thematic", thematic), ("sector", sector)):
        for t in sl:
            sleeve_of[t] = name
    for group, mp in (("factor", factor_map), ("credit", credit_map), ("rates", rates_map), ("crypto", crypto_map), ("size", size_map), ("bond", bond_q_map)):
        for k, ts in mp.items():
            for t in ts:
                sleeve_of.setdefault(t, group + ":" + k)
    for t in lev_bull:
        sleeve_of[t] = "lev:bull"
    for t in lev_bear:
        sleeve_of[t] = "lev:bear"
    for t in lev_vol:
        sleeve_of[t] = "lev:vol"
    for t, r in desk.items():
        px = px_by.get(t)
        top = (r.get("top") or [None])[0] or {}
        tw = _wfrac(top.get("w"))
        by_ticker[t] = {
            "kind": "etf",
            "sleeve": sleeve_of.get(t),
            "flow_1d": r.get("flow_1d"),
            "flow_5d": r.get("flow_5d"),
            "aum": r.get("aum"),
            "hhi": r.get("hhi"),
            "holdings_n": r.get("holdings_n"),
            "leverage_style": r.get("leverage_style") or r.get("leverage"),
            "levered_amount": r.get("levered_amount"),
            "levered": _is_levered(r),
            "px_flow": (px or {}).get("label"),
            "nav_5d_pct": (px or {}).get("nav_5d_pct"),
            "top": top.get("t"),
            "top_w": tw or None,
            "implied_top_1d": round((r.get("flow_1d") or 0) * tw, 2) if tw else None,
            "flow_label": r.get("flow_label"),
        }
    for rec in crowding + confirmed + disagreed:
        if not rec or not rec.get("t"):
            continue
        t = rec["t"]
        cur = by_ticker.get(t) or {"kind": "name"}
        cur["kind"] = cur.get("kind") or "name"
        cur["crowding_pct"] = rec.get("etf_ownership_pct")
        cur["confirmed"] = rec.get("confirmed")
        cur["flow_type"] = rec.get("flow_type")
        cur["implied_5d"] = rec.get("net_flow_5d_usd")
        cur["shares_delta_usd"] = rec.get("shares_delta_usd")
        if rec in disagreed or rec.get("why"):
            cur["disagreed"] = True
        by_ticker[t] = cur

    verdicts = {
        "risk": risk_verdict,
        "fear_greed": fear_greed,
        "rotation": rotation,
        "size": size_verdict,
        "wrapper": wrap_verdict,
        "levered": lev_verdict,
        "thematic_vs_index": tv_verdict,
        "factor": factor_rot,
        "credit": credit_verdict,
        "crypto": ("WRAPPER_BID" if (btc.get("flow_5d") or 0) > 1e8
                   else "WRAPPER_OFFER" if (btc.get("flow_5d") or 0) < -1e8
                   else "QUIET"),
        "leverage_flags": len(leverage),
    }
    return {
        "generated_at": generated,
        "engine": "justhodl-etf-global-desk",
        "version": VERSION,
        "evidence_tier": "mixed",
        "status": "LIVE",
        "thesis": "Cross-asset ETF Global tape: size, stocks vs Treasuries vs junk vs fallen angels, wrapper bid/offer, levered sentiment. Fund creations are facts. Name-level dollars are inferred.",
        "verdicts": verdicts,
        "cross_asset": cross_asset,
        "wrapper_intensity": wrapper_intensity,
        "levered_sentiment": levered_sentiment,
        "sleeves": {
            "index_beta": sleeves["index_beta"],
            "thematic": sleeves["thematic"],
            "sector": sleeves["sector"],
            "size": sleeves["size"],
        },
        "thematic_vs_index": thematic_vs_index,
        "factor": {"ranked": factor_ranked, "rotation": factor_rot, "evidence_tier": "measured_fact"},
        "credit_stack": credit_stack,
        "crypto_wrapper": crypto_wrapper,
        "leverage": leverage,
        "concentration": concentration,
        "price_vs_flow": price_vs_flow,
        "crowding": crowding,
        "confirmed": confirmed,
        "disagreed": disagreed,
        "bond_lookthrough": bond_lookthrough,
        "by_ticker": by_ticker,
        "methodology": {
            "fund_flow": "Massive ETF Global fund-flows — creation/redemption dollars, a fact",
            "name_flow": "fund_flow × holdings weight — inferred, not a print",
            "share_delta": "change in reported shares_held across two snapshots — inferred",
            "price_vs_flow": "wrapper NAV 5d vs fund_flow 5d",
            "hhi": "Σ (weight%²) on a complete holdings snapshot only",
            "leverage": "profile leverage_style / levered_amount — do not treat as 1:1",
            "crypto": "IBIT/FBTC/ETHA wrapper bid, not exchange volume",
            "bonds": "same look-through on HYG/LQD/EMB/TLT holdings",
            "cross_asset": "unlevered wrapper $ across mega/large/small equity vs duration vs T-bills vs IG vs HY vs fallen angels. SPY/VOO/IVV gross vs net flags share-class plumbing.",
            "wrapper": "gross creations vs redemptions on unlevered desk funds — cashing in/out of the ETF complex",
            "levered": "2x/3x/inverse creations as speculative positioning, never added into beta or duration",
        },
        "caveats": [
            "Do not label any of this institutional buying/selling of a stock.",
            "APs hedge a basket. SPY redemption hits every S&P name mechanically.",
            "Custom/cash baskets break flow×weight. Disagreement is a tell.",
            "Levered/inverse creations are not 1:1 underlying demand.",
            "21d windows on the desk may be partial — see flow_windows.complete.",
        ],
    }


def lambda_handler(event, context=None):
    t0 = time.time()
    if not KEY:
        payload = {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "status": "NO_KEY",
            "engine": "justhodl-etf-global-desk",
            "version": VERSION,
            "by_etf": {},
        }
        _put("data/etf-desk.json", payload)
        return payload
    tickers = list(dict.fromkeys(DESK))
    by = {}
    http = {"flows": {}, "profiles": {}, "constituents": {}}
    errs = {}
    with ThreadPoolExecutor(max_workers=8) as ex:
        futs = {ex.submit(harvest_one, t): t for t in tickers}
        for fut in as_completed(futs):
            t = futs[fut]
            try:
                row = fut.result()
            except Exception as e:
                row = {"ticker": t, "ok": {}, "http": {}, "error": str(e)[:160]}
            by[t] = row
            for prod in ("flows", "profiles", "constituents"):
                http[prod][t] = (row.get("http") or {}).get(prod)
            if row.get("err"):
                errs[t] = row["err"]
    n_ok = {
        prod: sum(1 for r in by.values() if (r.get("ok") or {}).get(prod))
        for prod in ("flows", "profiles", "constituents")
    }
    desk = {}
    complete_hold = {}
    for t, r in by.items():
        fw = r.get("flow_windows") or {}
        desk[t] = {
            "ticker": t,
            "name": r.get("name"),
            "issuer": r.get("issuer"),
            "aum": r.get("aum"),
            "nav": r.get("nav"),
            "shares": r.get("shares"),
            "er": r.get("er"),
            "asset_class": r.get("asset_class"),
            "category": r.get("category"),
            "benchmark": r.get("benchmark"),
            "inception": r.get("inception"),
            "holdings_n": r.get("holdings_n"),
            "holdings_n_received": r.get("holdings_n_received"),
            "holdings_complete": r.get("holdings_complete"),
            "holdings_weight_sum": r.get("holdings_weight_sum"),
            "hhi": r.get("hhi"),
            "adv": r.get("adv"),
            "spread": r.get("spread"),
            "discount_premium": r.get("discount_premium"),
            "leverage": r.get("leverage_style") or r.get("leverage"),
            "leverage_style": r.get("leverage_style"),
            "levered_amount": r.get("levered_amount"),
            "creation_unit": r.get("creation_unit"),
            "flow_1d": r.get("flow_1d"),
            "flow_5d": r.get("flow_5d"),
            "flow_21d": r.get("flow_21d"),
            "flow_63d": r.get("flow_63d"),
            "flow_windows": fw,
            "flow_z": r.get("flow_z"),
            "flow_label": _label(r.get("flow_1d"), r.get("flow_z")),
            "flow_asof": r.get("flow_asof"),
            "flow_effective": r.get("flow_effective"),
            "flow_hist": r.get("flow_hist") or [],
            "sector": r.get("sector") or [],
            "geo": r.get("geo") or [],
            "ccy": r.get("ccy") or [],
            "sector_full": r.get("sector_full") or {},
            "geo_full": r.get("geo_full") or {},
            "ccy_full": r.get("ccy_full") or {},
            "industry_full": r.get("industry_full") or {},
            "top": r.get("top") or [],
            "ok": r.get("ok") or {},
        }
        if r.get("all_holdings"):
            complete_hold[t] = {
                "asof": r.get("holdings_asof"),
                "n": r.get("holdings_n_received") or len(r["all_holdings"]),
                "n_profile": r.get("holdings_n"),
                "weight_sum": r.get("holdings_weight_sum"),
                "complete": r.get("holdings_complete"),
                "pages": r.get("holdings_pages"),
                "holdings": r["all_holdings"],
            }
    generated = datetime.now(timezone.utc).isoformat()
    status = "LIVE" if n_ok["flows"] else "EMPTY"
    if n_ok["constituents"] < max(1, int(0.5 * len(desk))):
        # Don't hide a holdings outage behind flows LIVE.
        status = "PARTIAL"
    payload = {
        "generated_at": generated,
        "engine": "justhodl-etf-global-desk",
        "version": VERSION,
        "source": "Massive ETF Global fund-flows + constituents + profiles",
        "status": status,
        "n": len(desk),
        "n_ok": n_ok,
        "elapsed_s": round(time.time() - t0, 1),
        "by_etf": desk,
        "inflows": sorted(
            [{"t": t, **desk[t]} for t in desk if (desk[t].get("flow_1d") or 0) > 0],
            key=lambda x: x.get("flow_1d") or 0, reverse=True,
        )[:15],
        "outflows": sorted(
            [{"t": t, **desk[t]} for t in desk if (desk[t].get("flow_1d") or 0) < 0],
            key=lambda x: x.get("flow_1d") or 0,
        )[:15],
    }
    _put("data/etf-desk.json", payload)
    _put("data/etf-holdings-complete.json", {
        "generated_at": generated,
        "engine": "justhodl-etf-global-desk",
        "version": VERSION,
        "n_etfs": len(complete_hold),
        "n_holdings": sum(len(v.get("holdings") or []) for v in complete_hold.values()),
        "by_etf": complete_hold,
    })
    idx = {}
    for t, rec in complete_hold.items():
        drow = desk.get(t) or {}
        for h in rec.get("holdings") or []:
            stck = (h.get("t") or "").upper()
            if not stck:
                continue
            idx.setdefault(stck, []).append({
                "etf": t,
                "w": h.get("w"),
                "n": h.get("n"),
                "flow_1d": drow.get("flow_1d"),
                "flow_5d": drow.get("flow_5d"),
                "flow_label": drow.get("flow_label"),
                "aum": drow.get("aum"),
                "name": drow.get("name"),
            })
    for stck in idx:
        idx[stck].sort(key=lambda x: abs(x.get("w") or 0), reverse=True)
    _put("data/etf-holdings-index.json", {
        "generated_at": generated,
        "engine": "justhodl-etf-global-desk",
        "version": VERSION,
        "n_stocks": len(idx),
        "n_links": sum(len(v) for v in idx.values()),
        "complete": True,
        "by_stock": idx,
    })
    _put("data/etf-global.json", {
        "generated_at": generated,
        "schema_version": 2,
        "source": "justhodl-etf-global-desk",
        "status": payload["status"],
        "n_ok": n_ok,
        "n": len(desk),
        "products": ["fund-flows", "constituents", "profiles"],
        "version": VERSION,
    })
    _put("data/etf-global-desk-meta.json", {
        "generated_at": generated,
        "n_ok": n_ok,
        "http": http,
        "errors": {k: v for k, v in list(errs.items())[:12]},
        "elapsed_s": payload["elapsed_s"],
        "version": VERSION,
    })
    lookthrough = _load_s3("data/flow-lookthrough.json")
    derived = _build_derived(desk, complete_hold, generated, lookthrough)
    _put("data/etf-derived.json", derived)
    return {
        "status": payload["status"],
        "n": len(desk),
        "n_ok": n_ok,
        "elapsed_s": payload["elapsed_s"],
        "version": VERSION,
        "derived": {
            "verdicts": derived.get("verdicts"),
            "n_by_ticker": len(derived.get("by_ticker") or {}),
            "n_crowding": len(derived.get("crowding") or []),
            "n_leverage": len(derived.get("leverage") or []),
        },
    }


if __name__ == "__main__":
    print(json.dumps(lambda_handler({}), indent=2))
