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

VERSION = "1.3.0"
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
    return {
        "status": payload["status"],
        "n": len(desk),
        "n_ok": n_ok,
        "elapsed_s": payload["elapsed_s"],
        "version": VERSION,
    }


if __name__ == "__main__":
    print(json.dumps(lambda_handler({}), indent=2))
