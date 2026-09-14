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

VERSION = "1.0.0"
BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
KEY = os.environ.get("POLYGON_KEY") or os.environ.get("POLYGON_API_KEY") or os.environ.get("MASSIVE_API_KEY") or ""
HOSTS = ("https://api.massive.com", "https://api.polygon.io")
UA = {"User-Agent": "JustHodl-ETFGlobalDesk/1.0"}
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


def _get(path, extra, timeout=20):
    params = {"limit": extra.pop("limit", "120"), "apiKey": KEY}
    params.update({k: v for k, v in extra.items() if v is not None})
    last = (0, {"error": "no host"})
    for host in HOSTS:
        url = host + path + "?" + urllib.parse.urlencode(params)
        req = urllib.request.Request(url, headers=UA)
        try:
            with urllib.request.urlopen(req, timeout=timeout) as r:
                body = json.loads(r.read().decode("utf-8", "replace"))
                rows = body.get("results") if isinstance(body, dict) else None
                if r.status == 200 and isinstance(rows, list):
                    return 200, body, host
                last = (r.status, body)
        except urllib.error.HTTPError as e:
            raw = e.read().decode("utf-8", "replace")[:240]
            last = (e.code, {"error": raw, "status": "HTTP_%s" % e.code})
        except Exception as e:
            last = (0, {"error": str(e)[:180]})
    return last[0], last[1], HOSTS[0]


def _latest(rows, date_key="processed_date"):
    if not rows:
        return None
    return max(rows, key=lambda r: str(r.get(date_key) or ""))


def _sum_n(rows, n):
    vals = [_num(r.get("fund_flow")) for r in rows if r.get("fund_flow") is not None]
    if not vals:
        return None
    take = vals[:n] if len(vals) >= n else vals
    return sum(take)


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
    for k in ("net_expense_ratio", "expense_ratio", "net_expenses", "management_fee",
              "net_expense", "total_expense_ratio"):
        v = _num(p.get(k))
        if v is None:
            continue
        # ETF Global stores some fees as decimals (0.000945) and some as percent.
        return v * 100 if v < 0.2 else v
    return None


def harvest_one(ticker):
    out = {"ticker": ticker, "ok": {}, "http": {}}
    # flows — last ~90 sessions, newest first
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
        out["ok"]["flows"] = True
        out["nav"] = nav
        out["shares"] = sh
        out["aum_from_nav"] = (nav * sh) if (nav is not None and sh is not None) else None
        out["flow_1d"] = flows[0] if flows else None
        out["flow_5d"] = _sum_n(rows, 5)
        out["flow_21d"] = _sum_n(rows, 21)
        out["flow_63d"] = _sum_n(rows, 63)
        out["flow_asof"] = latest.get("processed_date")
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
        out["holdings_n"] = p.get("holdings_count") or p.get("number_of_holdings")
        out["adv"] = _num(p.get("avg_daily_trading_volume"))
        out["spread"] = _num(p.get("bid_ask_spread"))
        out["leverage"] = p.get("leverage") or p.get("leverage_factor")
        out["sector"] = _top_exp(p.get("sector_exposure") or p.get("industry_exposure") or {})
        out["geo"] = _top_exp(p.get("geographic_exposure") or p.get("country_exposure") or {})
        out["ccy"] = _top_exp(p.get("currency_exposure") or {})
        out["profile_asof"] = p.get("processed_date") or p.get("effective_date")
    else:
        out["ok"]["profiles"] = False
        out["aum"] = out.get("aum_from_nav")

    st, body, host = _get("/etf-global/v1/constituents", {
        "composite_ticker": ticker,
        "sort": "constituent_rank.asc",
        "limit": "80",
    })
    out["http"]["constituents"] = st
    crows = (body or {}).get("results") if st == 200 else []
    # Keep only the newest processed_date slice so we don't mix as-ofs.
    if crows:
        asof = max(str(r.get("processed_date") or "") for r in crows)
        crows = [r for r in crows if str(r.get("processed_date") or "") == asof]
        crows.sort(key=lambda r: _num(r.get("weight")) or 0, reverse=True)
        weights = [_num(r.get("weight")) or 0 for r in crows]
        hhi = sum(w * w for w in weights) * 10000  # weights are fractions
        out["ok"]["constituents"] = True
        out["holdings_n"] = out.get("holdings_n") or len(crows)
        out["hhi"] = round(hhi, 1)
        out["top"] = [{
            "t": r.get("constituent_ticker"),
            "n": r.get("constituent_name"),
            "w": _num(r.get("weight")),
            "mv": _num(r.get("market_value")),
            "rank": r.get("constituent_rank"),
        } for r in crows[:12]]
        out["holdings_asof"] = asof
    else:
        out["ok"]["constituents"] = False
        out["top"] = []
        out["hhi"] = None
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
    n_ok = {
        prod: sum(1 for r in by.values() if (r.get("ok") or {}).get(prod))
        for prod in ("flows", "profiles", "constituents")
    }
    desk = {}
    for t, r in by.items():
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
            "hhi": r.get("hhi"),
            "adv": r.get("adv"),
            "spread": r.get("spread"),
            "flow_1d": r.get("flow_1d"),
            "flow_5d": r.get("flow_5d"),
            "flow_21d": r.get("flow_21d"),
            "flow_63d": r.get("flow_63d"),
            "flow_z": r.get("flow_z"),
            "flow_label": _label(r.get("flow_1d"), r.get("flow_z")),
            "flow_asof": r.get("flow_asof"),
            "sector": r.get("sector") or [],
            "geo": r.get("geo") or [],
            "top": r.get("top") or [],
            "ok": r.get("ok") or {},
        }
    generated = datetime.now(timezone.utc).isoformat()
    payload = {
        "generated_at": generated,
        "engine": "justhodl-etf-global-desk",
        "version": VERSION,
        "source": "Massive ETF Global fund-flows + constituents + profiles",
        "status": "LIVE" if n_ok["flows"] else "EMPTY",
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
    _put("data/etf-global.json", {
        "generated_at": generated,
        "schema_version": 2,
        "source": "justhodl-etf-global-desk",
        "status": payload["status"],
        "n_ok": n_ok,
        "n": len(desk),
        "products": ["fund-flows", "constituents", "profiles"],
    })
    _put("data/etf-global-desk-meta.json", {
        "generated_at": generated,
        "n_ok": n_ok,
        "http": http,
        "elapsed_s": payload["elapsed_s"],
    })
    return {
        "status": payload["status"],
        "n": len(desk),
        "n_ok": n_ok,
        "elapsed_s": payload["elapsed_s"],
    }


if __name__ == "__main__":
    print(json.dumps(lambda_handler({}), indent=2))
