"""justhodl-flow-lookthrough — single-name flow-pressure board via ETF look-through.

Thesis: when real dollars create/redeem ETF shares, the fund must mechanically
buy/sell its underlying holdings. Aggregating each ETF's net flow across ALL the
equity ETFs that hold a given stock reveals which single names are receiving the
most mechanical buying/selling pressure this week — and whether that pressure is
broad-market beta or genuine sector/thematic rotation (the alpha-relevant part).

Inputs:
  - etf-flows/daily.json  (per-ETF daily/5d/21d $ flows, 300-ETF universe)
  - ETF Global Constituents API (current holdings + market-value weights)
  - screener/data.json     (market caps, to normalise mega-cap dominance)

Output: data/flow-lookthrough.json
  inflow_leaders / outflow_leaders ranked by net 5d $ pressure, each with:
    net_flow_5d_usd, net_flow_daily_usd, flow_bps_mcap, n_etfs, drivers[],
    broad_flow_usd vs thematic_flow_usd (rotation isolation).

Design notes (institutional):
  - Only equity-like constituents counted (regex + name filter) -> leveraged /
    inverse / bond / commodity / crypto-futures ETFs self-exclude because their
    holdings are swaps/futures/cash, not clean equity tickers.
  - Weight = constituent market_value / sum(market_value) on the latest snapshot.
  - Constituents cached to S3 (etf-constituents/{ETF}.json, 7d TTL) — holdings are
    stable; avoids hammering the metered API daily.
  - Broad ETFs (SPY/VOO/QQQ/VTI...) tagged so we can separate index beta from
    sector/thematic rotation per name.
"""
import os
import re
import json
import time
import urllib.request
import urllib.error
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone, timedelta

import boto3

S3 = boto3.client("s3", region_name="us-east-1")
SSM = boto3.client("ssm", region_name="us-east-1")
BUCKET = "justhodl-dashboard-live"
FLOWS_KEY = "etf-flows/daily.json"
SCREENER_KEY = "screener/data.json"
OUT_KEY = "data/flow-lookthrough.json"
CACHE_PREFIX = "etf-constituents/"
CONSTIT_URL = "https://api.polygon.io/etf-global/v1/constituents"

TOP_N_ETFS = 70          # cap API load: deepest-flow ETFs only
CACHE_TTL_S = 7 * 86400  # holdings are stable; refresh weekly
MIN_FLOW_USD = 5e7       # ignore trivially-small ETF flows ($50M)

BROAD = {"SPY", "VOO", "IVV", "VTI", "QQQ", "QQQM", "IWM", "IWB", "DIA",
         "VUG", "VTV", "RSP", "SPLG", "SCHX", "ITOT", "SCHB"}

_KEY = {}


def massive_key():
    if _KEY.get("k") is not None:
        return _KEY["k"]
    k = os.environ.get("MASSIVE_API_KEY")
    if not k:
        try:
            k = SSM.get_parameter(Name="/justhodl/massive-api-key",
                                  WithDecryption=True)["Parameter"]["Value"]
        except Exception:
            k = ""
    _KEY["k"] = k
    return k


def getj(key):
    try:
        return json.loads(S3.get_object(Bucket=BUCKET, Key=key)["Body"].read())
    except Exception:
        return None


def http_json(url, timeout=20):
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "justhodl-lookthrough/1.0"})
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return json.loads(r.read())
    except Exception:
        return None


_EQUITY_RE = re.compile(r"^[A-Z]{1,5}([.\-/][A-Z])?$")
_BAD_NAME = re.compile(r"(future|curncy|swap|cash|repo|t-bill|bill|index|receivable|payable|margin|collateral)", re.I)


def is_equity(ticker, name):
    if not ticker or not isinstance(ticker, str):
        return False
    t = ticker.strip().upper()
    if " " in t or not _EQUITY_RE.match(t):
        return False
    if name and _BAD_NAME.search(str(name)):
        return False
    return True


def fetch_constituents(etf):
    """Latest equity-constituent weights for one ETF. S3-cached (7d TTL)."""
    cache_key = f"{CACHE_PREFIX}{etf}.json"
    # try cache
    try:
        head = S3.head_object(Bucket=BUCKET, Key=cache_key)
        age = (datetime.now(timezone.utc) - head["LastModified"]).total_seconds()
        if age < CACHE_TTL_S:
            cached = getj(cache_key)
            if cached and cached.get("holdings"):
                return cached
    except Exception:
        pass

    k = massive_key()
    if not k:
        return None
    gte = (datetime.now(timezone.utc).date() - timedelta(days=12)).isoformat()
    rows = []
    url = (f"{CONSTIT_URL}?composite_ticker={etf}&processed_date.gte={gte}"
           f"&order=desc&sort=processed_date&limit=1000&apiKey={k}")
    for _ in range(3):  # paginate a few pages max
        j = http_json(url)
        if not j:
            break
        rows.extend(j.get("results") or [])
        nxt = j.get("next_url")
        if not nxt:
            break
        url = nxt + (f"&apiKey={k}" if "apiKey=" not in nxt else "")
    if not rows:
        return None
    latest_pd = max((r.get("processed_date") or "") for r in rows)
    snap = [r for r in rows if r.get("processed_date") == latest_pd]
    holdings = []
    for r in snap:
        t = r.get("constituent_ticker")
        nm = r.get("constituent_name")
        mv = r.get("market_value")
        if not is_equity(t, nm):
            continue
        if not isinstance(mv, (int, float)) or mv <= 0:
            continue
        holdings.append({"ticker": t.strip().upper(), "mv": float(mv),
                         "rank": r.get("constituent_rank")})
    if not holdings:
        return None
    total = sum(h["mv"] for h in holdings)
    for h in holdings:
        h["weight"] = h["mv"] / total if total else 0.0
    out = {"etf": etf, "processed_date": latest_pd, "n_holdings": len(holdings),
           "holdings": holdings, "fetched_at": datetime.now(timezone.utc).isoformat()}
    try:
        S3.put_object(Bucket=BUCKET, Key=cache_key,
                      Body=json.dumps(out, default=str).encode(),
                      ContentType="application/json")
    except Exception:
        pass
    return out


def lambda_handler(event, context):
    t0 = time.time()
    flows = getj(FLOWS_KEY)
    if not flows or not flows.get("metrics"):
        return {"statusCode": 500, "body": "no etf-flows/daily.json"}
    metrics = flows["metrics"]

    # market caps for normalisation
    mcap = {}
    scr = getj(SCREENER_KEY)
    if scr:
        rows = scr.get("data") or scr.get("stocks") or scr.get("rows") or []
        for r in rows if isinstance(rows, list) else []:
            tk = (r.get("ticker") or r.get("symbol") or "").upper()
            mc = r.get("marketCap") or r.get("market_cap") or r.get("mktCap")
            if tk and isinstance(mc, (int, float)) and mc > 0:
                mcap[tk] = float(mc)

    # pick the deepest-flow ETFs (use 5d flow, fall back to daily)
    def flow5(m):
        v = m.get("flow_5d_usd")
        if v is None:
            v = m.get("daily_flow_usd")
        return v or 0.0

    cand = [m for m in metrics if abs(flow5(m)) >= MIN_FLOW_USD]
    cand.sort(key=lambda m: abs(flow5(m)), reverse=True)
    cand = cand[:TOP_N_ETFS]

    # fetch constituents in parallel
    constit = {}
    with ThreadPoolExecutor(max_workers=8) as ex:
        futs = {ex.submit(fetch_constituents, m["ticker"]): m["ticker"] for m in cand}
        for f in as_completed(futs):
            try:
                c = f.result()
            except Exception:
                c = None
            if c:
                constit[futs[f]] = c

    # attribute flow -> single names
    agg = {}  # ticker -> dict
    for m in cand:
        etf = m["ticker"]
        c = constit.get(etf)
        if not c:
            continue
        f5 = m.get("flow_5d_usd")
        if f5 is None:
            f5 = m.get("daily_flow_usd")
        fd = m.get("daily_flow_usd") or 0.0
        f21 = m.get("flow_21d_usd") or 0.0
        f5 = f5 or 0.0
        is_broad = etf in BROAD
        for h in c["holdings"]:
            w = h["weight"]
            tk = h["ticker"]
            a = agg.setdefault(tk, {"ticker": tk, "net_5d": 0.0, "net_daily": 0.0,
                                    "net_21d": 0.0, "broad_5d": 0.0, "thematic_5d": 0.0,
                                    "drivers": []})
            contrib5 = f5 * w
            a["net_5d"] += contrib5
            a["net_daily"] += fd * w
            a["net_21d"] += f21 * w
            if is_broad:
                a["broad_5d"] += contrib5
            else:
                a["thematic_5d"] += contrib5
            a["drivers"].append({"etf": etf, "contrib_5d_usd": round(contrib5, 0),
                                 "weight": round(w, 5), "broad": is_broad})

    # finalise rows
    rows = []
    for tk, a in agg.items():
        a["drivers"].sort(key=lambda d: abs(d["contrib_5d_usd"]), reverse=True)
        a["drivers"] = a["drivers"][:5]
        mc = mcap.get(tk)
        a["net_flow_5d_usd"] = round(a.pop("net_5d"), 0)
        a["net_flow_daily_usd"] = round(a.pop("net_daily"), 0)
        a["net_flow_21d_usd"] = round(a.pop("net_21d"), 0)
        a["broad_flow_5d_usd"] = round(a.pop("broad_5d"), 0)
        a["thematic_flow_5d_usd"] = round(a.pop("thematic_5d"), 0)
        a["n_etfs"] = len(a["drivers"])
        a["flow_bps_mcap"] = round(a["net_flow_5d_usd"] / mc * 1e4, 2) if mc else None
        a["market_cap_usd"] = mc
        # rotation tag: thematic-dominated demand is the alpha-relevant kind
        if abs(a["net_flow_5d_usd"]) > 0:
            share = a["thematic_flow_5d_usd"] / a["net_flow_5d_usd"]
            a["flow_type"] = ("THEMATIC_ROTATION" if share > 0.6 else
                              "BROAD_BETA" if share < 0.2 else "MIXED")
        else:
            a["flow_type"] = "NEUTRAL"
        rows.append(a)

    rows.sort(key=lambda r: r["net_flow_5d_usd"], reverse=True)
    inflow = [r for r in rows if r["net_flow_5d_usd"] > 0][:40]
    outflow = sorted([r for r in rows if r["net_flow_5d_usd"] < 0],
                     key=lambda r: r["net_flow_5d_usd"])[:40]
    # alpha view: strongest THEMATIC pressure (rotation, not beta), normalised by mcap
    thematic = sorted(
        [r for r in rows if r["flow_type"] == "THEMATIC_ROTATION" and r["flow_bps_mcap"] is not None],
        key=lambda r: abs(r["flow_bps_mcap"]), reverse=True)[:25]

    out = {
        "engine": "justhodl-flow-lookthrough",
        "version": "1.0.0",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "thesis": "ETF creations/redemptions force mechanical buying/selling of "
                  "underlying holdings. Aggregated across every equity ETF a name "
                  "sits in, this is the net single-name flow pressure — split into "
                  "broad-market beta vs sector/thematic rotation (the tradeable part).",
        "flows_asof": flows.get("generated_at"),
        "n_etfs_used": len(constit),
        "n_names": len(rows),
        "inflow_leaders": inflow,
        "outflow_leaders": outflow,
        "thematic_rotation_leaders": thematic,
        "methodology": {
            "attribution": "name_flow = sum over ETFs of (ETF_net_flow_usd * constituent_weight)",
            "weight": "constituent market_value / sum(market_value) on latest snapshot",
            "equity_filter": "non-equity constituents dropped -> leveraged/inverse/bond/commodity ETFs self-exclude",
            "flow_type": "THEMATIC_ROTATION when >60% of net pressure is from non-broad ETFs",
            "normalisation": "flow_bps_mcap = net 5d flow as basis points of the name's market cap",
        },
        "caveats": [
            "Flow data ~1d lagged (ETF Global EOD processing).",
            "Holdings cached 7d; intra-week rebalances may lag.",
            "Broad-index flows dominate mega-caps in $ terms; use flow_type / flow_bps_mcap for rotation.",
        ],
        "elapsed_s": round(time.time() - t0, 1),
    }
    S3.put_object(Bucket=BUCKET, Key=OUT_KEY,
                  Body=json.dumps(out, default=str).encode(),
                  ContentType="application/json",
                  CacheControl="public, max-age=600")
    return {"statusCode": 200, "body": json.dumps({
        "n_names": len(rows), "n_etfs_used": len(constit),
        "top_inflow": inflow[0]["ticker"] if inflow else None,
        "top_outflow": outflow[0]["ticker"] if outflow else None,
        "elapsed_s": out["elapsed_s"]})}
