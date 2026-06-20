"""justhodl-flow-lookthrough v2.0 — single-name flow-pressure via ETF look-through.

Two complementary mechanical-demand signals per name:
  1. FLOW ATTRIBUTION  : sum over ETFs of (ETF net $ flow * constituent weight).
     Smooth, daily, but an approximation.
  2. SHARE-COUNT DELTA : sum over ETFs of (shares_held_now - shares_held_prev) *
     implied price = the $ the ETF complex ACTUALLY added/removed for that name
     via creation/redemption + rebalancing. Precise, lumpier.
Names where BOTH agree = high-conviction mechanical demand.

Plus an INDEX-EVENTS view: constituent additions/deletions across ETFs
(reconstitution = the index-effect anomaly).

Uses the full ETF Global constituents surface: provided `weight`, `asset_class`/
`security_type` (clean equity filter, no regex), `shares_held` history.
"""
import os
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
CACHE_PREFIX = "etf-constituents-v2/"
CONSTIT_URL = "https://api.polygon.io/etf-global/v1/constituents"

TOP_N_ETFS = 70
CACHE_TTL_S = 3 * 86400      # refresh twice a week (capture rebalances)
MIN_FLOW_USD = 5e7

BROAD = {"SPY", "VOO", "IVV", "VTI", "QQQ", "QQQM", "IWM", "IWB", "DIA",
         "VUG", "VTV", "RSP", "SPLG", "SCHX", "ITOT", "SCHB"}
EQUITY_SECTYPES = ("common", "adr", "ads", "reit", "shs", "ordinary")

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


def http_json(url, timeout=25):
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "justhodl-lookthrough/2.0"})
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return json.loads(r.read())
    except Exception:
        return None


def is_equity_row(r):
    ac = (r.get("asset_class") or "").lower()
    st = (r.get("security_type") or "").lower()
    if ac and ac != "equity":
        return False
    if st and not any(k in st for k in EQUITY_SECTYPES):
        return False
    t = r.get("constituent_ticker")
    if not t or " " in str(t):
        return False
    return True


def _snapshot(etf, gte, lte, k):
    url = (f"{CONSTIT_URL}?composite_ticker={etf}&processed_date.gte={gte}"
           f"&processed_date.lte={lte}&order=desc&sort=processed_date&limit=700&apiKey={k}")
    j = http_json(url)
    rows = (j or {}).get("results") or []
    if not rows:
        return None, {}
    pd = max((r.get("processed_date") or "") for r in rows)
    snap = {}
    for r in rows:
        if r.get("processed_date") != pd or not is_equity_row(r):
            continue
        t = r["constituent_ticker"].strip().upper()
        mv = r.get("market_value")
        sh = r.get("shares_held")
        w = r.get("weight")
        if not isinstance(mv, (int, float)) or mv <= 0:
            continue
        snap[t] = {"ticker": t, "mv": float(mv),
                   "shares": float(sh) if isinstance(sh, (int, float)) else None,
                   "weight": float(w) if isinstance(w, (int, float)) else None,
                   "rank": r.get("constituent_rank")}
    return pd, snap


def fetch_constituents(etf):
    """Latest + ~30d-prior equity snapshots for one ETF. S3-cached."""
    cache_key = f"{CACHE_PREFIX}{etf}.json"
    try:
        head = S3.head_object(Bucket=BUCKET, Key=cache_key)
        if (datetime.now(timezone.utc) - head["LastModified"]).total_seconds() < CACHE_TTL_S:
            c = getj(cache_key)
            if c and c.get("holdings"):
                return c
    except Exception:
        pass
    k = massive_key()
    if not k:
        return None
    today = datetime.now(timezone.utc).date()
    pd_new, new = _snapshot(etf, (today - timedelta(days=8)).isoformat(), today.isoformat(), k)
    pd_old, old = _snapshot(etf, (today - timedelta(days=45)).isoformat(),
                            (today - timedelta(days=22)).isoformat(), k)
    if not new:
        return None
    total_mv = sum(h["mv"] for h in new.values())
    holdings = []
    for t, h in new.items():
        w = h["weight"] if h["weight"] is not None else (h["mv"] / total_mv if total_mv else 0.0)
        prev = old.get(t, {})
        sh_prev = prev.get("shares")
        sh_now = h["shares"]
        # $ the ETF actually added/removed for this name (share delta * implied price)
        delta_usd = None
        if sh_now is not None and sh_prev is not None and sh_now:
            price = h["mv"] / sh_now if sh_now else None
            if price:
                delta_usd = (sh_now - sh_prev) * price
        holdings.append({"ticker": t, "weight": w, "mv": h["mv"],
                         "shares": sh_now, "shares_delta_usd": delta_usd})
    adds = [t for t in new if t not in old] if old else []
    dels = [t for t in old if t not in new] if old else []
    out = {"etf": etf, "processed_date": pd_new, "prior_date": pd_old,
           "n_holdings": len(holdings), "holdings": holdings,
           "additions": adds, "deletions": dels,
           "has_delta": bool(old), "fetched_at": datetime.now(timezone.utc).isoformat()}
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

    mcap = {}
    scr = getj(SCREENER_KEY)
    if scr:
        rows = scr.get("data") or scr.get("stocks") or scr.get("rows") or []
        for r in rows if isinstance(rows, list) else []:
            tk = (r.get("ticker") or r.get("symbol") or "").upper()
            mc = r.get("marketCap") or r.get("market_cap") or r.get("mktCap")
            if tk and isinstance(mc, (int, float)) and mc > 0:
                mcap[tk] = float(mc)

    def flow5(m):
        v = m.get("flow_5d_usd")
        return (v if v is not None else m.get("daily_flow_usd")) or 0.0

    cand = sorted([m for m in metrics if abs(flow5(m)) >= MIN_FLOW_USD],
                  key=lambda m: abs(flow5(m)), reverse=True)[:TOP_N_ETFS]

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

    agg = {}
    index_events = []
    for m in cand:
        etf = m["ticker"]
        c = constit.get(etf)
        if not c:
            continue
        f5 = m.get("flow_5d_usd")
        f5 = (f5 if f5 is not None else m.get("daily_flow_usd")) or 0.0
        fd = m.get("daily_flow_usd") or 0.0
        is_broad = etf in BROAD
        for ad in c.get("additions", []):
            index_events.append({"ticker": ad, "etf": etf, "event": "ADDED"})
        for dl in c.get("deletions", []):
            index_events.append({"ticker": dl, "etf": etf, "event": "DROPPED"})
        for h in c["holdings"]:
            tk = h["ticker"]
            w = h["weight"] or 0.0
            a = agg.setdefault(tk, {"ticker": tk, "flow_5d": 0.0, "daily": 0.0,
                                    "broad_5d": 0.0, "thematic_5d": 0.0,
                                    "shares_delta_usd": 0.0, "delta_seen": False,
                                    "drivers": []})
            contrib5 = f5 * w
            a["flow_5d"] += contrib5
            a["daily"] += fd * w
            if is_broad:
                a["broad_5d"] += contrib5
            else:
                a["thematic_5d"] += contrib5
            if h.get("shares_delta_usd") is not None:
                a["shares_delta_usd"] += h["shares_delta_usd"]
                a["delta_seen"] = True
            a["drivers"].append({"etf": etf, "contrib_5d_usd": round(contrib5, 0),
                                 "weight": round(w, 5), "broad": is_broad})

    rows = []
    for tk, a in agg.items():
        a["drivers"].sort(key=lambda d: abs(d["contrib_5d_usd"]), reverse=True)
        a["drivers"] = a["drivers"][:5]
        mc = mcap.get(tk)
        a["net_flow_5d_usd"] = round(a.pop("flow_5d"), 0)
        a["net_flow_daily_usd"] = round(a.pop("daily"), 0)
        a["broad_flow_5d_usd"] = round(a.pop("broad_5d"), 0)
        a["thematic_flow_5d_usd"] = round(a.pop("thematic_5d"), 0)
        a["shares_delta_usd"] = round(a["shares_delta_usd"], 0) if a.pop("delta_seen") else None
        a["n_etfs"] = len(a["drivers"])
        a["flow_bps_mcap"] = round(a["net_flow_5d_usd"] / mc * 1e4, 2) if mc else None
        a["delta_bps_mcap"] = round(a["shares_delta_usd"] / mc * 1e4, 2) if (mc and a["shares_delta_usd"] is not None) else None
        a["market_cap_usd"] = mc
        if abs(a["net_flow_5d_usd"]) > 0:
            share = a["thematic_flow_5d_usd"] / a["net_flow_5d_usd"]
            a["flow_type"] = ("THEMATIC_ROTATION" if share > 0.6 else
                              "BROAD_BETA" if share < 0.2 else "MIXED")
        else:
            a["flow_type"] = "NEUTRAL"
        # cross-confirmation: flow attribution and actual share-buying agree on sign
        a["confirmed"] = (a["shares_delta_usd"] is not None
                          and a["net_flow_5d_usd"] != 0
                          and (a["shares_delta_usd"] > 0) == (a["net_flow_5d_usd"] > 0))
        rows.append(a)

    rows.sort(key=lambda r: r["net_flow_5d_usd"], reverse=True)
    inflow = [r for r in rows if r["net_flow_5d_usd"] > 0][:40]
    outflow = sorted([r for r in rows if r["net_flow_5d_usd"] < 0],
                     key=lambda r: r["net_flow_5d_usd"])[:40]
    thematic = sorted([r for r in rows if r["flow_type"] == "THEMATIC_ROTATION"
                       and r["flow_bps_mcap"] is not None],
                      key=lambda r: abs(r["flow_bps_mcap"]), reverse=True)[:25]
    # actual mechanical buying leaders (share-count delta, normalised by mcap)
    buying = sorted([r for r in rows if r.get("delta_bps_mcap") is not None],
                    key=lambda r: r["delta_bps_mcap"], reverse=True)
    selling = [r for r in buying if r["delta_bps_mcap"] < 0]
    accumulation = buying[:20]
    distribution = list(reversed(selling))[:20] if selling else []
    # harvestable picks: thematic-rotation bullish AND confirmed by actual share buying
    top_picks = sorted([r for r in rows if r["flow_type"] == "THEMATIC_ROTATION"
                        and r["net_flow_5d_usd"] > 0 and r["flow_bps_mcap"] is not None
                        and r.get("confirmed")],
                       key=lambda r: r["flow_bps_mcap"], reverse=True)[:15]
    if len(top_picks) < 8:  # fall back to unconfirmed thematic if few confirmed
        extra = [r for r in rows if r["flow_type"] == "THEMATIC_ROTATION"
                 and r["net_flow_5d_usd"] > 0 and r["flow_bps_mcap"] is not None
                 and r not in top_picks]
        top_picks += sorted(extra, key=lambda r: r["flow_bps_mcap"], reverse=True)[:8 - len(top_picks)]
    top_picks = [{"ticker": r["ticker"], "score": r["flow_bps_mcap"],
                  "net_flow_5d_usd": r["net_flow_5d_usd"], "flow_type": r["flow_type"],
                  "confirmed": r.get("confirmed")} for r in top_picks]

    # dedup index events (name added/dropped by >=1 ETF), thematic only
    ev_seen = {}
    for e in index_events:
        ev_seen.setdefault((e["ticker"], e["event"]), []).append(e["etf"])
    index_events_agg = [{"ticker": t, "event": ev, "etfs": etfs}
                        for (t, ev), etfs in ev_seen.items()][:30]

    out = {
        "engine": "justhodl-flow-lookthrough",
        "version": "2.0.0",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "thesis": "ETF creations/redemptions force mechanical buying/selling of "
                  "underlying holdings. Two views: flow attributed by weight, and "
                  "the ACTUAL change in shares the ETFs hold (creation/redemption + "
                  "rebalance). Names where both agree = high-conviction demand.",
        "flows_asof": flows.get("generated_at"),
        "n_etfs_used": len(constit),
        "n_etfs_with_delta": sum(1 for c in constit.values() if c.get("has_delta")),
        "n_names": len(rows),
        "top_picks": top_picks,
        "inflow_leaders": inflow,
        "outflow_leaders": outflow,
        "thematic_rotation_leaders": thematic,
        "actual_accumulation": accumulation,
        "actual_distribution": distribution,
        "index_events": index_events_agg,
        "methodology": {
            "flow_attribution": "name_flow = sum over ETFs of (ETF_net_flow_usd * weight)",
            "share_delta": "shares_delta_usd = sum over ETFs of (shares_now - shares_prev) * implied_price",
            "weight": "ETF Global provided weight field",
            "equity_filter": "asset_class==Equity AND security_type in common/adr/reit/...",
            "flow_type": "THEMATIC_ROTATION when >60% of net pressure is from non-broad ETFs",
            "confirmed": "flow attribution and actual share-buying agree on direction",
        },
        "caveats": [
            "Flow data ~1d lagged; holdings refresh ~2x/week (rebalances lumpy).",
            "Share-delta compares latest vs ~30d-prior snapshot; missing where no prior snapshot.",
            "Broad-index flows dominate mega-caps; use flow_type / bps_mcap for rotation.",
        ],
        "elapsed_s": round(time.time() - t0, 1),
    }
    S3.put_object(Bucket=BUCKET, Key=OUT_KEY,
                  Body=json.dumps(out, default=str).encode(),
                  ContentType="application/json", CacheControl="public, max-age=600")
    return {"statusCode": 200, "body": json.dumps({
        "n_names": len(rows), "n_etfs_used": len(constit),
        "n_with_delta": out["n_etfs_with_delta"],
        "top_accum": accumulation[0]["ticker"] if accumulation else None,
        "n_index_events": len(index_events_agg),
        "n_confirmed_picks": sum(1 for p in top_picks if p.get("confirmed")),
        "elapsed_s": out["elapsed_s"]})}
