"""justhodl-etf-constituents — Constituent Pull-Through (FMP-powered)

The institutional alpha edge from your FMP subscription.

For each ETF with |z-score| >= INSTITUTIONAL_FLOW_THRESHOLD (default 1.5σ),
we pull its top 50 constituents by weight from FMP's /stable/etf/holdings
endpoint. We then compute "implied flow pressure" per stock = (ETF flow $)
× (constituent weight). When the SAME STOCK appears in MULTIPLE high-z ETFs,
we sum the pressure — that's the true cross-ETF institutional positioning
signal.

Example: AAPL is in XLK (-$458M flow, 11% weight), QQQ (-$1500M, 7%), and
SPY (-$80M, 7%). Total implied pressure = -$458×.11 + -$1500×.07 + -$80×.07
= -$161M of institutional selling pressure on AAPL via ETF channels.

WHY FMP NOT POLYGON:
The user's Polygon ETF Global subscription covers Fund Flows but NOT
Constituents (separate $99/mo product on Polygon). FMP $99/mo includes
ETF Holdings as part of the existing plan — same data, zero incremental cost.
Verified ops 1192 (Polygon 403) + 1193 (FMP 200/505 rows).

OUTPUTS:
  etf-flows/constituent-pressure.json  — aggregated by stock
  etf-flows/constituents/{ETF}.json    — per-ETF top constituents (archive)
"""
import json
import os
import time
import urllib.request
import urllib.error
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Optional

import boto3

S3_BUCKET = "justhodl-dashboard-live"
FMP_KEY = os.environ.get("FMP_KEY", "")
FMP_BASE = "https://financialmodelingprep.com"
HOLDINGS_ENDPOINT = f"{FMP_BASE}/stable/etf/holdings"
FETCH_TIMEOUT = 15
MAX_WORKERS = 6

# Only pull constituents for ETFs with this z-score magnitude or higher.
INSTITUTIONAL_FLOW_THRESHOLD = 1.5

# How many top-weight constituents to pull per ETF.
TOP_N_CONSTITUENTS = 50

s3 = boto3.client("s3", region_name="us-east-1")


def fetch_constituents(etf_ticker: str) -> dict:
    """Fetch top-weight constituents for one ETF from FMP.

    FMP response is a JSON array. Each row has:
      symbol (ETF ticker), asset (stock ticker), name (stock name),
      isin, securityCusip, sharesNumber, weightPercentage, marketValue,
      updatedAt.

    FMP returns ALL holdings (no pagination needed). We sort by weight
    desc and take top N.
    """
    if not FMP_KEY:
        return {"etf": etf_ticker, "error": "FMP_KEY not set"}
    url = f"{HOLDINGS_ENDPOINT}?symbol={etf_ticker}&apikey={FMP_KEY}"
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "JustHodl-Constituents/1.0"})
        with urllib.request.urlopen(req, timeout=FETCH_TIMEOUT) as r:
            data = json.loads(r.read())
            if isinstance(data, dict) and ("error" in data or "Error Message" in data):
                return {"etf": etf_ticker, "error": "api_error",
                        "body": json.dumps(data)[:300]}
            if not isinstance(data, list):
                return {"etf": etf_ticker, "error": "unexpected_format",
                        "body": str(data)[:200]}
            if not data:
                return {"etf": etf_ticker, "error": "no_results"}
            # Sort by weight desc and clamp to top N
            sorted_holdings = sorted(
                [d for d in data if d.get("weightPercentage") is not None],
                key=lambda x: float(x.get("weightPercentage") or 0),
                reverse=True,
            )[:TOP_N_CONSTITUENTS]
            updated_at = sorted_holdings[0].get("updatedAt") if sorted_holdings else None
            return {
                "etf": etf_ticker,
                "processed_date": (updated_at or "")[:10],  # date portion
                "n_constituents": len(sorted_holdings),
                "n_total_holdings": len(data),
                "top_constituents": [
                    {
                        "stock": d.get("asset"),
                        "name": d.get("name"),
                        "weight_pct": float(d.get("weightPercentage") or 0),
                        "market_value": float(d.get("marketValue") or 0),
                        "shares_held": float(d.get("sharesNumber") or 0),
                        "isin": d.get("isin"),
                    }
                    for d in sorted_holdings if d.get("asset")
                ],
            }
    except urllib.error.HTTPError as e:
        body = ""
        try:
            body = e.read().decode("utf-8", errors="ignore")[:300]
        except Exception:
            pass
        return {"etf": etf_ticker, "error": f"http_{e.code}", "body": body}
    except Exception as e:
        return {"etf": etf_ticker, "error": str(e)[:200]}


def select_etfs_for_constituents(daily_metrics: list, mode: str = "all") -> list:
    """Pick which ETFs to fetch constituents for.

    mode='all': fetch all ETFs with flow data (gives complete coverage)
    mode='high_z': only |z|>=1.5σ (legacy mode, narrower)

    Even in 'all' mode we still skip leveraged ETFs (TQQQ, SOXL, UVXY,
    etc.) because their flows reflect retail speculation, not institutional
    positioning. Their holdings are derivative-based anyway, not actual
    stocks.
    """
    LEVERAGED_BLOCKLIST = {"TQQQ", "SQQQ", "SOXL", "SOXS", "UVXY", "SVIX",
                           "UPRO", "TMF", "BITX", "BITI", "LABU", "LABD"}
    out = []
    for m in daily_metrics:
        if m.get("error"):
            continue
        if m.get("ticker") in LEVERAGED_BLOCKLIST:
            continue
        if mode == "high_z":
            z = m.get("flow_zscore_90d")
            if z is None or abs(z) < INSTITUTIONAL_FLOW_THRESHOLD:
                continue
        out.append(m)
    return out


def select_high_z_etfs(daily_metrics: list) -> list:
    """Legacy alias — narrow to high-z only for the 'pressure' aggregation."""
    return select_etfs_for_constituents(daily_metrics, mode="high_z")


def compute_per_stock_etf_exposure(all_etfs: list, constituents_map: dict) -> dict:
    """Build complete per-stock ETF exposure map across ALL tracked ETFs.

    Output: {stock_ticker: {
      n_etfs_holding, cumulative_weight_pct,
      total_aggregate_flow_5d_usd, total_aggregate_flow_21d_usd,
      holding_etfs: [{etf, weight_pct, etf_flow_5d, etf_zscore, etf_label}],
    }}

    This is the institutional view: for ANY stock in research, instantly
    show which of the 84 ETFs hold it and what each ETF's flow signal is.
    Unlike top_constituents_by_pressure (which only covers high-z ETFs),
    this is comprehensive across the entire flow universe.
    """
    exposure = {}
    for etf in all_etfs:
        etf_ticker = etf["ticker"]
        etf_flow_5d = etf.get("flow_5d_usd") or 0
        etf_flow_21d = etf.get("flow_21d_usd") or 0
        etf_z = etf.get("flow_zscore_90d")
        etf_label = etf.get("signal_label")
        constituents = constituents_map.get(etf_ticker)
        if not constituents or constituents.get("error"):
            continue
        for c in constituents.get("top_constituents", []):
            stock = c.get("stock")
            if not stock:
                continue
            weight_decimal = (c.get("weight_pct") or 0) / 100.0
            rec = exposure.setdefault(stock, {
                "stock": stock,
                "name": c.get("name"),
                "n_etfs_holding": 0,
                "cumulative_weight_pct": 0,
                "total_aggregate_flow_5d_usd": 0,
                "total_aggregate_flow_21d_usd": 0,
                "holding_etfs": [],
            })
            rec["n_etfs_holding"] += 1
            rec["cumulative_weight_pct"] += c.get("weight_pct") or 0
            rec["total_aggregate_flow_5d_usd"] += etf_flow_5d * weight_decimal
            rec["total_aggregate_flow_21d_usd"] += etf_flow_21d * weight_decimal
            rec["holding_etfs"].append({
                "etf": etf_ticker,
                "weight_pct": c.get("weight_pct"),
                "etf_zscore": etf_z,
                "etf_label": etf_label,
                "etf_flow_5d_usd": etf_flow_5d,
                "etf_flow_21d_usd": etf_flow_21d,
                "implied_pressure_5d_usd": etf_flow_5d * weight_decimal,
                "implied_pressure_21d_usd": etf_flow_21d * weight_decimal,
            })

    # Finalize: round + sort each stock's holding_etfs by absolute pressure
    for stock, rec in exposure.items():
        rec["cumulative_weight_pct"] = round(rec["cumulative_weight_pct"], 2)
        rec["total_aggregate_flow_5d_usd"] = round(rec["total_aggregate_flow_5d_usd"], 2)
        rec["total_aggregate_flow_21d_usd"] = round(rec["total_aggregate_flow_21d_usd"], 2)
        rec["holding_etfs"].sort(
            key=lambda x: abs(x["implied_pressure_5d_usd"]), reverse=True,
        )
    return exposure


def compute_implied_pressure(high_z_etfs: list, constituents_map: dict) -> list:
    """Aggregate implied flow pressure across high-z ETFs for each constituent stock.

    Returns sorted list of:
      {stock, name, total_pressure_5d_usd, total_pressure_21d_usd,
       contributing_etfs[{etf, weight_pct, etf_flow_5d, etf_zscore, implied_pressure_5d}],
       n_etfs_pressuring, dominant_direction}
    """
    stock_pressure = {}

    for etf in high_z_etfs:
        etf_ticker = etf["ticker"]
        etf_flow_5d = etf.get("flow_5d_usd") or 0
        etf_flow_21d = etf.get("flow_21d_usd") or 0
        etf_zscore = etf.get("flow_zscore_90d")
        constituents = constituents_map.get(etf_ticker)
        if not constituents or constituents.get("error"):
            continue

        for c in constituents.get("top_constituents", []):
            stock = c.get("stock")
            if not stock:
                continue
            weight_decimal = (c.get("weight_pct") or 0) / 100.0
            implied_5d = etf_flow_5d * weight_decimal
            implied_21d = etf_flow_21d * weight_decimal

            rec = stock_pressure.setdefault(stock, {
                "stock": stock,
                "name": c.get("name"),
                "total_pressure_5d_usd": 0,
                "total_pressure_21d_usd": 0,
                "contributing_etfs": [],
            })
            rec["total_pressure_5d_usd"] += implied_5d
            rec["total_pressure_21d_usd"] += implied_21d
            rec["contributing_etfs"].append({
                "etf": etf_ticker,
                "etf_zscore": etf_zscore,
                "weight_pct": c.get("weight_pct"),
                "etf_flow_5d_usd": etf_flow_5d,
                "etf_flow_21d_usd": etf_flow_21d,
                "implied_pressure_5d_usd": implied_5d,
                "implied_pressure_21d_usd": implied_21d,
                "etf_signal_label": etf.get("signal_label"),
                "etf_subcategory": etf.get("subcategory"),
            })

    # Finalize each record with n_etfs + dominant direction
    out = []
    for stock, rec in stock_pressure.items():
        # Sort contributing ETFs by absolute pressure desc
        rec["contributing_etfs"].sort(
            key=lambda x: abs(x["implied_pressure_5d_usd"]), reverse=True,
        )
        rec["n_etfs_pressuring"] = len(rec["contributing_etfs"])
        rec["total_pressure_5d_usd"] = round(rec["total_pressure_5d_usd"], 2)
        rec["total_pressure_21d_usd"] = round(rec["total_pressure_21d_usd"], 2)
        # Cumulative weight for signal strength
        rec["cumulative_etf_weight_pct"] = round(sum(
            e["weight_pct"] for e in rec["contributing_etfs"]
        ), 2)
        # Direction: if 5d net pressure is negative -> SELLING_PRESSURE
        if rec["total_pressure_5d_usd"] > 1e6:
            rec["dominant_direction"] = "BUYING_PRESSURE"
        elif rec["total_pressure_5d_usd"] < -1e6:
            rec["dominant_direction"] = "SELLING_PRESSURE"
        else:
            rec["dominant_direction"] = "MIXED"
        out.append(rec)

    # Sort by absolute 5d pressure desc
    out.sort(key=lambda x: abs(x["total_pressure_5d_usd"]), reverse=True)
    return out


def lambda_handler(event, context):
    t0 = time.time()
    print(f"[constituents] starting at {datetime.now(timezone.utc).isoformat()}")

    # 1. Read latest flow data
    print("[constituents] reading etf-flows/daily.json...")
    try:
        obj = s3.get_object(Bucket=S3_BUCKET, Key="etf-flows/daily.json")
        daily = json.loads(obj["Body"].read())
    except Exception as e:
        return {"statusCode": 500,
                "body": json.dumps({"error": f"Could not read flow data: {str(e)[:200]}"})}

    metrics = daily.get("metrics", [])
    all_etfs = select_etfs_for_constituents(metrics, mode="all")
    high_z = select_etfs_for_constituents(metrics, mode="high_z")
    print(f"[constituents] ALL mode: {len(all_etfs)} ETFs · "
          f"high-z subset: {len(high_z)} ETFs (|z|>={INSTITUTIONAL_FLOW_THRESHOLD}σ)")

    # 2. Parallel fetch constituents for ALL ETFs (comprehensive coverage)
    print(f"[constituents] fetching top {TOP_N_CONSTITUENTS} per ETF, parallel x{MAX_WORKERS}...")
    constituents_map = {}
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        future_to_etf = {
            ex.submit(fetch_constituents, m["ticker"]): m["ticker"]
            for m in all_etfs
        }
        for fut in as_completed(future_to_etf):
            t = future_to_etf[fut]
            try:
                constituents_map[t] = fut.result()
            except Exception as e:
                constituents_map[t] = {"etf": t, "error": str(e)[:200]}

    n_ok = sum(1 for c in constituents_map.values() if not c.get("error"))
    print(f"[constituents] got constituents for {n_ok}/{len(all_etfs)} ETFs")

    # 3. Compute high-z pressure (focused: high signal only)
    pressure = compute_implied_pressure(high_z, constituents_map)
    print(f"[constituents] high-z pressure: {len(pressure)} unique stocks")

    # 4. Compute complete per-stock ETF exposure map (all-ETF coverage)
    per_stock_exposure = compute_per_stock_etf_exposure(all_etfs, constituents_map)
    print(f"[constituents] per-stock exposure: {len(per_stock_exposure)} stocks with ETF holdings")

    # 5. Sort per-stock exposure by abs aggregate flow (top movers across the whole universe)
    top_aggregate = sorted(
        per_stock_exposure.values(),
        key=lambda x: abs(x.get("total_aggregate_flow_5d_usd", 0)),
        reverse=True,
    )

    # 6. Write per-ETF archive (top constituents for each fetched ETF)
    for etf_t, c in constituents_map.items():
        if c.get("error"):
            continue
        try:
            s3.put_object(
                Bucket=S3_BUCKET,
                Key=f"etf-flows/constituents/{etf_t}.json",
                Body=json.dumps(c, default=str).encode(),
                ContentType="application/json",
                CacheControl="public, max-age=3600",
            )
        except Exception as e:
            print(f"[write] {etf_t}: {e}")

    # 7. Write main outputs
    elapsed = round(time.time() - t0, 1)
    output = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "as_of_etf_flows_date": daily.get("generated_at"),
        "threshold_z": INSTITUTIONAL_FLOW_THRESHOLD,
        "elapsed_s": elapsed,
        "mode": "all_etfs",
        "n_etfs_total": len(all_etfs),
        "n_etfs_high_z": len(high_z),
        "n_etfs_fetched": n_ok,
        "n_stocks_with_exposure": len(per_stock_exposure),
        "high_z_etfs": [
            {
                "ticker": e["ticker"],
                "zscore_90d": e.get("flow_zscore_90d"),
                "flow_5d_usd": e.get("flow_5d_usd"),
                "flow_21d_usd": e.get("flow_21d_usd"),
                "signal_label": e.get("signal_label"),
                "n_constituents_fetched": len(
                    (constituents_map.get(e["ticker"], {}) or {}).get("top_constituents") or []
                ),
            }
            for e in high_z
        ],
        "top_constituents_by_pressure": pressure[:50],
        "top_aggregate_exposure": [
            {
                "stock": s.get("stock"),
                "name": s.get("name"),
                "n_etfs_holding": s.get("n_etfs_holding"),
                "cumulative_weight_pct": s.get("cumulative_weight_pct"),
                "total_aggregate_flow_5d_usd": s.get("total_aggregate_flow_5d_usd"),
                "total_aggregate_flow_21d_usd": s.get("total_aggregate_flow_21d_usd"),
                "top_holding_etfs": s.get("holding_etfs", [])[:5],
            }
            for s in top_aggregate[:100]
        ],
        "per_stock_exposure": per_stock_exposure,  # full map (large)
    }
    s3.put_object(
        Bucket=S3_BUCKET, Key="etf-flows/constituent-pressure.json",
        Body=json.dumps(output, default=str).encode(),
        ContentType="application/json", CacheControl="public, max-age=600",
    )
    # Date-stamped archive
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    s3.put_object(
        Bucket=S3_BUCKET, Key=f"etf-flows/constituent-history/{today}.json",
        Body=json.dumps(output, default=str).encode(),
        ContentType="application/json", CacheControl="public, max-age=86400",
    )
    # Slim "per-stock lookup" file for fast per-ticker access
    slim_lookup = {
        stock: {
            "n_etfs_holding": r.get("n_etfs_holding"),
            "cumulative_weight_pct": r.get("cumulative_weight_pct"),
            "total_aggregate_flow_5d_usd": r.get("total_aggregate_flow_5d_usd"),
            "total_aggregate_flow_21d_usd": r.get("total_aggregate_flow_21d_usd"),
            "top_etfs": r.get("holding_etfs", [])[:5],
        }
        for stock, r in per_stock_exposure.items()
    }
    s3.put_object(
        Bucket=S3_BUCKET, Key="etf-flows/stock-exposure-lookup.json",
        Body=json.dumps(slim_lookup, default=str).encode(),
        ContentType="application/json", CacheControl="public, max-age=600",
    )

    print(f"[constituents] DONE — {n_ok}/{len(all_etfs)} ETFs, "
          f"{len(per_stock_exposure)} stocks, "
          f"top mover: {top_aggregate[0]['stock'] if top_aggregate else '—'} "
          f"(${(top_aggregate[0]['total_aggregate_flow_5d_usd'] if top_aggregate else 0)/1e6:+.0f}M)")

    return {
        "statusCode": 200,
        "headers": {"Content-Type": "application/json",
                     "Access-Control-Allow-Origin": "*"},
        "body": json.dumps({
            "ok": True,
            "elapsed_s": elapsed,
            "n_etfs_total": len(all_etfs),
            "n_etfs_high_z": len(high_z),
            "n_stocks_pressured": len(pressure),
            "n_stocks_with_exposure": len(per_stock_exposure),
            "top_5_high_z_pressure": [
                {"stock": p["stock"],
                 "pressure_5d_usd": p["total_pressure_5d_usd"],
                 "direction": p["dominant_direction"],
                 "n_etfs": p["n_etfs_pressuring"]}
                for p in pressure[:5]
            ],
            "top_5_aggregate_exposure": [
                {"stock": s["stock"],
                 "aggregate_flow_5d_usd": s["total_aggregate_flow_5d_usd"],
                 "n_etfs_holding": s["n_etfs_holding"]}
                for s in top_aggregate[:5]
            ],
        }),
    }
