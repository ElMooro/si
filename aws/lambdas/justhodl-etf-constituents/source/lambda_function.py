"""justhodl-etf-constituents — Constituent Pull-Through

The institutional alpha edge from the Polygon ETF Global subscription.

For each ETF with |z-score| >= INSTITUTIONAL_FLOW_THRESHOLD (default 1.5σ),
we pull its top 50 constituents by weight. We then compute "implied flow
pressure" per stock = (ETF flow $) × (constituent weight). When the SAME
STOCK appears in MULTIPLE high-z ETFs, we sum the pressure — that's the
true cross-ETF institutional positioning signal.

Example: AAPL is in XLK (-$458M flow, 14% weight), QQQ (-$1500M, 12%), and
VTI (-$80M, 6%). Total implied pressure = -$458×.14 + -$1500×.12 + -$80×.06
= -$246M of institutional selling pressure on AAPL via ETF channels.

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
POLYGON_KEY = os.environ.get("POLYGON_KEY", "")
POLYGON_BASE = "https://api.polygon.io"
CONSTITUENTS_ENDPOINT = f"{POLYGON_BASE}/etf-global/v1/constituents"
FETCH_TIMEOUT = 15
MAX_WORKERS = 6

# Only pull constituents for ETFs with this z-score magnitude or higher.
# Lower threshold = more API calls = more compute. 1.5σ is roughly the
# 7th/93rd percentile — already an institutional signal.
INSTITUTIONAL_FLOW_THRESHOLD = 1.5

# How many top-weight constituents to pull per ETF.
TOP_N_CONSTITUENTS = 50

s3 = boto3.client("s3", region_name="us-east-1")


def fetch_constituents(etf_ticker: str) -> dict:
    """Fetch top-weight constituents for one ETF.

    Polygon's /etf-global/v1/constituents endpoint defaults to ASC sort
    with limit=1. We explicitly pass order=desc + sort=weight + limit=100
    to get the top-weight constituents.
    """
    if not POLYGON_KEY:
        return {"etf": etf_ticker, "error": "POLYGON_KEY not set"}
    url = (
        f"{CONSTITUENTS_ENDPOINT}"
        f"?composite_ticker={etf_ticker}"
        f"&order=desc"
        f"&sort=weight"
        f"&limit=100"
        f"&apiKey={POLYGON_KEY}"
    )
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "JustHodl-Constituents/1.0"})
        with urllib.request.urlopen(req, timeout=FETCH_TIMEOUT) as r:
            data = json.loads(r.read())
            results = data.get("results") or []
            if not results:
                return {"etf": etf_ticker, "error": "no_results",
                        "raw_status": data.get("status")}
            # Already sorted desc by API, but be defensive — keep top N
            results = sorted(
                results, key=lambda x: x.get("weight") or 0, reverse=True,
            )[:TOP_N_CONSTITUENTS]
            processed_date = results[0].get("processed_date")
            return {
                "etf": etf_ticker,
                "processed_date": processed_date,
                "n_constituents": len(results),
                "top_constituents": [
                    {
                        "stock": r.get("constituent_ticker"),
                        "name": r.get("constituent_name"),
                        "weight_pct": (r.get("weight") or 0) * 100,  # to %
                        "market_value": r.get("market_value"),
                        "shares_held": r.get("shares_held"),
                        "asset_class": r.get("asset_class"),
                    }
                    for r in results if r.get("constituent_ticker")
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


def select_high_z_etfs(daily_metrics: list) -> list:
    """Pick ETFs with |flow_zscore_90d| >= threshold. Skip leveraged ETFs."""
    LEVERAGED_BLOCKLIST = {"TQQQ", "SQQQ", "SOXL", "SOXS", "UVXY", "SVIX", "UPRO", "TMF"}
    out = []
    for m in daily_metrics:
        if m.get("error"):
            continue
        z = m.get("flow_zscore_90d")
        if z is None or abs(z) < INSTITUTIONAL_FLOW_THRESHOLD:
            continue
        if m.get("ticker") in LEVERAGED_BLOCKLIST:
            continue
        out.append(m)
    return out


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
    high_z = select_high_z_etfs(metrics)
    print(f"[constituents] {len(high_z)} ETFs above |z|>={INSTITUTIONAL_FLOW_THRESHOLD}σ "
          f"(out of {len(metrics)} total)")

    if not high_z:
        # No high-z ETFs today — write empty + bail
        out = {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "as_of_etf_flows_date": daily.get("generated_at"),
            "n_high_z_etfs": 0,
            "high_z_etfs": [],
            "top_constituents_by_pressure": [],
            "threshold_z": INSTITUTIONAL_FLOW_THRESHOLD,
            "message": "No ETFs cleared institutional flow threshold today.",
        }
        s3.put_object(
            Bucket=S3_BUCKET, Key="etf-flows/constituent-pressure.json",
            Body=json.dumps(out, default=str).encode(),
            ContentType="application/json", CacheControl="public, max-age=600",
        )
        return {
            "statusCode": 200,
            "body": json.dumps({"ok": True, "n_high_z_etfs": 0}),
        }

    # 2. Parallel fetch constituents
    print(f"[constituents] fetching top {TOP_N_CONSTITUENTS} constituents per ETF...")
    constituents_map = {}
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        future_to_etf = {
            ex.submit(fetch_constituents, m["ticker"]): m["ticker"]
            for m in high_z
        }
        for fut in as_completed(future_to_etf):
            t = future_to_etf[fut]
            try:
                constituents_map[t] = fut.result()
            except Exception as e:
                constituents_map[t] = {"etf": t, "error": str(e)[:200]}

    n_ok_constituents = sum(1 for c in constituents_map.values() if not c.get("error"))
    print(f"[constituents] got constituents for {n_ok_constituents}/{len(high_z)} ETFs")

    # 3. Compute aggregated stock-level pressure
    print(f"[constituents] computing implied pressure aggregation...")
    pressure = compute_implied_pressure(high_z, constituents_map)
    print(f"[constituents] {len(pressure)} unique stocks under pressure across high-z ETFs")

    # 4. Write per-ETF archive (top constituents)
    for etf_t, c in constituents_map.items():
        if c.get("error"):
            continue
        try:
            s3.put_object(
                Bucket=S3_BUCKET,
                Key=f"etf-flows/constituents/{etf_t}.json",
                Body=json.dumps(c, default=str).encode(),
                ContentType="application/json",
                CacheControl="public, max-age=3600",  # constituents change slowly
            )
        except Exception as e:
            print(f"[write] {etf_t}: {e}")

    # 5. Write main aggregated output
    elapsed = round(time.time() - t0, 1)
    output = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "as_of_etf_flows_date": daily.get("generated_at"),
        "threshold_z": INSTITUTIONAL_FLOW_THRESHOLD,
        "elapsed_s": elapsed,
        "n_high_z_etfs": len(high_z),
        "n_etfs_with_constituents": n_ok_constituents,
        "high_z_etfs": [
            {
                "ticker": e["ticker"],
                "zscore_90d": e.get("flow_zscore_90d"),
                "flow_5d_usd": e.get("flow_5d_usd"),
                "flow_21d_usd": e.get("flow_21d_usd"),
                "signal_label": e.get("signal_label"),
                "subcategory": e.get("subcategory"),
                "persistence_days": e.get("persistence_days"),
                "n_constituents_fetched": len(
                    (constituents_map.get(e["ticker"], {}) or {}).get("top_constituents") or []
                ),
            }
            for e in high_z
        ],
        "top_constituents_by_pressure": pressure[:50],  # top 50 stocks
        "all_constituents": pressure,  # full list for analytics
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

    print(f"[constituents] DONE — {len(pressure)} stocks, top pressure: "
          f"{pressure[0]['stock'] if pressure else '—'} ({pressure[0]['total_pressure_5d_usd']/1e6:.0f}M)" if pressure else "")

    return {
        "statusCode": 200,
        "headers": {"Content-Type": "application/json",
                     "Access-Control-Allow-Origin": "*"},
        "body": json.dumps({
            "ok": True,
            "elapsed_s": elapsed,
            "n_high_z_etfs": len(high_z),
            "n_stocks_pressured": len(pressure),
            "top_5_pressure": [
                {"stock": p["stock"],
                 "pressure_5d_usd": p["total_pressure_5d_usd"],
                 "direction": p["dominant_direction"],
                 "n_etfs": p["n_etfs_pressuring"]}
                for p in pressure[:5]
            ],
        }),
    }
